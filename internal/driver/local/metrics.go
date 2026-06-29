package local

import (
	"context"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"

	"github.com/honest-hosting/nomad-csi-driver/internal/config"
	"github.com/honest-hosting/nomad-csi-driver/internal/zfs"
)

// poolCollector emits pool health/capacity gauges, computed on scrape by
// querying ZFS (cheap local commands) — so values are always fresh and there's
// no background poller. Registered on the shared registry in backend.New.
type poolCollector struct {
	z             *zfs.ZFS
	cfg           *config.LocalConfig
	parentDataset string
	log           *zap.Logger

	// Two distinct accounting axes, deliberately not collapsed:
	//   physical (what's WRITTEN to disk): size = allocated + free  [from zpool]
	//   provisioned (thick reservations):  available + reserve      [from zfs]
	// A thick zvol charges its FULL size against `available` the moment it's
	// created, even while `allocated` (bytes actually written) stays tiny — so
	// these axes legitimately diverge. See also the per-volume (filesystem-layer)
	// nomad_csi_volume_* gauges, which measure usage INSIDE a mounted zvol.
	online, sizeBytes, allocatedBytes, freeBytes, availBytes, reserveBytes, volumes *prometheus.Desc
}

func newPoolCollector(z *zfs.ZFS, cfg *config.LocalConfig, parentDataset string, log *zap.Logger) *poolCollector {
	return &poolCollector{
		z: z, cfg: cfg, parentDataset: parentDataset, log: log,
		online:         prometheus.NewDesc("nomad_csi_local_pool_online", "1 if the pool is imported and ONLINE on this node, else 0.", []string{"pool"}, nil),
		sizeBytes:      prometheus.NewDesc("nomad_csi_local_pool_size_bytes", "Total pool size in bytes (zpool 'size').", []string{"pool"}, nil),
		allocatedBytes: prometheus.NewDesc("nomad_csi_local_pool_allocated_bytes", "Physically allocated (written) bytes (zpool 'allocated' = size - free); the pool-layer analogue of filesystem 'used'. Tracks bytes ON DISK, not provisioning.", []string{"pool"}, nil),
		freeBytes:      prometheus.NewDesc("nomad_csi_local_pool_free_bytes", "Physically free bytes (zpool 'free'): reflects bytes actually WRITTEN, not provisioning headroom. NOT the placement signal — use available_bytes.", []string{"pool"}, nil),
		availBytes:     prometheus.NewDesc("nomad_csi_local_pool_available_bytes", "Provisioning headroom in bytes: space allocatable to NEW thick zvols, after the pool's reserve (zfs 'available' minus reserve_bytes). This is what placement and the create-time guard enforce.", []string{"pool"}, nil),
		reserveBytes:   prometheus.NewDesc("nomad_csi_local_pool_reserve_bytes", "Configured reserve floor in bytes, kept free and subtracted from provisioning headroom.", []string{"pool"}, nil),
		volumes:        prometheus.NewDesc("nomad_csi_local_pool_volumes", "Number of CSI zvols under the pool's parent dataset.", []string{"pool"}, nil),
	}
}

func (c *poolCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- c.online
	ch <- c.sizeBytes
	ch <- c.allocatedBytes
	ch <- c.freeBytes
	ch <- c.availBytes
	ch <- c.reserveBytes
	ch <- c.volumes
}

func (c *poolCollector) Collect(ch chan<- prometheus.Metric) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	gauge := func(d *prometheus.Desc, v float64, pool string) {
		ch <- prometheus.MustNewConstMetric(d, prometheus.GaugeValue, v, pool)
	}
	for _, p := range c.cfg.Pools {
		pool := p.Name
		present, online, err := c.z.PoolStatus(ctx, pool)
		up := 0.0
		if err == nil && present && online {
			up = 1.0
		}
		gauge(c.online, up, pool)
		if err != nil || !present {
			continue // can't read capacity/volumes of an absent pool
		}
		// Physical axis (zpool): size = allocated + free, all bytes ON DISK.
		total, terr := c.z.PoolSize(ctx, pool)
		if terr == nil {
			gauge(c.sizeBytes, float64(total), pool)
		}
		if free, ferr := c.z.PoolFree(ctx, pool); ferr == nil {
			gauge(c.freeBytes, float64(free), pool)
			if terr == nil {
				gauge(c.allocatedBytes, float64(max64(total-free, 0)), pool)
			}
		}
		// Provisioned axis (zfs available): headroom for new thick zvols, reported
		// both raw-after-reserve (availBytes) and with the reserve broken out.
		if pa, perr := c.z.PoolAvailable(ctx, pool); perr == nil && terr == nil {
			rb := reserveBytes(total, c.cfg.ReserveFor(pool))
			gauge(c.reserveBytes, float64(rb), pool)
			gauge(c.availBytes, float64(max64(pa-rb, 0)), pool)
		}
		if vols, verr := c.z.ListZvols(ctx, parentDatasetForPool(c.cfg, pool, c.parentDataset)); verr == nil {
			gauge(c.volumes, float64(len(vols)), pool)
		}
	}
}

// parentDatasetForPool is the pool's CSI parent dataset (<pool>/<parent>).
// parentDatasetForPool is the dataset zvols live under in a pool: the pool's
// explicit parent_dataset override if set, else the deployment default.
func parentDatasetForPool(cfg *config.LocalConfig, pool, defaultParent string) string {
	parent := defaultParent
	if pc, ok := cfg.PoolByName(pool); ok && pc.ParentDataset != "" {
		parent = pc.ParentDataset
	}
	return pool + "/" + parent
}

// localMetrics holds the --driver=local domain collectors, registered on the
// shared registry in backend.New and threaded into the controller. The ZFS
// wrapper stays metrics-free; counting happens at the logical-op boundary here.
// Forwarding/resolution collectors moved to the shared metrics.ClusterMetrics
// (cluster_* subsystem), reached via the controller's cluster field, so both
// backends share that path now.
type localMetrics struct {
	zfsOpTotal     *prometheus.CounterVec   // {op, outcome}
	zfsOpDuration  *prometheus.HistogramVec // {op}
	placementTotal *prometheus.CounterVec   // {strategy=content|host|auto, outcome=ok|error}
	capacityReject prometheus.Counter
}

func newLocalMetrics(reg prometheus.Registerer) *localMetrics {
	m := &localMetrics{
		zfsOpTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "nomad_csi", Subsystem: "local", Name: "zfs_op_total",
			Help: "Local ZFS logical operations by op and outcome (ok|error).",
		}, []string{"op", "outcome"}),
		zfsOpDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: "nomad_csi", Subsystem: "local", Name: "zfs_op_duration_seconds",
			Help: "Local ZFS logical operation duration by op.", Buckets: prometheus.DefBuckets,
		}, []string{"op"}),
		placementTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "nomad_csi", Subsystem: "local", Name: "placement_total",
			Help: "Volume placements by strategy (content|host|auto) and outcome (ok|error). 'strategy' was renamed from 'mode' to free the deployment-identity 'mode' label.",
		}, []string{"strategy", "outcome"}),
		capacityReject: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: "nomad_csi", Subsystem: "local", Name: "capacity_reject_total",
			Help: "Creates refused because the pool would drop below its reserve.",
		}),
	}
	reg.MustRegister(m.zfsOpTotal, m.zfsOpDuration, m.placementTotal, m.capacityReject)
	return m
}

// recordForward / recordResolve delegate to the shared cluster metrics (nil-safe).
func (c *controller) recordForward(method, outcome string) {
	c.cluster.Forward(method, outcome)
}

func (c *controller) recordResolve(err error) {
	c.cluster.Resolve(err)
}

func (c *controller) recordPlacement(mode string, err error) {
	if c.metrics == nil {
		return
	}
	outcome := "ok"
	if err != nil {
		outcome = "error"
	}
	c.metrics.placementTotal.WithLabelValues(mode, outcome).Inc()
}

func (c *controller) recordCapacityReject() {
	if c.metrics != nil {
		c.metrics.capacityReject.Inc()
	}
}

// observeZFS records one logical local op. Use deferred with a named error so it
// captures the final outcome on every return path:
//
//	func (c *controller) localDelete(...) (err error) {
//	    defer c.observeZFS("destroy", time.Now(), &err)
//	    ...
//	}
//
// Nil-safe: a controller without metrics (tests, node-only) is a no-op.
func (c *controller) observeZFS(op string, start time.Time, err *error) {
	if c.metrics == nil {
		return
	}
	outcome := "ok"
	if err != nil && *err != nil {
		outcome = "error"
	}
	c.metrics.zfsOpTotal.WithLabelValues(op, outcome).Inc()
	c.metrics.zfsOpDuration.WithLabelValues(op).Observe(time.Since(start).Seconds())
}

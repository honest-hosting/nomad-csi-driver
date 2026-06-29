package qnap

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"sync"
	"time"

	"go.uber.org/zap"

	"github.com/honest-hosting/nomad-csi-driver/internal/cluster"
	"github.com/honest-hosting/nomad-csi-driver/internal/metrics"
	"github.com/honest-hosting/nomad-csi-driver/internal/stats"
)

// perNodeFanoutTimeout bounds each node's stats dump call so one slow/down node
// can't stall the aggregation (the forward client has no whole-request timeout).
const perNodeFanoutTimeout = 10 * time.Second

// qnapSource implements stats.Source for the qnap controller. qnap volumes carry
// no owning-node in their ID and the controller isn't mounted, so it maintains a
// controller-side aggregate populated by periodically fanning out to every node
// daemon's stats forwarding server. Queries and /metrics serve from the
// aggregate (staleness bounded by AggregateInterval; a down node's volumes age
// out and trip the `stale` gauge).
type qnapSource struct {
	res         cluster.Resolver
	fwd         *cluster.Client
	mapper      stats.Mapper            // Nomad id ↔ external id
	cluster     *metrics.ClusterMetrics // shared forward/resolve/peers (nil-safe)
	namespace   string                  // default namespace for metrics relabel
	interval    time.Duration
	nodeTimeout time.Duration
	log         *zap.Logger

	base   context.Context
	cancel context.CancelFunc

	mu    sync.RWMutex
	cache map[string]stats.CSIVolumeStats // keyed by external id
}

func newQNAPSource(res cluster.Resolver, fwd *cluster.Client, mapper stats.Mapper, namespace string, interval time.Duration, cm *metrics.ClusterMetrics, log *zap.Logger) *qnapSource {
	if interval <= 0 {
		interval = stats.DefaultAggregateInterval
	}
	if namespace == "" {
		namespace = stats.DefaultNamespace
	}
	if log == nil {
		log = zap.NewNop()
	}
	base, cancel := context.WithCancel(context.Background())
	q := &qnapSource{
		res: res, fwd: fwd, mapper: mapper, cluster: cm, namespace: namespace,
		interval: interval, nodeTimeout: perNodeFanoutTimeout,
		log: log, base: base, cancel: cancel, cache: map[string]stats.CSIVolumeStats{},
	}
	go q.loop()
	return q
}

func (q *qnapSource) loop() {
	q.collectOnce(q.base) // prime promptly at startup
	t := time.NewTicker(q.interval)
	defer t.Stop()
	for {
		select {
		case <-q.base.Done():
			return
		case <-t.C:
			q.collectOnce(q.base)
		}
	}
}

// collectOnce fans out to all nodes in parallel and rebuilds the aggregate from
// whatever responds. Failed/slow nodes are skipped (their volumes go stale); the
// collector never blocks on a single node.
func (q *qnapSource) collectOnce(ctx context.Context) {
	nodes, err := q.res.List(ctx)
	q.cluster.Resolve(err)
	if err != nil {
		q.log.Warn("qnap stats: node discovery failed", zap.Error(err))
		return
	}
	q.cluster.SetPeers(len(nodes))
	agg := make(map[string]stats.CSIVolumeStats)
	var mu sync.Mutex
	var wg sync.WaitGroup
	for _, n := range nodes {
		wg.Add(1)
		go func(n cluster.NodeInfo) {
			defer wg.Done()
			cctx, cancel := context.WithTimeout(ctx, q.nodeTimeout)
			defer cancel()
			body, err := q.fwd.Call(cctx, n.Addr, stats.MethodVolStatsDump, nil)
			if err != nil {
				q.cluster.Forward(stats.MethodVolStatsDump, forwardOutcome(err))
				q.log.Debug("qnap stats: node fan-out failed", zap.String("node", n.Node), zap.Error(err))
				return
			}
			var items []stats.CSIVolumeStats
			if err := json.Unmarshal(body, &items); err != nil {
				q.cluster.Forward(stats.MethodVolStatsDump, "error")
				q.log.Debug("qnap stats: decode failed", zap.String("node", n.Node), zap.Error(err))
				return
			}
			q.cluster.Forward(stats.MethodVolStatsDump, "ok")
			mu.Lock()
			for _, it := range items {
				agg[it.VolumeID] = it // a volume lives on one node; last writer wins
			}
			mu.Unlock()
		}(n)
	}
	wg.Wait()
	q.mu.Lock()
	q.cache = agg
	q.mu.Unlock()
}

// Stats serves one volume from the aggregate, resolving the Nomad id to the
// external id the aggregate is keyed by.
func (q *qnapSource) Stats(ctx context.Context, nomadID, namespace string) (stats.PublicVolumeStats, bool, error) {
	extID, found, err := q.mapper.ExternalID(ctx, namespace, nomadID)
	if err != nil {
		return stats.PublicVolumeStats{}, false, err
	}
	if !found {
		return stats.PublicVolumeStats{}, false, nil
	}
	q.mu.RLock()
	cs, ok := q.cache[extID]
	q.mu.RUnlock()
	if !ok {
		// Known to Nomad, but absent from the fan-out aggregate → not staged on
		// any node (modulo the aggregate's refresh lag for a just-mounted volume).
		return stats.PublicVolumeStats{}, false, stats.NotMounted(nomadID)
	}
	return cs.ToPublic(nomadID, namespace), true, nil
}

// All serves the whole aggregate, relabeled to Nomad ids (for the list endpoint).
func (q *qnapSource) All(ctx context.Context, namespace string) ([]stats.PublicVolumeStats, error) {
	rev, err := q.mapper.Reverse(ctx, namespace)
	if err != nil {
		return nil, err
	}
	return stats.Relabel(q.dump(), rev, namespace), nil
}

// metricsSnapshot is the collector provider: the aggregate relabeled to Nomad
// ids for the default namespace (cached reverse map; non-blocking on scrape).
func (q *qnapSource) metricsSnapshot() []stats.PublicVolumeStats {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	out, err := q.All(ctx, q.namespace)
	if err != nil {
		q.log.Debug("qnap stats: metrics snapshot relabel failed", zap.Error(err))
		return nil
	}
	return out
}

// dump returns the raw external-id-keyed aggregate.
func (q *qnapSource) dump() []stats.CSIVolumeStats {
	q.mu.RLock()
	defer q.mu.RUnlock()
	out := make([]stats.CSIVolumeStats, 0, len(q.cache))
	for _, s := range q.cache {
		out = append(out, s)
	}
	return out
}

// Close stops the collector loop.
func (q *qnapSource) Close() {
	if q != nil {
		q.cancel()
	}
}

// forwardOutcome classifies a fan-out forward error for cluster_forward_total: a
// transport/network failure (node down, dial timeout, per-node deadline) is
// "unreachable"; anything else (a node-reported op error) is "error".
func forwardOutcome(err error) string {
	var ne net.Error
	if errors.As(err, &ne) {
		return "unreachable"
	}
	return "error"
}

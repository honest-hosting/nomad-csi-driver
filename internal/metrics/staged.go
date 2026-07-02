package metrics

import (
	"context"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"
)

// stagedScrapeTimeout bounds the host command (findmnt / iscsiadm) run on the
// scrape path so a hung device probe cannot stall a Prometheus scrape.
const stagedScrapeTimeout = 5 * time.Second

// StagedCounter counts the volumes currently staged on this node from live host
// state — the mount table for local, iSCSI sessions for qnap — computed at call
// time. Implemented per-backend (see the local/qnap node StagedCount methods).
// Deriving the count from host truth (rather than an in-process Inc/Dec gauge)
// is what makes it correct across plugin restarts and impossible to drive
// negative.
type StagedCounter interface {
	StagedCount(ctx context.Context) (int, error)
}

// RegisterStagedGauge registers nomad_csi_node_staged_volumes as a GaugeFunc
// backed by src, evaluated on every scrape. No-op if reg or src is nil.
//
// On a transient read error the gauge reports the last successfully observed
// value (0 before the first success), so a momentary findmnt/iscsiadm hiccup
// never renders a misleading spike or a negative.
func RegisterStagedGauge(reg prometheus.Registerer, src StagedCounter, log *zap.Logger) error {
	if reg == nil || src == nil {
		return nil
	}
	if log == nil {
		log = zap.NewNop()
	}
	var lastGood int64 // atomic; 0 until the first successful read
	return reg.Register(prometheus.NewGaugeFunc(prometheus.GaugeOpts{
		Namespace: "nomad_csi", Subsystem: "node", Name: "staged_volumes",
		Help: "Volumes currently staged on this node, counted from live host state " +
			"(mount table / iSCSI sessions) at scrape time.",
	}, func() float64 {
		ctx, cancel := context.WithTimeout(context.Background(), stagedScrapeTimeout)
		defer cancel()
		n, err := src.StagedCount(ctx)
		if err != nil {
			log.Warn("staged_volumes: counting host state failed; reporting last value", zap.Error(err))
			return float64(atomic.LoadInt64(&lastGood))
		}
		atomic.StoreInt64(&lastGood, int64(n))
		return float64(n)
	}))
}

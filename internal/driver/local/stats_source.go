package local

import (
	"context"
	"errors"
	"time"

	"go.uber.org/zap"

	"github.com/honest-hosting/nomad-csi-driver/internal/driver"
	"github.com/honest-hosting/nomad-csi-driver/internal/stats"
)

// The local controller implements stats.Source, keyed by the Nomad volume id.
// It resolves id→externalID via the Nomad mapper, then routes exactly like
// DeleteVolume: the owning node is embedded in the external id, so a co-located
// lookup reads the registry and any other node is reached over the forward
// transport.

// Stats returns one volume's usage by Nomad id + namespace.
func (c *controller) Stats(ctx context.Context, nomadID, namespace string) (stats.PublicVolumeStats, bool, error) {
	extID, found, err := c.mapper.ExternalID(ctx, namespace, nomadID)
	if err != nil {
		return stats.PublicVolumeStats{}, false, err
	}
	if !found {
		return stats.PublicVolumeStats{}, false, nil // Nomad doesn't know this id in this namespace
	}
	eid, err := parseExternalID(extID)
	if err != nil {
		return stats.PublicVolumeStats{}, false, err
	}

	var cs stats.CSIVolumeStats
	if eid.Node == c.res.LocalNode() {
		got, ok := c.statsReg.Get(extID)
		if !ok {
			// Known to Nomad, but this owner node has never staged it.
			return stats.PublicVolumeStats{}, false, stats.NotMounted(nomadID)
		}
		cs = got
	} else {
		if ferr := c.forward(ctx, eid.Node, mVolStats, idArgs{ID: extID}, &cs); ferr != nil {
			var de *driver.Error
			if errors.As(ferr, &de) && de.Code == driver.CodeNotFound {
				return stats.PublicVolumeStats{}, false, stats.NotMounted(nomadID)
			}
			return stats.PublicVolumeStats{}, false, ferr
		}
	}
	return cs.ToPublic(nomadID, namespace), true, nil
}

// All returns this monolith's own node's readings, relabeled to Nomad ids.
// Records with no Nomad mapping in the namespace are dropped. Prometheus
// aggregates across node scrapes; cross-node queries go through Stats.
func (c *controller) All(ctx context.Context, namespace string) ([]stats.PublicVolumeStats, error) {
	rev, err := c.mapper.Reverse(ctx, namespace)
	if err != nil {
		return nil, err
	}
	return stats.Relabel(c.statsReg.Dump(), rev, namespace), nil
}

// metricsSnapshot is the collector provider: the own-node readings relabeled to
// Nomad ids for the default namespace. Best-effort and non-blocking on scrape
// (the mapper's reverse map is cached).
func (c *controller) metricsSnapshot() []stats.PublicVolumeStats {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	out, err := c.All(ctx, c.statsNS)
	if err != nil {
		c.log.Debug("stats: metrics snapshot relabel failed", zap.Error(err))
		return nil
	}
	return out
}

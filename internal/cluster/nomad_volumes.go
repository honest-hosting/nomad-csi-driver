package cluster

import (
	"context"
	"net/url"
	"sync"
	"time"

	"go.uber.org/zap"
)

// NomadVolumes resolves between a Nomad CSI volume id (what operators/the control
// plane manage, e.g. "local-data") and the driver's external id (what the stats
// cache is keyed by, e.g. "local/v1/node/...") using Nomad's /v1/volumes API over
// the task API socket. It keeps a per-namespace, TTL-cached bidirectional map and
// refreshes on miss, so the stats query/metrics paths never make a live Nomad
// call when the map is fresh.
//
// It satisfies the stats package's Mapper interface structurally (no import of
// stats), so the backends can wire it in without a dependency cycle.
type NomadVolumes struct {
	h   *nomadHTTP
	ttl time.Duration
	log *zap.Logger

	mu sync.Mutex
	ns map[string]*nsVolCache // namespace -> cached maps
}

type nsVolCache struct {
	fetchedAt time.Time
	fwd       map[string]string // nomadID -> externalID
	rev       map[string]string // externalID -> nomadID
}

// volStub is the subset of a /v1/volumes (type=csi) list entry we read. The list
// stub carries ExternalID + Namespace directly, so one call builds both maps.
type volStub struct {
	ID         string
	Namespace  string
	ExternalID string
}

// NewNomadVolumes builds a volume resolver from the same Nomad task-API options
// the peer resolver uses (only SocketPath/TokenPath/Token/CacheTTL/Logger matter).
func NewNomadVolumes(opts NomadOptions) (*NomadVolumes, error) {
	log := opts.Logger
	if log == nil {
		log = zap.NewNop()
	}
	h, err := newNomadHTTP(opts.SocketPath, opts.TokenPath, opts.Token, log)
	if err != nil {
		return nil, err
	}
	ttl := opts.CacheTTL
	if ttl <= 0 {
		ttl = defaultCacheTTL
	}
	return &NomadVolumes{h: h, ttl: ttl, log: log, ns: map[string]*nsVolCache{}}, nil
}

// ExternalID resolves a Nomad volume id within a namespace to the driver external
// id. found is false when the namespace has no such volume. On a cache miss it
// forces one fresh fetch (a just-created volume) before giving up.
func (v *NomadVolumes) ExternalID(ctx context.Context, namespace, nomadID string) (string, bool, error) {
	c, err := v.get(ctx, namespace, false)
	if err != nil {
		return "", false, err
	}
	if ext, ok := c.fwd[nomadID]; ok {
		return ext, true, nil
	}
	c, err = v.get(ctx, namespace, true) // miss: force one refresh
	if err != nil {
		return "", false, err
	}
	ext, ok := c.fwd[nomadID]
	return ext, ok, nil
}

// Reverse returns the externalID -> nomadID map for a namespace (for relabeling
// the cache to Nomad ids on the list/metrics paths). The returned map is shared
// read-only; callers must not mutate it.
func (v *NomadVolumes) Reverse(ctx context.Context, namespace string) (map[string]string, error) {
	c, err := v.get(ctx, namespace, false)
	if err != nil {
		return nil, err
	}
	return c.rev, nil
}

// get returns the namespace's cached maps, refreshing when stale or forced. A
// refresh error falls back to a stale cache when one exists.
func (v *NomadVolumes) get(ctx context.Context, namespace string, force bool) (*nsVolCache, error) {
	v.mu.Lock()
	c := v.ns[namespace]
	v.mu.Unlock()
	if c != nil && !force && time.Since(c.fetchedAt) < v.ttl {
		return c, nil
	}
	fresh, err := v.refresh(ctx, namespace)
	if err != nil {
		if c != nil {
			v.log.Warn("nomad: volume map refresh failed; serving stale",
				zap.String("namespace", namespace), zap.Int("volumes", len(c.fwd)), zap.Error(err))
			return c, nil
		}
		return nil, err
	}
	return fresh, nil
}

func (v *NomadVolumes) refresh(ctx context.Context, namespace string) (*nsVolCache, error) {
	path := "/v1/volumes?type=csi"
	if namespace != "" {
		path += "&namespace=" + url.QueryEscape(namespace)
	}
	var stubs []volStub
	if err := v.h.getJSON(ctx, path, &stubs); err != nil {
		return nil, err
	}
	c := &nsVolCache{fetchedAt: time.Now(), fwd: make(map[string]string, len(stubs)), rev: make(map[string]string, len(stubs))}
	for _, s := range stubs {
		if s.ID == "" || s.ExternalID == "" {
			continue
		}
		c.fwd[s.ID] = s.ExternalID
		c.rev[s.ExternalID] = s.ID
	}
	v.mu.Lock()
	v.ns[namespace] = c
	v.mu.Unlock()
	v.log.Debug("nomad: volume map refreshed", zap.String("namespace", namespace), zap.Int("volumes", len(c.fwd)))
	return c, nil
}

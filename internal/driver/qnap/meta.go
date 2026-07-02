package qnap

import (
	"os"
	"sync"
)

// stageMeta is the per-volume attachment identity a stage records: the
// iSCSI/multipath details NodeUnstageVolume needs to detach (the unstage RPC
// carries only volume_id + staging path). It is held in an in-memory cache for
// the process lifetime (see memMetaStore) and reconstructed from host/SAN state
// after a restart — it is NEVER written to durable storage.
type stageMeta struct {
	// Portals are the iSCSI portals we logged into (one path each). Portal is the
	// pre-multipath single-portal field, kept for read-compat with metadata
	// written by an older node plugin.
	Portals   []string `json:"portals,omitempty"`
	Portal    string   `json:"portal,omitempty"`
	IQN       string   `json:"iqn"`
	LUNNumber int      `json:"lun"`
	WWID      string   `json:"wwid,omitempty"`
}

// portalList returns the portals to log out of / rescan, tolerating older
// single-Portal metadata.
func (m stageMeta) portalList() []string {
	if len(m.Portals) > 0 {
		return m.Portals
	}
	if m.Portal != "" {
		return []string{m.Portal}
	}
	return nil
}

// metaStore caches stageMeta keyed by CSI volume ID. Load returns os.ErrNotExist
// on a miss (the tier-1 cache is cold — e.g. after a plugin restart), which the
// unstage path treats as a signal to reconstruct from host/SAN state.
type metaStore interface {
	Save(volumeID string, m stageMeta) error
	Load(volumeID string) (stageMeta, error)
	Delete(volumeID string) error
	// iqns returns the target IQNs of volumes staged this process lifetime, so the
	// reconciler can treat their sessions as live (not leaked).
	iqns() map[string]struct{}
}

// memMetaStore is the in-memory tier-1 identity cache. A successful stage records
// the volume's teardown identity here so an unstage in the SAME process lifetime
// resolves it without any host scan or SAN call. It is a PURE CACHE — process
// memory, never durable storage — so it honors the zero-durable-storage
// requirement; after a restart it is empty and teardown falls back to host/SAN
// reconstruction (tiers 2–3). Safe for concurrent use.
type memMetaStore struct {
	mu sync.RWMutex
	m  map[string]stageMeta
}

func newMemMetaStore() *memMetaStore { return &memMetaStore{m: map[string]stageMeta{}} }

func (s *memMetaStore) Save(volumeID string, m stageMeta) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.m[volumeID] = m
	return nil
}

func (s *memMetaStore) Load(volumeID string) (stageMeta, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	m, ok := s.m[volumeID]
	if !ok {
		return stageMeta{}, os.ErrNotExist
	}
	return m, nil
}

func (s *memMetaStore) Delete(volumeID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.m, volumeID)
	return nil
}

func (s *memMetaStore) iqns() map[string]struct{} {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make(map[string]struct{}, len(s.m))
	for _, m := range s.m {
		if m.IQN != "" {
			out[m.IQN] = struct{}{}
		}
	}
	return out
}

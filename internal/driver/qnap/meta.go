package qnap

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
)

// stageMeta is the per-volume attachment state the node records at
// NodeStageVolume. NodeUnstageVolume receives only volume_id + staging path
// (no volume context), so the iSCSI/multipath details needed to detach must be
// persisted at stage time and read back at unstage.
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

// metaStore persists stageMeta keyed by CSI volume ID.
type metaStore interface {
	Save(volumeID string, m stageMeta) error
	Load(volumeID string) (stageMeta, error)
	Delete(volumeID string) error
}

// fileMetaStore stores stageMeta as JSON files under a node-local directory.
type fileMetaStore struct{ dir string }

func newFileMetaStore(dir string) *fileMetaStore { return &fileMetaStore{dir: dir} }

func (f *fileMetaStore) path(volumeID string) string {
	return filepath.Join(f.dir, sanitizeLUNName(volumeID)+".json")
}

func (f *fileMetaStore) Save(volumeID string, m stageMeta) error {
	if err := os.MkdirAll(f.dir, 0o700); err != nil {
		return fmt.Errorf("creating state dir %s: %w", f.dir, err)
	}
	b, err := json.Marshal(m)
	if err != nil {
		return err
	}
	if err := os.WriteFile(f.path(volumeID), b, 0o600); err != nil {
		return fmt.Errorf("writing stage metadata: %w", err)
	}
	return nil
}

func (f *fileMetaStore) Load(volumeID string) (stageMeta, error) {
	var m stageMeta
	b, err := os.ReadFile(f.path(volumeID))
	if err != nil {
		return m, err
	}
	return m, json.Unmarshal(b, &m)
}

func (f *fileMetaStore) Delete(volumeID string) error {
	err := os.Remove(f.path(volumeID))
	if os.IsNotExist(err) {
		return nil
	}
	return err
}

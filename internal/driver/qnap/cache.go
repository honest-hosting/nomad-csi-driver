package qnap

import (
	"sync"
	"time"

	goqnap "github.com/honest-hosting/go-qnap"
)

// lunCache is the controller-side LUN/target index that fulfills the
// internal/cache role for the qnap backend. At 2k–20k LUNs a full ListLUNs scan
// on every CreateVolume idempotency check or ListVolumes is the exact hot-path
// behavior the requirements warn against, so the controller consults this index
// instead: it is refreshed in bulk at most once per TTL, and kept warm by
// incremental add/remove on create/delete.
//
// Staleness is safe: a missed entry at worst causes a duplicate create that
// QNAP rejects with a name conflict (surfaced as AlreadyExists), never data
// loss.
type lunCache struct {
	mu          sync.Mutex
	byName      map[string]goqnap.LUN
	lunToTarget map[int]goqnap.Target
	loaded      bool
	lastRefresh time.Time
	ttl         time.Duration
	now         func() time.Time
}

func newLUNCache(ttl time.Duration) *lunCache {
	return &lunCache{
		byName:      map[string]goqnap.LUN{},
		lunToTarget: map[int]goqnap.Target{},
		ttl:         ttl,
		now:         time.Now,
	}
}

// refreshFunc fetches the full LUN and target lists from the appliance.
type refreshFunc func() ([]goqnap.LUN, []goqnap.Target, error)

// ensureFresh rebuilds the index via fetch if it has never loaded or the TTL
// has elapsed.
func (c *lunCache) ensureFresh(fetch refreshFunc) error {
	c.mu.Lock()
	fresh := c.loaded && c.now().Sub(c.lastRefresh) < c.ttl
	c.mu.Unlock()
	if fresh {
		return nil
	}

	luns, targets, err := fetch()
	if err != nil {
		return err
	}

	byName := make(map[string]goqnap.LUN, len(luns))
	for _, l := range luns {
		byName[l.Name] = l
	}
	lunToTarget := map[int]goqnap.Target{}
	for _, t := range targets {
		for _, idx := range t.LUNs {
			lunToTarget[idx] = t
		}
	}

	c.mu.Lock()
	c.byName = byName
	c.lunToTarget = lunToTarget
	c.loaded = true
	c.lastRefresh = c.now()
	c.mu.Unlock()
	return nil
}

func (c *lunCache) lookupByName(name string) (goqnap.LUN, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	l, ok := c.byName[name]
	return l, ok
}

func (c *lunCache) targetForLUN(lunIndex int) (goqnap.Target, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	t, ok := c.lunToTarget[lunIndex]
	return t, ok
}

// all returns a snapshot of the cached LUNs.
func (c *lunCache) all() []goqnap.LUN {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]goqnap.LUN, 0, len(c.byName))
	for _, l := range c.byName {
		out = append(out, l)
	}
	return out
}

// addLUN inserts/updates a LUN and its target mapping after a create.
func (c *lunCache) addLUN(lun goqnap.LUN, target goqnap.Target) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.byName[lun.Name] = lun
	c.lunToTarget[lun.Index] = target
}

// removeLUN drops a LUN after a delete.
func (c *lunCache) removeLUN(lunIndex int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for name, l := range c.byName {
		if l.Index == lunIndex {
			delete(c.byName, name)
			break
		}
	}
	delete(c.lunToTarget, lunIndex)
}

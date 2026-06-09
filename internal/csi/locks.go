package csi

import (
	"sync"

	"github.com/honest-hosting/nomad-csi-driver/internal/driver"
)

// keyedLocker provides per-key mutual exclusion with NON-blocking acquisition.
// CSI requires a plugin to serialize concurrent RPCs that reference the same
// volume (CreateVolume by name; Delete/Expand/Stage/Publish/etc. by volume id).
// Per the project decision, a contended key fails fast with Aborted rather than
// queueing: the caller (Nomad) is expected to retry, which avoids unbounded
// in-process queueing and matches the CSI "operation already in progress for
// the specified volume" guidance.
type keyedLocker struct {
	mu   sync.Mutex
	held map[string]struct{}
}

func newKeyedLocker() *keyedLocker {
	return &keyedLocker{held: make(map[string]struct{})}
}

// tryAcquire takes the lock for key. It returns a release func and true if the
// key was free; if the key is already held it returns nil, false and the caller
// should reject the RPC with Aborted.
func (k *keyedLocker) tryAcquire(key string) (release func(), ok bool) {
	k.mu.Lock()
	defer k.mu.Unlock()
	if _, busy := k.held[key]; busy {
		return nil, false
	}
	k.held[key] = struct{}{}
	return func() {
		k.mu.Lock()
		delete(k.held, key)
		k.mu.Unlock()
	}, true
}

// acquire takes the per-key lock or returns an Aborted error naming the
// resource. On success it returns a release func the caller must defer.
func (k *keyedLocker) acquire(kind, key string) (release func(), err error) {
	release, ok := k.tryAcquire(key)
	if !ok {
		return nil, driver.Aborted("an operation is already in progress for %s %q", kind, key)
	}
	return release, nil
}

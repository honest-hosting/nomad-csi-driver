package qnap

import (
	"context"
	"errors"
	"net/http"
	"sync"

	goqnap "github.com/honest-hosting/go-qnap"
)

// Client is the narrow slice of the go-qnap API the backend depends on.
// *goqnap.Client satisfies it (asserted below), and unit tests substitute a
// fake — so the controller is testable without a live appliance.
type Client interface {
	Login(ctx context.Context, user, password string, opts ...goqnap.LoginOption) (goqnap.Session, error)
	Validate(ctx context.Context, sess goqnap.Session) (bool, error)

	CreateBlockLUN(ctx context.Context, sess goqnap.Session, req goqnap.CreateBlockLUNRequest) (int, error)
	GetLUN(ctx context.Context, sess goqnap.Session, lunIndex int) (goqnap.LUN, error)
	ListLUNs(ctx context.Context, sess goqnap.Session) ([]goqnap.LUN, error)
	DeleteLUN(ctx context.Context, sess goqnap.Session, lunIndex int) error
	UnmapLUN(ctx context.Context, sess goqnap.Session, lunIndex, targetIndex int) error
	WaitForLUNGone(ctx context.Context, sess goqnap.Session, lunIndex int, opts goqnap.WaitOptions) error
	ResizeLUN(ctx context.Context, sess goqnap.Session, lunIndex, newSizeGB int) error
	WaitForResizeComplete(ctx context.Context, sess goqnap.Session, lunIndex int, expectedBytes int64, opts goqnap.WaitOptions) (goqnap.LUN, error)

	CreateTarget(ctx context.Context, sess goqnap.Session, req goqnap.CreateTargetRequest) (int, error)
	GetTarget(ctx context.Context, sess goqnap.Session, targetIndex int) (goqnap.Target, error)
	ListTargets(ctx context.Context, sess goqnap.Session) ([]goqnap.Target, error)
	DeleteTarget(ctx context.Context, sess goqnap.Session, targetIndex int) error

	ListPools(ctx context.Context, sess goqnap.Session) ([]goqnap.Pool, error)
	GetPool(ctx context.Context, sess goqnap.Session, poolID int) (goqnap.Pool, error)

	CreateSnapshot(ctx context.Context, sess goqnap.Session, lunIndex int, req goqnap.CreateSnapshotRequest) (goqnap.Snapshot, error)
	GetSnapshot(ctx context.Context, sess goqnap.Session, snapshotID int64) (goqnap.Snapshot, error)
	ListSnapshots(ctx context.Context, sess goqnap.Session, lunIndex int) ([]goqnap.Snapshot, error)
	DeleteSnapshot(ctx context.Context, sess goqnap.Session, snapshotID int64) error
	WaitForSnapshotGone(ctx context.Context, sess goqnap.Session, lunIndex int, snapshotID int64, opts goqnap.WaitOptions) error

	CreateLUNFromSnapshot(ctx context.Context, sess goqnap.Session, req goqnap.CreateLUNFromSnapshotRequest) (int, error)
	CloneLUN(ctx context.Context, sess goqnap.Session, req goqnap.CloneLUNRequest) (int, error)
}

// Compile-time proof that the real client satisfies the narrow interface.
var _ Client = (*goqnap.Client)(nil)

// sessionManager owns a single QNAP session, logging in lazily and re-logging
// in once if a call fails because the session expired. It is concurrency-safe;
// the controller is the sole talker to the appliance.
type sessionManager struct {
	cl   Client
	user string
	pass string

	mu   sync.Mutex
	sess goqnap.Session
}

func newSessionManager(cl Client, user, pass string) *sessionManager {
	return &sessionManager{cl: cl, user: user, pass: pass}
}

// get returns a valid session, logging in if none is cached.
func (sm *sessionManager) get(ctx context.Context) (goqnap.Session, error) {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	if sm.sess.Valid() {
		return sm.sess, nil
	}
	s, err := sm.cl.Login(ctx, sm.user, sm.pass, goqnap.WithRememberMe(true))
	if err != nil {
		return goqnap.Session{}, err
	}
	sm.sess = s
	return s, nil
}

// reset drops the cached session so the next get re-logs in.
func (sm *sessionManager) reset() {
	sm.mu.Lock()
	sm.sess = goqnap.Session{}
	sm.mu.Unlock()
}

// do runs fn with a valid session, retrying once with a fresh login if the
// first attempt fails due to an expired/invalid session.
func (sm *sessionManager) do(ctx context.Context, fn func(sess goqnap.Session) error) error {
	sess, err := sm.get(ctx)
	if err != nil {
		return err
	}
	err = fn(sess)
	if err != nil && isAuthError(err) {
		sm.reset()
		sess, lerr := sm.get(ctx)
		if lerr != nil {
			return lerr
		}
		return fn(sess)
	}
	return err
}

// isAuthError reports whether err indicates the session is no longer valid and
// a re-login is warranted.
func isAuthError(err error) bool {
	if errors.Is(err, goqnap.ErrSessionInvalid) {
		return true
	}
	var ae *goqnap.APIError
	if errors.As(err, &ae) {
		return ae.HTTPStatus == http.StatusUnauthorized || ae.HTTPStatus == http.StatusForbidden
	}
	return false
}

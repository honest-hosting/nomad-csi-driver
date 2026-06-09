package cluster

import (
	"bytes"
	"context"
	"crypto/subtle"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"time"
)

// authHeader carries the shared secret authenticating a forward request.
const authHeader = "X-NCD-Secret"

// forwardedHeader marks a request that is already a forward, so the receiver can
// refuse to forward it onward (anti-loop hop guard).
const forwardedHeader = "X-NCD-Forwarded"

// forwardPathPrefix is the URL prefix for forwarded operations.
const forwardPathPrefix = "/forward/"

// ctxKey is the unexported type for context values set by this package.
type ctxKey int

const forwardedCtxKey ctxKey = iota

// IsForwarded reports whether ctx belongs to a request that already arrived via
// the forwarding transport. A handler should refuse to forward such a request
// onward, preventing A→B→A loops.
func IsForwarded(ctx context.Context) bool {
	v, _ := ctx.Value(forwardedCtxKey).(bool)
	return v
}

// Handler executes a forwarded operation locally. method is the operation name
// (e.g. "create"); body is the raw JSON request; the returned bytes are the raw
// JSON response. A returned error is conveyed to the caller with its Code.
type Handler func(ctx context.Context, method string, body []byte) ([]byte, error)

// CodedError lets a Handler convey a numeric code (mapped to a driver code by
// the caller) across the wire.
type CodedError struct {
	Code int
	Msg  string
}

func (e *CodedError) Error() string { return e.Msg }

// RemoteError is returned by Client.Call when the peer reported an error. Code
// mirrors the peer's CodedError.Code (0 if unspecified). Callers map it back to
// a typed driver error so the original gRPC code survives the forwarding
// boundary — see the local backend's remoteToDriver (forwardapi.go). Do not
// "fix" lost codes here: the mapping lives at the call site by design.
type RemoteError struct {
	Code int
	Msg  string
}

func (e *RemoteError) Error() string { return fmt.Sprintf("remote forward error: %s", e.Msg) }

type wireError struct {
	Code int    `json:"code"`
	Msg  string `json:"error"`
}

// Server is the HTTP handler peers POST forwarded operations to. It
// authenticates the shared secret, then dispatches to the Handler.
type Server struct {
	secret  string
	handler Handler
}

// NewServer builds a forwarding Server. secret must be non-empty.
func NewServer(secret string, h Handler) *Server {
	return &Server{secret: secret, handler: h}
}

func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if subtle.ConstantTimeCompare([]byte(r.Header.Get(authHeader)), []byte(s.secret)) != 1 {
		writeWireError(w, http.StatusUnauthorized, &wireError{Msg: "unauthorized"})
		return
	}
	method, ok := methodFromPath(r.URL.Path)
	if !ok {
		writeWireError(w, http.StatusBadRequest, &wireError{Msg: "bad forward path"})
		return
	}
	body, err := io.ReadAll(io.LimitReader(r.Body, 1<<20))
	if err != nil {
		writeWireError(w, http.StatusBadRequest, &wireError{Msg: "reading body"})
		return
	}

	// Mark the handler's context as forwarded so it refuses to forward onward.
	ctx := context.WithValue(r.Context(), forwardedCtxKey, true)
	resp, herr := s.handler(ctx, method, body)
	if herr != nil {
		we := &wireError{Code: 0, Msg: herr.Error()}
		var ce *CodedError
		if asCoded(herr, &ce) {
			we.Code = ce.Code
			we.Msg = ce.Msg
		}
		writeWireError(w, http.StatusUnprocessableEntity, we)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_, _ = w.Write(resp)
}

// Client posts forwarded operations to a peer's Server.
type Client struct {
	secret string
	hc     *http.Client
}

// NewClient builds a forwarding Client using the shared secret. There is no
// whole-request timeout: the caller's context bounds each call, so a long node
// operation (format/expand/clone of a large device) is not cut short. A dial
// timeout still fails fast against an unreachable peer.
func NewClient(secret string) *Client {
	return &Client{
		secret: secret,
		hc: &http.Client{
			Transport: &http.Transport{
				DialContext:           (&net.Dialer{Timeout: 10 * time.Second}).DialContext,
				TLSHandshakeTimeout:   10 * time.Second,
				ResponseHeaderTimeout: 0, // bounded by the caller's context
			},
		},
	}
}

// Call forwards method+reqBody to the peer at addr and returns the raw response
// body. A peer-reported error becomes a *RemoteError.
func (c *Client) Call(ctx context.Context, addr, method string, reqBody []byte) ([]byte, error) {
	endpoint := fmt.Sprintf("http://%s%s%s", addr, forwardPathPrefix, method)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(reqBody))
	if err != nil {
		return nil, err
	}
	req.Header.Set(authHeader, c.secret)
	req.Header.Set(forwardedHeader, "1")
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.hc.Do(req)
	if err != nil {
		return nil, fmt.Errorf("forwarding to %s: %w", addr, err)
	}
	defer func() { _ = resp.Body.Close() }()
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if resp.StatusCode != http.StatusOK {
		var we wireError
		if json.Unmarshal(body, &we) == nil && we.Msg != "" {
			return nil, &RemoteError{Code: we.Code, Msg: we.Msg}
		}
		return nil, &RemoteError{Msg: fmt.Sprintf("peer %s returned %s", addr, resp.Status)}
	}
	return body, nil
}

func methodFromPath(path string) (string, bool) {
	if len(path) <= len(forwardPathPrefix) || path[:len(forwardPathPrefix)] != forwardPathPrefix {
		return "", false
	}
	return path[len(forwardPathPrefix):], true
}

func writeWireError(w http.ResponseWriter, status int, we *wireError) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(we)
}

func asCoded(err error, target **CodedError) bool {
	for err != nil {
		if ce, ok := err.(*CodedError); ok {
			*target = ce
			return true
		}
		u, ok := err.(interface{ Unwrap() error })
		if !ok {
			return false
		}
		err = u.Unwrap()
	}
	return false
}

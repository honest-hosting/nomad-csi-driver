package stats

import (
	"context"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"net"
	"net/http"
	"strings"
	"time"

	"go.uber.org/zap"
)

// QueryPathPrefix is the base path of the public stats query API.
const QueryPathPrefix = "/v1/volume-stats"

// QueryServer is the controller's public HTTP+JSON stats endpoint, consumed by
// the klm API. It is intentionally separate from the internal forward transport
// (different trust domain → its own token/header).
//
//	GET /v1/volume-stats/{volumeID}  -> 200 CSIVolumeStats | 404 | 503 (unhydrated)
//	GET /v1/volume-stats             -> 200 [CSIVolumeStats]
//
// Auth is an opt-in bearer token: when Token is empty the endpoint is OPEN.
type QueryServer struct {
	src    Source
	token  string
	header string
	log    *zap.Logger
	srv    *http.Server
	ln     net.Listener
}

// NewQueryServer binds a listener on addr (returns nil,nil when addr is "" — the
// endpoint is disabled). header defaults to X-NCD-Query-Token. Call Serve to
// start accepting; Close to stop.
func NewQueryServer(addr string, src Source, token, header string, log *zap.Logger) (*QueryServer, error) {
	if addr == "" {
		return nil, nil // disabled
	}
	if header == "" {
		header = DefaultQueryTokenHeader
	}
	if log == nil {
		log = zap.NewNop()
	}
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, err
	}
	qs := &QueryServer{src: src, token: token, header: header, log: log, ln: ln}
	mux := http.NewServeMux()
	mux.HandleFunc(QueryPathPrefix, qs.handle)     // list
	mux.HandleFunc(QueryPathPrefix+"/", qs.handle) // by id (ids contain '/')
	qs.srv = &http.Server{Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	return qs, nil
}

// Serve starts accepting in the background.
func (qs *QueryServer) Serve() {
	if qs == nil {
		return
	}
	go func() {
		if err := qs.srv.Serve(qs.ln); err != nil && err != http.ErrServerClosed {
			qs.log.Error("stats query server error", zap.Error(err))
		}
	}()
}

// Close stops the server.
func (qs *QueryServer) Close(ctx context.Context) error {
	if qs == nil {
		return nil
	}
	return qs.srv.Shutdown(ctx)
}

// Addr returns the bound address (useful when addr was ":0" in tests).
func (qs *QueryServer) Addr() string {
	if qs == nil || qs.ln == nil {
		return ""
	}
	return qs.ln.Addr().String()
}

func (qs *QueryServer) handle(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if !qs.authOK(r) {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}
	ns := r.URL.Query().Get("namespace")
	if ns == "" {
		ns = DefaultNamespace
	}
	id := strings.TrimSpace(strings.TrimPrefix(strings.TrimPrefix(r.URL.Path, QueryPathPrefix), "/"))
	if id == "" {
		all, err := qs.src.All(r.Context(), ns)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadGateway)
			return
		}
		writeJSON(w, http.StatusOK, all)
		return
	}
	cs, found, err := qs.src.Stats(r.Context(), id, ns)
	if err != nil {
		if errors.Is(err, ErrNotMounted) { // known to Nomad, just not staged anywhere
			http.Error(w, err.Error(), http.StatusPreconditionFailed)
			return
		}
		http.Error(w, err.Error(), http.StatusBadGateway)
		return
	}
	if !found {
		http.Error(w, "volume not found", http.StatusNotFound)
		return
	}
	if cs.StatfsAt.IsZero() { // tracked but not measured yet
		writeJSON(w, http.StatusServiceUnavailable, cs)
		return
	}
	writeJSON(w, http.StatusOK, cs)
}

// authOK enforces the bearer token when one is configured. An empty token means
// the endpoint is open (explicit, supported operator choice).
func (qs *QueryServer) authOK(r *http.Request) bool {
	if qs.token == "" {
		return true
	}
	got := r.Header.Get(qs.header)
	return subtle.ConstantTimeCompare([]byte(got), []byte(qs.token)) == 1
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

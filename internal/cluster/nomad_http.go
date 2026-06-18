package cluster

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"os"
	"strings"
	"time"

	"go.uber.org/zap"
)

// nomadHTTP is the shared authenticated client for Nomad's task API over the
// api.sock unix socket. Both peer discovery (NomadResolver) and volume-id
// resolution (NomadVolumes) use it: a workload-identity bearer token (re-read per
// call for rotation) over a unix-socket transport. A missing socket or token is
// a hard error at construction — that means the plugin task lacks its `identity`
// block and could never reach the API.
type nomadHTTP struct {
	socketPath string
	tokenPath  string
	tokenLit   string
	http       *http.Client
	log        *zap.Logger
}

func newNomadHTTP(socketPath, tokenPath, tokenLit string, log *zap.Logger) (*nomadHTTP, error) {
	if log == nil {
		log = zap.NewNop()
	}
	h := &nomadHTTP{socketPath: socketPath, tokenPath: tokenPath, tokenLit: tokenLit, log: log}
	if h.socketPath == "" {
		return nil, fmt.Errorf("cluster: nomad api needs the task API socket " +
			"(NOMAD_SECRETS_DIR/api.sock); is this running under Nomad with an identity block?")
	}
	if _, err := h.token(); err != nil {
		return nil, fmt.Errorf("cluster: nomad api has no usable token "+
			"(set an `identity { env = true, file = true }` block on the plugin task): %w", err)
	}
	socket := h.socketPath
	h.http = &http.Client{
		Timeout: 10 * time.Second,
		Transport: &http.Transport{
			DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
				var d net.Dialer
				return d.DialContext(ctx, "unix", socket)
			},
		},
	}
	return h, nil
}

// token resolves the bearer token: the TokenPath file first (re-read each call so
// workload-identity rotation is picked up), then the literal override, then the
// NOMAD_TOKEN env. Errors if none yields a non-empty token.
func (h *nomadHTTP) token() (string, error) {
	if h.tokenPath != "" {
		if b, err := os.ReadFile(h.tokenPath); err == nil {
			if tok := strings.TrimSpace(string(b)); tok != "" {
				return tok, nil
			}
		}
	}
	if h.tokenLit != "" {
		return h.tokenLit, nil
	}
	if tok := strings.TrimSpace(os.Getenv("NOMAD_TOKEN")); tok != "" {
		return tok, nil
	}
	return "", fmt.Errorf("no token in file %q, literal, or $NOMAD_TOKEN", h.tokenPath)
}

// getJSON does an authenticated GET of path (e.g. "/v1/nodes?...") and decodes
// the JSON body into out.
func (h *nomadHTTP) getJSON(ctx context.Context, path string, out any) error {
	tok, err := h.token()
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://localhost"+path, nil)
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+tok)
	resp, err := h.http.Do(req)
	if err != nil {
		return fmt.Errorf("cluster: querying nomad api.sock: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("cluster: nomad %s returned %s", path, resp.Status)
	}
	return json.NewDecoder(resp.Body).Decode(out)
}

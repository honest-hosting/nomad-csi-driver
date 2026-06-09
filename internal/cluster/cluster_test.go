package cluster

import (
	"context"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestForwardRoundTrip(t *testing.T) {
	handler := func(_ context.Context, method string, body []byte) ([]byte, error) {
		assert.Equal(t, "echo", method)
		return body, nil // echo back
	}
	srv := httptest.NewServer(NewServer("secret", handler))
	defer srv.Close()

	addr := strings.TrimPrefix(srv.URL, "http://")
	out, err := NewClient("secret").Call(context.Background(), addr, "echo", []byte(`{"x":1}`))
	require.NoError(t, err)
	assert.JSONEq(t, `{"x":1}`, string(out))
}

// A request received via the forwarding server must carry the forwarded marker
// in its context, so a handler can refuse to forward it onward (hop guard).
func TestForward_MarksContextForwarded(t *testing.T) {
	var sawForwarded bool
	handler := func(ctx context.Context, _ string, _ []byte) ([]byte, error) {
		sawForwarded = IsForwarded(ctx)
		return []byte("{}"), nil
	}
	srv := httptest.NewServer(NewServer("secret", handler))
	defer srv.Close()

	addr := strings.TrimPrefix(srv.URL, "http://")
	_, err := NewClient("secret").Call(context.Background(), addr, "ping", nil)
	require.NoError(t, err)
	assert.True(t, sawForwarded, "handler context must be marked forwarded")

	// A locally-originated context is not forwarded.
	assert.False(t, IsForwarded(context.Background()))
}

func TestForwardUnauthorized(t *testing.T) {
	srv := httptest.NewServer(NewServer("right", func(context.Context, string, []byte) ([]byte, error) {
		return nil, nil
	}))
	defer srv.Close()

	addr := strings.TrimPrefix(srv.URL, "http://")
	_, err := NewClient("wrong").Call(context.Background(), addr, "m", nil)
	require.Error(t, err)
	var re *RemoteError
	require.ErrorAs(t, err, &re)
}

func TestForwardErrorCodePropagation(t *testing.T) {
	srv := httptest.NewServer(NewServer("s", func(context.Context, string, []byte) ([]byte, error) {
		return nil, &CodedError{Code: 7, Msg: "boom"}
	}))
	defer srv.Close()

	addr := strings.TrimPrefix(srv.URL, "http://")
	_, err := NewClient("s").Call(context.Background(), addr, "m", nil)
	require.Error(t, err)
	var re *RemoteError
	require.ErrorAs(t, err, &re)
	assert.Equal(t, 7, re.Code)
	assert.Equal(t, "boom", re.Msg)
}

func TestStaticResolver(t *testing.T) {
	r := &StaticResolver{Self: "A", Peers: []NodeInfo{{Node: "A", Addr: "a:1"}, {Node: "B", Addr: "b:2"}}}
	assert.Equal(t, "A", r.LocalNode())
	addr, err := r.Resolve(context.Background(), "B")
	require.NoError(t, err)
	assert.Equal(t, "b:2", addr)
	_, err = r.Resolve(context.Background(), "Z")
	assert.Error(t, err)
}

package stats

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"go.uber.org/zap"
)

type fakeSource struct {
	byID       map[string]PublicVolumeStats
	notMounted map[string]bool // known to Nomad but not staged
	all        []PublicVolumeStats
	err        error
}

func (f *fakeSource) Stats(_ context.Context, id, _ string) (PublicVolumeStats, bool, error) {
	if f.err != nil {
		return PublicVolumeStats{}, false, f.err
	}
	if f.notMounted[id] {
		return PublicVolumeStats{}, false, NotMounted(id)
	}
	cs, ok := f.byID[id]
	return cs, ok, nil
}

func (f *fakeSource) All(context.Context, string) ([]PublicVolumeStats, error) {
	return f.all, f.err
}

func startQuery(t *testing.T, src Source, token, header string) string {
	t.Helper()
	qs, err := NewQueryServer("127.0.0.1:0", src, token, header, zap.NewNop())
	if err != nil {
		t.Fatalf("NewQueryServer: %v", err)
	}
	qs.Serve()
	t.Cleanup(func() { _ = qs.Close(context.Background()) })
	return "http://" + qs.Addr()
}

func get(t *testing.T, url, header, token string) (int, []byte) {
	t.Helper()
	req, _ := http.NewRequest(http.MethodGet, url, nil)
	if header != "" {
		req.Header.Set(header, token)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("GET %s: %v", url, err)
	}
	defer func() { _ = resp.Body.Close() }()
	b, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, b
}

func TestQuery_StatusRouting(t *testing.T) {
	src := &fakeSource{byID: map[string]PublicVolumeStats{
		"hydrated":   {ID: "hydrated", Namespace: "default", Node: "nodeA", AccessType: AccessMount, UsedBytes: 40, StatfsAt: time.Now()},
		"unhydrated": {ID: "unhydrated", Namespace: "default", Node: "nodeA", AccessType: AccessMount}, // StatfsAt zero
	}}
	base := startQuery(t, src, "", "")

	if code, body := get(t, base+QueryPathPrefix+"/hydrated", "", ""); code != 200 {
		t.Fatalf("hydrated: got %d (%s); want 200", code, body)
	} else {
		var cs PublicVolumeStats
		if err := json.Unmarshal(body, &cs); err != nil || cs.UsedBytes != 40 || cs.ID != "hydrated" {
			t.Fatalf("hydrated body decode: %v cs=%+v", err, cs)
		}
	}
	if code, _ := get(t, base+QueryPathPrefix+"/unhydrated", "", ""); code != 503 {
		t.Fatalf("unhydrated: got %d; want 503", code)
	}
	if code, _ := get(t, base+QueryPathPrefix+"/missing", "", ""); code != 404 {
		t.Fatalf("missing: got %d; want 404", code)
	}
}

func TestQuery_NotMounted412(t *testing.T) {
	src := &fakeSource{notMounted: map[string]bool{"created-not-mounted": true}}
	base := startQuery(t, src, "", "")
	code, body := get(t, base+QueryPathPrefix+"/created-not-mounted", "", "")
	if code != 412 {
		t.Fatalf("known-but-unmounted: got %d; want 412", code)
	}
	if !strings.Contains(string(body), "not mounted on any node") {
		t.Fatalf("412 body should explain why; got %q", body)
	}
}

func TestQuery_ListEndpoint(t *testing.T) {
	src := &fakeSource{all: []PublicVolumeStats{
		{ID: "a", StatfsAt: time.Now()},
		{ID: "b", StatfsAt: time.Now()},
	}}
	base := startQuery(t, src, "", "")
	code, body := get(t, base+QueryPathPrefix, "", "")
	if code != 200 {
		t.Fatalf("list: got %d; want 200", code)
	}
	var got []PublicVolumeStats
	if err := json.Unmarshal(body, &got); err != nil || len(got) != 2 {
		t.Fatalf("list decode: %v len=%d", err, len(got))
	}
}

func TestQuery_AuthMatrix(t *testing.T) {
	src := &fakeSource{byID: map[string]PublicVolumeStats{
		"v": {ID: "v", StatfsAt: time.Now()},
	}}
	const hdr = "X-Custom-Token"
	base := startQuery(t, src, "s3cr3t", hdr)
	url := base + QueryPathPrefix + "/v"

	if code, _ := get(t, url, hdr, "s3cr3t"); code != 200 {
		t.Fatalf("correct token: got %d; want 200", code)
	}
	if code, _ := get(t, url, hdr, "wrong"); code != 401 {
		t.Fatalf("wrong token: got %d; want 401", code)
	}
	if code, _ := get(t, url, "", ""); code != 401 {
		t.Fatalf("missing token: got %d; want 401", code)
	}
}

func TestQuery_OpenWhenNoToken(t *testing.T) {
	src := &fakeSource{byID: map[string]PublicVolumeStats{"v": {ID: "v", StatfsAt: time.Now()}}}
	base := startQuery(t, src, "", "") // open
	if code, _ := get(t, base+QueryPathPrefix+"/v", "", ""); code != 200 {
		t.Fatalf("open endpoint: got %d; want 200", code)
	}
}

func TestQuery_DisabledWhenNoAddr(t *testing.T) {
	qs, err := NewQueryServer("", &fakeSource{}, "", "", zap.NewNop())
	if err != nil || qs != nil {
		t.Fatalf("empty addr should yield nil server, no error; got qs=%v err=%v", qs, err)
	}
	qs.Serve()                         // nil-safe
	_ = qs.Close(context.Background()) // nil-safe
}

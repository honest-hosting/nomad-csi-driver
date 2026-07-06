package local

import (
	"context"
	"errors"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	"github.com/honest-hosting/nomad-csi-driver/internal/cluster"
	"github.com/honest-hosting/nomad-csi-driver/internal/metrics"
)

// errResolver is a cluster.Resolver whose List always fails, to prove a roster
// read error leaves the cluster_peers gauge unchanged (never reset to 0).
type errResolver struct{}

func (errResolver) LocalNode() string                                { return "A" }
func (errResolver) List(context.Context) ([]cluster.NodeInfo, error) { return nil, errors.New("down") }
func (errResolver) Resolve(context.Context, string) (string, error)  { return "", errors.New("down") }

func peersGauge(t *testing.T, reg *prometheus.Registry) (float64, bool) {
	t.Helper()
	mfs, err := reg.Gather()
	require.NoError(t, err)
	for _, mf := range mfs {
		if mf.GetName() != "nomad_csi_cluster_peers" {
			continue
		}
		return mf.GetMetric()[0].GetGauge().GetValue(), true
	}
	return 0, false
}

func TestRefreshPeersOnce_SetsGaugeThenHoldsOnError(t *testing.T) {
	reg := prometheus.NewRegistry()
	cm := metrics.NewClusterMetrics(reg)
	b := &backend{}

	// A successful roster read sets the gauge to the peer count.
	res := &cluster.StaticResolver{Self: "A", Peers: []cluster.NodeInfo{
		{Node: "A", Addr: "a:1"}, {Node: "B", Addr: "b:2"}, {Node: "C", Addr: "c:3"},
	}}
	b.refreshPeersOnce(context.Background(), res, cm, zap.NewNop())
	v, ok := peersGauge(t, reg)
	require.True(t, ok, "cluster_peers should be emitted")
	require.Equal(t, float64(3), v)

	// A subsequent roster read error must leave the last-good value in place.
	b.refreshPeersOnce(context.Background(), errResolver{}, cm, zap.NewNop())
	v, _ = peersGauge(t, reg)
	require.Equal(t, float64(3), v, "an error must not reset the peers gauge")
}

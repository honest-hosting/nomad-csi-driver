package metrics

import (
	"context"
	"errors"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// fakeCounter is a scriptable StagedCounter.
type fakeCounter struct {
	n   int
	err error
}

func (f *fakeCounter) StagedCount(context.Context) (int, error) { return f.n, f.err }

// gaugeVal gathers reg and returns the single-series value of the named gauge.
func gaugeVal(t *testing.T, reg *prometheus.Registry, name string) float64 {
	t.Helper()
	mfs, err := reg.Gather()
	require.NoError(t, err)
	for _, mf := range mfs {
		if mf.GetName() != name {
			continue
		}
		require.Len(t, mf.GetMetric(), 1)
		return mf.GetMetric()[0].GetGauge().GetValue()
	}
	t.Fatalf("gauge %q not found", name)
	return 0
}

const stagedName = "nomad_csi_node_staged_volumes"

func TestRegisterStagedGauge_TracksSource(t *testing.T) {
	reg := prometheus.NewRegistry()
	src := &fakeCounter{n: 3}
	require.NoError(t, RegisterStagedGauge(reg, src, nil))

	assert.Equal(t, 3.0, gaugeVal(t, reg, stagedName))
	src.n = 7 // GaugeFunc re-reads on every scrape
	assert.Equal(t, 7.0, gaugeVal(t, reg, stagedName))
	src.n = 0
	assert.Equal(t, 0.0, gaugeVal(t, reg, stagedName))
}

func TestRegisterStagedGauge_ErrorHoldsLastGood(t *testing.T) {
	reg := prometheus.NewRegistry()
	src := &fakeCounter{n: 5}
	require.NoError(t, RegisterStagedGauge(reg, src, nil))
	assert.Equal(t, 5.0, gaugeVal(t, reg, stagedName))

	// A transient host-command failure must not reset to 0 or go negative;
	// the gauge holds the last observed value.
	src.err = errors.New("findmnt boom")
	assert.Equal(t, 5.0, gaugeVal(t, reg, stagedName))

	// Recovery updates again.
	src.err = nil
	src.n = 9
	assert.Equal(t, 9.0, gaugeVal(t, reg, stagedName))
}

func TestRegisterStagedGauge_ErrorBeforeFirstSuccessIsZero(t *testing.T) {
	reg := prometheus.NewRegistry()
	require.NoError(t, RegisterStagedGauge(reg, &fakeCounter{err: errors.New("boom")}, nil))
	assert.Equal(t, 0.0, gaugeVal(t, reg, stagedName), "last-good defaults to 0, never negative")
}

func TestRegisterStagedGauge_InheritsConstLabels(t *testing.T) {
	// Registered on the identity-wrapping registerer, the gauge carries the
	// constant driver/mode/node_id/plugin_id labels like every other series.
	reg := prometheus.NewRegistry()
	wrapped := prometheus.WrapRegistererWith(prometheus.Labels{
		"driver": "local", "mode": "monolith", "node_id": "n1", "plugin_id": "p1",
	}, reg)
	require.NoError(t, RegisterStagedGauge(wrapped, &fakeCounter{n: 1}, nil))

	mfs, err := reg.Gather()
	require.NoError(t, err)
	var found bool
	for _, mf := range mfs {
		if mf.GetName() != stagedName {
			continue
		}
		found = true
		labels := map[string]string{}
		for _, lp := range mf.GetMetric()[0].GetLabel() {
			labels[lp.GetName()] = lp.GetValue()
		}
		assert.Equal(t, "local", labels["driver"])
		assert.Equal(t, "p1", labels["plugin_id"])
		assert.Equal(t, "n1", labels["node_id"])
	}
	assert.True(t, found)
}

func TestRegisterStagedGauge_NilIsNoop(t *testing.T) {
	require.NoError(t, RegisterStagedGauge(nil, &fakeCounter{}, nil))
	require.NoError(t, RegisterStagedGauge(prometheus.NewRegistry(), nil, nil))
}

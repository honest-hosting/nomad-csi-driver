package driver

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestValidateModeForDriver(t *testing.T) {
	cases := []struct {
		driver string
		mode   Mode
		ok     bool
	}{
		{"qnap", ModeController, true},
		{"qnap", ModeNode, true},
		{"qnap", ModeMonolith, false}, // qnap is controller XOR node (deploy as service + daemonset)
		{"local", ModeMonolith, true},
		{"local", ModeController, false}, // local is monolith-only
		{"local", ModeNode, false},
		{"qnap", "bogus", false},
	}
	for _, tc := range cases {
		err := ValidateModeForDriver(tc.driver, tc.mode)
		if tc.ok {
			assert.NoErrorf(t, err, "%s/%s", tc.driver, tc.mode)
		} else {
			assert.Errorf(t, err, "%s/%s", tc.driver, tc.mode)
		}
	}
}

func TestModeHelpers(t *testing.T) {
	assert.True(t, ModeController.HasController())
	assert.False(t, ModeController.HasNode())
	assert.True(t, ModeNode.HasNode())
	assert.False(t, ModeNode.HasController())
	assert.True(t, ModeMonolith.HasController())
	assert.True(t, ModeMonolith.HasNode())
}

func TestRegistry(t *testing.T) {
	Register("test-reg", func(context.Context, Deps) (Backend, error) { return nil, nil })
	assert.Contains(t, Registered(), "test-reg")

	_, err := New(context.Background(), "does-not-exist", Deps{})
	require.Error(t, err)

	assert.Panics(t, func() {
		Register("test-reg", func(context.Context, Deps) (Backend, error) { return nil, nil })
	}, "duplicate registration must panic")
}

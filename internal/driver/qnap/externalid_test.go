package qnap

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestExternalIDRoundTrip(t *testing.T) {
	in := externalID{LUNIndex: 7, TargetIndex: 3, OwnTarget: true}
	out, err := parseExternalID(in.String())
	require.NoError(t, err)
	assert.Equal(t, in, out)

	shared := externalID{LUNIndex: 1, TargetIndex: 9, OwnTarget: false}
	out2, err := parseExternalID(shared.String())
	require.NoError(t, err)
	assert.Equal(t, shared, out2)

	// The LUN name (data-safety guard) round-trips and appears in the wire form.
	named := externalID{LUNIndex: 5, TargetIndex: 2, OwnTarget: true, LUNName: "vol-abc"}
	assert.Equal(t, "qnap/v1/5/2/t/vol-abc", named.String())
	out3, err := parseExternalID(named.String())
	require.NoError(t, err)
	assert.Equal(t, named, out3)
}

func TestExternalIDMalformed(t *testing.T) {
	for _, bad := range []string{"", "lun-1", "qnap/v2/1/2/t", "qnap/v1/x/2/t", "qnap/v1/1/2"} {
		_, err := parseExternalID(bad)
		assert.Error(t, err, bad)
	}
}

func TestSnapshotIDRoundTrip(t *testing.T) {
	in := snapshotID{LUNIndex: 4, SnapshotID: 123456}
	out, err := parseSnapshotID(in.String())
	require.NoError(t, err)
	assert.Equal(t, in, out)
}

func TestSanitizeLUNName(t *testing.T) {
	assert.Equal(t, "vol_a-1", sanitizeLUNName("vol_a-1"))
	assert.Equal(t, "a-b-c", sanitizeLUNName("a/b.c"))
	assert.Len(t, sanitizeLUNName(string(make([]byte, 200))), 64)
}

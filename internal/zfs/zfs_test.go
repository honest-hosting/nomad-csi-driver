package zfs

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	cexec "github.com/honest-hosting/nomad-csi-driver/internal/exec"
)

func TestCreateZvolCommand(t *testing.T) {
	fr := &cexec.FakeRunner{}
	require.NoError(t, New(fr).CreateZvol(context.Background(), "tank/csi/v1", 16384, "16K"))
	assert.Equal(t, []string{"zfs create -p -V 16384 -b 16K tank/csi/v1"}, fr.Commands())
}

func TestExists(t *testing.T) {
	t.Run("present", func(t *testing.T) {
		fr := &cexec.FakeRunner{Responder: func(cexec.Command) (cexec.Output, error) {
			return cexec.Output{Stdout: []byte("tank/csi/v1")}, nil
		}}
		ok, err := New(fr).Exists(context.Background(), "tank/csi/v1")
		require.NoError(t, err)
		assert.True(t, ok)
	})
	t.Run("absent (exit 1)", func(t *testing.T) {
		fr := &cexec.FakeRunner{Responder: func(cexec.Command) (cexec.Output, error) {
			return cexec.Output{}, &cexec.Error{ExitCode: 1}
		}}
		ok, err := New(fr).Exists(context.Background(), "tank/csi/none")
		require.NoError(t, err)
		assert.False(t, ok)
	})
}

func TestGetVolsize(t *testing.T) {
	fr := &cexec.FakeRunner{Responder: func(cexec.Command) (cexec.Output, error) {
		return cexec.Output{Stdout: []byte("1073741824\n")}, nil
	}}
	size, err := New(fr).GetVolsize(context.Background(), "tank/csi/v1")
	require.NoError(t, err)
	assert.Equal(t, int64(1073741824), size)
}

func TestListZvols(t *testing.T) {
	fr := &cexec.FakeRunner{Responder: func(cexec.Command) (cexec.Output, error) {
		return cexec.Output{Stdout: []byte("tank/csi/a\t16384\ntank/csi/b\t32768\n")}, nil
	}}
	vols, err := New(fr).ListZvols(context.Background(), "tank/csi")
	require.NoError(t, err)
	require.Len(t, vols, 2)
	assert.Equal(t, "tank/csi/a", vols[0].Name)
	assert.Equal(t, int64(32768), vols[1].SizeByte)
}

func TestCloneIndependentPipesSendRecv(t *testing.T) {
	fr := &cexec.FakeRunner{}
	require.NoError(t, New(fr).CloneIndependent(context.Background(), "tank/csi/src", "snap1", "tank/csi/dst"))
	// RunPipe records both legs, then the received snapshot is dropped.
	require.Len(t, fr.Calls, 3)
	assert.Equal(t, "zfs", fr.Calls[0].Name)
	assert.Equal(t, []string{"send", "tank/csi/src@snap1"}, fr.Calls[0].Args)
	assert.Equal(t, []string{"recv", "tank/csi/dst"}, fr.Calls[1].Args)
	// The snapshot reconstituted on the destination by zfs recv is dropped, so the
	// clone is a clean, fully independent dataset.
	assert.Equal(t, []string{"destroy", "tank/csi/dst@snap1"}, fr.Calls[2].Args)
}

// A failure in the `zfs send` leg must surface (not be masked by a successful
// recv) — the pipefail guarantee that shells without `set -o pipefail` lose.
func TestCloneIndependent_SendFailureSurfaces(t *testing.T) {
	fr := &cexec.FakeRunner{Responder: func(c cexec.Command) (cexec.Output, error) {
		if len(c.Args) > 0 && c.Args[0] == "send" {
			return cexec.Output{}, &cexec.Error{Name: "zfs", Args: c.Args, ExitCode: 1, Stderr: "dataset busy"}
		}
		return cexec.Output{}, nil
	}}
	err := New(fr).CloneIndependent(context.Background(), "tank/csi/src", "snap1", "tank/csi/dst")
	require.Error(t, err, "a failed send must fail the clone")
	// The received-snapshot cleanup must not have run (no destroy recorded).
	for _, c := range fr.Commands() {
		assert.NotContains(t, c, "destroy")
	}
}

func TestDevicePath(t *testing.T) {
	assert.Equal(t, "/dev/zvol/tank/csi/v1", DevicePath("tank/csi/v1"))
}

func TestPoolHealthy(t *testing.T) {
	fr := &cexec.FakeRunner{Responder: func(cexec.Command) (cexec.Output, error) {
		return cexec.Output{Stdout: []byte("ONLINE\n")}, nil
	}}
	ok, err := New(fr).PoolHealthy(context.Background(), "tank")
	require.NoError(t, err)
	assert.True(t, ok)
}

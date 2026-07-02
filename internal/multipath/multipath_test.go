package multipath

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	cexec "github.com/honest-hosting/nomad-csi-driver/internal/exec"
)

func TestWriteDropin(t *testing.T) {
	dir := t.TempDir()
	m := New(&cexec.FakeRunner{}, dir)
	require.NoError(t, m.WriteDropin(DefaultDropin()))

	b, err := os.ReadFile(filepath.Join(dir, dropinName))
	require.NoError(t, err)
	assert.Contains(t, string(b), `vendor "QNAP"`)
	assert.Contains(t, string(b), "path_grouping_policy multibus")
	assert.Contains(t, string(b), "prio alua")
	// The WWID-based device path depends on friendly names being off.
	assert.Contains(t, string(b), "user_friendly_names no")
}

func TestWWID(t *testing.T) {
	fr := &cexec.FakeRunner{Responder: func(c cexec.Command) (cexec.Output, error) {
		if c.Name == "/lib/udev/scsi_id" {
			return cexec.Output{Stdout: []byte("3600a098000abcdef\n")}, nil
		}
		return cexec.Output{}, nil
	}}
	wwid, err := New(fr, t.TempDir()).WWID(context.Background(), "/dev/sdb")
	require.NoError(t, err)
	assert.Equal(t, "3600a098000abcdef", wwid)
}

func TestMapperPathAndCommands(t *testing.T) {
	fr := &cexec.FakeRunner{}
	m := New(fr, t.TempDir())
	assert.Equal(t, "/dev/mapper/3600x", m.MapperPath("3600x"))
	require.NoError(t, m.Reload(context.Background()))
	require.NoError(t, m.Flush(context.Background(), "3600x"))
	assert.Equal(t, []string{"multipathd reconfigure", "multipath -f 3600x"}, fr.Commands())
}

func TestMapWWID(t *testing.T) {
	fr := &cexec.FakeRunner{Responder: func(c cexec.Command) (cexec.Output, error) {
		if c.Name == "multipathd" {
			// multipathd show paths format "%d %w"
			return cexec.Output{Stdout: []byte("sda 360000sysdisk\nsdg 36e843b666fa5422d85b2d4a34db90bd1\n")}, nil
		}
		return cexec.Output{}, nil
	}}
	m := New(fr, t.TempDir())

	wwid, err := m.MapWWID(context.Background(), "/dev/sdg")
	require.NoError(t, err)
	assert.Equal(t, "36e843b666fa5422d85b2d4a34db90bd1", wwid, "uses multipathd's WWID, not scsi_id's")

	// A device not (yet) in any map returns empty, no error.
	none, err := m.MapWWID(context.Background(), "/dev/sdz")
	require.NoError(t, err)
	assert.Empty(t, none)
}

func TestMembers(t *testing.T) {
	fr := &cexec.FakeRunner{Responder: func(c cexec.Command) (cexec.Output, error) {
		if c.Name == "multipathd" {
			return cexec.Output{Stdout: []byte("sda wwidA\nsdb wwidB\nsdc wwidA\n")}, nil
		}
		return cexec.Output{}, nil
	}}
	m := New(fr, t.TempDir())

	got, err := m.Members(context.Background(), "wwidA")
	require.NoError(t, err)
	assert.Equal(t, []string{"sda", "sdc"}, got, "both path devices of wwidA, not wwidB")

	none, err := m.Members(context.Background(), "nope")
	require.NoError(t, err)
	assert.Empty(t, none)
}

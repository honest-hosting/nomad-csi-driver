package exec

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestOSRunner_Success(t *testing.T) {
	out, err := NewOSRunner().Run(context.Background(), Command{Name: "printf", Args: []string{"hello"}})
	require.NoError(t, err)
	assert.Equal(t, "hello", string(out.Stdout))
}

func TestOSRunner_NonZeroExit(t *testing.T) {
	_, err := NewOSRunner().Run(context.Background(), Command{Name: "false"})
	require.Error(t, err)
	var ce *Error
	require.True(t, errors.As(err, &ce))
	assert.Equal(t, 1, ce.ExitCode)
}

func TestOSRunner_Stdin(t *testing.T) {
	out, err := NewOSRunner().Run(context.Background(), Command{
		Name:  "cat",
		Stdin: strings.NewReader("piped"),
	})
	require.NoError(t, err)
	assert.Equal(t, "piped", string(out.Stdout))
}

func TestFakeRunner_RecordsAndResponds(t *testing.T) {
	fr := &FakeRunner{Responder: func(c Command) (Output, error) {
		if c.Name == "blkid" {
			return Output{Stdout: []byte("ext4")}, nil
		}
		return Output{}, nil
	}}
	out, err := fr.Run(context.Background(), Command{Name: "blkid", Args: []string{"/dev/x"}})
	require.NoError(t, err)
	assert.Equal(t, "ext4", string(out.Stdout))

	_, _ = fr.Run(context.Background(), Command{Name: "mount", Args: []string{"-t", "ext4"}})
	assert.Equal(t, []string{"blkid /dev/x", "mount -t ext4"}, fr.Commands())
}

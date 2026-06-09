// Package exec is the shared command-runner seam. Every side-effecting host
// command the driver issues — iscsiadm, multipath, zfs, mkfs, mount, blkid,
// resize2fs, … — goes through a Runner so unit tests can fake it and assert the
// exact command + args without touching the host. The default OS-backed
// implementation is the only thing that actually shells out.
package exec

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strings"
)

// Command describes a single command invocation. Stdin is optional and used by
// pipe-style operations (e.g. `zfs send | zfs recv`, where the recv side reads
// from a pipe).
type Command struct {
	Name  string
	Args  []string
	Stdin io.Reader
	// Env, when non-nil, replaces the child environment entirely. Nil inherits
	// the parent environment.
	Env []string
}

// Output is the captured result of a successful command.
type Output struct {
	Stdout []byte
	Stderr []byte
}

// Runner executes host commands. Implementations must honor ctx cancellation.
type Runner interface {
	Run(ctx context.Context, c Command) (Output, error)
	// RunPipe streams producer's stdout into consumer's stdin (the
	// `producer | consumer` pattern) without buffering the stream in memory, and
	// returns an error if EITHER leg fails to start or exits non-zero. This gives
	// the `set -o pipefail` guarantee shell-free: a failure in the producing
	// command is never masked by a successful consumer. consumer.Stdin is ignored
	// (replaced by the pipe).
	RunPipe(ctx context.Context, producer, consumer Command) error
}

// Error is returned when a command exits non-zero or fails to start. It carries
// enough context to map to a gRPC code and to log usefully, without leaking the
// full environment.
type Error struct {
	Name     string
	Args     []string
	ExitCode int
	Stderr   string
	Err      error
}

func (e *Error) Error() string {
	cmd := e.Name
	if len(e.Args) > 0 {
		cmd += " " + strings.Join(e.Args, " ")
	}
	msg := fmt.Sprintf("command %q failed", cmd)
	if e.ExitCode != 0 {
		msg += fmt.Sprintf(" (exit %d)", e.ExitCode)
	}
	if s := strings.TrimSpace(e.Stderr); s != "" {
		msg += ": " + s
	}
	return msg
}

func (e *Error) Unwrap() error { return e.Err }

// IsExit reports whether err is (or wraps) an *Error with the given exit code.
// Useful for treating specific command exit codes as non-errors (e.g. blkid
// exit 2 = "no filesystem", zfs list exit 1 = "does not exist").
func IsExit(err error, code int) bool {
	var ce *Error
	return errors.As(err, &ce) && ce.ExitCode == code
}

// OSRunner is the production Runner backed by os/exec.
type OSRunner struct{}

// NewOSRunner returns the default OS-backed Runner.
func NewOSRunner() *OSRunner { return &OSRunner{} }

// Run executes the command, capturing stdout and stderr separately.
func (r *OSRunner) Run(ctx context.Context, c Command) (Output, error) {
	cmd := exec.CommandContext(ctx, c.Name, c.Args...)
	if c.Stdin != nil {
		cmd.Stdin = c.Stdin
	}
	if c.Env != nil {
		cmd.Env = c.Env
	}

	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	err := cmd.Run()
	out := Output{Stdout: stdout.Bytes(), Stderr: stderr.Bytes()}
	if err != nil {
		return out, cmdError(c, stderr.String(), err)
	}
	return out, nil
}

// RunPipe wires producer's stdout into consumer's stdin via an OS pipe and runs
// both concurrently, checking both exit codes so a producer failure is never
// masked by the consumer (shell-free pipefail).
func (r *OSRunner) RunPipe(ctx context.Context, producer, consumer Command) error {
	pr, pw, err := os.Pipe()
	if err != nil {
		return fmt.Errorf("creating pipe: %w", err)
	}

	pcmd := exec.CommandContext(ctx, producer.Name, producer.Args...)
	ccmd := exec.CommandContext(ctx, consumer.Name, consumer.Args...)
	if producer.Env != nil {
		pcmd.Env = producer.Env
	}
	if consumer.Env != nil {
		ccmd.Env = consumer.Env
	}
	var pStderr, cStderr bytes.Buffer
	pcmd.Stdout = pw
	pcmd.Stderr = &pStderr
	ccmd.Stdin = pr
	ccmd.Stderr = &cStderr

	if err := pcmd.Start(); err != nil {
		_ = pr.Close()
		_ = pw.Close()
		return cmdError(producer, pStderr.String(), err)
	}
	if err := ccmd.Start(); err != nil {
		_ = pr.Close()
		_ = pw.Close()
		_ = pcmd.Process.Kill()
		_ = pcmd.Wait()
		return cmdError(consumer, cStderr.String(), err)
	}

	// Wait for the producer, then close the parent's write end so the consumer
	// sees EOF and finishes. Both legs are checked; the producer's failure wins.
	pErr := pcmd.Wait()
	_ = pw.Close()
	cErr := ccmd.Wait()
	_ = pr.Close()

	if pErr != nil {
		return cmdError(producer, pStderr.String(), pErr)
	}
	if cErr != nil {
		return cmdError(consumer, cStderr.String(), cErr)
	}
	return nil
}

// cmdError builds an *Error from a failed command, extracting the exit code.
func cmdError(c Command, stderr string, err error) *Error {
	ce := &Error{Name: c.Name, Args: c.Args, Stderr: stderr, Err: err}
	var ee *exec.ExitError
	if errors.As(err, &ee) {
		ce.ExitCode = ee.ExitCode()
	}
	return ce
}

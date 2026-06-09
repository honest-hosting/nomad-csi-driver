package exec

import (
	"context"
	"io"
	"strings"
	"sync"
)

// FakeRunner is a test Runner. It records every invocation and consults a
// caller-supplied Responder for the result. With no Responder it succeeds with
// empty output, which is the common "command exists and did nothing
// interesting" case.
type FakeRunner struct {
	mu    sync.Mutex
	Calls []Command

	// Responder returns the canned result for a call. Implementations typically
	// switch on c.Name / c.Args[0]. If nil, every call succeeds emptily.
	Responder func(c Command) (Output, error)
}

// Run records the command (draining Stdin first so pipe-style callers behave)
// and returns the Responder's result.
func (f *FakeRunner) Run(ctx context.Context, c Command) (Output, error) {
	// Drain stdin so callers that write to a pipe don't block, and so the
	// recorded call can be asserted on its bytes if needed.
	if c.Stdin != nil {
		_, _ = io.Copy(io.Discard, c.Stdin)
		c.Stdin = nil
	}

	f.mu.Lock()
	f.Calls = append(f.Calls, c)
	resp := f.Responder
	f.mu.Unlock()

	if resp == nil {
		return Output{}, nil
	}
	return resp(c)
}

// RunPipe records both legs (so Commands() shows them in order) and consults the
// Responder for each. A Responder error on the producer leg is returned without
// running the consumer, modeling the pipefail guarantee.
func (f *FakeRunner) RunPipe(_ context.Context, producer, consumer Command) error {
	f.mu.Lock()
	f.Calls = append(f.Calls, producer, consumer)
	resp := f.Responder
	f.mu.Unlock()

	if resp == nil {
		return nil
	}
	if _, err := resp(producer); err != nil {
		return err
	}
	if _, err := resp(consumer); err != nil {
		return err
	}
	return nil
}

// Commands returns the recorded invocations as "name arg0 arg1 …" strings, in
// order, for convenient assertions.
func (f *FakeRunner) Commands() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]string, 0, len(f.Calls))
	for _, c := range f.Calls {
		parts := append([]string{c.Name}, c.Args...)
		out = append(out, strings.Join(parts, " "))
	}
	return out
}

// Reset clears recorded calls.
func (f *FakeRunner) Reset() {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.Calls = nil
}

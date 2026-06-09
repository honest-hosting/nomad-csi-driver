package driver

import "fmt"

// Code is a transport-neutral error classification. Backends return *Error with
// one of these; the CSI layer (internal/csi) maps them to gRPC status codes.
// Keeping the vocabulary here means backends never import the gRPC packages.
type Code int

// The neutral error codes backends classify failures with.
const (
	CodeUnknown Code = iota
	CodeInvalidArgument
	CodeNotFound
	CodeAlreadyExists
	CodeFailedPrecondition
	CodeOutOfRange
	CodeResourceExhausted
	CodeAborted
	CodeUnimplemented
	CodeInternal
	CodeUnavailable
)

// Error is a backend error carrying a neutral Code, a safe message, and an
// optional wrapped cause.
type Error struct {
	Code Code
	Msg  string
	Err  error
}

func (e *Error) Error() string {
	if e.Err != nil {
		return e.Msg + ": " + e.Err.Error()
	}
	return e.Msg
}

func (e *Error) Unwrap() error { return e.Err }

// newf builds an *Error with a formatted message.
func newf(code Code, format string, args ...any) *Error {
	return &Error{Code: code, Msg: fmt.Sprintf(format, args...)}
}

// Wrap attaches a cause to an *Error while preserving its Code, so lower layers
// can add context: return driver.Internal("zfs create").Wrap(err).
func (e *Error) Wrap(err error) *Error {
	e.Err = err
	return e
}

// Constructors — one per Code that backends actually return. Each builds an
// *Error with the corresponding Code and a formatted message.

// InvalidArgument reports a malformed or unsupported request.
func InvalidArgument(format string, args ...any) *Error {
	return newf(CodeInvalidArgument, format, args...)
}

// NotFound reports that the referenced resource does not exist.
func NotFound(format string, args ...any) *Error { return newf(CodeNotFound, format, args...) }

// AlreadyExists reports a conflicting resource that already exists.
func AlreadyExists(format string, args ...any) *Error {
	return newf(CodeAlreadyExists, format, args...)
}

// FailedPrecondition reports that the system is not in a state required for the
// operation (e.g. attaching a sibling LUN on the wrong node).
func FailedPrecondition(format string, args ...any) *Error {
	return newf(CodeFailedPrecondition, format, args...)
}

// OutOfRange reports a value outside the valid range (e.g. capacity > max).
func OutOfRange(format string, args ...any) *Error { return newf(CodeOutOfRange, format, args...) }

// ResourceExhausted reports that a limit or quota has been reached.
func ResourceExhausted(format string, args ...any) *Error {
	return newf(CodeResourceExhausted, format, args...)
}

// Aborted reports a concurrency conflict the caller may retry.
func Aborted(format string, args ...any) *Error { return newf(CodeAborted, format, args...) }

// Unimplemented reports an operation the backend does not support.
func Unimplemented(format string, args ...any) *Error {
	return newf(CodeUnimplemented, format, args...)
}

// Internal reports an unexpected internal failure.
func Internal(format string, args ...any) *Error { return newf(CodeInternal, format, args...) }

// Unavailable reports that a dependency is temporarily unreachable.
func Unavailable(format string, args ...any) *Error { return newf(CodeUnavailable, format, args...) }

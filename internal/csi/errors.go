package csi

import (
	"context"
	"errors"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/honest-hosting/nomad-csi-driver/internal/driver"
)

// toGRPC maps an error returned by the csi layer or a backend to a gRPC status.
// Already-formed status errors pass through; *driver.Error maps by its Code;
// context errors map to Canceled/DeadlineExceeded; everything else is Internal.
func toGRPC(err error) error {
	if err == nil {
		return nil
	}
	if _, ok := status.FromError(err); ok && isStatusError(err) {
		return err
	}
	var de *driver.Error
	if errors.As(err, &de) {
		return status.Error(codeFor(de.Code), de.Error())
	}
	switch {
	case errors.Is(err, context.Canceled):
		return status.Error(codes.Canceled, err.Error())
	case errors.Is(err, context.DeadlineExceeded):
		return status.Error(codes.DeadlineExceeded, err.Error())
	}
	return status.Error(codes.Internal, err.Error())
}

// isStatusError reports whether err is (or wraps) a gRPC status error, as
// opposed to a plain error that status.FromError merely coerced to Unknown.
func isStatusError(err error) bool {
	type grpcStatus interface{ GRPCStatus() *status.Status }
	var gs grpcStatus
	return errors.As(err, &gs)
}

func codeFor(c driver.Code) codes.Code {
	switch c {
	case driver.CodeInvalidArgument:
		return codes.InvalidArgument
	case driver.CodeNotFound:
		return codes.NotFound
	case driver.CodeAlreadyExists:
		return codes.AlreadyExists
	case driver.CodeFailedPrecondition:
		return codes.FailedPrecondition
	case driver.CodeOutOfRange:
		return codes.OutOfRange
	case driver.CodeResourceExhausted:
		return codes.ResourceExhausted
	case driver.CodeAborted:
		return codes.Aborted
	case driver.CodeUnimplemented:
		return codes.Unimplemented
	case driver.CodeUnavailable:
		return codes.Unavailable
	case driver.CodeInternal:
		return codes.Internal
	default:
		return codes.Unknown
	}
}

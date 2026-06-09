package csi

import (
	"context"
	"strings"
	"time"

	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/honest-hosting/nomad-csi-driver/internal/metrics"
)

// unaryInterceptor centralizes cross-cutting concerns for every CSI RPC: panic
// recovery, neutral-error → gRPC-status mapping, latency/outcome metrics, and
// structured logging. Handlers and backends therefore return plain
// *driver.Error values and never touch gRPC status codes directly.
func unaryInterceptor(log *zap.Logger, m *metrics.Metrics) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (resp any, err error) {
		start := time.Now()
		method := shortMethod(info.FullMethod)

		defer func() {
			if r := recover(); r != nil {
				log.Error("panic recovered in RPC", zap.String("method", method), zap.Any("panic", r))
				resp = nil
				err = status.Errorf(codes.Internal, "panic recovered in %s: %v", method, r)
			} else {
				err = toGRPC(err)
			}
			code := status.Code(err)
			m.ObserveRPC(method, code.String(), time.Since(start))
			if err != nil {
				log.Warn("rpc failed",
					zap.String("method", method), zap.String("code", code.String()), zap.Error(err))
			} else {
				log.Debug("rpc ok",
					zap.String("method", method), zap.Duration("dur", time.Since(start)))
			}
		}()

		resp, err = handler(ctx, req)
		return resp, err
	}
}

// shortMethod turns "/csi.v1.Controller/CreateVolume" into "CreateVolume".
func shortMethod(full string) string {
	if i := strings.LastIndex(full, "/"); i >= 0 {
		return full[i+1:]
	}
	return full
}

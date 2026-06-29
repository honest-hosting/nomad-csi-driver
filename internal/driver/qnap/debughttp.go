package qnap

import (
	"context"
	"net/url"
	"time"

	goqnap "github.com/honest-hosting/go-qnap"
	"github.com/honest-hosting/go-qnap/transport"
	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"

	"github.com/honest-hosting/nomad-csi-driver/internal/config"
)

const qnapClientTimeout = 30 * time.Second

// newQNAPClient builds the go-qnap controller client with Prometheus + zap
// observability hooks. When cfg.DebugHTTP is set it ALSO wraps the transport so
// every raw request path + response body is logged at debug level — the
// in-driver equivalent of `qnapctl --debug-http`, usable from a Nomad job by
// setting `qnap { debug_http = true }` and running with --log-level=debug.
func newQNAPClient(cfg *config.QNAPConfig, reg prometheus.Registerer, log *zap.Logger) (*goqnap.Client, error) {
	opts := []goqnap.Option{
		goqnap.WithHooks(qnapHooks(reg, log)),
	}

	if cfg.DebugHTTP {
		// WithTransport bypasses WithTimeout/WithInsecureSkipVerify/WithRetry, so
		// bake those into the base transport we wrap.
		base, err := transport.NewRestyTransport(transport.Config{
			BaseURL:   cfg.BaseURL,
			UserAgent: "nomad-csi-driver",
			Timeout:   qnapClientTimeout,
			Retry:     transport.DefaultRetryConfig(),
			TLS:       transport.TLSConfig{InsecureSkipVerify: cfg.Insecure},
		})
		if err != nil {
			return nil, err
		}
		opts = append(opts, goqnap.WithTransport(&loggingDoer{inner: base, log: log}))
		log.Info("qnap debug_http enabled: raw request paths + response bodies will be logged at debug level (verbose; troubleshooting only)")
	} else {
		opts = append(opts, goqnap.WithInsecureSkipVerify(cfg.Insecure))
	}

	return goqnap.NewClient(cfg.BaseURL, opts...)
}

// loggingDoer wraps a transport.HTTPDoer and logs each raw request path +
// response body to zap at debug level. Sensitive query params (sid, pwd,
// password) are redacted so the trace is safe to share. Mirrors qnapctl's
// debugDoer, but writes structured zap (so it lands in the Nomad alloc logs)
// instead of stderr.
type loggingDoer struct {
	inner transport.HTTPDoer
	log   *zap.Logger
}

func (d *loggingDoer) Do(ctx context.Context, req transport.Request) (*transport.Response, error) {
	pretty := req.Path
	if len(req.Query) > 0 {
		clone := url.Values{}
		for k, v := range req.Query {
			switch k {
			case "sid", "pwd", "password":
				clone.Set(k, "REDACTED")
			default:
				clone[k] = v
			}
		}
		pretty += "?" + clone.Encode()
	}

	resp, err := d.inner.Do(ctx, req)
	if err != nil {
		d.log.Debug("qnap http",
			zap.String("method", req.Method), zap.String("path", pretty), zap.Error(err))
		return resp, err
	}
	d.log.Debug("qnap http",
		zap.String("method", req.Method), zap.String("path", pretty),
		zap.Int("status", resp.StatusCode), zap.Int("bytes", len(resp.Body)),
		zap.Duration("duration", resp.Duration),
		zap.ByteString("body", resp.Body))
	return resp, err
}

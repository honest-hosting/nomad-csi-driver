package qnap

import (
	"errors"
	"fmt"
	"net/http"
	"testing"

	goqnap "github.com/honest-hosting/go-qnap"
	"github.com/stretchr/testify/assert"
)

// qnapOutcome maps the go-qnap error taxonomy onto the bounded op_total label.
func TestQNAPOutcome(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want string
	}{
		{"nil", nil, "ok"},
		{"auth", goqnap.ErrAuthFailed, "auth"},
		{"session", goqnap.ErrSessionInvalid, "auth"},
		{"busy", goqnap.ErrResourceBusy, "busy"},
		{"conflict", goqnap.ErrNameConflict, "conflict"},
		{"poolmissing", goqnap.ErrPoolMissing, "notfound"},
		{"timeout", goqnap.ErrTimeout, "timeout"},
		{"unsupported", goqnap.ErrUnsupported, "unsupported"},
		{"wrapped busy", fmt.Errorf("delete: %w", goqnap.ErrResourceBusy), "busy"},
		{"ratelimit", &goqnap.APIError{Op: "X", HTTPStatus: http.StatusTooManyRequests}, "ratelimit"},
		{"other api error", &goqnap.APIError{Op: "X", HTTPStatus: 500, Code: -99}, "other"},
		{"transport (non-api)", errors.New("dial tcp: connection refused"), "transport"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, qnapOutcome(tc.err))
		})
	}
}

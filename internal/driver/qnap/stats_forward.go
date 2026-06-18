package qnap

import (
	"context"
	"encoding/json"

	"github.com/honest-hosting/nomad-csi-driver/internal/cluster"
	"github.com/honest-hosting/nomad-csi-driver/internal/driver"
	"github.com/honest-hosting/nomad-csi-driver/internal/stats"
)

// dispatchForward is the cluster.Handler for the node's stats forwarding server.
// The controller fans these methods out to every node to aggregate per-volume
// usage (qnap volumes carry no owning-node in their ID, so there is no targeted
// route — see qnapSource).
func (n *node) dispatchForward(_ context.Context, method string, body []byte) ([]byte, error) {
	switch method {
	case stats.MethodVolStats:
		var a stats.VolStatsArgs
		if err := json.Unmarshal(body, &a); err != nil {
			return nil, &cluster.CodedError{Code: int(driver.CodeInvalidArgument), Msg: "decoding volstats args: " + err.Error()}
		}
		cs, ok := n.stats.Get(a.ID)
		if !ok {
			return nil, &cluster.CodedError{Code: int(driver.CodeNotFound), Msg: "volume " + a.ID + " not tracked on this node"}
		}
		return marshalForward(cs)
	case stats.MethodVolStatsDump:
		return marshalForward(n.stats.Dump())
	default:
		return nil, &cluster.CodedError{Code: int(driver.CodeInvalidArgument), Msg: "unknown forward method " + method}
	}
}

func marshalForward(v any) ([]byte, error) {
	b, err := json.Marshal(v)
	if err != nil {
		return nil, &cluster.CodedError{Code: int(driver.CodeInternal), Msg: "encoding forward result: " + err.Error()}
	}
	return b, nil
}

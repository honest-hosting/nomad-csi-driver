package driver

import (
	"context"
	"fmt"
	"sort"

	"go.uber.org/zap"

	"github.com/honest-hosting/nomad-csi-driver/internal/config"
	"github.com/honest-hosting/nomad-csi-driver/internal/exec"
	"github.com/honest-hosting/nomad-csi-driver/internal/metrics"
)

// Mode is the process role.
type Mode string

// The supported process roles.
const (
	ModeController Mode = "controller"
	ModeNode       Mode = "node"
	ModeMonolith   Mode = "monolith"
)

// HasController reports whether this mode runs the controller half.
func (m Mode) HasController() bool { return m == ModeController || m == ModeMonolith }

// HasNode reports whether this mode runs the node half.
func (m Mode) HasNode() bool { return m == ModeNode || m == ModeMonolith }

// Deps is everything a backend constructor needs. Backends read their own
// section of Config and ignore the rest.
type Deps struct {
	Mode Mode
	// NodeID is this node's stable id (node/monolith modes).
	NodeID string
	// ParentDataset is the ZFS dataset under each pool that holds provisioned
	// zvols for --driver=local (<pool>/<ParentDataset>/<volume-id>); a pool's
	// parent_dataset config overrides it. Defaults to "nomad-csi". qnap ignores
	// it.
	ParentDataset string
	Config        *config.Config
	Runner        exec.Runner
	Logger        *zap.Logger
	Metrics       *metrics.Metrics
}

// Constructor builds a backend from Deps.
type Constructor func(ctx context.Context, d Deps) (Backend, error)

// registry maps driver name -> constructor. Backend packages register
// themselves in init(); main imports them for their side effects. This keeps
// the driver package free of imports of its own sub-packages (no cycle).
var registry = map[string]Constructor{}

// Register adds a backend constructor. Panics on duplicate registration, which
// can only be a programming error.
func Register(name string, c Constructor) {
	if _, dup := registry[name]; dup {
		panic("driver: duplicate registration for " + name)
	}
	registry[name] = c
}

// New constructs the backend for the given driver name.
func New(ctx context.Context, name string, d Deps) (Backend, error) {
	c, ok := registry[name]
	if !ok {
		return nil, fmt.Errorf("driver: unknown driver %q (available: %v)", name, Registered())
	}
	return c(ctx, d)
}

// Registered returns the sorted list of registered driver names.
func Registered() []string {
	names := make([]string, 0, len(registry))
	for n := range registry {
		names = append(names, n)
	}
	sort.Strings(names)
	return names
}

// ValidateModeForDriver enforces the per-backend mode rules: qnap is
// controller XOR node (deployed as a service controller job + a system node
// daemonset sharing one plugin_id; creds stay off worker nodes); local is
// monolith only (single plugin_id + controller-to-controller forwarding).
func ValidateModeForDriver(driverName string, mode Mode) error {
	switch mode {
	case ModeController, ModeNode, ModeMonolith:
	default:
		return fmt.Errorf("invalid --mode %q (want controller|node|monolith)", mode)
	}
	switch driverName {
	case "qnap":
		if mode == ModeMonolith {
			return fmt.Errorf("driver=qnap does not support --mode=monolith; use controller or node")
		}
	case "local":
		if mode != ModeMonolith {
			return fmt.Errorf("driver=local requires --mode=monolith (single plugin_id + controller-to-controller forwarding)")
		}
	}
	return nil
}

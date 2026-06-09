// Package iscsi wraps iscsiadm for the qnap node backend: target discovery,
// login/logout, and session rescan. All host commands go through the
// exec.Runner seam so the command sequence is unit-testable; the actual
// /dev/disk/by-path resolution is a pure string helper (ByPath) verified in
// integration.
package iscsi

import (
	"context"
	"fmt"

	cexec "github.com/honest-hosting/nomad-csi-driver/internal/exec"
)

// iscsiadm exit code 15 means "object already exists" — e.g. logging into a
// session that is already established. We treat that as success (idempotent).
const exitAlreadyExists = 15

// Connector performs iSCSI operations via iscsiadm.
type Connector struct {
	run cexec.Runner
}

// New returns a Connector backed by the given Runner.
func New(run cexec.Runner) *Connector { return &Connector{run: run} }

// Discover runs SendTargets discovery against the portal, populating the node
// database so a subsequent Login can succeed.
func (c *Connector) Discover(ctx context.Context, portal string) error {
	_, err := c.run.Run(ctx, cexec.Command{
		Name: "iscsiadm",
		Args: []string{"-m", "discovery", "-t", "sendtargets", "-p", portal},
	})
	if err != nil {
		return fmt.Errorf("iscsi discovery against %s: %w", portal, err)
	}
	return nil
}

// Login logs into the target at the portal. It is idempotent: an existing
// session is not an error.
func (c *Connector) Login(ctx context.Context, portal, iqn string) error {
	_, err := c.run.Run(ctx, cexec.Command{
		Name: "iscsiadm",
		Args: []string{"-m", "node", "-T", iqn, "-p", portal, "--login"},
	})
	if err != nil && !isExit(err, exitAlreadyExists) {
		return fmt.Errorf("iscsi login to %s via %s: %w", iqn, portal, err)
	}
	return nil
}

// Logout logs out of the target at the portal. It is idempotent: not being
// logged in is not an error.
func (c *Connector) Logout(ctx context.Context, portal, iqn string) error {
	_, err := c.run.Run(ctx, cexec.Command{
		Name: "iscsiadm",
		Args: []string{"-m", "node", "-T", iqn, "-p", portal, "--logout"},
	})
	if err != nil && !isExit(err, exitAlreadyExists) {
		return fmt.Errorf("iscsi logout of %s via %s: %w", iqn, portal, err)
	}
	return nil
}

// Rescan rescans the target's session so a server-side resize becomes visible.
func (c *Connector) Rescan(ctx context.Context, portal, iqn string) error {
	_, err := c.run.Run(ctx, cexec.Command{
		Name: "iscsiadm",
		Args: []string{"-m", "node", "-T", iqn, "-p", portal, "-R"},
	})
	if err != nil {
		return fmt.Errorf("iscsi rescan of %s: %w", iqn, err)
	}
	return nil
}

// ByPath returns the canonical /dev/disk/by-path symlink for a LUN reached over
// a given portal IP and target. Note: udev always names the link with the
// portal's IP address (e.g. ip-10.0.0.1:3260-...), never a hostname — so this
// only matches when portal is the resolved IP. Prefer DeviceGlob, which is
// portal-agnostic.
func ByPath(portal, iqn string, lun int) string {
	return fmt.Sprintf("/dev/disk/by-path/ip-%s-iscsi-%s-lun-%d", portal, iqn, lun)
}

// DeviceGlob returns a /dev/disk/by-path glob that matches a LUN by target IQN
// and LUN number, independent of the portal's address form. Because udev names
// the by-path link with the portal's IP (never the hostname), matching on
// "*-iscsi-<iqn>-lun-N" is portal-agnostic and survives DHCP IP changes — the
// IQN + LUN uniquely identify the device. (IQNs contain only '.'/':'/'-', none
// of which are glob metacharacters, so the pattern is literal apart from the
// leading '*'.)
func DeviceGlob(iqn string, lun int) string {
	return fmt.Sprintf("/dev/disk/by-path/*-iscsi-%s-lun-%d", iqn, lun)
}

func isExit(err error, code int) bool {
	return cexec.IsExit(err, code)
}

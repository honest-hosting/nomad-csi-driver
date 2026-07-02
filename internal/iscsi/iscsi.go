// Package iscsi wraps iscsiadm for the qnap node backend: target discovery,
// login/logout, and session rescan. All host commands go through the
// exec.Runner seam so the command sequence is unit-testable; the actual
// /dev/disk/by-path resolution is a pure string helper (ByPath) verified in
// integration.
package iscsi

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	cexec "github.com/honest-hosting/nomad-csi-driver/internal/exec"
)

// iscsiadm exit code 15 means "object already exists" — e.g. logging into a
// session that is already established. We treat that as success (idempotent).
const exitAlreadyExists = 15

// iscsiadm exit code 21 (ISCSI_ERR_NO_OBJS_FOUND) is what `-m session` returns
// when there are no active sessions. We treat that as an empty list, not error.
const exitNoObjectsFound = 21

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

// Session is one iSCSI session path currently established on this host: a target
// IQN reached over a portal, exposing a LUN on a backing device. With multipath
// the same (IQN, LUN) appears once per portal — one Session each, distinct
// Device — so callers that want a volume count must de-dupe on (IQN, LUN).
type Session struct {
	IQN    string // target IQN
	Portal string // "host:port" (the ",tpgt" suffix stripped)
	LUN    int    // per-target LUN number
	Device string // backing device, e.g. "/dev/sda"; "" if not yet attached
}

// ListSessions returns every iSCSI session path on this host, parsed from
// `iscsiadm -m session -P 3`. "No active sessions" (exit 21) is not an error —
// it yields an empty slice. The result is host-wide and unscoped; callers that
// care about one plugin's SAN must filter by their own portals.
func (c *Connector) ListSessions(ctx context.Context) ([]Session, error) {
	out, err := c.run.Run(ctx, cexec.Command{
		Name: "iscsiadm",
		Args: []string{"-m", "session", "-P", "3"},
	})
	if err != nil {
		if isExit(err, exitNoObjectsFound) {
			return nil, nil
		}
		return nil, fmt.Errorf("iscsi session list: %w", err)
	}
	return parseSessions(string(out.Stdout)), nil
}

// parseSessions walks the hierarchical `-P 3` output. Structure (indentation
// trimmed): a "Target: <iqn> (flags)" line opens a target; one or more
// "Current Portal: <ip:port>,<tpgt>" lines open a session path within it; an
// "Attached SCSI devices:" block then lists "scsi<H> Channel .. Lun: <N>"
// lines, each optionally followed by "Attached scsi disk <dev>". We emit one
// Session per LUN line (device filled if present).
func parseSessions(text string) []Session {
	var (
		out    []Session
		iqn    string
		portal string
		pend   *Session
	)
	flush := func() {
		if pend != nil {
			out = append(out, *pend)
			pend = nil
		}
	}
	for _, raw := range strings.Split(text, "\n") {
		line := strings.TrimSpace(raw)
		switch {
		case strings.HasPrefix(line, "Target:"):
			flush()
			iqn = firstField(strings.TrimPrefix(line, "Target:"))
			portal = ""
		case strings.HasPrefix(line, "Current Portal:"):
			flush()
			portal = stripTPGT(firstField(strings.TrimPrefix(line, "Current Portal:")))
		case strings.HasPrefix(line, "scsi") && strings.Contains(line, "Lun:"):
			flush()
			if lun, ok := lunOf(line); ok && iqn != "" {
				pend = &Session{IQN: iqn, Portal: portal, LUN: lun}
			}
		case strings.HasPrefix(line, "Attached scsi disk") && pend != nil:
			if dev := attachedDisk(line); dev != "" {
				pend.Device = "/dev/" + dev
			}
		}
	}
	flush()
	return out
}

// firstField returns the first whitespace-delimited token of s (empty if none).
func firstField(s string) string {
	if f := strings.Fields(s); len(f) > 0 {
		return f[0]
	}
	return ""
}

// stripTPGT drops the ",<tpgt>" suffix iscsiadm appends to a portal address.
func stripTPGT(portal string) string {
	if i := strings.IndexByte(portal, ','); i >= 0 {
		return portal[:i]
	}
	return portal
}

// lunOf extracts N from a "scsi5 Channel 00 Id 0 Lun: N" line.
func lunOf(line string) (int, bool) {
	fields := strings.Fields(line)
	for i, f := range fields {
		if f == "Lun:" && i+1 < len(fields) {
			n, err := strconv.Atoi(fields[i+1])
			return n, err == nil
		}
	}
	return 0, false
}

// attachedDisk extracts the device name from an "Attached scsi disk sdX  State:
// running" line (the token after "disk").
func attachedDisk(line string) string {
	fields := strings.Fields(line)
	for i, f := range fields {
		if f == "disk" && i+1 < len(fields) {
			return fields[i+1]
		}
	}
	return ""
}

func isExit(err error, code int) bool {
	return cexec.IsExit(err, code)
}

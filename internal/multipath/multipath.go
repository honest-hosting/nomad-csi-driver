// Package multipath manages device-mapper multipath for the qnap node backend.
// It renders a QNAP-specific drop-in into multipath's conf.d (never touching
// the host's /etc/multipath.conf), reloads multipathd, resolves a device's
// WWID, and flushes maps on teardown. Commands go through the exec.Runner seam.
package multipath

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	cexec "github.com/honest-hosting/nomad-csi-driver/internal/exec"
)

// DefaultConfigDir is multipath-tools' standard drop-in directory.
const DefaultConfigDir = "/etc/multipath/conf.d"

// dropinName is the file we own under the config dir.
const dropinName = "nomad-csi-qnap.conf"

// Manager renders/reloads multipath config and resolves devices.
type Manager struct {
	run       cexec.Runner
	configDir string
}

// New returns a Manager writing drop-ins into configDir (DefaultConfigDir if
// empty).
func New(run cexec.Runner, configDir string) *Manager {
	if configDir == "" {
		configDir = DefaultConfigDir
	}
	return &Manager{run: run, configDir: configDir}
}

// DefaultDropin is the QNAP device stanza. multipath-tools ships no built-in
// hwtable entry for QNAP; QNAP iSCSI reports active/active equal-priority paths
// with implicit ALUA, so we group all paths (multibus) and prioritize via alua.
// Validated against a production TVS-h874 (single TPG, all Active/Optimized).
//
// The defaults block pins user_friendly_names off: the node resolves the
// device by its WWID and expects /dev/mapper/<wwid> (see node attach). With
// user_friendly_names on, the map node is /dev/mapper/mpathN instead and the
// device wait would never resolve, failing every stage. Setting it here in our
// drop-in asserts the assumption the WWID-based path depends on.
func DefaultDropin() []byte {
	return []byte(`# Managed by nomad-csi-driver (--driver=qnap). Do not edit.
defaults {
    user_friendly_names no
}
devices {
    device {
        vendor "QNAP"
        product "iSCSI Storage"
        path_grouping_policy multibus
        prio alua
        path_checker tur
        failback immediate
        no_path_retry 12
    }
}
`)
}

// WriteDropin writes content to the drop-in file, creating the config dir.
func (m *Manager) WriteDropin(content []byte) error {
	if err := os.MkdirAll(m.configDir, 0o755); err != nil {
		return fmt.Errorf("creating multipath config dir %s: %w", m.configDir, err)
	}
	path := filepath.Join(m.configDir, dropinName)
	if err := os.WriteFile(path, content, 0o644); err != nil {
		return fmt.Errorf("writing multipath drop-in %s: %w", path, err)
	}
	return nil
}

// Reload asks multipathd to re-read its configuration.
func (m *Manager) Reload(ctx context.Context) error {
	if _, err := m.run.Run(ctx, cexec.Command{Name: "multipathd", Args: []string{"reconfigure"}}); err != nil {
		return fmt.Errorf("multipathd reconfigure: %w", err)
	}
	return nil
}

// WWID returns the multipath World Wide Identifier for a SCSI device (e.g.
// /dev/sdb), used to address its /dev/mapper entry.
func (m *Manager) WWID(ctx context.Context, device string) (string, error) {
	out, err := m.run.Run(ctx, cexec.Command{
		Name: "/lib/udev/scsi_id",
		Args: []string{"-g", "-u", "-d", device},
	})
	if err != nil {
		return "", fmt.Errorf("resolving WWID for %s: %w", device, err)
	}
	wwid := strings.TrimSpace(string(out.Stdout))
	if wwid == "" {
		return "", fmt.Errorf("empty WWID for %s", device)
	}
	return wwid, nil
}

// MapperPath returns the /dev/mapper path for a WWID.
func (m *Manager) MapperPath(wwid string) string {
	return "/dev/mapper/" + wwid
}

// MapWWID returns the WWID of the multipath map that a path device (e.g.
// /dev/sdg) belongs to, as multipathd reports it. This is the AUTHORITATIVE
// /dev/mapper name when user_friendly_names is off, and is preferred over WWID
// (scsi_id): some arrays (e.g. QNAP) report a different identifier on VPD page
// 0x83 (scsi_id -g) than the page-0x80 serial multipath's ID_SERIAL uses, so
// recomputing the WWID with scsi_id can name a /dev/mapper path that never
// exists. Returns "" (nil error) if the device is not part of a map yet.
func (m *Manager) MapWWID(ctx context.Context, device string) (string, error) {
	out, err := m.run.Run(ctx, cexec.Command{
		Name: "multipathd",
		Args: []string{"show", "paths", "format", "%d %w"},
	})
	if err != nil {
		return "", fmt.Errorf("multipathd show paths: %w", err)
	}
	base := filepath.Base(device)
	for _, line := range strings.Split(strings.TrimSpace(string(out.Stdout)), "\n") {
		f := strings.Fields(line)
		if len(f) >= 2 && f[0] == base {
			return f[1], nil
		}
	}
	return "", nil
}

// Flush removes the multipath map for a WWID on teardown. It is best-effort:
// "map in use" is surfaced so the caller can decide.
func (m *Manager) Flush(ctx context.Context, wwid string) error {
	if _, err := m.run.Run(ctx, cexec.Command{Name: "multipath", Args: []string{"-f", wwid}}); err != nil {
		return fmt.Errorf("flushing multipath map %s: %w", wwid, err)
	}
	return nil
}

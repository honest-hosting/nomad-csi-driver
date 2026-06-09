package local

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"sync"

	cexec "github.com/honest-hosting/nomad-csi-driver/internal/exec"
)

// simCreationUnix is the fixed epoch the simulator reports for the ZFS
// `creation` property, so tests can assert the authoritative snapshot time is
// plumbed through.
const simCreationUnix = int64(1_700_000_000) // 2023-11-14T22:13:20Z

// memZfs is an in-memory exec.Runner that simulates the subset of zfs/zpool
// commands the local backend issues, so controller logic can be tested without
// real ZFS.
type memZfs struct {
	mu    sync.Mutex
	vols  map[string]int64  // dataset -> volsize bytes
	snaps map[string]bool   // "dataset@snap"
	props map[string]string // "dataset|prop" -> value (user properties)
	free  int64
	size  int64
	// blocksize is the volblocksize the sim reports for every zvol (bytes).
	blocksize int64
	// absentPools, if a pool name is present, makes zpool list health exit 1
	// (pool not imported on this node).
	absentPools map[string]bool
	// failOps, keyed by zfs subcommand (create/destroy/snapshot/…), forces that
	// command to exit 1 — fault injection for the op-outcome metrics.
	failOps map[string]bool
}

func newMemZfs() *memZfs {
	return &memZfs{
		vols: map[string]int64{}, snaps: map[string]bool{}, props: map[string]string{},
		free: 1 << 42, size: 1 << 43, blocksize: 16 << 10,
		absentPools: map[string]bool{}, failOps: map[string]bool{},
	}
}

func exit1() error { return &cexec.Error{ExitCode: 1} }

func (m *memZfs) Run(_ context.Context, c cexec.Command) (cexec.Output, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	switch c.Name {
	case "zfs":
		if len(c.Args) > 0 && m.failOps[c.Args[0]] {
			return cexec.Output{}, exit1()
		}
		return m.zfs(c.Args)
	case "zpool":
		return m.zpool(c.Args)
	}
	return cexec.Output{}, nil
}

// RunPipe models `zfs send <src@snap> | zfs recv <dest>`: it honors failOps for
// either leg (so a simulated send failure surfaces) then reconstitutes the
// dataset on the destination.
func (m *memZfs) RunPipe(_ context.Context, producer, consumer cexec.Command) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if len(producer.Args) > 0 && m.failOps[producer.Args[0]] {
		return exit1() // e.g. failOps["send"]
	}
	if len(consumer.Args) > 0 && m.failOps[consumer.Args[0]] {
		return exit1() // e.g. failOps["recv"]
	}
	// producer: zfs send <src@snap>; consumer: zfs recv <dest>
	src := producer.Args[len(producer.Args)-1]
	dest := consumer.Args[len(consumer.Args)-1]
	parts := strings.SplitN(src, "@", 2)
	m.vols[dest] = m.vols[parts[0]]
	if len(parts) == 2 {
		m.snaps[dest+"@"+parts[1]] = true // recv reconstitutes the sent snapshot
	}
	return nil
}

func (m *memZfs) zfs(args []string) (cexec.Output, error) {
	switch args[0] {
	case "list":
		return m.list(args)
	case "create":
		ds := args[len(args)-1]
		var size int64
		for i, a := range args {
			if a == "-V" && i+1 < len(args) {
				size, _ = strconv.ParseInt(args[i+1], 10, 64)
			}
		}
		m.vols[ds] = size
		return cexec.Output{}, nil
	case "destroy":
		ds := args[len(args)-1]
		if strings.Contains(ds, "@") {
			if !m.snaps[ds] {
				return cexec.Output{}, exit1()
			}
			delete(m.snaps, ds)
			return cexec.Output{}, nil
		}
		if _, ok := m.vols[ds]; !ok {
			return cexec.Output{}, exit1()
		}
		delete(m.vols, ds)
		return cexec.Output{}, nil
	case "get": // zfs get -H[p] -o value <prop> <ds>
		prop, ds := args[len(args)-2], args[len(args)-1]
		if prop == "creation" {
			return cexec.Output{Stdout: []byte(strconv.FormatInt(simCreationUnix, 10) + "\n")}, nil
		}
		if prop == "volblocksize" {
			return cexec.Output{Stdout: []byte(strconv.FormatInt(m.blocksize, 10) + "\n")}, nil
		}
		if strings.Contains(prop, ":") { // user property, e.g. nomad-csi:source
			v, ok := m.props[ds+"|"+prop]
			if !ok {
				v = "-" // ZFS reports "-" for an unset user property
			}
			return cexec.Output{Stdout: []byte(v + "\n")}, nil
		}
		size, ok := m.vols[ds]
		if !ok {
			return cexec.Output{}, exit1()
		}
		return cexec.Output{Stdout: []byte(strconv.FormatInt(size, 10) + "\n")}, nil
	case "set": // zfs set <prop>=<value> <ds>
		ds := args[len(args)-1]
		for _, a := range args {
			switch {
			case strings.HasPrefix(a, "volsize="):
				n, _ := strconv.ParseInt(strings.TrimPrefix(a, "volsize="), 10, 64)
				m.vols[ds] = n
			case strings.Contains(a, ":") && strings.Contains(a, "="):
				kv := strings.SplitN(a, "=", 2)
				m.props[ds+"|"+kv[0]] = kv[1]
			}
		}
		return cexec.Output{}, nil
	case "snapshot":
		m.snaps[args[len(args)-1]] = true
		return cexec.Output{}, nil
	}
	return cexec.Output{}, nil
}

func (m *memZfs) list(args []string) (cexec.Output, error) {
	target := args[len(args)-1]
	// Volume listing: zfs list -Hp -t volume -r -o name,volsize <parent>
	isVolumeList := false
	for _, a := range args {
		if a == "volume" {
			isVolumeList = true
		}
	}
	if isVolumeList {
		var b strings.Builder
		for ds, size := range m.vols {
			if ds == target || strings.HasPrefix(ds, target+"/") {
				fmt.Fprintf(&b, "%s\t%d\n", ds, size)
			}
		}
		return cexec.Output{Stdout: []byte(b.String())}, nil
	}
	// Snapshot listing: zfs list -Hp -t snapshot -r -o name,volsize,creation <target>
	for _, a := range args {
		if a == "snapshot" {
			var b strings.Builder
			for full := range m.snaps {
				ds, _, _ := strings.Cut(full, "@")
				if ds == target || strings.HasPrefix(ds, target+"/") {
					fmt.Fprintf(&b, "%s\t%d\t%d\n", full, m.vols[ds], simCreationUnix)
				}
			}
			return cexec.Output{Stdout: []byte(b.String())}, nil
		}
	}
	// Existence check: zfs list -H -o name <ds>
	if _, ok := m.vols[target]; ok {
		return cexec.Output{Stdout: []byte(target)}, nil
	}
	if m.snaps[target] {
		return cexec.Output{Stdout: []byte(target)}, nil
	}
	return cexec.Output{}, exit1()
}

func (m *memZfs) zpool(args []string) (cexec.Output, error) {
	switch args[0] {
	case "get": // zpool get -Hp -o value <prop> <pool>
		prop := args[len(args)-2]
		v := m.free
		if prop == "size" {
			v = m.size
		}
		return cexec.Output{Stdout: []byte(strconv.FormatInt(v, 10) + "\n")}, nil
	case "list": // zpool list -H -o health <pool>
		pool := args[len(args)-1]
		if m.absentPools[pool] {
			return cexec.Output{}, exit1() // not imported on this node
		}
		return cexec.Output{Stdout: []byte("ONLINE\n")}, nil
	}
	return cexec.Output{}, nil
}

func (m *memZfs) hasVol(ds string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	_, ok := m.vols[ds]
	return ok
}

// Command nomad-csi-driver is the single binary for the multi-backend Nomad CSI
// driver. The storage backend is selected by --driver and the process role by
// --mode; see `nomad-csi-driver run --help`.
package main

import "github.com/honest-hosting/nomad-csi-driver/cmd/nomad-csi-driver/cmd"

func main() {
	cmd.Execute()
}

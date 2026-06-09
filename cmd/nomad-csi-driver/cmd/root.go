package cmd

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"
)

var rootCmd = &cobra.Command{
	Use:   "nomad-csi-driver",
	Short: "Multi-backend CSI driver for HashiCorp Nomad",
	Long: "nomad-csi-driver is a Container Storage Interface (CSI) driver for " +
		"HashiCorp Nomad with pluggable storage backends (--driver=qnap|local). " +
		"There is no Kubernetes support.",
	SilenceUsage: true,
}

// Execute runs the root command.
func Execute() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}

func init() {
	rootCmd.AddCommand(runCmd)
	rootCmd.AddCommand(versionCmd)
}

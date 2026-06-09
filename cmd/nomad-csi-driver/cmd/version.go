package cmd

import (
	"fmt"

	"github.com/spf13/cobra"

	"github.com/honest-hosting/nomad-csi-driver/internal/version"
)

var versionCmd = &cobra.Command{
	Use:   "version",
	Short: "Print build version information",
	RunE: func(cmd *cobra.Command, _ []string) error {
		_, err := fmt.Fprintln(cmd.OutOrStdout(), version.String())
		return err
	},
}

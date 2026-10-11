//go:build !realtools

package main

import (
	"fmt"

	"piperoom/internal/actions"
	"piperoom/internal/config"
	"piperoom/internal/tools"
)

// wireRealTools is a placeholder until the A2/A3 client/tool packages merge.
// Rebuild with `-tags realtools` once they land (and pass --fake-tools=false).
func wireRealTools(_ config.Config, _ *tools.Registry, _ *actions.Registry) error {
	return fmt.Errorf("investigate: real tools are not available yet (A2/A3 packages not merged); run with --fake-tools (default) or rebuild with -tags realtools")
}

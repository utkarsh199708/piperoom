package wiring

import (
	"piperoom/internal/config"
	"piperoom/internal/owners"
)

// PipelineConfig returns the resolved client settings of a pipeline that has a
// connections: block, read from the process environment and files exactly as
// OpenStacks reads them. It exists for the admin connection test
// (docs/design/onboarding.md §6.4), which probes the settings a pipeline's
// client set is built from. The result holds secrets: never log or return it.
func PipelineConfig(p owners.Pipeline) (config.Config, error) {
	return pipelineConfig(p, osConnSource)
}

package agent

import (
	"fmt"

	"piperoom/internal/tools"
)

// checkReadOnly refuses a Deps.Tools that holds anything but KindRead tools.
// The model runs every tool in Deps.Tools without approval, so a tool that
// can change a system must never get there; the agent's own KindRoom tools
// are added by the agent, never through the registry. A registry from
// tools.NewReadOnlyRegistry already refuses such tools at Register; this
// check also covers a plain registry handed in by a caller not yet switched
// (cmd/server demo mode until R5a), so the guarantee holds for every Run.
func checkReadOnly(reg *tools.Registry) error {
	for _, t := range reg.All() {
		if k := t.Kind(); k != tools.KindRead {
			return fmt.Errorf("agent: Deps.Tools: tool %q has kind %q: %w", t.Name(), k, tools.ErrNotReadOnly)
		}
	}
	return nil
}

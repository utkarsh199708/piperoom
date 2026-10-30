package managed

import (
	"piperoom/internal/setup/plan"
)

// The ladder reads of the managed targets: the ops of this package that
// plan.ManagedLadder's rungs 1 and 2 read.
const (
	ReadMSKConnector       = plan.ReadMSKConnector
	ReadConfluentConnector = plan.ReadConfluentConnector
	ReadAivenConnector     = plan.ReadAivenConnector
)

// Ladder returns the verification ladder of a managed target; it is
// plan.ManagedLadder, which the server builds without linking this package.
func Ladder(target, source, sink string) ([]plan.Rung, error) {
	return plan.ManagedLadder(target, source, sink)
}

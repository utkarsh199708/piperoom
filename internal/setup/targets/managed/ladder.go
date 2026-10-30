package managed

import (
	"fmt"

	"piperoom/internal/setup/plan"
)

// Ladder reads of the managed targets: rungs 1 and 2 read the provider's own
// connector status through the runner's op (describe-connector), because the
// managed services expose no Connect REST endpoint to the room. Their Data
// carries "state" and "tasks" in the Connect REST shape, so plan's rung
// predicates judge them unchanged.
const (
	ReadMSKConnector       = "msk_describe_connector"
	ReadConfluentConnector = "ccloud_connector_status"
	ReadAivenConnector     = "aiven_connector_status"
)

// Ladder returns the verification ladder of a managed target ("msk",
// "confluent" or "aiven", as events.SetupRequest.Target names it). Rungs 3
// to 8 read Kafka, the schema registry and the sink and do not depend on
// where the connector runs, so they are plan.Ladder's own rungs (taken from
// its REST ladder, the only form plan.Ladder builds); rungs 1 and 2 read the
// provider's status op instead of connect_status. MSK Connect reports no
// per-task state: its rung 2 is not applicable, and rung 1 reads the
// connector's own state, which MSK reports RUNNING only once its workers run.
func Ladder(target, source, sink string) ([]plan.Rung, error) {
	var read string
	switch target {
	case "msk":
		read = ReadMSKConnector
	case "confluent":
		read = ReadConfluentConnector
	case "aiven":
		read = ReadAivenConnector
	default:
		return nil, fmt.Errorf("%w: %q is not a managed target", plan.ErrLadder, clipText(target, 32))
	}
	rungs, err := plan.Ladder(source, "compose", sink)
	if err != nil {
		return nil, err
	}
	for i := range rungs {
		switch rungs[i].Name {
		case plan.RungConnector, plan.RungTasks:
			rungs[i].Reads = []string{read}
		}
	}
	if target == "msk" {
		for i := range rungs {
			if rungs[i].Name == plan.RungTasks {
				rungs[i].NotApplicable = "MSK Connect reports no per-task state; rung 1 reads the connector's state"
			}
		}
	}
	return rungs, nil
}

package room

import (
	"testing"

	"piperoom/internal/setup/plan"
)

// Rungs 1 and 2 read what the target exposes: Connect REST on compose and
// host, the KafkaConnector CR through the runner on Strimzi, the provider's
// status op on the managed services. Rungs 3 to 8 are the same everywhere.
func TestSetupLadderRungsPerTarget(t *testing.T) {
	for target, want := range map[string]string{
		"compose":   "connect_status",
		"host":      "connect_status",
		"strimzi":   plan.ReadK8sGet,
		"msk":       plan.ReadMSKConnector,
		"confluent": plan.ReadConfluentConnector,
		"aiven":     plan.ReadAivenConnector,
	} {
		rungs, err := setupLadderRungs("postgres", target)
		if err != nil {
			t.Fatalf("%s: %v", target, err)
		}
		if rungs[0].Name != plan.RungConnector || len(rungs[0].Reads) != 1 || rungs[0].Reads[0] != want {
			t.Errorf("%s: rung 1 reads %v, want %s", target, rungs[0].Reads, want)
		}
		if rungs[2].Name != plan.RungCanaryTopic {
			t.Errorf("%s: rung 3 is %s", target, rungs[2].Name)
		}
	}
	for _, target := range []string{"dbzserver", "embedded"} {
		if _, err := setupLadderRungs("postgres", target); err == nil {
			t.Errorf("%s: a ladder was built for a target that defines none", target)
		}
	}
}

func TestSetupLadderReadInputs(t *testing.T) {
	l := &setupLadder{connector: "orders-src"}
	in, err := l.readInput(plan.RungConnector, plan.ReadK8sGet)
	if err != nil || in["kind"] != "KafkaConnector" || in["name"] != "orders-src" {
		t.Errorf("k8s_get input = %v, %v", in, err)
	}
	for _, read := range []string{plan.ReadMSKConnector, plan.ReadConfluentConnector, plan.ReadAivenConnector} {
		in, err := l.readInput(plan.RungConnector, read)
		if err != nil || in["name"] != "orders-src" || len(in) != 1 {
			t.Errorf("%s input = %v, %v", read, in, err)
		}
	}
	empty := &setupLadder{}
	if _, err := empty.readInput(plan.RungConnector, plan.ReadK8sGet); err == nil {
		t.Error("k8s_get input built with no connector")
	}
}

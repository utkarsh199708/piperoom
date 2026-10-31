package strimzi

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"piperoom/internal/events"
)

// TestCelcheckCasesMatchTheRunner runs every KafkaConnector and KafkaConnect
// case of the admission policy's celcheck/cases.json through the runner's
// allow-list and target binding, with the CI values' names (namespace data,
// Kafka prod-kafka, pipeline orders, prefix pg, database pg:5432, registry
// http://registry.data.svc:8081). The policy mirrors the runner, so both
// give the same verdict; a case whose "runner" field is false is one the
// runner refuses on a fact the policy cannot see (the Kafka CR's listener
// list), and is only ever stricter.
func TestCelcheckCasesMatchTheRunner(t *testing.T) {
	b, err := os.ReadFile(filepath.Join(chartDir, "ci/setup/celcheck/cases.json"))
	if err != nil {
		t.Fatal(err)
	}
	var cases []struct {
		Name   string         `json:"name"`
		Allow  bool           `json:"allow"`
		Runner *bool          `json:"runner"`
		Skip   string         `json:"runner_skip"`
		Obj    map[string]any `json:"obj"`
	}
	if err := json.Unmarshal(b, &cases); err != nil {
		t.Fatal(err)
	}
	h := newHarness(t, "", nil)
	withConnectFacts(h)
	h.env.Targets = append(h.env.Targets, events.SetupTarget{Kind: targetSchemaRegistry, Address: "registry.data.svc:8081"})
	n := 0
	for _, c := range cases {
		spec, _ := c.Obj["spec"].(map[string]any)
		if c.Skip != "" {
			continue
		}
		want := c.Allow
		if c.Runner != nil {
			if *c.Runner && !c.Allow {
				t.Errorf("%s: a runner override may only make the runner stricter", c.Name)
			}
			want = *c.Runner
		}
		var err error
		switch c.Obj["kind"] {
		case "KafkaConnector":
			if _, ok := spec["config"]; !ok {
				continue // the chart's paused shell
			}
			if err = h.d.ValidateConnector(spec, testNS, testKafka, "pg"); err == nil {
				err = h.d.connectorFacts(context.Background(), rendered{kind: "KafkaConnector", name: "orders-src", value: spec}, testNS, testKafka, h.env)
			}
		case "KafkaConnect":
			if err = h.d.ValidateConnect(spec, testKafka, testNS); err == nil {
				cfg, _ := asMap(spec["config"])
				err = registryFacts(cfg, h.env)
			}
		default:
			continue
		}
		n++
		if (err == nil) != want {
			t.Errorf("%s: runner err = %v, policy allow %v", c.Name, err, c.Allow)
		}
	}
	if n < 100 {
		t.Fatalf("only %d cases ran", n)
	}
}

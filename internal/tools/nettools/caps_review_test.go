package nettools

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"piperoom/internal/config"
	"piperoom/internal/owners"
)

func okChecks(n int) []Check {
	out := make([]Check, 0, n)
	for i := 0; i < n; i++ {
		out = append(out, Check{ID: "tcp", Addr: fmt.Sprintf("h%d:1", i), Status: StatusOK, Detail: "TCP connected"})
	}
	return out
}

// A long broker list cannot push the one failing check out of the verdict,
// the Summary or Data.
func TestResultCapKeepsFailures(t *testing.T) {
	checks := append(okChecks(60), Check{ID: "listeners", Status: StatusFail, Problem: "node 2 advertises an unreachable address"})
	res := (&netProbe{}).result(probeData{Target: LabelKafka, Kind: KindKafka, Endpoints: []string{"h:1"}, Checks: checks})
	data := res.Data.(probeData)
	if len(data.Checks) != maxChecks || !res.Truncated {
		t.Fatalf("checks = %d truncated = %v", len(data.Checks), res.Truncated)
	}
	if data.Checks[len(data.Checks)-1].ID != "listeners" {
		t.Fatalf("the failing check was capped away: last = %+v", data.Checks[len(data.Checks)-1])
	}
	for _, want := range []string{"1 of 61 checks failed", "\n- listeners fail", "Data carries 48 of the 61"} {
		if !strings.Contains(res.Summary, want) {
			t.Fatalf("summary lacks %q:\n%s", want, res.Summary)
		}
	}
	if strings.Contains(res.Summary, "all checks ok") {
		t.Fatalf("summary claims success:\n%s", res.Summary)
	}
}

// Failures are listed before passing steps, so the summary line cap never
// hides one; the "more" line counts what is actually not shown.
func TestSummaryListsFailuresFirst(t *testing.T) {
	checks := append(okChecks(24), Check{ID: "tcp", Addr: "dead:9092", Status: StatusFail, Problem: "connection refused"})
	res := (&netProbe{}).result(probeData{Target: LabelKafka, Kind: KindKafka, Checks: checks})
	lines := strings.Split(res.Summary, "\n")
	if len(lines) < 2 || !strings.Contains(lines[1], "dead:9092") {
		t.Fatalf("the failure is not the first check line:\n%s", res.Summary)
	}
	if !strings.Contains(res.Summary, "… 5 more checks not shown (all are in Data)") {
		t.Fatalf("summary = %s", res.Summary)
	}
}

// A broker listed without a port is the Kafka client's 9092, and every
// configured address is in Endpoint.Addr form.
func TestBrokerWithoutPort(t *testing.T) {
	src := RoomTargets(config.Config{KafkaBrokers: "LOCALHOST, b2:29092"}, nil)
	tgt, err := src.Target(context.Background(), owners.Pipeline{ID: "orders"}, LabelKafka)
	if err != nil {
		t.Fatal(err)
	}
	var got []string
	for _, e := range tgt.Endpoints {
		got = append(got, e.Addr)
	}
	if strings.Join(got, ",") != "localhost:9092,b2:29092" {
		t.Fatalf("endpoints = %v", got)
	}
}

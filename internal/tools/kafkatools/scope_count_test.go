package kafkatools

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"piperoom/internal/clients"
	"piperoom/internal/tools"
)

// Task T2: the cluster-wide list tools return only the room pipeline's
// objects and a count-only line for the rest. The other pipelines' names carry
// foreignMark, so a leak into Summary or the marshalled Data is a substring
// check.

const foreignMark = "FOREIGNMARK"

func assertNoForeign(t *testing.T, r tools.Result) {
	t.Helper()
	data, err := json.Marshal(r.Data)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(r.Summary, foreignMark) || strings.Contains(string(data), foreignMark) {
		t.Fatalf("output names another pipeline's object:\nsummary: %s\ndata: %s", r.Summary, data)
	}
}

// markedFakes serves the "orders" pipeline's objects next to the "billing"
// pipeline's and unowned ones, all of the latter with foreignMark in the name.
// The pipeline has a second group, order-audit, that reads none of its topics.
func markedFakes() Deps {
	d, _ := scopeFakes()
	d.Owners.Pipelines[0].ConsumerGroups = append(d.Owners.Pipelines[0].ConsumerGroups, "order-audit")
	d.Connect.(*fakeConnect).listFn = func(context.Context) ([]string, error) {
		return []string{"bill-" + foreignMark, "pg-orders-connector", "other-" + foreignMark}, nil
	}
	fk := d.Kafka.(*fakeKafkaConfig)
	fk.groupsFn = func(context.Context) ([]string, error) {
		return []string{"order-sync", "order-audit", "bill-sync-" + foreignMark, "other-" + foreignMark, "x-" + foreignMark}, nil
	}
	fk.lagFn = func(_ context.Context, g string) (clients.GroupLag, error) {
		if g == "order-audit" {
			return clients.GroupLag{Group: g, State: "Empty", Partitions: []clients.PartitionLag{{Topic: "audit.events", Partition: 0}}}, nil
		}
		return clients.GroupLag{Group: g, State: "Stable", Members: 2, TotalLag: 990012, Partitions: []clients.PartitionLag{
			{Topic: "pg.public.orders", Partition: 0, Committed: 5, End: 17, Lag: 12},
			{Topic: "bill.public." + foreignMark, Partition: 0, Lag: 990000},
			{Topic: "bill.public." + foreignMark, Partition: 1},
		}}, nil
	}
	d.Registry.(*fakeRegistry).subjectsFn = func(context.Context) ([]string, error) {
		return []string{"pg.public.orders-value", "bill.public." + foreignMark + "-value", "pg.public.orders-key", foreignMark + "-value"}, nil
	}
	return d
}

func TestScopedListsCountOtherObjects(t *testing.T) {
	cases := []struct {
		tool, in  string
		mine      string // a line of the pipeline's own object
		absent    string // a pipeline object the input's filter drops: not shown, not counted
		countLine string
	}{
		{"connect_list_connectors", `{}`, "pg-orders-connector (source): RUNNING, 0 failed tasks", "",
			"2 other connectors not shown (not part of this room's pipeline)"},
		{"kafka_list_groups", `{}`, "order-audit: Empty", "",
			"3 other consumer groups not shown (not part of this room's pipeline)"},
		// The count is taken before the topic filter: order-audit, the
		// pipeline's group that does not read the topic, is dropped but not
		// counted, and a topic cannot probe other groups through the count.
		{"kafka_list_groups", `{"topic":"pg.public.orders"}`, "order-sync: Stable", "order-audit",
			"3 other consumer groups not shown (not part of this room's pipeline)"},
		{"schema_registry_subjects", `{}`, "pg.public.orders-key", "",
			"2 other subjects not shown (not part of this room's pipeline)"},
		{"schema_registry_subjects", `{"prefix":"bill"}`, "", "pg.public.orders",
			"2 other subjects not shown (not part of this room's pipeline)"},
		{"kafka_consumer_lag", `{"group":"order-sync"}`, "order-sync: Stable, 2 members, total lag 12", "",
			"2 other topic partitions not shown (not part of this room's pipeline); the total lag above counts only this pipeline's partitions"},
	}
	for _, tc := range cases {
		t.Run(tc.tool+" "+tc.in, func(t *testing.T) {
			r, err := scopeTool(t, markedFakes(), tc.tool).Run(roomCtx(), json.RawMessage(tc.in))
			if err != nil {
				t.Fatal(err)
			}
			assertNoForeign(t, r)
			if !strings.Contains(r.Summary, tc.mine) {
				t.Fatalf("summary misses %q: %q", tc.mine, r.Summary)
			}
			if tc.absent != "" && strings.Contains(r.Summary, tc.absent) {
				t.Fatalf("summary shows %q, which the input filters out: %q", tc.absent, r.Summary)
			}
			lines := strings.Split(r.Summary, "\n")
			if last := lines[len(lines)-1]; last != tc.countLine {
				t.Fatalf("last line = %q, want %q", last, tc.countLine)
			}
			if strings.Contains(string(mustJSON(t, r.Data)), "990") {
				t.Fatalf("data carries another pipeline's lag: %s", mustJSON(t, r.Data))
			}
		})
	}
}

// Unscoped (CLI, dev) output is unchanged: every object, no count line.
func TestUnscopedListsHaveNoCountLine(t *testing.T) {
	for _, tc := range []struct{ tool, in string }{
		{"connect_list_connectors", `{}`},
		{"kafka_list_groups", `{}`},
		{"schema_registry_subjects", `{}`},
		{"kafka_consumer_lag", `{"group":"order-sync"}`},
	} {
		t.Run(tc.tool, func(t *testing.T) {
			r, err := scopeTool(t, markedFakes(), tc.tool).Run(tools.WithUnscoped(context.Background()), json.RawMessage(tc.in))
			if err != nil {
				t.Fatal(err)
			}
			if !strings.Contains(r.Summary, foreignMark) || strings.Contains(r.Summary, "not shown") {
				t.Fatalf("unscoped summary changed: %q", r.Summary)
			}
		})
	}
}

func mustJSON(t *testing.T, v any) []byte {
	t.Helper()
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

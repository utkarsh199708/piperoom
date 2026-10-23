//go:build xwavegen

package main

import (
	"flag"
	"fmt"
	"math/rand/v2"
	"os"
	"slices"
	"strconv"
	"time"

	"piperoom/internal/owners"
)

// The two X2e recovery scenarios, each the state its devctl break leaves.

const reprocessTopic = "pg.public.orders.reprocess"

// rowsLostState: `devctl break rows-lost-after-retention`. order-sync is
// stopped at 09:04:30; retention.ms=1000 is set at 09:05:00; 50 orders go in
// five batches of 10, two seconds apart, so each batch starts a new segment;
// every segment before the last batch's is deleted. The override is removed
// at 09:06:00, order-sync starts at 09:06:10 (its committed offset is below
// the log start: auto.offset.reset=earliest resumes at the last batch), and
// the marker order 1351 is loaded at 09:06:20.
func rowsLostState() *xstate {
	rng := rand.New(rand.NewPCG(21, 1))
	s := baseState(rng, fx(9, 3, 58, 0))
	consumerDown := func(r orow, _ string) (orow, bool) { return r, false }
	var lastBatch time.Time
	for b := 0; b < 5; b++ {
		at := fx(9, 5, 2+2*b, 117+31*b)
		for g := 1; g <= 10; g++ {
			c := int64(9000 + 10*b + g)
			sink := consumerDown
			if b == 4 {
				sink = goodSink // the last batch survives and is read after the restart
			}
			s.insert(at, c, 300+c-9000, "placed", sink)
		}
		lastBatch = at
	}
	s.cutBefore(lastBatch)
	// order-sync starts at 09:06:10 and loads the last batch then.
	for id := int64(1341); id <= 1350; id++ {
		s.snk[id].loaded = fx(9, 6, 11, int(id-1341)*37)
	}
	s.insert(fx(9, 6, 20, 412), 77, 4200, "placed", goodSink) // marker
	return s
}

// skippedState: `devctl break consumer-skipped-range`. order-sync is stopped
// at 08:50:00 for its 2.17.0 deploy; 40 orders are written at 08:51:10 and
// reach the topic; at 08:52:40 the deploy runbook's leftover step resets
// order-sync to the latest offsets; 2.17.0 starts at 08:53:05 and loads the
// marker order 1341 at 08:53:20. The 40 change events stay on the topic.
func skippedState() *xstate {
	rng := rand.New(rand.NewPCG(22, 1))
	s := baseState(rng, fx(8, 47, 13, 402))
	skipped := func(r orow, _ string) (orow, bool) { return r, false }
	for g := int64(1); g <= 40; g++ {
		s.insert(fx(8, 51, 10, 233), 9100+g, 500+g, "placed", skipped)
	}
	s.insert(fx(8, 53, 20, 87), 77, 4200, "placed", goodSink) // marker
	return s
}

// lostKeys are the source ids the sink lacks.
func lostKeys(s *xstate) []string {
	var ids []int64
	for id := range s.src {
		if _, ok := s.snk[id]; !ok {
			ids = append(ids, id)
		}
	}
	slices.Sort(ids)
	out := make([]string, len(ids))
	for i, id := range ids {
		out[i] = strconv.FormatInt(id, 10)
	}
	return out
}

// edgeKeys are the first and last lost key of each partition: the ends of
// the offset ranges.
func edgeKeys(s *xstate, lost []string) []string {
	var out []string
	for p := int64(0); p < 3; p++ {
		var in []string
		for _, k := range lost {
			id, _ := strconv.ParseInt(k, 10, 64)
			if id%3 == p {
				in = append(in, k)
			}
		}
		if len(in) > 0 {
			out = append(out, in[0])
			if len(in) > 1 {
				out = append(out, in[len(in)-1])
			}
		}
	}
	return out
}

func recoveryCalls(lost, present []string, reconcileFrom, reconcileTo time.Time) []call {
	pipe := map[string]any{"pipeline_id": "orders"}
	calls := []call{
		{tool: "pipeline_map", input: pipe},
		{tool: "connect_list_connectors"},
		{tool: "connect_status", input: map[string]any{"connector": "pg-orders-connector"}},
		{tool: "connect_config", input: map[string]any{"connector": "pg-orders-connector"}},
		{tool: "schema_registry_subjects"},
		{tool: "schema_registry_versions", input: map[string]any{"subject": "pg.public.orders-key"}},
		{tool: "schema_registry_versions", input: map[string]any{"subject": "pg.public.orders-value"}},
		{tool: "kafka_topic_offsets", input: map[string]any{"topic": ordersTopic}},
		{tool: "kafka_topic_offsets", input: map[string]any{"topic": reprocessTopic}},
		{tool: "kafka_topic_config", input: map[string]any{"pipeline_id": "orders", "table": "public.orders"}},
		{tool: "kafka_list_groups"},
		{tool: "kafka_consumer_lag", input: map[string]any{"group": "order-sync"}},
		{tool: "pg_replication_slots"},
		{tool: "pg_recent_ddl", input: map[string]any{"since": ago(24 * time.Hour)}, free: []string{"since"}},
		{tool: "pg_long_transactions"},
		{tool: "pg_publication_tables", input: pipe},
		{tool: "warehouse_freshness", input: map[string]any{"table": "analytics.orders", "ts_column": "updated_at"}},
		{tool: "reconcile", input: map[string]any{"table": "public.orders", "since": reconcileFrom.Format(time.RFC3339), "until": reconcileTo.Format(time.RFC3339)}, free: []string{"since", "until"}},
		{tool: "kafka_dup_keys", input: map[string]any{"topic": ordersTopic, "since": ago(2 * time.Hour)}, free: []string{"since"}},
	}
	for _, k := range lost {
		calls = append(calls, call{tool: "pg_row", input: map[string]any{"pipeline_id": "orders", "table": "public.orders", "pk_column": "id", "key": k}})
	}
	for _, k := range append(append([]string(nil), lost...), present...) {
		calls = append(calls, call{tool: "kafka_find_by_key", input: map[string]any{"topic": ordersTopic, "key": k, "since": ago(3 * time.Hour)}, free: []string{"since"}})
	}
	for _, k := range append(append([]string(nil), lost...), present...) {
		calls = append(calls, call{tool: "warehouse_row", input: map[string]any{"pipeline_id": "orders", "table": "analytics.orders", "pk_column": "id", "key": k}})
	}
	for _, k := range append(append([]string(nil), lost...), present...) {
		calls = append(calls, call{tool: "trace_record", input: map[string]any{"pipeline_id": "orders", "table": "public.orders", "key": k}})
	}
	return append(calls,
		call{tool: "pipeline_map", input: map[string]any{"pipeline_id": "default"}, wildcard: true},
		call{tool: "connect_status", input: map[string]any{"connector": "orders-connector"}, wildcard: true},
		call{tool: "connect_config", input: map[string]any{"connector": "orders-connector"}, wildcard: true},
		call{tool: "schema_registry_versions", input: map[string]any{"subject": "orders-value"}, wildcard: true},
		call{tool: "kafka_topic_offsets", input: map[string]any{"topic": "orders.events"}, wildcard: true},
		call{tool: "kafka_consumer_lag", input: map[string]any{"group": "orders-consumer"}, wildcard: true},
		call{tool: "pg_row", input: map[string]any{"pipeline_id": "orders", "table": "public.orders", "pk_column": "id", "key": "999999"}, wildcard: true},
		call{tool: "warehouse_row", input: map[string]any{"pipeline_id": "orders", "table": "analytics.orders", "pk_column": "id", "key": "999999"}, wildcard: true},
		call{tool: "kafka_find_by_key", input: map[string]any{"topic": ordersTopic, "key": "999999", "since": ago(3 * time.Hour)}, wildcard: true},
		call{tool: "trace_record", input: map[string]any{"pipeline_id": "orders", "table": "public.orders", "key": "999999"}, wildcard: true},
	)
}

func scens() []scen {
	lostSt, skipSt := rowsLostState(), skippedState()
	lostA, lostB := lostKeys(lostSt), lostKeys(skipSt)
	return []scen{
		{file: "evals/scenarios/rows-lost-after-retention.yaml", build: rowsLostState, narrow: []time.Time{fx(9, 0, 0, 0), fx(9, 10, 0, 0)},
			calls: recoveryCalls(lostA, []string{"1341", "1350", "1351"}, fx(8, 40, 0, 0), fx(9, 38, 0, 0))},
		{file: "evals/scenarios/consumer-skipped-range.yaml", build: skippedState, narrow: []time.Time{fx(8, 45, 0, 0), fx(8, 55, 0, 0)},
			calls: recoveryCalls(lostB, []string{"1341"}, fx(8, 40, 0, 0), fx(9, 38, 0, 0))},
	}
}

func main() {
	write := flag.Bool("write", false, "fill the scenario files")
	ownersFile := flag.String("owners", "dev/owners.recovery.yaml", "owners file the tools are scoped by")
	flag.Parse()
	o, err := owners.Load(*ownersFile)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	for _, s := range scens() {
		s.st = s.build()
		lost := lostKeys(s.st)
		fmt.Printf("%s: lost %d keys %s..%s; log start %v; next %v\n", s.file, len(lost), lost[0], lost[len(lost)-1], s.st.start, s.st.next)
		for p := int32(0); p < 3; p++ {
			var offs []int64
			for _, e := range s.st.events {
				if e.part == p && e.id >= 1301 && e.id <= 1340 {
					offs = append(offs, e.offset)
				}
			}
			if len(offs) > 0 {
				fmt.Printf("   partition %d: lost-key offsets %d..%d (%d)\n", p, offs[0], offs[len(offs)-1], len(offs))
			}
		}
		if !*write {
			continue
		}
		out, err := s.render(o)
		if err != nil {
			fmt.Fprintln(os.Stderr, s.file, err)
			os.Exit(1)
		}
		if err := os.WriteFile(s.file, out, 0o644); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		fmt.Println("wrote", s.file)
	}
}

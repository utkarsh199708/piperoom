package topology

import (
	"context"
	"slices"
	"strings"
	"testing"
	"time"

	"piperoom/internal/clients"
)

// noScanReport hides fakeKafka's FindByKeyScan: a Kafka client without
// clients.KeyScanReader (an eval replay fake, an older client).
type noScanReport struct{ clients.KafkaAdmin }

// The trace must never state "not there", "newest" or "consumed" as fact over
// a key scan that may have been cut or a partition whose offsets could not be
// read (#306): those stages read "unknown" with the reason.
func TestTraceOverTruncatedOrUnknownPartitions(t *testing.T) {
	now := time.Now()
	rec := clients.RecordRef{Key: "7", Partition: 0, Offset: 42, Timestamp: now}
	committed := func(c int64) map[string]clients.GroupLag {
		return map[string]clients.GroupLag{"order-sync": {State: "Stable", Partitions: []clients.PartitionLag{{Topic: "pg.public.orders", Partition: 0, Committed: c}}}}
	}
	cases := []struct {
		name      string
		refs      []clients.RecordRef
		truncated []int32
		hide      bool // Kafka client without KeyScanReader
		lags      map[string]clients.GroupLag
		stuck     bool // connector FAILED
		want      [5]string
		wantFacts map[int]map[string]string // stage index -> facts that must be present
	}{
		{
			name: "complete scan, consumed",
			refs: []clients.RecordRef{rec}, lags: committed(43),
			want: [5]string{"seen", "seen", "seen", "seen", "seen"},
		},
		{
			name: "complete scan, not in topic, connector failed",
			lags: committed(43), stuck: true,
			want: [5]string{"seen", "stopped_here", "not_seen", "not_seen", "not_seen"},
		},
		{
			name: "truncated scan, committed past the match: newer match may not be consumed",
			refs: []clients.RecordRef{rec}, truncated: []int32{0}, lags: committed(43),
			want: [5]string{"seen", "seen", "seen", "unknown", "unknown"},
			wantFacts: map[int]map[string]string{
				2: {"newest": "unknown: key scan truncated on partition(s) 0"},
				3: {"error": "key scan truncated on partition(s) 0; a newer match may not be consumed"},
				4: {"error": "consumer stage unknown"},
			},
		},
		{
			name: "truncated scan, committed before the match: not consumed is still a fact",
			refs: []clients.RecordRef{rec}, truncated: []int32{0}, lags: committed(40),
			want: [5]string{"seen", "seen", "seen", "stopped_here", "not_seen"},
		},
		{
			name:      "truncated scan, no match: not there is unknown",
			truncated: []int32{0, 2}, lags: committed(43), stuck: true,
			want: [5]string{"seen", "unknown", "unknown", "unknown", "unknown"},
			wantFacts: map[int]map[string]string{
				1: {"error": "key scan truncated on partition(s) 0,2", "state": "FAILED"},
				2: {"error": "key scan truncated on partition(s) 0,2"},
				3: {"error": "key scan truncated on partition(s) 0,2"},
			},
		},
		{
			name: "no KeyScanReader, committed past the match",
			refs: []clients.RecordRef{rec}, hide: true, lags: committed(43),
			want: [5]string{"seen", "seen", "seen", "unknown", "unknown"},
			wantFacts: map[int]map[string]string{
				2: {"newest": "unknown: key scan completeness unknown"},
			},
		},
		{
			name: "no KeyScanReader, no match, connector failed",
			hide: true, lags: committed(43), stuck: true,
			want:      [5]string{"seen", "unknown", "unknown", "unknown", "unknown"},
			wantFacts: map[int]map[string]string{2: {"error": "key scan completeness unknown"}},
		},
		{
			name:      "record's partition in GroupLag.Unknown, committed not read",
			refs:      []clients.RecordRef{rec},
			lags:      map[string]clients.GroupLag{"order-sync": {State: "Stable", Unknown: []clients.PartitionLag{{Topic: "pg.public.orders", Partition: 0, Committed: -1, End: -1, Lag: -1}}}},
			want:      [5]string{"seen", "seen", "seen", "unknown", "unknown"},
			wantFacts: map[int]map[string]string{3: {"error": "partition offsets unreadable"}},
		},
		{
			name:      "record's partition in GroupLag.Unknown, committed read",
			refs:      []clients.RecordRef{rec},
			lags:      map[string]clients.GroupLag{"order-sync": {State: "Stable", Unknown: []clients.PartitionLag{{Topic: "pg.public.orders", Partition: 0, Committed: 50, End: -1, Lag: -1}}}},
			want:      [5]string{"seen", "seen", "seen", "seen", "seen"},
			wantFacts: map[int]map[string]string{3: {"committed": "50"}},
		},
		{
			// The last ref is only the highest partition's; the newest match
			// is p0's (later timestamp), which is not consumed.
			name: "matches in two partitions: newest by timestamp",
			refs: []clients.RecordRef{
				{Key: "7", Partition: 0, Offset: 5, Timestamp: now},
				{Key: "7", Partition: 1, Offset: 900, Timestamp: now.Add(-time.Hour)},
			},
			lags: map[string]clients.GroupLag{"order-sync": {State: "Stable", Partitions: []clients.PartitionLag{
				{Topic: "pg.public.orders", Partition: 0, Committed: 3},
				{Topic: "pg.public.orders", Partition: 1, Committed: 901},
			}}},
			want:      [5]string{"seen", "seen", "seen", "stopped_here", "not_seen"},
			wantFacts: map[int]map[string]string{2: {"partition": "0", "offset": "5"}},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			state := "RUNNING"
			if tc.stuck {
				state = "FAILED"
			}
			fk := &fakeKafka{
				findByKey: map[string][]clients.RecordRef{"pg.public.orders": tc.refs},
				truncated: map[string][]int32{"pg.public.orders": tc.truncated},
				lags:      tc.lags,
			}
			var kafka clients.KafkaAdmin = fk
			if tc.hide {
				kafka = noScanReport{fk}
			}
			b := NewBuilder(
				testOwners(),
				&fakeConnect{
					config: map[string]map[string]string{"pg-orders-connector": {"topic.prefix": "pg", "table.include.list": "public.orders"}},
					status: map[string]clients.ConnectorStatus{"pg-orders-connector": {State: state}},
				},
				kafka,
				&fakeSource{rows: map[string]map[string]any{"public.orders|7": {"id": "7", "updated_at": now.Format(time.RFC3339Nano)}}},
				&fakeWarehouse{rows: map[string]map[string]any{"analytics.orders|7": {"id": "7"}}},
			)
			stages, err := b.Trace(context.Background(), "orders", "public.orders", "7")
			if err != nil {
				t.Fatalf("Trace: %v", err)
			}
			var got [5]string
			for i, s := range stages {
				got[i] = s.Status
			}
			if got != tc.want {
				t.Fatalf("statuses = %v, want %v\nstages %+v", got, tc.want, stages)
			}
			for i, facts := range tc.wantFacts {
				for k, v := range facts {
					if stages[i].Facts[k] != v {
						t.Errorf("stage %d (%s) fact %s = %q, want %q (facts %v)", i, stages[i].Layer, k, stages[i].Facts[k], v, stages[i].Facts)
					}
				}
			}
			for _, s := range stages {
				if s.Status == "unknown" && s.Facts["error"] == "" {
					t.Errorf("stage %s is unknown without a reason", s.Layer)
				}
				if s.Status == "unknown" && strings.Contains(s.Facts["error"], "unreachable") {
					t.Errorf("stage %s reads unreachable, want the scan or partition reason", s.Layer)
				}
			}
		})
	}
}

// With several groups, a group whose partitions of the topic all sit in
// GroupLag.Unknown still reads the topic: the stage names it and says why it
// is unknown, not "no group reads the topic" (#306).
func TestTraceGroupForCountsUnknownPartitions(t *testing.T) {
	o := twoTableOwners([]string{"order-sync", "customer-sync"}, []string{"analytics.orders", "analytics.customers"})
	b := twoTableBuilder(o, map[string]clients.GroupLag{
		"order-sync":    {State: "Stable", Partitions: []clients.PartitionLag{{Topic: "pg.public.orders", Partition: 0, Committed: 1}}},
		"customer-sync": {State: "Stable", Unknown: []clients.PartitionLag{{Topic: "pg.public.customers", Partition: 0, Committed: -1, End: -1, Lag: -1}}},
	})
	stages, err := b.Trace(context.Background(), "orders", "public.customers", "7")
	if err != nil {
		t.Fatal(err)
	}
	if g := stages[3]; g.Name != "customer-sync" || g.Status != "unknown" || g.Facts["error"] != "partition offsets unreadable" {
		t.Fatalf("consumer stage = %+v, want customer-sync unknown (partition offsets unreadable)", g)
	}
}

// newestRef takes the last match of each partition (offset order), then the
// newest of those by timestamp; a later ref of one partition must not stand
// in for another partition's newer match (#479 review).
func TestNewestRef(t *testing.T) {
	ts := func(s int) time.Time { return time.Unix(int64(s), 0) }
	ref := func(p int32, o int64, s int) clients.RecordRef {
		return clients.RecordRef{Key: "7", Partition: p, Offset: o, Timestamp: ts(s)}
	}
	cases := []struct {
		name     string
		refs     []clients.RecordRef
		want     clients.RecordRef
		wantTied []int32
	}{
		{"one partition: last by offset, not by timestamp", []clients.RecordRef{ref(0, 10, 5), ref(0, 11, 3)}, ref(0, 11, 3), nil},
		{"review example", []clients.RecordRef{ref(0, 4, 10), ref(1, 0, 15), ref(1, 3, 8)}, ref(0, 4, 10), nil},
		{"non-monotonic partition then another", []clients.RecordRef{ref(0, 10, 5), ref(0, 11, 3), ref(1, 7, 4)}, ref(1, 7, 4), nil},
		{"tie across partitions: highest partition, both reported", []clients.RecordRef{ref(0, 4, 10), ref(2, 9, 10), ref(1, 1, 2)}, ref(2, 9, 10), []int32{0, 2}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, tied := newestRef(tc.refs)
			if got != tc.want {
				t.Fatalf("newestRef = %+v, want %+v", got, tc.want)
			}
			if !slices.Equal(tied, tc.wantTied) {
				t.Fatalf("tied = %v, want %v", tied, tc.wantTied)
			}
		})
	}
}

// On a timestamp tie across partitions the newest match is not known: a
// commit past the chosen match does not prove the other one consumed.
func TestTraceNewestTieIsUnknown(t *testing.T) {
	now := time.Now()
	b := NewBuilder(
		testOwners(),
		&fakeConnect{
			config: map[string]map[string]string{"pg-orders-connector": {"topic.prefix": "pg", "table.include.list": "public.orders"}},
			status: map[string]clients.ConnectorStatus{"pg-orders-connector": {State: "RUNNING", Tasks: []clients.TaskStatus{{State: "RUNNING"}}}},
		},
		&fakeKafka{
			findByKey: map[string][]clients.RecordRef{"pg.public.orders": {
				{Key: "7", Partition: 0, Offset: 5, Timestamp: now},
				{Key: "7", Partition: 1, Offset: 9, Timestamp: now},
			}},
			lags: map[string]clients.GroupLag{"order-sync": {State: "Stable", Partitions: []clients.PartitionLag{
				{Topic: "pg.public.orders", Partition: 0, Committed: 3},
				{Topic: "pg.public.orders", Partition: 1, Committed: 10},
			}}},
		},
		&fakeSource{rows: map[string]map[string]any{"public.orders|7": {"id": "7", "updated_at": now.Format(time.RFC3339Nano)}}},
		&fakeWarehouse{rows: map[string]map[string]any{"analytics.orders|7": {"id": "7"}}},
	)
	stages, err := b.Trace(context.Background(), "orders", "public.orders", "7")
	if err != nil {
		t.Fatal(err)
	}
	if g := stages[3]; g.Status != "unknown" || !strings.Contains(g.Facts["error"], "partitions 0,1") {
		t.Fatalf("consumer stage = %+v, want unknown (tie on partitions 0,1)", g)
	}
	if s := stages[2]; s.Facts["newest"] == "" {
		t.Fatalf("topic stage = %+v, want a newest: unknown fact", s)
	}
}

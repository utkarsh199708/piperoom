package topology

import (
	"testing"

	"piperoom/internal/clients"
)

// Partitions in GroupLag.Unknown are not in TotalLag: a group with unreadable
// partitions on the pipeline's topics is never "ok, lag 0" on the map (#508,
// #495).
func TestGroupLagUnknownPartitions(t *testing.T) {
	own := map[string]bool{"pg.public.orders": true}
	unreadable := func(p int32) clients.PartitionLag {
		return clients.PartitionLag{Topic: "pg.public.orders", Partition: p, Committed: -1, End: -1, Lag: -1}
	}
	cases := []struct {
		name       string
		lag        clients.GroupLag
		wantOK     bool
		wantHealth Health
		wantLag    string
		wantUnread string
	}{
		{"only unknown", clients.GroupLag{State: "Stable", Unknown: []clients.PartitionLag{unreadable(0)}},
			true, HealthUnknown, "unknown (1 partition(s) unreadable)", "1"},
		{"mixed", clients.GroupLag{State: "Stable", Partitions: []clients.PartitionLag{{Topic: "pg.public.orders", Partition: 0, Lag: 0}}, Unknown: []clients.PartitionLag{unreadable(1)}},
			true, HealthUnknown, "at least 0 (1 partition(s) unreadable)", "1"},
		{"mixed over threshold", clients.GroupLag{State: "Stable", TotalLag: 5000, Partitions: []clients.PartitionLag{{Topic: "pg.public.orders", Partition: 0, Lag: 5000}}, Unknown: []clients.PartitionLag{unreadable(1), unreadable(2)}},
			true, HealthWarn, "at least 5000 (2 partition(s) unreadable)", "2"},
		{"foreign unknown only", clients.GroupLag{State: "Stable", Partitions: []clients.PartitionLag{{Topic: "pg.public.orders", Partition: 0, Lag: 3}}, Unknown: []clients.PartitionLag{{Topic: "other", Partition: 0, Lag: -1}}},
			true, HealthOK, "3", ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			lag, ok := pipelineLag(tc.lag, own)
			if ok != tc.wantOK {
				t.Fatalf("pipelineLag ok = %v, want %v", ok, tc.wantOK)
			}
			h, facts := groupHealth(lag, DefaultThresholds())
			if h != tc.wantHealth || facts["lag"] != tc.wantLag || facts["unknown_partitions"] != tc.wantUnread {
				t.Fatalf("health %s facts %v; want %s lag %q unknown_partitions %q", h, facts, tc.wantHealth, tc.wantLag, tc.wantUnread)
			}
		})
	}
}

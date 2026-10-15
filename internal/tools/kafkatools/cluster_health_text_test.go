package kafkatools

import (
	"strings"
	"testing"

	"piperoom/internal/clients"
)

func TestClusterSummaryDiskClaims(t *testing.T) {
	full := clients.BrokerHealth{NodeID: 1, Host: "k1", LogDirTotalBytes: 100e9, LogDirUsableBytes: 5e9}
	ok := clients.BrokerHealth{NodeID: 2, Host: "k2", LogDirTotalBytes: 100e9, LogDirUsableBytes: 50e9}
	none := clients.BrokerHealth{NodeID: 3, Host: "k3", LogDirTotalBytes: -1, LogDirUsableBytes: -1}
	tests := []struct {
		name    string
		brokers []clients.BrokerHealth
		want    string
		notWant string
	}{
		{"over threshold", []clients.BrokerHealth{full, ok}, "log dir 95% used", "below 85%"},
		{"all fine", []clients.BrokerHealth{ok}, "log dirs below 85% used", "unknown"},
		{"no data", []clients.BrokerHealth{none}, "disk usage unknown", "below 85%"},
		{"over and unmeasured", []clients.BrokerHealth{full, none}, "unknown for 1 of 2", "below 85%"},
		{"no brokers", nil, "(0 brokers", "below 85%"},
		{"partial data", []clients.BrokerHealth{ok, none}, "disk usage unknown", "below 85%"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			s := clusterSummary(clients.ClusterHealth{ClusterID: "c", Brokers: tc.brokers}, nil)
			if !strings.Contains(s, tc.want) || strings.Contains(s, tc.notWant) {
				t.Errorf("summary %q: want %q, not %q", s, tc.want, tc.notWant)
			}
		})
	}
}

func TestClusterSummaryNotEnoughReplicasNamesTopics(t *testing.T) {
	h := clients.ClusterHealth{Partitions: []clients.PartitionHealth{
		{Topic: "a", Partition: 0, Replicas: []int32{1, 2}, ISR: []int32{1}, MinISR: 2, Leader: 1},
		{Topic: "b", Partition: 0, Replicas: []int32{1, 2}, ISR: []int32{1}, MinISR: 2, Leader: 1},
	}}
	s := clusterSummary(h, []string{"a", "b"})
	if !strings.Contains(s, "a/p0, b/p0") {
		t.Errorf("summary %q lacks topic-qualified partitions", s)
	}
}

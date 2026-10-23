package kafka

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
)

// fakeConnectAdmin is a kadm seam answering the four calls ConnectWorkerGroups,
// InternalTopics and BrokerCount make.
type fakeConnectAdmin struct {
	listed      kadm.ListedGroups
	listedErr   error
	described   kadm.DescribedGroups
	describeErr error
	md          kadm.Metadata
	mdErr       error
	cfgs        kadm.ResourceConfigs
	cfgErr      error
	brokerMD    kadm.Metadata
	brokerErr   error

	describedIDs []string
	mdTopics     []string
}

func (f *fakeConnectAdmin) ListGroups(context.Context, ...string) (kadm.ListedGroups, error) {
	return f.listed, f.listedErr
}

func (f *fakeConnectAdmin) DescribeGroups(_ context.Context, groups ...string) (kadm.DescribedGroups, error) {
	f.describedIDs = append([]string(nil), groups...)
	return f.described, f.describeErr
}

func (f *fakeConnectAdmin) Metadata(_ context.Context, topics ...string) (kadm.Metadata, error) {
	f.mdTopics = append([]string(nil), topics...)
	return f.md, f.mdErr
}

func (f *fakeConnectAdmin) DescribeTopicConfigs(context.Context, ...string) (kadm.ResourceConfigs, error) {
	return f.cfgs, f.cfgErr
}

func (f *fakeConnectAdmin) BrokerMetadata(context.Context) (kadm.Metadata, error) {
	return f.brokerMD, f.brokerErr
}

func listedGroup(id, protocolType, state string) kadm.ListedGroup {
	return kadm.ListedGroup{Group: id, ProtocolType: protocolType, State: state}
}

func describedGroup(id, state string, members ...kadm.DescribedGroupMember) kadm.DescribedGroup {
	return kadm.DescribedGroup{Group: id, State: state, ProtocolType: connectProtocolType, Members: members}
}

func cfgEntry(key, value string) kadm.Config {
	v := value
	return kadm.Config{Key: key, Value: &v}
}

func topicMeta(name string, partitions, replicas int) kadm.TopicDetail {
	td := kadm.TopicDetail{Topic: name, Partitions: kadm.PartitionDetails{}}
	for p := 0; p < partitions; p++ {
		rs := make([]int32, replicas)
		for r := range rs {
			rs[r] = int32(r)
		}
		td.Partitions[int32(p)] = kadm.PartitionDetail{
			Topic: name, Partition: int32(p), Leader: 0, Replicas: rs, ISR: rs,
		}
	}
	return td
}

func TestConnectWorkerGroupsFiltersByProtocolType(t *testing.T) {
	f := &fakeConnectAdmin{
		listed: kadm.ListedGroups{
			"g-consumer":      listedGroup("g-consumer", "consumer", "Stable"),
			"connect-cluster": listedGroup("connect-cluster", "connect", "Stable"),
			"connect-billing": listedGroup("connect-billing", "connect", "Empty"),
			"g-no-protocol":   listedGroup("g-no-protocol", "", "Dead"),
			"connect-upper":   listedGroup("connect-upper", "CONNECT", "Stable"),
		},
		described: kadm.DescribedGroups{
			"connect-cluster": describedGroup("connect-cluster", "Stable",
				kadm.DescribedGroupMember{MemberID: "worker-b", ClientID: "worker-b", ClientHost: "10.0.0.2"},
				kadm.DescribedGroupMember{MemberID: "worker-a", ClientID: "worker-a", ClientHost: "10.0.0.1"},
			),
			"connect-billing": describedGroup("connect-billing", "Empty"),
			"connect-upper":   describedGroup("connect-upper", "Stable", kadm.DescribedGroupMember{MemberID: "w", ClientHost: "10.0.0.3"}),
		},
	}
	groups, err := (&Client{connectFn: f}).ConnectWorkerGroups(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if got, want := f.describedIDs, []string{"connect-billing", "connect-cluster", "connect-upper"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("described groups %v, want the connect groups only, sorted: %v", got, want)
	}
	if len(groups) != 3 {
		t.Fatalf("%d groups, want 3 (consumer and blank protocol type dropped)", len(groups))
	}
	byID := map[string]ConnectWorkerGroup{}
	for _, g := range groups {
		byID[g.GroupID] = g
	}
	cluster := byID["connect-cluster"]
	if cluster.State != "Stable" || len(cluster.Workers) != 2 {
		t.Fatalf("connect-cluster = %+v", cluster)
	}
	// Members are sorted by member id, not left in the broker's order.
	if cluster.Workers[0].MemberID != "worker-a" || cluster.Workers[0].Host != "10.0.0.1" {
		t.Fatalf("workers not sorted by member id: %+v", cluster.Workers)
	}
	// A member whose join metadata is not Connect metadata (the fake cannot
	// build kadm's unexported metadata type) keeps the host only.
	if cluster.Workers[0].URL != "" || cluster.Workers[1].URL != "" {
		t.Fatalf("invented a URL without connect metadata: %+v", cluster.Workers)
	}
	if len(byID["connect-billing"].Workers) != 0 {
		t.Fatalf("empty group carries workers: %+v", byID["connect-billing"])
	}
}

func TestConnectWorkerGroupsNoConnectGroups(t *testing.T) {
	f := &fakeConnectAdmin{listed: kadm.ListedGroups{"g": listedGroup("g", "consumer", "Stable")}}
	groups, err := (&Client{connectFn: f}).ConnectWorkerGroups(context.Background())
	if err != nil || groups != nil {
		t.Fatalf("groups=%v err=%v, want nil/nil", groups, err)
	}
	if f.describedIDs != nil {
		t.Fatalf("DescribeGroups called with no connect group: %v", f.describedIDs)
	}
}

func TestConnectWorkerGroupsErrors(t *testing.T) {
	boom := errors.New("broker down")
	cases := []struct {
		name string
		f    *fakeConnectAdmin
		want error
	}{
		{"list", &fakeConnectAdmin{listedErr: boom}, boom},
		{"describe", &fakeConnectAdmin{
			listed:      kadm.ListedGroups{"c": listedGroup("c", "connect", "Stable")},
			describeErr: boom,
		}, boom},
		{"group error", &fakeConnectAdmin{
			listed:    kadm.ListedGroups{"c": listedGroup("c", "connect", "Stable")},
			described: kadm.DescribedGroups{"c": {Group: "c", Err: kerr.GroupAuthorizationFailed}},
		}, kerr.GroupAuthorizationFailed},
		{"group missing", &fakeConnectAdmin{
			listed:    kadm.ListedGroups{"c": listedGroup("c", "connect", "Stable")},
			described: kadm.DescribedGroups{},
		}, nil},
		{"deadline", &fakeConnectAdmin{listedErr: context.DeadlineExceeded}, context.DeadlineExceeded},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := (&Client{connectFn: tc.f}).ConnectWorkerGroups(context.Background())
			if err == nil {
				t.Fatal("expected an error")
			}
			if tc.want != nil && !errors.Is(err, tc.want) {
				t.Fatalf("err %v, want %v", err, tc.want)
			}
		})
	}
}

func TestInternalTopicsReadsShapeAndConfigs(t *testing.T) {
	f := &fakeConnectAdmin{
		md: kadm.Metadata{Topics: kadm.TopicDetails{
			"connect-cluster-configs": topicMeta("connect-cluster-configs", 1, 3),
			"connect-cluster-offsets": topicMeta("connect-cluster-offsets", 25, 2),
			"custom-status":           topicMeta("custom-status", 5, 1),
		}},
		cfgs: kadm.ResourceConfigs{
			{Name: "connect-cluster-configs", Configs: []kadm.Config{cfgEntry("cleanup.policy", "compact"), cfgEntry("min.insync.replicas", "2")}},
			{Name: "connect-cluster-offsets", Configs: []kadm.Config{cfgEntry("cleanup.policy", "delete")}},
			// A topic missing from the metadata read is not invented, and a
			// metadata topic missing here keeps an empty policy.
		},
	}
	got, err := (&Client{connectFn: f}).InternalTopics(context.Background(), []string{
		"connect-cluster-configs", "connect-cluster-offsets", "custom-status", "gone-topic", "connect-cluster-configs",
	})
	if err != nil {
		t.Fatal(err)
	}
	if want := []string{"connect-cluster-configs", "connect-cluster-offsets", "custom-status", "gone-topic", "connect-cluster-configs"}; !reflect.DeepEqual(f.mdTopics, want) {
		t.Fatalf("metadata topics %v, want %v", f.mdTopics, want)
	}
	want := []InternalTopic{
		{Name: "connect-cluster-configs", Role: "config", Partitions: 1, ReplicationFactor: 3, CleanupPolicy: "compact", MinInSyncReplicas: 2},
		{Name: "connect-cluster-offsets", Role: "offset", Partitions: 25, ReplicationFactor: 2, CleanupPolicy: "delete"},
		// The role comes from the suffix, so a renamed status topic is "".
		{Name: "custom-status", Role: "status", Partitions: 5, ReplicationFactor: 1},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got  %+v\nwant %+v", got, want)
	}
}

func TestInternalTopicsEmptyNames(t *testing.T) {
	f := &fakeConnectAdmin{mdErr: errors.New("must not be called")}
	got, err := (&Client{connectFn: f}).InternalTopics(context.Background(), nil)
	if err != nil || got != nil {
		t.Fatalf("got=%v err=%v, want nil/nil", got, err)
	}
}

func TestInternalTopicsErrors(t *testing.T) {
	boom := errors.New("broker down")
	cases := []struct {
		name string
		f    *fakeConnectAdmin
		want error
	}{
		{"metadata", &fakeConnectAdmin{mdErr: boom}, boom},
		{"configs", &fakeConnectAdmin{md: kadm.Metadata{}, cfgErr: boom}, boom},
		{"per-topic config", &fakeConnectAdmin{
			md: kadm.Metadata{Topics: kadm.TopicDetails{"t": topicMeta("t", 1, 1)}},
			cfgs: kadm.ResourceConfigs{
				{Name: "t", Err: kerr.TopicAuthorizationFailed},
			},
		}, kerr.TopicAuthorizationFailed},
		{"deadline", &fakeConnectAdmin{mdErr: context.DeadlineExceeded}, context.DeadlineExceeded},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := (&Client{connectFn: tc.f}).InternalTopics(context.Background(), []string{"t"})
			if !errors.Is(err, tc.want) {
				t.Fatalf("err %v, want %v", err, tc.want)
			}
		})
	}
}

func TestBrokerCount(t *testing.T) {
	f := &fakeConnectAdmin{brokerMD: kadm.Metadata{Brokers: []kadm.BrokerDetail{{NodeID: 0}, {NodeID: 1}, {NodeID: 2}}}}
	n, err := (&Client{connectFn: f}).BrokerCount(context.Background())
	if err != nil || n != 3 {
		t.Fatalf("n=%d err=%v, want 3", n, err)
	}
	boom := errors.New("broker down")
	if _, err := (&Client{connectFn: &fakeConnectAdmin{brokerErr: boom}}).BrokerCount(context.Background()); !errors.Is(err, boom) {
		t.Fatalf("err %v, want %v", err, boom)
	}
}

func TestInternalTopicRole(t *testing.T) {
	cases := map[string]string{
		"connect-cluster-configs": "config",
		"connect-cluster-offsets": "offset",
		"connect-cluster-status":  "status",
		"custom":                  "",
		"my-configs-backup":       "",
	}
	for name, want := range cases {
		if got := internalTopicRole(name); got != want {
			t.Errorf("internalTopicRole(%q) = %q, want %q", name, got, want)
		}
	}
}

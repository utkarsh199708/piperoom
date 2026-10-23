package kafka

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// joinConnectWorker joins group as a Kafka Connect worker would: protocol type
// "connect" with ConnectMemberMetadata carrying url, then (as the only member,
// so the leader) a SyncGroup assigning connectors. It returns the member id.
func joinConnectWorker(t *testing.T, cl *kgo.Client, group, url string, connectors ...string) string {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	md := kmsg.NewConnectMemberMetadata()
	md.URL = url
	md.ConfigOffset = 3
	join := kmsg.NewPtrJoinGroupRequest()
	join.Group = group
	join.SessionTimeoutMillis = 30000
	join.RebalanceTimeoutMillis = 30000
	join.ProtocolType = "connect"
	p := kmsg.NewJoinGroupRequestProtocol()
	p.Name = "default"
	p.Metadata = md.AppendTo(nil)
	join.Protocols = append(join.Protocols, p)
	var resp *kmsg.JoinGroupResponse
	for range 3 {
		var err error
		resp, err = join.RequestWith(ctx, cl)
		if err != nil {
			t.Fatalf("join: %v", err)
		}
		if resp.ErrorCode == kerr.MemberIDRequired.Code {
			join.MemberID = resp.MemberID
			continue
		}
		if err := kerr.ErrorForCode(resp.ErrorCode); err != nil {
			t.Fatalf("join: %v", err)
		}
		break
	}
	as := kmsg.NewConnectMemberAssignment()
	as.Leader = resp.LeaderID
	as.LeaderURL = url
	as.ConfigOffset = 3
	for _, c := range connectors {
		a := kmsg.NewConnectMemberAssignmentAssignment()
		a.Connector = c
		a.Tasks = []int16{0}
		as.Assignment = append(as.Assignment, a)
	}
	sync := kmsg.NewPtrSyncGroupRequest()
	sync.Group = group
	sync.Generation = resp.Generation
	sync.MemberID = resp.MemberID
	ga := kmsg.NewSyncGroupRequestGroupAssignment()
	ga.MemberID = resp.MemberID
	ga.MemberAssignment = as.AppendTo(nil)
	sync.GroupAssignment = append(sync.GroupAssignment, ga)
	sresp, err := sync.RequestWith(ctx, cl)
	if err != nil {
		t.Fatalf("sync: %v", err)
	}
	if err := kerr.ErrorForCode(sresp.ErrorCode); err != nil {
		t.Fatalf("sync: %v", err)
	}
	return resp.MemberID
}

func kfakeClient(t *testing.T, brokers int) (*Client, *kgo.Client) {
	t.Helper()
	c, err := kfake.NewCluster(kfake.NumBrokers(brokers))
	if err != nil {
		t.Fatalf("kfake: %v", err)
	}
	t.Cleanup(c.Close)
	cl, err := New(c.ListenAddrs(), nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(cl.Close)
	raw, err := kgo.NewClient(kgo.SeedBrokers(c.ListenAddrs()...))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(raw.Close)
	return cl, raw
}

func TestKFakeConnectWorkerGroups(t *testing.T) {
	cl, raw := kfakeClient(t, 1)
	member := joinConnectWorker(t, raw, "connect-cluster-a", "http://10.0.0.5:8083/", "pg-orders", "other-sink")

	// A plain consumer group must not be listed.
	cons, err := kgo.NewClient(kgo.SeedBrokers(cl.brokers...), kgo.ConsumerGroup("app-consumer"), kgo.ConsumeTopics("t"))
	if err != nil {
		t.Fatal(err)
	}
	defer cons.Close()

	groups, err := cl.ConnectWorkerGroups(context.Background())
	if err != nil {
		t.Fatalf("ConnectWorkerGroups: %v", err)
	}
	if len(groups) != 1 {
		t.Fatalf("groups = %+v, want the one connect group", groups)
	}
	g := groups[0]
	if g.GroupID != "connect-cluster-a" || g.Error != "" || len(g.Workers) != 1 {
		t.Fatalf("group = %+v", g)
	}
	w := g.Workers[0]
	if w.MemberID != member || w.URL != "http://10.0.0.5:8083/" || !w.Leader || w.Host == "" || strings.HasPrefix(w.Host, "/") {
		t.Errorf("worker = %+v", w)
	}
	if strings.Join(w.Connectors, ",") != "other-sink,pg-orders" {
		t.Errorf("connectors = %v", w.Connectors)
	}
}

func TestKFakeInternalTopics(t *testing.T) {
	cl, raw := kfakeClient(t, 3)
	adm := kadm.NewClient(raw)
	ctx := context.Background()
	compact, del := "compact", "delete"
	for _, tc := range []struct {
		name  string
		parts int32
		rf    int16
		cfg   map[string]*string
	}{
		{"connect-configs", 3, 1, map[string]*string{"cleanup.policy": &compact}},
		{"connect-offsets", 25, 3, map[string]*string{"cleanup.policy": &compact}},
		{"connect-status", 5, 3, map[string]*string{"cleanup.policy": &del}},
		{"orders.public.status", 1, 1, nil}, // a data topic named like a role
	} {
		if _, err := adm.CreateTopic(ctx, tc.parts, tc.rf, tc.cfg, tc.name); err != nil {
			t.Fatalf("create %s: %v", tc.name, err)
		}
	}

	got, err := cl.InternalTopics(ctx, nil)
	if err != nil {
		t.Fatalf("InternalTopics: %v", err)
	}
	if len(got) != 3 {
		t.Fatalf("discovered = %+v, want the three connect topics", got)
	}
	byName := map[string]InternalTopic{}
	for _, it := range got {
		byName[it.Name] = it
	}
	cfg := byName["connect-configs"]
	if cfg.Role != InternalRoleConfig || cfg.Partitions != 3 || cfg.ReplicationFactor != 1 || cfg.CleanupPolicy != "compact" || cfg.Brokers != 3 {
		t.Errorf("configs = %+v", cfg)
	}
	if st := byName["connect-status"]; st.Role != InternalRoleStatus || st.CleanupPolicy != "delete" || st.ReplicationFactor != 3 {
		t.Errorf("status = %+v", st)
	}

	named, err := cl.InternalTopics(ctx, []string{"connect-offsets", "nope-configs"})
	if err != nil {
		t.Fatalf("InternalTopics named: %v", err)
	}
	if len(named) != 2 || named[0].Partitions != 25 || !named[1].Missing || named[1].Partitions != 0 {
		t.Errorf("named = %+v", named)
	}
}

func TestInternalTopicRole(t *testing.T) {
	for _, tc := range []struct{ name, want string }{
		{"connect-configs", InternalRoleConfig},
		{"my_connect_configs", InternalRoleConfig},
		{"my_connect_offsets", InternalRoleOffset},
		{"my_connect_statuses", InternalRoleStatus},
		{"_connect_status", InternalRoleStatus},
		{"_connect_configs_staging", InternalRoleConfig},
		{"strimzi-cluster-offsets", InternalRoleOffset},
		{"configs", ""},                // one word: not a Connect name
		{"orders.public.status", ""},   // data topic
		{"app-events", ""},             // no role word
		{"connect-cluster-events", ""}, // connect but no role word
		{"", ""},
	} {
		if got := InternalTopicRole(tc.name); got != tc.want {
			t.Errorf("InternalTopicRole(%q) = %q, want %q", tc.name, got, tc.want)
		}
	}
}

func TestAttributeInternalTopics(t *testing.T) {
	for _, tc := range []struct {
		name   string
		topics []string
		groups []string
		want   map[string]string
	}{
		{"dev stack: generic names go to the group no suffixed topic claims",
			[]string{"_connect_configs", "_connect_offsets", "_connect_configs_staging", "_connect_status_staging"},
			[]string{"piperoom-connect", "piperoom-connect-staging"},
			map[string]string{"_connect_configs": "piperoom-connect", "_connect_offsets": "piperoom-connect",
				"_connect_configs_staging": "piperoom-connect-staging", "_connect_status_staging": "piperoom-connect-staging"}},
		{"strimzi: <group>-<role>, exact beats a partial match",
			[]string{"orders-connect-configs", "orders-eu-connect-offsets"},
			[]string{"orders-connect", "orders-eu-connect"},
			map[string]string{"orders-connect-configs": "orders-connect", "orders-eu-connect-offsets": "orders-eu-connect"}},
		{"one group: generic names are its own",
			[]string{"my_connect_configs", "connect-status"}, []string{"1"},
			map[string]string{"my_connect_configs": "1", "connect-status": "1"}},
		{"two unclaimed groups: generic names are anyone's",
			[]string{"connect-configs"}, []string{"connect-a", "connect-b"}, map[string]string{}},
		{"a stopped cluster's topics belong to no group",
			[]string{"legacy-billing-configs"}, []string{"connect-prod"}, map[string]string{}},
		{"two groups matching equally: ambiguous",
			[]string{"eu-configs"}, []string{"eu-west", "eu-north"}, map[string]string{}},
		{"no groups (unreadable): nothing tied",
			[]string{"connect-configs", "prod-configs"}, nil, map[string]string{}},
		{"case and separators do not matter",
			[]string{"Connect_Prod-offsets"}, []string{"connect-prod"}, map[string]string{"Connect_Prod-offsets": "connect-prod"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := AttributeInternalTopics(tc.topics, tc.groups)
			if len(got) != len(tc.want) {
				t.Fatalf("got %v, want %v", got, tc.want)
			}
			for k, v := range tc.want {
				if got[k] != v {
					t.Errorf("%s -> %q, want %q (all: %v)", k, got[k], v, got)
				}
			}
		})
	}
}

// fakeConnectAdm is the connectAdmin seam for the error paths.
type fakeConnectAdm struct {
	listed    kadm.ListedGroups
	listErr   error
	described kadm.DescribedGroups
	descErr   error
	md        kadm.Metadata
	mdErr     error
	cfgErr    error
}

func (f *fakeConnectAdm) ListGroups(context.Context, ...string) (kadm.ListedGroups, error) {
	return f.listed, f.listErr
}
func (f *fakeConnectAdm) DescribeGroups(context.Context, ...string) (kadm.DescribedGroups, error) {
	return f.described, f.descErr
}
func (f *fakeConnectAdm) Metadata(context.Context, ...string) (kadm.Metadata, error) {
	return f.md, f.mdErr
}
func (f *fakeConnectAdm) DescribeTopicConfigs(context.Context, ...string) (kadm.ResourceConfigs, error) {
	return nil, f.cfgErr
}

func TestConnectWorkerGroupsErrors(t *testing.T) {
	down := errors.New("broker down")
	c := &Client{connectFn: &fakeConnectAdm{listErr: down}}
	if _, err := c.ConnectWorkerGroups(context.Background()); !errors.Is(err, down) {
		t.Errorf("list error = %v", err)
	}

	listed := kadm.ListedGroups{
		"cg":  {Group: "cg", ProtocolType: "connect", State: "Stable"},
		"app": {Group: "app", ProtocolType: "consumer"},
	}
	c = &Client{connectFn: &fakeConnectAdm{listed: listed, descErr: down}}
	if _, err := c.ConnectWorkerGroups(context.Background()); !errors.Is(err, down) {
		t.Errorf("describe error = %v", err)
	}

	// One group the broker refuses to describe stays in the list with Error.
	c = &Client{connectFn: &fakeConnectAdm{listed: listed, described: kadm.DescribedGroups{
		"cg": {Group: "cg", Err: kerr.GroupAuthorizationFailed},
	}}}
	groups, err := c.ConnectWorkerGroups(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(groups) != 1 || groups[0].Error != "GROUP_AUTHORIZATION_FAILED" || len(groups[0].Workers) != 0 {
		t.Errorf("groups = %+v", groups)
	}

	// No connect group at all: an empty list, not an error.
	c = &Client{connectFn: &fakeConnectAdm{listed: kadm.ListedGroups{"app": {Group: "app", ProtocolType: "consumer"}}}}
	groups, err = c.ConnectWorkerGroups(context.Background())
	if err != nil || len(groups) != 0 || groups == nil {
		t.Errorf("groups = %#v, err = %v", groups, err)
	}
}

func TestInternalTopicsErrors(t *testing.T) {
	down := errors.New("broker down")
	c := &Client{connectFn: &fakeConnectAdm{mdErr: down}}
	if _, err := c.InternalTopics(context.Background(), nil); !errors.Is(err, down) {
		t.Errorf("metadata error = %v", err)
	}
	md := kadm.Metadata{Topics: kadm.TopicDetails{
		"connect-configs": {Topic: "connect-configs", Partitions: kadm.PartitionDetails{0: {Replicas: []int32{1}}}},
	}}
	c = &Client{connectFn: &fakeConnectAdm{md: md, cfgErr: down}}
	if _, err := c.InternalTopics(context.Background(), nil); !errors.Is(err, down) {
		t.Errorf("configs error = %v", err)
	}
	md.Topics["connect-offsets"] = kadm.TopicDetail{Topic: "connect-offsets", Err: kerr.TopicAuthorizationFailed}
	c = &Client{connectFn: &fakeConnectAdm{md: md}}
	if _, err := c.InternalTopics(context.Background(), nil); !errors.Is(err, kerr.TopicAuthorizationFailed) {
		t.Errorf("topic error = %v", err)
	}
}

func TestWorkerURLAndText(t *testing.T) {
	for _, tc := range []struct{ in, want string }{
		{"http://connect:8083/", "http://connect:8083/"},
		{"https://10.0.0.1:8443", "https://10.0.0.1:8443"},
		{"ftp://connect:21/", ""},
		{"http://user:pw@connect:8083/", ""},
		{"http:///nohost", ""},
		{"http://connect:8083/\nforged", ""},
		{"", ""},
	} {
		if got := workerURL(tc.in); got != tc.want {
			t.Errorf("workerURL(%q) = %q, want %q", tc.in, got, tc.want)
		}
	}
	if got := workerText("a\nb\x1b[31m"); strings.ContainsAny(got, "\n\x1b") {
		t.Errorf("workerText kept a control rune: %q", got)
	}
	if got := workerText(strings.Repeat("é", 300)); len(got) > maxWorkerTextBytes || !strings.HasSuffix(got, "é") {
		t.Errorf("workerText cut badly: %d bytes", len(got))
	}
}

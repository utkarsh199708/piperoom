package managed

import (
	"context"
	"encoding/json"
	"errors"
	"net/netip"
	"strings"
	"testing"
	"time"

	"piperoom/internal/clients/mskconnect"
	"piperoom/internal/events"
	"piperoom/internal/setup/ops"
	"piperoom/internal/setup/plan"
	"piperoom/internal/setup/protocol"
)

// fakeMSK is an MSKAPI that records what the ops send.
type fakeMSK struct {
	account   string
	state     string
	created   []mskconnect.ConnectorSpec
	workers   []mskconnect.WorkerConfigSpec
	deleted   []string
	calls     int
	tags      map[string]string
	bootstrap string
}

func newFake() *fakeMSK {
	return &fakeMSK{account: mAccount, state: "RUNNING", tags: map[string]string{TagRoom: mRoom}, bootstrap: "scrubbed.amazonaws.com:9098"}
}

func (f *fakeMSK) Identity(context.Context) (mskconnect.Identity, error) {
	f.calls++
	return mskconnect.Identity{Account: f.account, Principal: "PiperoomSetup"}, nil
}
func (f *fakeMSK) DescribeCluster(context.Context, string) (mskconnect.Cluster, error) {
	f.calls++
	return mskconnect.Cluster{Name: "demo", State: "ACTIVE", BootstrapIAM: f.bootstrap}, nil
}
func (f *fakeMSK) ListPlugins(context.Context, string) ([]mskconnect.Plugin, error) { return nil, nil }
func (f *fakeMSK) DescribePlugin(context.Context, string) (mskconnect.Plugin, error) {
	return mskconnect.Plugin{State: f.state}, nil
}
func (f *fakeMSK) FindConnector(context.Context, string) (string, error) { return mConnector, nil }
func (f *fakeMSK) DescribeConnector(context.Context, string) (mskconnect.Connector, error) {
	f.calls++
	return mskconnect.Connector{Name: "it_orders", State: f.state, CurrentVersion: "V1"}, nil
}
func (f *fakeMSK) ConnectorTags(context.Context, string) (map[string]string, error) {
	return f.tags, nil
}
func (f *fakeMSK) DescribeNetwork(context.Context, []string, []string) (mskconnect.Network, error) {
	return mskconnect.Network{}, nil
}
func (f *fakeMSK) GetRole(context.Context, string) (mskconnect.Role, error) {
	return mskconnect.Role{Name: "r", TrustsKafkaConnect: false}, nil
}
func (f *fakeMSK) SimulateRole(context.Context, string, []string) ([]mskconnect.Decision, error) {
	return nil, nil
}
func (f *fakeMSK) HeadObject(context.Context, string, string, string) (mskconnect.Object, error) {
	return mskconnect.Object{Size: 10}, nil
}
func (f *fakeMSK) CreatePlugin(context.Context, mskconnect.PluginSpec) (mskconnect.Created, error) {
	return mskconnect.Created{State: "CREATING"}, nil
}
func (f *fakeMSK) CreateWorkerConfig(_ context.Context, s mskconnect.WorkerConfigSpec) (mskconnect.WorkerConfig, error) {
	f.workers = append(f.workers, s)
	return mskconnect.WorkerConfig{Name: s.Name, State: "ACTIVE"}, nil
}
func (f *fakeMSK) CreateConnector(_ context.Context, s mskconnect.ConnectorSpec) (mskconnect.Created, error) {
	f.created = append(f.created, s)
	return mskconnect.Created{Name: s.Name, State: "CREATING"}, nil
}
func (f *fakeMSK) DeleteConnector(_ context.Context, arn, _ string) (mskconnect.Created, error) {
	f.deleted = append(f.deleted, arn)
	return mskconnect.Created{State: "DELETING"}, nil
}

func TestMSKTiersAndClass(t *testing.T) {
	cases := []struct {
		name, person string
		patterns     []string
		class, write string
	}{
		{"shared by default", "", nil, protocol.ClassShared, protocol.TierT2},
		{"a local floor cannot lower a managed target", protocol.ClassLocal, nil, protocol.ClassShared, protocol.TierT2},
		{"the account matches a prod pattern", "", []string{"0000000000*"}, protocol.ClassProduction, protocol.TierT3},
		{"another account's pattern", "", []string{"123456789012"}, protocol.ClassShared, protocol.TierT2},
		{"the person's production floor", protocol.ClassProduction, nil, protocol.ClassProduction, protocol.TierT3},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			reg, d := newMSK(t, newFake(), tc.person, tc.patterns)
			if d.Class() != tc.class {
				t.Errorf("class = %s, want %s", d.Class(), tc.class)
			}
			for _, op := range []string{"msk_create_plugin", "msk_create_worker_config", "msk_create_connector", "msk_delete_connector"} {
				if floor, _ := reg.Floor(op); floor != protocol.TierT2 {
					t.Errorf("%s floor %s", op, floor)
				}
				tier, ok, err := reg.FloorFor(op, nil)
				if !ok || err != nil || tier != tc.write {
					t.Errorf("%s FloorFor = %s %v %v, want %s", op, tier, ok, err, tc.write)
				}
			}
			for _, op := range []string{"msk_describe_cluster", "msk_list_plugins", ReadMSKConnector, "msk_vpc_check", "msk_role_check", "msk_plugin_object_check", "msk_wait"} {
				if tier, _, _ := reg.FloorFor(op, nil); tier != protocol.TierT0 {
					t.Errorf("%s tier %s, want T0", op, tier)
				}
			}
		})
	}
}

func TestMSKRegisterRefuses(t *testing.T) {
	reg, err := ops.New(ops.Config{ProjectDir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer reg.Close()
	for name, o := range map[string]MSKOptions{
		"no client":       {Target: mskTarget, RoomID: mRoom},
		"wrong kind":      {Client: newFake(), Target: events.SetupTarget{Kind: "connect", Address: mskTarget.Address}, RoomID: mRoom},
		"bad address":     {Client: newFake(), Target: events.SetupTarget{Kind: KindAWS, Address: "acme/us-east-1"}, RoomID: mRoom},
		"bad region":      {Client: newFake(), Target: events.SetupTarget{Kind: KindAWS, Address: mAccount + "/mars"}, RoomID: mRoom},
		"foreign cluster": {Client: newFake(), Target: events.SetupTarget{Kind: KindAWS, Address: mAccount + "/us-east-1/arn:aws:kafka:us-east-1:123456789012:cluster/x/y"}, RoomID: mRoom},
		"no room id":      {Client: newFake(), Target: mskTarget},
		"unknown class":   {Client: newFake(), Target: mskTarget, RoomID: mRoom, Class: "prod"},
	} {
		if _, err := RegisterMSK(reg, o); err == nil {
			t.Errorf("%s: registered", name)
		}
	}
}

func TestMSKRefusesWithoutTheRoomTarget(t *testing.T) {
	reg, err := ops.New(ops.Config{ProjectDir: t.TempDir(), Targets: []events.SetupTarget{{Kind: KindAWS, Address: "111111111111/us-east-1"}}})
	if err != nil {
		t.Fatal(err)
	}
	defer reg.Close()
	f := newFake()
	if _, err := RegisterMSK(reg, MSKOptions{Client: f, Target: mskTarget, RoomID: mRoom}); err != nil {
		t.Fatal(err)
	}
	_, err = exec(t, reg, ReadMSKConnector, map[string]any{"name": "it_orders"})
	if !errors.Is(err, ops.ErrTarget) || f.calls != 0 {
		t.Errorf("err = %v, calls %d; want ErrTarget before any call", err, f.calls)
	}
}

func TestMSKRefusesAnotherAccountsIdentity(t *testing.T) {
	f := newFake()
	f.account = "111111111111"
	reg, _ := newMSK(t, f, "", nil)
	res, err := exec(t, reg, "msk_create_connector", connectorParams())
	if !errors.Is(err, ErrAccount) || len(f.created) != 0 {
		t.Fatalf("err = %v, created %d", err, len(f.created))
	}
	if strings.Contains(res.Summary, "111111111111") {
		t.Errorf("the summary names the runner's account: %s", res.Summary)
	}
}

func TestMSKConnectorConfigSecrets(t *testing.T) {
	f := newFake()
	reg, _ := newMSK(t, f, "", nil)
	res, err := exec(t, reg, "msk_create_connector", connectorParams())
	if err != nil {
		t.Fatal(err)
	}
	if len(f.created) != 1 || f.created[0].Config["database.password"] != "pw-live-1" {
		t.Fatalf("the client did not get the materialised password: %+v", f.created)
	}
	if f.created[0].BootstrapServers != f.bootstrap || f.created[0].Tags[TagRoom] != mRoom {
		t.Errorf("spec = %+v", f.created[0])
	}
	if strings.Contains(res.Summary+string(res.Data), "pw-live-1") {
		t.Errorf("the password reached the result")
	}

	bad := []struct {
		name string
		edit func(cfg map[string]string)
		want error
	}{
		{"raw password", func(c map[string]string) { c["database.password"] = "hunter2" }, ErrSecretValue},
		{"half reference", func(c map[string]string) { c["database.password"] = "x${secret:db_password}" }, ErrSecretValue},
		{"reference on a plain key", func(c map[string]string) { c["database.hostname"] = "${secret:db_password}" }, ops.ErrParams},
		{"line break", func(c map[string]string) { c["topic.prefix"] = "a\nb" }, ops.ErrParams},
		{"bad key", func(c map[string]string) { c["bad key"] = "x" }, ops.ErrParams},
	}
	for _, b := range bad {
		p := connectorParams()
		cfg := map[string]string{}
		for k, v := range p["config"].(map[string]string) {
			cfg[k] = v
		}
		b.edit(cfg)
		p["config"] = cfg
		before := len(f.created)
		res, err := exec(t, reg, "msk_create_connector", p)
		if !errors.Is(err, b.want) || len(f.created) != before {
			t.Errorf("%s: err = %v, want %v (created %d)", b.name, err, b.want, len(f.created)-before)
		}
		if strings.Contains(res.Summary, "hunter2") {
			t.Errorf("%s: the summary quotes the value", b.name)
		}
	}
}

func TestMSKRefusesForeignARNs(t *testing.T) {
	f := newFake()
	reg, _ := newMSK(t, f, "", nil)
	for name, edit := range map[string]func(p map[string]any){
		"plugin in another account": func(p map[string]any) {
			p["plugin_arn"] = strings.Replace(mPlugin, mAccount, "123456789012", 1)
		},
		"role in another account": func(p map[string]any) { p["service_role_arn"] = "arn:aws:iam::123456789012:role/x" },
		"cluster in another region": func(p map[string]any) {
			p["cluster_arn"] = strings.Replace(mCluster, "us-east-1", "eu-west-1", 1)
		},
		"unknown field": func(p map[string]any) { p["bootstrap_servers"] = "evil:9092" },
	} {
		p := connectorParams()
		edit(p)
		if _, err := exec(t, reg, "msk_create_connector", p); !errors.Is(err, ops.ErrParams) {
			t.Errorf("%s: %v", name, err)
		}
	}
	if len(f.created) != 0 {
		t.Errorf("a refused step created a connector")
	}
}

func TestMSKDeleteOnlyWhatTheRoomCreated(t *testing.T) {
	f := newFake()
	f.tags = map[string]string{}
	reg, _ := newMSK(t, f, protocol.ClassProduction, nil)
	if _, err := exec(t, reg, "msk_delete_connector", map[string]any{"name": "it_orders"}); !errors.Is(err, ErrNotCreatedByRoom) || len(f.deleted) != 0 {
		t.Fatalf("untagged: %v, deleted %v", err, f.deleted)
	}
	f.tags = map[string]string{TagRoom: mRoom}
	if _, err := exec(t, reg, "msk_delete_connector", map[string]any{"name": "it_orders"}); err != nil || len(f.deleted) != 1 {
		t.Fatalf("tagged: %v, deleted %v", err, f.deleted)
	}
}

func TestMSKWaitStopsBeforeTheDeadline(t *testing.T) {
	f := newFake()
	f.state = "CREATING"
	reg, _ := newMSK(t, f, "", nil)
	raw, _ := json.Marshal(map[string]any{"kind": "connector", "arn": mConnector})
	start := time.Now()
	res, err := reg.Execute(context.Background(), protocol.OpRequest{ID: "w", Op: "msk_wait", Params: raw, Deadline: time.Now().Add(2500 * time.Millisecond)})
	if err != nil || !res.OK {
		t.Fatalf("err = %v, %s", err, res.Summary)
	}
	if !strings.Contains(string(res.Data), `"done":false`) || !strings.Contains(res.Summary, "still CREATING") {
		t.Errorf("result = %s | %s", res.Summary, res.Data)
	}
	if time.Since(start) > 2500*time.Millisecond {
		t.Errorf("msk_wait ran past the deadline")
	}
}

func TestMSKRoleCheckFailsOnTrust(t *testing.T) {
	reg, _ := newMSK(t, newFake(), "", nil)
	res, err := exec(t, reg, "msk_role_check", map[string]any{"role_arn": mRole})
	if !errors.Is(err, ErrCheckFailed) || !strings.Contains(res.Summary, "does not trust") {
		t.Errorf("err = %v, %s", err, res.Summary)
	}
}

func TestMSKPluginObjectSizeMismatch(t *testing.T) {
	reg, _ := newMSK(t, newFake(), "", nil)
	res, err := exec(t, reg, "msk_plugin_object_check", map[string]any{"bucket": mBucket, "key": mKey, "expected_size": 11})
	if !errors.Is(err, ErrCheckFailed) || !strings.Contains(res.Summary, "the index says 11") {
		t.Errorf("err = %v, %s", err, res.Summary)
	}
}

func TestMSKPreview(t *testing.T) {
	reg, d := newMSK(t, newFake(), "", nil)
	env := ops.Env{Targets: []events.SetupTarget{mskTarget}}
	op, _ := reg.Get("msk_create_worker_config")
	raw, _ := json.Marshal(map[string]any{"name": "w", "entries": map[string]string{"key.converter": "org.apache.kafka.connect.storage.StringConverter"}})
	p, err := op.(mskOp).Preview(context.Background(), raw, env)
	if err != nil || !strings.Contains(p.Expected, "key.converter=org.apache.kafka.connect.storage.StringConverter") || p.Target != "aws:"+d.address {
		t.Errorf("preview = %+v, %v", p, err)
	}
	op, _ = reg.Get("msk_create_connector")
	raw, _ = json.Marshal(connectorParams())
	p, err = op.(mskOp).Preview(context.Background(), raw, env)
	if err != nil || !p.Reversible || !strings.Contains(p.Rollback, "msk_delete_connector") || strings.Contains(p.Expected, "pw-live-1") {
		t.Errorf("preview = %+v, %v", p, err)
	}
}

func TestRenderWorkerProperties(t *testing.T) {
	got, err := RenderWorkerProperties(map[string]string{
		"value.converter": "org.apache.kafka.connect.json.JsonConverter", "key.converter": "org.apache.kafka.connect.storage.StringConverter",
		"value.converter.schemas.enable": "false", "offset.flush.interval.ms": "10000",
	})
	if err != nil {
		t.Fatal(err)
	}
	want := "key.converter=org.apache.kafka.connect.storage.StringConverter\noffset.flush.interval.ms=10000\nvalue.converter=org.apache.kafka.connect.json.JsonConverter\nvalue.converter.schemas.enable=false\n"
	if !strings.HasSuffix(got, want) || !strings.HasPrefix(got, "# Rendered by piperoom-setup") {
		t.Errorf("rendered:\n%s", got)
	}
	for name, entries := range map[string]map[string]string{
		"empty":                {},
		"config provider":      {"config.providers": "secretsmanager"},
		"offset topic":         {"offset.storage.topic": "x"},
		"bootstrap":            {"bootstrap.servers": "evil:9092"},
		"unknown key":          {"consumer.override.sasl.jaas.config": "x"},
		"converter not listed": {"value.converter": "io.confluent.connect.avro.AvroConverter"},
		"value with a newline": {"key.converter.schemas.enable": "true\nplugin.path=/tmp"},
		"bool not a bool":      {"key.converter.schemas.enable": "yes"},
		"number out of range":  {"offset.flush.interval.ms": "1"},
		"upper case key":       {"Key.Converter": "x"},
	} {
		if _, err := RenderWorkerProperties(entries); !errors.Is(err, ops.ErrParams) {
			t.Errorf("%s: %v", name, err)
		}
	}
}

func TestVPCVerdict(t *testing.T) {
	ip := netip.MustParseAddr("172.31.5.10")
	base := mskconnect.Network{
		Subnets:        []mskconnect.Subnet{{ID: mSubnetA, VPC: mVPC}},
		SecurityGroups: []mskconnect.SecurityGroup{{ID: mGroup, Egress: []mskconnect.Rule{{Protocol: "tcp", FromPort: 5432, ToPort: 5432, CIDRs: []string{"172.31.0.0/16"}}}}},
		RouteTables: []mskconnect.RouteTable{
			{ID: "rtb-main", VPC: mVPC, Main: true, Routes: []mskconnect.Route{{Destination: "10.0.0.0/16", Target: "local"}}},
			{ID: "rtb-a", VPC: mVPC, Subnets: []string{mSubnetA}, Routes: []mskconnect.Route{
				{Destination: "10.0.0.0/16", Target: "local"}, {Destination: "172.31.0.0/16", Target: "pcx-1", State: "active"}, {Destination: "172.31.5.0/24", Target: "tgw-1", State: "blackhole"}}},
		},
	}
	checks, v := VPCVerdict(base, []string{mSubnetA}, []string{mGroup}, ip, 5432)
	if v != VerdictOK || !strings.Contains(checks[0].Note, "pcx-1") {
		t.Errorf("explicit association with peering: %s %+v", v, checks)
	}
	// The main table only: no route.
	noAssoc := base
	noAssoc.RouteTables = base.RouteTables[:1]
	if _, v := VPCVerdict(noAssoc, []string{mSubnetA}, []string{mGroup}, ip, 5432); v != VerdictNoRoute {
		t.Errorf("main table only: %s", v)
	}
	// Egress to another port only.
	blocked := base
	blocked.SecurityGroups = []mskconnect.SecurityGroup{{ID: mGroup, Egress: []mskconnect.Rule{{Protocol: "tcp", FromPort: 443, ToPort: 443, CIDRs: []string{"0.0.0.0/0"}}}}}
	if _, v := VPCVerdict(blocked, []string{mSubnetA}, []string{mGroup}, ip, 5432); v != VerdictEgressBlocked {
		t.Errorf("egress 443 only: %s", v)
	}
	// Egress by group reference cannot be judged and does not count.
	byGroup := base
	byGroup.SecurityGroups = []mskconnect.SecurityGroup{{ID: mGroup, Egress: []mskconnect.Rule{{Protocol: "-1", Groups: []string{"sg-0a1b2c3d4e5f60009"}}}}}
	if _, v := VPCVerdict(byGroup, []string{mSubnetA}, []string{mGroup}, ip, 5432); v != VerdictEgressBlocked {
		t.Errorf("egress by group: %s", v)
	}
	// A group the connector does not use does not count.
	if _, v := VPCVerdict(base, []string{mSubnetA}, []string{"sg-0a1b2c3d4e5f60009"}, ip, 5432); v != VerdictEgressBlocked {
		t.Errorf("another group: %s", v)
	}
}

func TestVPCParams(t *testing.T) {
	for name, p := range map[string]VPCCheckParams{
		"host name": {Subnets: []string{mSubnetA}, SecurityGroups: []string{mGroup}, DatabaseIP: "db.example.com", DatabasePort: 5432},
		"port":      {Subnets: []string{mSubnetA}, SecurityGroups: []string{mGroup}, DatabaseIP: "10.0.0.1", DatabasePort: 0},
		"no group":  {Subnets: []string{mSubnetA}, DatabaseIP: "10.0.0.1", DatabasePort: 5432},
		"subnet":    {Subnets: []string{"subnet-zz"}, SecurityGroups: []string{mGroup}, DatabaseIP: "10.0.0.1", DatabasePort: 5432},
	} {
		if err := checkVPC(p); !errors.Is(err, ops.ErrParams) {
			t.Errorf("%s: %v", name, err)
		}
	}
}

func TestManagedLadders(t *testing.T) {
	rungs, err := Ladder("msk", "postgres", "")
	if err != nil {
		t.Fatal(err)
	}
	if rungs[0].Reads[0] != ReadMSKConnector || rungs[1].NotApplicable == "" || rungs[2].Reads[0] != "kafka_topic_offsets" {
		t.Errorf("msk rungs: %+v %+v %+v", rungs[0], rungs[1], rungs[2])
	}
	ok, _ := rungs[0].Predicate(json.RawMessage(`{"state":"RUNNING","tasks":[]}`))
	notYet, note := rungs[0].Predicate(json.RawMessage(`{"state":"CREATING","tasks":[]}`))
	if !ok || notYet || !strings.Contains(note, "CREATING") {
		t.Errorf("rung 1 predicate: %v %v %q", ok, notYet, note)
	}
	for target, read := range map[string]string{"confluent": ReadConfluentConnector, "aiven": ReadAivenConnector} {
		rungs, err := Ladder(target, "mysql", "postgres")
		if err != nil {
			t.Fatal(err)
		}
		if rungs[0].Reads[0] != read || rungs[1].Reads[0] != read || rungs[1].NotApplicable != "" || rungs[6].NotApplicable != "" {
			t.Errorf("%s rungs: %+v %+v", target, rungs[0], rungs[1])
		}
	}
	if _, err := Ladder("compose", "postgres", ""); !errors.Is(err, plan.ErrLadder) {
		t.Errorf("compose: %v", err)
	}
	if _, err := Ladder("msk", "db2", ""); !errors.Is(err, plan.ErrLadder) {
		t.Errorf("unknown source: %v", err)
	}
}

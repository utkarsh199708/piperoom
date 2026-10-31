package managed

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"testing"

	"piperoom/internal/clients"
	"piperoom/internal/clients/confluentcloud"
	"piperoom/internal/events"
	"piperoom/internal/setup/ops"
	"piperoom/internal/setup/protocol"
	"piperoom/internal/setup/targets/managed/replay"
)

var ccTarget = events.SetupTarget{Kind: KindConfluent, Address: "env-00000/lkc-00000"}

const ccSource = "hand-written from the ccloud-sdk-go-v2 OpenAPI documents (connect/v1, cmk/v2, kafkarest/v3; read 2026-10-30); requests as the managed ops send them through confluentcloud; not from a live account"

func ccJSON(status int, body string) replay.Response {
	return replay.Response{Status: status, Headers: map[string]string{"Content-Type": "application/json"}, Body: body}
}

func ccParams() map[string]any {
	return map[string]any{"name": "it_orders", "plugin": "PostgresCdcSourceV2", "config": map[string]string{
		"kafka.auth.mode": "SERVICE_ACCOUNT", "kafka.service.account.id": "sa-00000", "database.hostname": "orders.example.com", "database.port": "5432",
		"database.user": "debezium", "database.password": "${secret:db_password}", "database.dbname": "orders", "topic.prefix": "orders",
		"output.data.format": "JSON", "tasks.max": "1"}}
}

const ccStatusFailedACL = `{"name":"it_orders","type":"source","connector":{"state":"FAILED","worker_id":"it_orders","trace":""},"tasks":[{"id":0,"state":"FAILED","worker_id":"it_orders","msg":"org.apache.kafka.common.errors.TopicAuthorizationException: Not authorized to access topics: [orders.public.orders]"}]}`

// ccFlow runs one Confluent op; acl says the responses belong to the
// cluster REST (ACL) client rather than the Connect client.
type ccFlow struct {
	flow
	acl bool
}

var ccFlows = []ccFlow{
	{flow: flow{name: "plugins", op: "ccloud_list_plugins", params: map[string]any{},
		responses: []replay.Response{ccJSON(200, `[{"class":"PostgresCdcSourceV2","type":"source","version":"0.1.0"},{"class":"MySqlCdcSourceV2","type":"source"},{"class":"S3_SINK","type":"sink"}]`)},
		check:     okFlow("PostgresCdcSourceV2", "MySqlCdcSourceV2")}},
	{flow: flow{name: "status_running", op: ReadConfluentConnector, params: map[string]any{"name": "it_orders"},
		responses: []replay.Response{ccJSON(200, `{"name":"it_orders","type":"source","connector":{"state":"RUNNING","worker_id":"it_orders","trace":""},"tasks":[{"id":0,"state":"RUNNING","worker_id":"it_orders"}]}`)},
		check:     okFlow("1/1 task(s) RUNNING", `"state":"RUNNING"`)}},
	{flow: flow{name: "status_failed_acl", op: ReadConfluentConnector, params: map[string]any{"name": "it_orders"},
		responses: []replay.Response{ccJSON(200, ccStatusFailedACL)},
		check:     okFlow("is FAILED", "TopicAuthorizationException")}},
	{flow: flow{name: "status_failed_egress", op: ReadConfluentConnector, params: map[string]any{"name": "it_orders"},
		responses: []replay.Response{ccJSON(200, `{"name":"it_orders","type":"source","connector":{"state":"FAILED","worker_id":"it_orders","trace":"io.debezium.DebeziumException: Couldn't obtain database connection: Connection refused"},"tasks":[]}`)},
		check:     okFlow("allow-list the egress ranges")}},
	{flow: flow{name: "cluster", op: "ccloud_cluster", params: map[string]any{},
		responses: []replay.Response{ccJSON(200, `{"api_version":"cmk/v2","kind":"Cluster","id":"lkc-00000","spec":{"display_name":"orders","availability":"SINGLE_ZONE","cloud":"AWS","region":"us-east-1","kafka_bootstrap_endpoint":"SASL_SSL://pkc-00000.us-east-1.aws.confluent.cloud:9092","http_endpoint":"https://pkc-00000.us-east-1.aws.confluent.cloud:443"},"status":{"phase":"PROVISIONED"}}`)},
		check:     okFlow("PROVISIONED", "us-east-1")}},
	{flow: flow{name: "create_validate_errors", op: "ccloud_create_connector", params: ccParams(),
		responses: []replay.Response{ccJSON(200, `{"name":"PostgresCdcSourceV2","error_count":1,"configs":[{"value":{"name":"database.hostname","value":"orders.example.com","errors":["Could not connect to orders.example.com:5432"]}},{"value":{"name":"database.password","value":"pw-live-1","errors":[]}}]}`)},
		check:     failFlow(ErrNotValid, "database.hostname", "nothing was created", `"created":false`)}},
	{flow: flow{name: "create_ok", op: "ccloud_create_connector", params: ccParams(),
		responses: []replay.Response{ccJSON(200, `{"name":"PostgresCdcSourceV2","error_count":0,"configs":[]}`),
			ccJSON(201, `{"name":"it_orders","type":"source","config":{"connector.class":"PostgresCdcSourceV2","database.password":"pw-live-1"},"tasks":[]}`)},
		check: okFlow("created after a clean validation", "${secret:db_password}")}},
	{flow: flow{name: "create_conflict", op: "ccloud_create_connector", params: ccParams(),
		responses: []replay.Response{ccJSON(200, `{"name":"PostgresCdcSourceV2","error_count":0,"configs":[]}`), ccJSON(409, `{"error":{"code":409,"message":"Connector it_orders already exists"}}`)},
		check:     failFlow(confluentcloud.ErrConflict, `"error_class":"conflict"`)}},
	{acl: true, flow: flow{name: "acl_connector", op: "ccloud_acl_create", params: map[string]any{"template": "connector", "principal": "User:sa-00000", "topic_prefix": "orders"},
		responses: []replay.Response{{Status: 201}, {Status: 201}, {Status: 201}, {Status: 201}},
		check:     okFlow("4 ACL rows of the connector template", `"operation":"WRITE"`)}},
	{acl: true, flow: flow{name: "acl_watcher_forbidden", op: "ccloud_acl_create", params: map[string]any{"template": "watcher", "principal": "User:sa-00000", "topic_prefix": "orders", "group": "connect-it_orders"},
		responses: []replay.Response{{Status: 201}, ccJSON(403, `{"error_code":40301,"message":"Request is not authorized"}`)},
		check:     failFlow(confluentcloud.ErrForbidden, "stopped after 1 of 4 rows")}},
}

func newConfluent(t *testing.T, connect, acl http.RoundTripper) *ops.Registry {
	t.Helper()
	c, err := confluentcloud.New(confluentcloud.Config{APIKey: "TESTKEY000000000", APISecret: "s", Environment: "env-00000", Cluster: "lkc-00000", Transport: connect})
	if err != nil {
		t.Fatal(err)
	}
	a, err := confluentcloud.NewACLClient(confluentcloud.ACLConfig{Endpoint: "https://pkc-00000.us-east-1.aws.confluent.cloud:443", Cluster: "lkc-00000", Key: "TESTKEY000000000", Secret: "s", Transport: acl})
	if err != nil {
		t.Fatal(err)
	}
	reg, err := ops.New(ops.Config{ProjectDir: t.TempDir(), Targets: []events.SetupTarget{ccTarget}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = reg.Close() })
	// The flows' principal (sa-00000) is the one entered on the runner for
	// both templates.
	principal := func(string) (string, error) { return "sa-00000", nil }
	if _, err := RegisterConfluent(reg, ConfluentOptions{Client: c, ACL: a, Target: ccTarget, Secrets: fakeStore{}, Principal: principal}); err != nil {
		t.Fatal(err)
	}
	return reg
}

func TestConfluentFlows(t *testing.T) {
	for _, f := range ccFlows {
		t.Run(f.name, func(t *testing.T) {
			r := replay.Replay(t, "testdata/confluent/"+f.name+".json", replay.Scrubber{})
			var reg *ops.Registry
			if f.acl {
				reg = newConfluent(t, replay.NewCanned(), r)
			} else {
				reg = newConfluent(t, r, replay.NewCanned())
			}
			res, err := exec(t, reg, f.op, f.params)
			f.check(t, res, err)
			if strings.Contains(res.Summary+string(res.Data), "pw-live-1") {
				t.Errorf("the password reached the result")
			}
			if n := r.Remaining(); n != 0 {
				t.Errorf("%d interactions unused", n)
			}
		})
	}
}

// fakeCC records the config the ops send.
type fakeCC struct {
	validated, created []map[string]string
	errors             int
}

func (f *fakeCC) Plugins(context.Context) ([]confluentcloud.Plugin, error) { return nil, nil }
func (f *fakeCC) Validate(_ context.Context, _ string, cfg map[string]string) (confluentcloud.Validation, error) {
	f.validated = append(f.validated, cfg)
	return confluentcloud.Validation{ErrorCount: f.errors}, nil
}
func (f *fakeCC) CreateConnector(_ context.Context, name string, cfg map[string]string) (confluentcloud.Created, error) {
	f.created = append(f.created, cfg)
	return confluentcloud.Created{Name: name}, nil
}
func (f *fakeCC) Status(context.Context, string) (clients.ConnectorStatus, error) {
	return clients.ConnectorStatus{}, nil
}
func (f *fakeCC) Cluster(context.Context) (confluentcloud.Cluster, error) {
	return confluentcloud.Cluster{}, nil
}

func ccReg(t *testing.T, f ConfluentAPI, o ConfluentOptions) (*ops.Registry, *ConfluentDriver) {
	t.Helper()
	reg, err := ops.New(ops.Config{ProjectDir: t.TempDir(), Targets: []events.SetupTarget{ccTarget}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = reg.Close() })
	o.Client = f
	if o.Target.Kind == "" {
		o.Target = ccTarget
	}
	d, err := RegisterConfluent(reg, o)
	if err != nil {
		t.Fatal(err)
	}
	return reg, d
}

func TestConfluentValidateThenCreate(t *testing.T) {
	f := &fakeCC{}
	reg, _ := ccReg(t, f, ConfluentOptions{Secrets: fakeStore{}})
	if _, err := exec(t, reg, "ccloud_create_connector", ccParams()); err != nil {
		t.Fatal(err)
	}
	if len(f.validated) != 1 || len(f.created) != 1 || f.created[0]["database.password"] != "pw-live-1" || f.created[0]["connector.class"] != "PostgresCdcSourceV2" || f.created[0]["name"] != "it_orders" {
		t.Fatalf("validated %v created %v", f.validated, f.created)
	}
	f.errors = 2
	if _, err := exec(t, reg, "ccloud_create_connector", ccParams()); !errors.Is(err, ErrNotValid) || len(f.created) != 1 {
		t.Errorf("a red validation still created: %v", err)
	}
	p := ccParams()
	p["config"].(map[string]string)["connector.class"] = "MySqlCdcSourceV2"
	if _, err := exec(t, reg, "ccloud_create_connector", p); !errors.Is(err, ops.ErrParams) {
		t.Errorf("a class unlike the plugin: %v", err)
	}
	p = ccParams()
	p["config"].(map[string]string)["kafka.api.secret"] = "raw"
	if _, err := exec(t, reg, "ccloud_create_connector", p); !errors.Is(err, ErrSecretValue) {
		t.Errorf("a raw secret: %v", err)
	}
}

func TestConfluentClassAndTiers(t *testing.T) {
	for _, tc := range []struct {
		patterns []string
		person   string
		want     string
	}{
		{nil, "", protocol.ClassShared},
		{[]string{"env-00000"}, "", protocol.ClassProduction},
		{[]string{"lkc-0*"}, "", protocol.ClassProduction},
		{[]string{"env-prod*"}, "", protocol.ClassShared},
		{nil, protocol.ClassProduction, protocol.ClassProduction},
	} {
		reg, d := ccReg(t, &fakeCC{}, ConfluentOptions{ProdPatterns: tc.patterns, Class: tc.person})
		if d.Class() != tc.want {
			t.Errorf("%v %q: class %s, want %s", tc.patterns, tc.person, d.Class(), tc.want)
		}
		want := protocol.TierT2
		if tc.want == protocol.ClassProduction {
			want = protocol.TierT3
		}
		for _, op := range []string{"ccloud_create_connector", "ccloud_acl_create"} {
			if tier, _, _ := reg.FloorFor(op, nil); tier != want {
				t.Errorf("%s tier %s, want %s", op, tier, want)
			}
		}
		if tier, _, _ := reg.FloorFor(ReadConfluentConnector, nil); tier != protocol.TierT0 {
			t.Errorf("status tier %s", tier)
		}
	}
	reg, err := ops.New(ops.Config{ProjectDir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer reg.Close()
	for _, addr := range []string{"env-00000", "env-00000/lkc-00000/x", "prod/lkc-1", "env-00000/cluster"} {
		if _, err := RegisterConfluent(reg, ConfluentOptions{Client: &fakeCC{}, Target: events.SetupTarget{Kind: KindConfluent, Address: addr}}); err == nil {
			t.Errorf("address %q registered", addr)
		}
	}
}

func TestConfluentNeedsTheRoomTarget(t *testing.T) {
	reg, err := ops.New(ops.Config{ProjectDir: t.TempDir(), Targets: []events.SetupTarget{{Kind: KindConfluent, Address: "env-11111/lkc-11111"}}})
	if err != nil {
		t.Fatal(err)
	}
	defer reg.Close()
	f := &fakeCC{}
	if _, err := RegisterConfluent(reg, ConfluentOptions{Client: f, Target: ccTarget, Secrets: fakeStore{}}); err != nil {
		t.Fatal(err)
	}
	if _, err := exec(t, reg, "ccloud_create_connector", ccParams()); !errors.Is(err, ops.ErrTarget) || len(f.validated) != 0 {
		t.Errorf("err = %v", err)
	}
}

func TestACLRows(t *testing.T) {
	rows, err := ACLRows(ACLParams{Template: ACLTemplateWatcher, Principal: "User:sa-00000", TopicPrefix: "orders", Group: "connect-it_orders"})
	if err != nil || len(rows) != 4 {
		t.Fatalf("watcher rows = %v, %v", rows, err)
	}
	b, _ := json.Marshal(rows)
	for _, w := range []string{`"operation":"READ"`, `"operation":"DESCRIBE_CONFIGS"`, `"resource_type":"GROUP"`, `"pattern_type":"PREFIXED"`} {
		if !strings.Contains(string(b), w) {
			t.Errorf("watcher rows lack %s", w)
		}
	}
	if strings.Contains(string(b), "WRITE") {
		t.Errorf("the watcher can write: %s", b)
	}
	for name, p := range map[string]ACLParams{
		"watcher without group":   {Template: ACLTemplateWatcher, Principal: "User:sa-00000", TopicPrefix: "orders"},
		"connector with group":    {Template: ACLTemplateConnector, Principal: "User:sa-00000", TopicPrefix: "orders", Group: "g"},
		"wildcard principal":      {Template: ACLTemplateConnector, Principal: "User:*", TopicPrefix: "orders"},
		"wildcard prefix":         {Template: ACLTemplateConnector, Principal: "User:sa-00000", TopicPrefix: "*"},
		"unknown template":        {Template: "admin", Principal: "User:sa-00000", TopicPrefix: "orders"},
		"a principal that is not": {Template: ACLTemplateConnector, Principal: "sa-00000", TopicPrefix: "orders"},
		"one-character prefix":    {Template: ACLTemplateConnector, Principal: "User:sa-00000", TopicPrefix: "o"},
		"two-character prefix":    {Template: ACLTemplateConnector, Principal: "User:sa-00000", TopicPrefix: "or"},
		"internal topics prefix":  {Template: ACLTemplateConnector, Principal: "User:sa-00000", TopicPrefix: "_confluent"},
	} {
		if _, err := ACLRows(p); !errors.Is(err, ops.ErrParams) {
			t.Errorf("%s: %v", name, err)
		}
	}
	reg, _ := ccReg(t, &fakeCC{}, ConfluentOptions{})
	if _, err := exec(t, reg, "ccloud_acl_create", map[string]any{"template": "connector", "principal": "User:sa-00000", "topic_prefix": "orders"}); !errors.Is(err, ErrNoACLKey) {
		t.Errorf("no ACL client: %v", err)
	}
}

// countingACL records the rows ccloud_acl_create sends.
type countingACL struct{ rows []confluentcloud.ACL }

func (a *countingACL) CreateACL(_ context.Context, acl confluentcloud.ACL) error {
	a.rows = append(a.rows, acl)
	return nil
}

// TestACLPrincipalIsThePersons (review M2): the principal is bound to the
// service account the person entered on the runner for the template; a
// model-chosen principal, a missing entry and a runner without the source
// are refused before any ACL row is sent.
func TestACLPrincipalIsThePersons(t *testing.T) {
	entered := map[string]string{ACLTemplateWatcher: "sa-00001", ACLTemplateConnector: "User:sa-00000"}
	principal := func(tpl string) (string, error) {
		v, ok := entered[tpl]
		if !ok {
			return "", errors.New("not entered")
		}
		return v, nil
	}
	watcher := map[string]any{"template": "watcher", "principal": "User:sa-00001", "topic_prefix": "orders", "group": "connect-it_orders"}
	connector := map[string]any{"template": "connector", "principal": "User:sa-00000", "topic_prefix": "orders"}

	acl := &countingACL{}
	reg, _ := ccReg(t, &fakeCC{}, ConfluentOptions{ACL: acl, Principal: principal})
	for _, p := range []map[string]any{watcher, connector} {
		if _, err := exec(t, reg, "ccloud_acl_create", p); err != nil {
			t.Fatalf("%v: %v", p["template"], err)
		}
	}
	if len(acl.rows) != 8 {
		t.Fatalf("%d rows sent, want 8", len(acl.rows))
	}

	acl.rows = nil
	other := map[string]any{"template": "connector", "principal": "User:sa-99999", "topic_prefix": "orders"}
	if _, err := exec(t, reg, "ccloud_acl_create", other); !errors.Is(err, ErrPrincipal) {
		t.Errorf("a principal the person did not enter: %v, want ErrPrincipal", err)
	}
	swapped := map[string]any{"template": "watcher", "principal": "User:sa-00000", "topic_prefix": "orders", "group": "connect-it_orders"}
	if _, err := exec(t, reg, "ccloud_acl_create", swapped); !errors.Is(err, ErrPrincipal) {
		t.Errorf("the connector's account on the watcher template: %v, want ErrPrincipal", err)
	}
	delete(entered, ACLTemplateConnector)
	if _, err := exec(t, reg, "ccloud_acl_create", connector); !errors.Is(err, ErrNoPrincipal) {
		t.Errorf("no entered account: %v, want ErrNoPrincipal", err)
	}
	none, _ := ccReg(t, &fakeCC{}, ConfluentOptions{ACL: acl})
	if _, err := exec(t, none, "ccloud_acl_create", watcher); !errors.Is(err, ErrNoPrincipal) {
		t.Errorf("no principal source: %v, want ErrNoPrincipal", err)
	}
	if len(acl.rows) != 0 {
		t.Errorf("%d rows sent for refused principals", len(acl.rows))
	}
}

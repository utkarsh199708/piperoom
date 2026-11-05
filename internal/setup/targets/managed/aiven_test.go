package managed

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"testing"

	"piperoom/internal/clients"
	"piperoom/internal/clients/aiven"
	"piperoom/internal/events"
	"piperoom/internal/setup/ops"
	"piperoom/internal/setup/protocol"
	"piperoom/internal/setup/targets/managed/replay"
)

var avTarget = events.SetupTarget{Kind: KindAiven, Address: "project-0/connect-0"}

const avSource = "hand-written from aiven/aiven-client's client.py paths (read 2026-10-30); response shapes beyond its result keys are unconfirmed; requests as the managed ops send them through internal/clients/aiven; not from a live account"

func avJSON(status int, body string) replay.Response {
	return replay.Response{Status: status, Headers: map[string]string{"Content-Type": "application/json"}, Body: body}
}

const avPluginsNoPostgres = `{"plugins":[{"class":"io.debezium.connector.mysql.MySqlConnector","title":"Debezium-MySQL","type":"source","version":"2.7.3.Final"},{"class":"io.aiven.connect.jdbc.JdbcSinkConnector","title":"JDBC sink","type":"sink","version":"6.10.0"}]}`

const avPluginsWithPostgres = `{"plugins":[{"class":"io.debezium.connector.postgresql.PostgresConnector","title":"Debezium-PostgreSQL","type":"source","version":"2.7.3.Final"},{"class":"io.debezium.connector.mysql.MySqlConnector","title":"Debezium-MySQL","type":"source","version":"2.7.3.Final"}]}`

func avParams() map[string]any {
	return map[string]any{"name": "it_orders", "config": map[string]string{
		"connector.class": "io.debezium.connector.postgresql.PostgresConnector", "database.hostname": "orders.example.com", "database.port": "5432",
		"database.user": "debezium", "database.password": "${secret:db_password}", "database.dbname": "orders", "topic.prefix": "orders", "plugin.name": "pgoutput"}}
}

var avFlows = []flow{
	{name: "service_get", op: "aiven_service_get", params: map[string]any{},
		responses: []replay.Response{avJSON(200, `{"service":{"service_name":"connect-0","service_type":"kafka_connect","state":"RUNNING","plan":"startup-4","cloud_name":"aws-us-east-1","service_uri":"https://avnadmin:AVNS_pw-live-2@connect-0-project-0.aivencloud.com:443","users":[{"username":"avnadmin","password":"AVNS_pw-live-2"}]}}`)},
		check:     okFlow("kafka_connect", "RUNNING", "avnadmin")},
	{name: "plugins", op: "aiven_list_plugins", params: map[string]any{},
		responses: []replay.Response{avJSON(200, avPluginsNoPostgres)}, check: okFlow("2 plugin(s)", "MySqlConnector")},
	{name: "status_running", op: ReadAivenConnector, params: map[string]any{"name": "it_orders"},
		responses: []replay.Response{avJSON(200, `{"status":{"state":"RUNNING","tasks":[{"id":0,"state":"RUNNING","trace":""}]}}`)},
		check:     okFlow("1/1 task(s) RUNNING")},
	{name: "create_plugin_unavailable", op: "aiven_create_connector", params: avParams(),
		responses: []replay.Response{avJSON(200, avPluginsNoPostgres)},
		check:     failFlow(ErrCheckFailed, "is not an available plugin", "MySqlConnector", `"created":false`)},
	{name: "create_ok", op: "aiven_create_connector", params: avParams(),
		responses: []replay.Response{avJSON(200, avPluginsWithPostgres), avJSON(200, `{"connector":{"name":"it_orders"}}`)},
		check:     okFlow("connector it_orders created", "${secret:db_password}")},
	{name: "create_rejected", op: "aiven_create_connector", params: avParams(),
		responses: []replay.Response{avJSON(200, avPluginsWithPostgres), avJSON(400, `{"message":"Connector configuration is invalid: database.dbname: Missing required configuration","errors":[{"message":"invalid"}]}`)},
		check:     failFlow(aiven.ErrBadRequest, `"error_class":"bad_request"`)},
	{name: "integration_ok", op: "aiven_integration_create", params: map[string]any{"source_service": "kafka-0"},
		responses: []replay.Response{avJSON(200, `{"service":{"service_name":"kafka-0","service_type":"kafka","state":"RUNNING"}}`),
			avJSON(200, `{"service_integration":{"service_integration_id":"0a1b2c3d-0000-4000-8000-000000000001","integration_type":"kafka_connect"}}`)},
		check: okFlow("kafka-0 -> connect-0")},
	{name: "integration_not_kafka", op: "aiven_integration_create", params: map[string]any{"source_service": "pg-0"},
		responses: []replay.Response{avJSON(200, `{"service":{"service_name":"pg-0","service_type":"pg","state":"RUNNING"}}`)},
		check:     failFlow(ErrCheckFailed, "not kafka")},
}

func newAiven(t *testing.T, rt http.RoundTripper) *ops.Registry {
	t.Helper()
	c, err := aiven.New(aiven.Config{Token: "test-token-0123456789abcdef", Project: "project-0", Service: "connect-0", Transport: rt})
	if err != nil {
		t.Fatal(err)
	}
	reg, err := ops.New(ops.Config{ProjectDir: t.TempDir(), Targets: []events.SetupTarget{avTarget}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = reg.Close() })
	if _, err := RegisterAiven(reg, AivenOptions{Client: c, Target: avTarget, Secrets: fakeStore{}}); err != nil {
		t.Fatal(err)
	}
	return reg
}

func TestAivenFlows(t *testing.T) {
	for _, f := range avFlows {
		t.Run(f.name, func(t *testing.T) {
			r := replay.Replay(t, "testdata/aiven/"+f.name+".json", replay.Scrubber{})
			reg := newAiven(t, r)
			res, err := exec(t, reg, f.op, f.params)
			f.check(t, res, err)
			for _, s := range []string{"pw-live-1", "AVNS_pw-live-2", "test-token"} {
				if strings.Contains(res.Summary+string(res.Data), s) {
					t.Errorf("%q reached the result", s)
				}
			}
			if n := r.Remaining(); n != 0 {
				t.Errorf("%d interactions unused", n)
			}
		})
	}
}

type fakeAiven struct {
	created []map[string]string
}

func (f *fakeAiven) Service(context.Context) (aiven.Service, error) { return aiven.Service{}, nil }
func (f *fakeAiven) ServiceNamed(context.Context, string) (aiven.Service, error) {
	return aiven.Service{Type: "kafka"}, nil
}
func (f *fakeAiven) Plugins(context.Context) ([]aiven.Plugin, error) {
	return []aiven.Plugin{{Class: "io.debezium.connector.postgresql.PostgresConnector", Type: "source"}}, nil
}
func (f *fakeAiven) Status(context.Context, string) (clients.ConnectorStatus, error) {
	return clients.ConnectorStatus{}, nil
}
func (f *fakeAiven) CreateConnector(_ context.Context, cfg map[string]string) (string, error) {
	f.created = append(f.created, cfg)
	return cfg["name"], nil
}
func (f *fakeAiven) CreateIntegration(context.Context, string) (string, error) { return "id", nil }

func TestAivenCreateAndClass(t *testing.T) {
	f := &fakeAiven{}
	reg, err := ops.New(ops.Config{ProjectDir: t.TempDir(), Targets: []events.SetupTarget{avTarget}})
	if err != nil {
		t.Fatal(err)
	}
	defer reg.Close()
	d, err := RegisterAiven(reg, AivenOptions{Client: f, Target: avTarget, ProdPatterns: []string{"project-*"}, Secrets: fakeStore{}})
	if err != nil {
		t.Fatal(err)
	}
	if d.Class() != protocol.ClassProduction {
		t.Errorf("class %s", d.Class())
	}
	if tier, _, _ := reg.FloorFor("aiven_create_connector", nil); tier != protocol.TierT3 {
		t.Errorf("tier %s, want T3 on production", tier)
	}
	if _, err := exec(t, reg, "aiven_create_connector", avParams()); err != nil {
		t.Fatal(err)
	}
	if len(f.created) != 1 || f.created[0]["database.password"] != "pw-live-1" || f.created[0]["name"] != "it_orders" {
		t.Errorf("created %v", f.created)
	}
	p := avParams()
	delete(p["config"].(map[string]string), "connector.class")
	if _, err := exec(t, reg, "aiven_create_connector", p); !errors.Is(err, ops.ErrParams) {
		t.Errorf("no class: %v", err)
	}
	p = avParams()
	p["config"].(map[string]string)["database.password"] = "hunter2"
	if _, err := exec(t, reg, "aiven_create_connector", p); !errors.Is(err, ErrSecretValue) {
		t.Errorf("raw password: %v", err)
	}
	for _, addr := range []string{"project-0", "Project/x", "project-0/connect-0/x"} {
		if _, err := RegisterAiven(reg, AivenOptions{Client: f, Target: events.SetupTarget{Kind: KindAiven, Address: addr}}); err == nil {
			t.Errorf("address %q registered", addr)
		}
	}
}

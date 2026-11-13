package aiven

import (
	"context"
	"errors"
	"net/http"
	"strconv"
	"strings"
	"testing"

	"piperoom/internal/actions"
	"piperoom/internal/setup/targets/managed/replay"
)

const (
	tProject = "project-0"
	tService = "connect-0"
	tKafka   = "kafka-0"
	// A test token: never in a cassette (the Authorization header is
	// dropped, and Check refuses "aivenv1 ").
	tToken = "test-token-0123456789abcdef"
)

// Scrub is the scrubber every Aiven cassette is recorded and replayed with.
var Scrub = replay.Scrubber{}

const cassetteSource = "hand-written from aiven/aiven-client's client.py paths (read 2026-10-30); response shapes beyond the result keys it names are unconfirmed; not from a live account"

func jsonResp(status int, body string) replay.Response {
	return replay.Response{Status: status, Headers: map[string]string{"Content-Type": "application/json"}, Body: body}
}

func apiErr(status int, msg string) replay.Response {
	return jsonResp(status, `{"message":"`+msg+`","errors":[{"message":"`+msg+`","status":`+strconv.Itoa(status)+`}]}`)
}

func throttled() replay.Response {
	return replay.Response{Status: 429, Headers: map[string]string{"Retry-After": "0"}}
}

func testClient(t *testing.T, rt http.RoundTripper) *Client {
	t.Helper()
	c, err := New(Config{Token: tToken, Project: tProject, Service: tService, Transport: rt})
	if err != nil {
		t.Fatal(err)
	}
	return c
}

func wantErr(t *testing.T, err, class error) {
	t.Helper()
	if !errors.Is(err, class) {
		t.Fatalf("err = %v, want %v", err, class)
	}
}

func pgConfig() map[string]string {
	return map[string]string{"name": "it_orders", "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
		"database.hostname": "orders.example.com", "database.port": "5432", "database.user": "debezium", "database.password": "pw-live-1",
		"database.dbname": "orders", "topic.prefix": "orders", "plugin.name": "pgoutput"}
}

type avCase struct {
	name      string
	responses []replay.Response
	run       func(t *testing.T, ctx context.Context, rt http.RoundTripper)
}

const serviceBody = `{"service":{"service_name":"connect-0","service_type":"kafka_connect","state":"RUNNING","plan":"startup-4","cloud_name":"aws-us-east-1",` +
	`"service_uri":"https://avnadmin:AVNS_pw-live-2@connect-0-project-0.aivencloud.com:443",` +
	`"service_uri_params":{"host":"connect-0-project-0.aivencloud.com","port":"443","user":"avnadmin","password":"AVNS_pw-live-2"},` +
	`"users":[{"username":"avnadmin","password":"AVNS_pw-live-2","type":"primary"}],` +
	`"connection_info":{"kafka_connect_uri":"https://avnadmin:AVNS_pw-live-2@connect-0-project-0.aivencloud.com:443"}}}`

var avCases = []avCase{
	{name: "service_get_secrets_dropped", responses: []replay.Response{jsonResp(200, serviceBody)},
		run: func(t *testing.T, ctx context.Context, rt http.RoundTripper) {
			s, err := testClient(t, rt).Service(ctx)
			if err != nil {
				t.Fatal(err)
			}
			if s.State != "RUNNING" || s.Type != "kafka_connect" || s.Plan != "startup-4" || len(s.Users) != 1 || s.Users[0] != "avnadmin" {
				t.Errorf("service = %+v", s)
			}
		}},
	{name: "service_unauthorized", responses: []replay.Response{apiErr(403, "Invalid token")},
		run: func(t *testing.T, ctx context.Context, rt http.RoundTripper) {
			_, err := testClient(t, rt).Service(ctx)
			wantErr(t, err, ErrForbidden)
		}},
	{name: "service_not_found", responses: []replay.Response{apiErr(404, "Service does not exist")},
		run: func(t *testing.T, ctx context.Context, rt http.RoundTripper) {
			_, err := testClient(t, rt).ServiceNamed(ctx, tKafka)
			wantErr(t, err, ErrNotFound)
		}},
	{name: "plugins_ok", responses: []replay.Response{jsonResp(200, `{"plugins":[{"class":"io.debezium.connector.mysql.MySqlConnector","title":"Debezium-MySQL","type":"source","version":"2.7.3.Final"},{"class":"io.aiven.connect.jdbc.JdbcSinkConnector","title":"JDBC sink","type":"sink","version":"6.10.0"}]}`)},
		run: func(t *testing.T, ctx context.Context, rt http.RoundTripper) {
			ps, err := testClient(t, rt).Plugins(ctx)
			if err != nil || len(ps) != 2 || ps[0].Type != "source" {
				t.Fatalf("plugins = %+v, %v", ps, err)
			}
		}},
	{name: "list_ok", responses: []replay.Response{jsonResp(200, `{"connectors":[{"name":"it_orders","config":{"connector.class":"io.debezium.connector.postgresql.PostgresConnector","database.password":"pw-live-1"}},{"name":"billing"}]}`)},
		run: func(t *testing.T, ctx context.Context, rt http.RoundTripper) {
			names, err := testClient(t, rt).ListConnectors(ctx)
			if err != nil || strings.Join(names, ",") != "billing,it_orders" {
				t.Fatalf("names = %v, %v", names, err)
			}
		}},
	{name: "config_redacted", responses: []replay.Response{jsonResp(200, `{"connectors":[{"name":"it_orders","config":{"connector.class":"io.debezium.connector.postgresql.PostgresConnector","database.password":"pw-live-1","database.user":"debezium"}}]}`)},
		run: func(t *testing.T, ctx context.Context, rt http.RoundTripper) {
			cfg, err := testClient(t, rt).Config(ctx, "it_orders")
			if err != nil || cfg["database.password"] != actions.RedactedValue || cfg["database.user"] != "debezium" {
				t.Fatalf("config = %v, %v", cfg, err)
			}
		}},
	{name: "status_running", responses: []replay.Response{jsonResp(200, `{"status":{"state":"RUNNING","tasks":[{"id":0,"state":"RUNNING","trace":""}]}}`)},
		run: func(t *testing.T, ctx context.Context, rt http.RoundTripper) {
			st, err := testClient(t, rt).Status(ctx, "it_orders")
			if err != nil || st.State != "RUNNING" || len(st.Tasks) != 1 {
				t.Fatalf("status = %+v, %v", st, err)
			}
		}},
	{name: "status_throttled", responses: []replay.Response{throttled(), throttled(), throttled(), throttled()},
		run: func(t *testing.T, ctx context.Context, rt http.RoundTripper) {
			_, err := testClient(t, rt).Status(ctx, "it_orders")
			wantErr(t, err, ErrThrottled)
		}},
	{name: "create_ok", responses: []replay.Response{jsonResp(200, `{"connector":{"name":"it_orders","config":{"database.password":"pw-live-1"},"plugin":{"class":"io.debezium.connector.postgresql.PostgresConnector"}}}`)},
		run: func(t *testing.T, ctx context.Context, rt http.RoundTripper) {
			name, err := testClient(t, rt).CreateConnector(ctx, pgConfig())
			if err != nil || name != "it_orders" {
				t.Fatalf("name = %q, %v", name, err)
			}
		}},
	{name: "create_plugin_unavailable", responses: []replay.Response{apiErr(400, "Connector class io.debezium.connector.postgresql.PostgresConnector is not available on this service")},
		run: func(t *testing.T, ctx context.Context, rt http.RoundTripper) {
			_, err := testClient(t, rt).CreateConnector(ctx, pgConfig())
			wantErr(t, err, ErrBadRequest)
			if !strings.Contains(err.Error(), "not available") {
				t.Errorf("message lost: %v", err)
			}
		}},
	{name: "create_conflict", responses: []replay.Response{apiErr(409, "Connector it_orders already exists")},
		run: func(t *testing.T, ctx context.Context, rt http.RoundTripper) {
			_, err := testClient(t, rt).CreateConnector(ctx, pgConfig())
			wantErr(t, err, ErrConflict)
		}},
	{name: "create_throttled_then_ok", responses: []replay.Response{throttled(), jsonResp(200, `{"connector":{"name":"it_orders"}}`)},
		run: func(t *testing.T, ctx context.Context, rt http.RoundTripper) {
			if _, err := testClient(t, rt).CreateConnector(ctx, pgConfig()); err != nil {
				t.Fatal(err)
			}
		}},
	{name: "integration_ok", responses: []replay.Response{jsonResp(200, `{"service_integration":{"service_integration_id":"0a1b2c3d-0000-4000-8000-000000000001","integration_type":"kafka_connect","source_service":"kafka-0","dest_service":"connect-0","active":true,"enabled":true}}`)},
		run: func(t *testing.T, ctx context.Context, rt http.RoundTripper) {
			id, err := testClient(t, rt).CreateIntegration(ctx, tKafka)
			if err != nil || id == "" {
				t.Fatalf("id = %q, %v", id, err)
			}
		}},
	{name: "integration_conflict", responses: []replay.Response{apiErr(409, "Integration already exists")},
		run: func(t *testing.T, ctx context.Context, rt http.RoundTripper) {
			_, err := testClient(t, rt).CreateIntegration(ctx, tKafka)
			wantErr(t, err, ErrConflict)
		}},
}

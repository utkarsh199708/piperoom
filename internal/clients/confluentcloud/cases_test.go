package confluentcloud

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

// The scrubber's placeholders, so a recording and a replay name the same
// environment, cluster and service account.
const (
	tEnv       = "env-00000"
	tCluster   = "lkc-00000"
	tPrincipal = "User:sa-00000"
	tEndpoint  = "https://pkc-00000.us-east-1.aws.confluent.cloud:443"
	// Test keys: never in a cassette (the Authorization header is dropped).
	tKey    = "TESTKEY000000000"
	tSecret = "test-secret"
)

// Scrub is the scrubber every Confluent cassette is recorded and replayed
// with.
var Scrub = replay.Scrubber{}

const cassetteSource = "hand-written from the ccloud-sdk-go-v2 OpenAPI documents (connect/v1, cmk/v2, kafkarest/v3; read 2026-10-30); requests as this client sends them; not from a live account"

func jsonResp(status int, body string) replay.Response {
	return replay.Response{Status: status, Headers: map[string]string{"Content-Type": "application/json"}, Body: body}
}

func connectErr(status int, msg string) replay.Response {
	return jsonResp(status, `{"error":{"code":`+strconv.Itoa(status)+`,"message":"`+msg+`"}}`)
}

func throttled() replay.Response {
	return replay.Response{Status: 429, Headers: map[string]string{"Retry-After": "0"}}
}

func testClient(t *testing.T, rt http.RoundTripper) *Client {
	t.Helper()
	c, err := New(Config{APIKey: tKey, APISecret: tSecret, Environment: tEnv, Cluster: tCluster, Transport: rt})
	if err != nil {
		t.Fatal(err)
	}
	return c
}

func testACL(t *testing.T, rt http.RoundTripper) *ACLClient {
	t.Helper()
	a, err := NewACLClient(ACLConfig{Endpoint: tEndpoint, Cluster: tCluster, Key: tKey, Secret: tSecret, Transport: rt})
	if err != nil {
		t.Fatal(err)
	}
	return a
}

func wantErr(t *testing.T, err, class error) {
	t.Helper()
	if !errors.Is(err, class) {
		t.Fatalf("err = %v, want %v", err, class)
	}
}

const pgConfig = `{"connector.class":"PostgresCdcSourceV2","name":"it_orders","kafka.auth.mode":"SERVICE_ACCOUNT","kafka.service.account.id":"sa-abc123","database.hostname":"orders.example.com","database.port":"5432","database.user":"debezium","database.password":"pw-live-1","database.dbname":"orders","topic.prefix":"orders","output.data.format":"JSON","tasks.max":"1"}`

func cfg() map[string]string {
	return map[string]string{"connector.class": "PostgresCdcSourceV2", "name": "it_orders", "kafka.auth.mode": "SERVICE_ACCOUNT", "kafka.service.account.id": "sa-00000",
		"database.hostname": "orders.example.com", "database.port": "5432", "database.user": "debezium", "database.password": "pw-live-1",
		"database.dbname": "orders", "topic.prefix": "orders", "output.data.format": "JSON", "tasks.max": "1"}
}

type ccCase struct {
	name      string
	responses []replay.Response
	run       func(t *testing.T, ctx context.Context, rt http.RoundTripper)
}

var ccCases = []ccCase{
	{name: "plugins_ok", responses: []replay.Response{jsonResp(200, `[{"class":"PostgresCdcSourceV2","type":"source","version":"0.1.0"},{"class":"MySqlCdcSourceV2","type":"source"},{"class":"S3_SINK","type":"sink"}]`)},
		run: func(t *testing.T, ctx context.Context, rt http.RoundTripper) {
			ps, err := testClient(t, rt).Plugins(ctx)
			if err != nil {
				t.Fatal(err)
			}
			if len(ps) != 3 || ps[0].Class != "PostgresCdcSourceV2" || ps[0].Type != "source" {
				t.Errorf("plugins = %+v", ps)
			}
		}},
	{name: "plugins_unauthorized", responses: []replay.Response{connectErr(401, "Unauthorized")},
		run: func(t *testing.T, ctx context.Context, rt http.RoundTripper) {
			_, err := testClient(t, rt).Plugins(ctx)
			wantErr(t, err, ErrForbidden)
		}},
	{name: "validate_errors", responses: []replay.Response{jsonResp(200, `{"name":"PostgresCdcSourceV2","groups":["Database"],"error_count":1,"configs":[`+
		`{"definition":{"name":"database.hostname"},"value":{"name":"database.hostname","value":"orders.example.com","errors":["Could not connect to orders.example.com:5432: connection timed out"]}},`+
		`{"definition":{"name":"database.password"},"value":{"name":"database.password","value":"pw-live-1","errors":[]}}]}`)},
		run: func(t *testing.T, ctx context.Context, rt http.RoundTripper) {
			v, err := testClient(t, rt).Validate(ctx, "PostgresCdcSourceV2", cfg())
			if err != nil {
				t.Fatal(err)
			}
			if v.ErrorCount != 1 || len(v.Errors) != 1 || v.Errors[0].Name != "database.hostname" || !strings.Contains(v.Errors[0].Errors[0], "timed out") {
				t.Errorf("validation = %+v", v)
			}
		}},
	{name: "validate_ok", responses: []replay.Response{jsonResp(200, `{"name":"PostgresCdcSourceV2","error_count":0,"configs":[{"value":{"name":"database.hostname","value":"orders.example.com","errors":[]}}]}`)},
		run: func(t *testing.T, ctx context.Context, rt http.RoundTripper) {
			v, err := testClient(t, rt).Validate(ctx, "PostgresCdcSourceV2", cfg())
			if err != nil || v.ErrorCount != 0 || len(v.Errors) != 0 {
				t.Fatalf("validation = %+v, %v", v, err)
			}
		}},
	{name: "validate_plugin_not_found", responses: []replay.Response{connectErr(404, "resource not found")},
		run: func(t *testing.T, ctx context.Context, rt http.RoundTripper) {
			_, err := testClient(t, rt).Validate(ctx, "NoSuchSource", cfg())
			wantErr(t, err, ErrNotFound)
		}},
	{name: "create_ok", responses: []replay.Response{jsonResp(201, `{"name":"it_orders","type":"source","config":`+pgConfig+`,"tasks":[]}`)},
		run: func(t *testing.T, ctx context.Context, rt http.RoundTripper) {
			c, err := testClient(t, rt).CreateConnector(ctx, "it_orders", cfg())
			if err != nil {
				t.Fatal(err)
			}
			if c.Name != "it_orders" || c.Config["database.password"] != actions.RedactedValue {
				t.Errorf("created = %+v", c)
			}
		}},
	{name: "create_conflict", responses: []replay.Response{connectErr(409, "Connector it_orders already exists")},
		run: func(t *testing.T, ctx context.Context, rt http.RoundTripper) {
			_, err := testClient(t, rt).CreateConnector(ctx, "it_orders", cfg())
			wantErr(t, err, ErrConflict)
		}},
	{name: "create_bad_request", responses: []replay.Response{connectErr(400, "Invalid configuration: connector.class is required")},
		run: func(t *testing.T, ctx context.Context, rt http.RoundTripper) {
			_, err := testClient(t, rt).CreateConnector(ctx, "it_orders", map[string]string{"name": "it_orders"})
			wantErr(t, err, ErrBadRequest)
			if !strings.Contains(err.Error(), "connector.class is required") {
				t.Errorf("message lost: %v", err)
			}
		}},
	{name: "create_throttled_then_ok", responses: []replay.Response{throttled(), jsonResp(201, `{"name":"it_orders","type":"source","config":`+pgConfig+`}`)},
		run: func(t *testing.T, ctx context.Context, rt http.RoundTripper) {
			if _, err := testClient(t, rt).CreateConnector(ctx, "it_orders", cfg()); err != nil {
				t.Fatal(err)
			}
		}},
	{name: "status_throttled", responses: []replay.Response{throttled(), throttled(), throttled(), throttled()},
		run: func(t *testing.T, ctx context.Context, rt http.RoundTripper) {
			_, err := testClient(t, rt).Status(ctx, "it_orders")
			wantErr(t, err, ErrThrottled)
		}},
	{name: "status_running", responses: []replay.Response{jsonResp(200, `{"name":"it_orders","type":"source","connector":{"state":"RUNNING","worker_id":"it_orders","trace":""},"tasks":[{"id":0,"state":"RUNNING","worker_id":"it_orders"}]}`)},
		run: func(t *testing.T, ctx context.Context, rt http.RoundTripper) {
			st, err := testClient(t, rt).Status(ctx, "it_orders")
			if err != nil {
				t.Fatal(err)
			}
			if st.State != "RUNNING" || len(st.Tasks) != 1 || st.Tasks[0].State != "RUNNING" || st.Type != "source" {
				t.Errorf("status = %+v", st)
			}
		}},
	{name: "status_provisioning", responses: []replay.Response{jsonResp(200, `{"name":"it_orders","type":"source","connector":{"state":"PROVISIONING","worker_id":"it_orders","trace":""},"tasks":[]}`)},
		run: func(t *testing.T, ctx context.Context, rt http.RoundTripper) {
			st, err := testClient(t, rt).Status(ctx, "it_orders")
			if err != nil || st.State != "PROVISIONING" || len(st.Tasks) != 0 {
				t.Fatalf("status = %+v, %v", st, err)
			}
		}},
	{name: "status_failed_acl", responses: []replay.Response{jsonResp(200, `{"name":"it_orders","type":"source","connector":{"state":"FAILED","worker_id":"it_orders","trace":"org.apache.kafka.common.errors.TopicAuthorizationException: Not authorized to access topics: [orders.public.orders]"},"tasks":[{"id":0,"state":"FAILED","worker_id":"it_orders","msg":"TopicAuthorizationException: Not authorized to access topics: [orders.public.orders]"}]}`)},
		run: func(t *testing.T, ctx context.Context, rt http.RoundTripper) {
			st, err := testClient(t, rt).Status(ctx, "it_orders")
			if err != nil {
				t.Fatal(err)
			}
			if st.State != "FAILED" || !strings.Contains(st.Tasks[0].Trace, "TopicAuthorizationException") {
				t.Errorf("status = %+v", st)
			}
		}},
	{name: "status_not_found", responses: []replay.Response{connectErr(404, "Connector it_none not found")},
		run: func(t *testing.T, ctx context.Context, rt http.RoundTripper) {
			_, err := testClient(t, rt).Status(ctx, "it_none")
			wantErr(t, err, ErrNotFound)
		}},
	{name: "list_ok", responses: []replay.Response{jsonResp(200, `["it_orders","billing-sink"]`)},
		run: func(t *testing.T, ctx context.Context, rt http.RoundTripper) {
			names, err := testClient(t, rt).ListConnectors(ctx)
			if err != nil || strings.Join(names, ",") != "billing-sink,it_orders" {
				t.Fatalf("names = %v, %v", names, err)
			}
		}},
	{name: "config_redacted", responses: []replay.Response{jsonResp(200, pgConfig)},
		run: func(t *testing.T, ctx context.Context, rt http.RoundTripper) {
			c, err := testClient(t, rt).Config(ctx, "it_orders")
			if err != nil || c["database.password"] != actions.RedactedValue || c["database.user"] != "debezium" {
				t.Fatalf("config = %v, %v", c, err)
			}
		}},
	{name: "cluster_ok", responses: []replay.Response{jsonResp(200, `{"api_version":"cmk/v2","kind":"Cluster","id":"lkc-abc123","spec":{"display_name":"orders","availability":"SINGLE_ZONE","cloud":"AWS","region":"us-east-1","kafka_bootstrap_endpoint":"SASL_SSL://pkc-abc12.us-east-1.aws.confluent.cloud:9092","http_endpoint":"https://pkc-abc12.us-east-1.aws.confluent.cloud:443","environment":{"id":"env-abc123"}},"status":{"phase":"PROVISIONED"}}`)},
		run: func(t *testing.T, ctx context.Context, rt http.RoundTripper) {
			cl, err := testClient(t, rt).Cluster(ctx)
			if err != nil {
				t.Fatal(err)
			}
			if cl.Phase != "PROVISIONED" || cl.Name != "orders" || !strings.HasPrefix(cl.HTTPEndpoint, "https://") {
				t.Errorf("cluster = %+v", cl)
			}
		}},
	{name: "cluster_forbidden", responses: []replay.Response{jsonResp(403, `{"errors":[{"status":"403","detail":"Forbidden Access"}]}`)},
		run: func(t *testing.T, ctx context.Context, rt http.RoundTripper) {
			_, err := testClient(t, rt).Cluster(ctx)
			wantErr(t, err, ErrForbidden)
			if !strings.Contains(err.Error(), "Forbidden Access") {
				t.Errorf("detail lost: %v", err)
			}
		}},
	{name: "acl_create_ok", responses: []replay.Response{{Status: 201}},
		run: func(t *testing.T, ctx context.Context, rt http.RoundTripper) {
			err := testACL(t, rt).CreateACL(ctx, ACL{ResourceType: "TOPIC", ResourceName: "orders", PatternType: "PREFIXED", Principal: tPrincipal, Host: "*", Operation: "READ", Permission: "ALLOW"})
			if err != nil {
				t.Fatal(err)
			}
		}},
	{name: "acl_create_forbidden", responses: []replay.Response{jsonResp(403, `{"error_code":40301,"message":"Request is not authorized"}`)},
		run: func(t *testing.T, ctx context.Context, rt http.RoundTripper) {
			err := testACL(t, rt).CreateACL(ctx, ACL{ResourceType: "GROUP", ResourceName: "connect-it_orders", PatternType: "LITERAL", Principal: tPrincipal, Host: "*", Operation: "DESCRIBE", Permission: "ALLOW"})
			wantErr(t, err, ErrForbidden)
		}},
}

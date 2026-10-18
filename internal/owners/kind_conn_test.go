package owners

import (
	"strings"
	"testing"
)

// A connections: block requires only the keys of its pipeline's kind
// (tasks/P-board.md P4a-2) and refuses a key of the other kind, as Load
// refuses a field of the other kind.
func TestConnectionsPerKind(t *testing.T) {
	const wh = "      warehouse_db_url: {env: REV_WAREHOUSE_DB_URL}\n"
	const cdcRest = "      source_db_url: {env: A_SOURCE}\n      kafka_brokers: {env: A_KAFKA}\n" +
		"      connect_url: {env: A_CONNECT}\n      schema_registry_url: {env: A_SR}\n"
	batch := func(conns string) string {
		return "pipelines:\n" + batchYAML("daily-revenue", "    dags: [revenue_daily]\n    connections:\n"+conns) +
			"layers:\n  warehouse: meera\n"
	}
	cdc := func(kind, conns string) string {
		return "pipelines:\n" + pipelineYAML("orders", kind+"    connections:\n"+conns) + sixLayers
	}
	cases := []struct {
		name    string
		yaml    string
		wantErr string // "" = loads
	}{
		{"batch needs only the warehouse", batch(wh), ""},
		{"batch may set the warehouse kind", batch(wh + "      warehouse_kind: {value: snowflake}\n"), ""},
		{"batch without the warehouse", batch("      warehouse_kind: {value: postgres}\n"), "connections: warehouse_db_url is required for kind batch"},
		{"batch with a kafka key", batch(wh + "      kafka_brokers: {env: REV_KAFKA}\n"), "connections.kafka_brokers is a cdc setting, but the pipeline kind is batch"},
		{"batch with a source key", batch(wh + "      source_db_url: {env: REV_SRC}\n"), "connections.source_db_url is a cdc setting"},
		{"batch with a connect secret", batch(wh + "      connect_password: {env: REV_PW}\n"), "connections.connect_password is a cdc setting"},
		{"batch with a registry TLS file", batch(wh + "      schema_registry_tls_ca_file: {file: /etc/ca.pem}\n"), "connections.schema_registry_tls_ca_file is a cdc setting"},
		{"batch with an unknown key", batch(wh + "      airflow_url: {env: REV_AIRFLOW}\n"), `unknown key "airflow_url"`},
		{"cdc needs all five", cdc("", wh+cdcRest), ""},
		{"explicit cdc needs all five", cdc("    kind: cdc\n", wh+cdcRest), ""},
		{"cdc with only the warehouse", cdc("", wh), "connections: connect_url is required for kind cdc"},
		{"cdc without the warehouse", cdc("", cdcRest), "connections: warehouse_db_url is required for kind cdc"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			o, err := Load(writeTemp(t, c.yaml))
			if c.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), c.wantErr) {
					t.Fatalf("err = %v, want it to contain %q", err, c.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("Load: %v", err)
			}
			if !o.Pipelines[0].HasConnections() {
				t.Fatal("the block was dropped")
			}
		})
	}
}

// Every connection key belongs to cdc, or to both kinds; a batch pipeline
// must be able to set the warehouse, and every required key must be one a
// pipeline of its kind may set.
func TestConnKeyKinds(t *testing.T) {
	for _, k := range ConnectionKeys() {
		info := connKeys[k]
		switch info.kind {
		case "", "cdc", "batch":
		default:
			t.Errorf("%s: kind %q", k, info.kind)
		}
	}
	for _, k := range []string{"warehouse_db_url", "warehouse_kind"} {
		if !connKeys[k].belongsTo("batch") || !connKeys[k].belongsTo("cdc") {
			t.Errorf("%s must belong to both kinds", k)
		}
	}
	required := func(kind string) []string {
		var out []string
		for _, k := range ConnectionKeys() {
			if info := connKeys[k]; info.required && info.belongsTo(kind) {
				out = append(out, k)
			}
		}
		return out
	}
	if got := strings.Join(required("batch"), ","); got != "warehouse_db_url" {
		t.Errorf("batch requires %s, want warehouse_db_url only", got)
	}
	if got := strings.Join(required("cdc"), ","); got != "connect_url,kafka_brokers,schema_registry_url,source_db_url,warehouse_db_url" {
		t.Errorf("cdc requires %s", got)
	}
}

// A batch block with a cdc key is refused without the reference's value in
// the error.
func TestBatchConnectionsNeverEchoValues(t *testing.T) {
	const secret = "s3cr3t-Pa55"
	y := "pipelines:\n" + batchYAML("daily-revenue", "    dags: [d1]\n    connections:\n"+
		"      warehouse_db_url: {env: WH}\n      kafka_sasl_password: {file: /run/"+secret+"}\n") + "layers:\n  warehouse: meera\n"
	_, err := Load(writeTemp(t, y))
	if err == nil || strings.Contains(err.Error(), secret) {
		t.Fatalf("err = %v, want a refusal without the value", err)
	}
}

func TestModelName(t *testing.T) {
	cases := []struct {
		ref  string
		want string // "" = not a model
	}{
		{"fct_revenue", "fct_revenue"},
		{"model.analytics.fct_revenue", "fct_revenue"},
		{"model.analytics.fct_revenue.v2", "fct_revenue"},
		{"source.analytics.raw.orders", ""},
		{"seed.analytics.customers", ""},
		{"test.analytics.not_null_fct_revenue_id.abc123", ""},
		{"snapshot.analytics.orders_snapshot", ""},
		{"model.analytics", ""},
		{"model..fct_revenue", ""},
		{"model.analytics.fct-revenue", ""},
		{"model.analytics.fct_revenue.v2.extra", ""},
		{"", ""},
		{"fct revenue", ""},
		{"fct_revenue\n", ""},
		{".fct_revenue", ""},
	}
	for _, c := range cases {
		got, ok := ModelName(c.ref)
		if got != c.want || ok != (c.want != "") {
			t.Errorf("ModelName(%q) = %q, %v; want %q", c.ref, got, ok, c.want)
		}
	}
}

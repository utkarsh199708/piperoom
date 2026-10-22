package owners

import (
	"errors"
	"slices"
	"strings"
	"testing"
)

// stagingDoc is a two-pipeline file: orders uses the process-wide settings,
// billing has its own connections: (conns("B")). extra is appended to the
// orders pipeline (a staging: block, require_staging:).
func stagingDoc(extra string) string {
	return `
pipelines:
  - id: orders
    topic_prefix: pg
    source_tables: [public.orders]
    connectors: [orders-src]
` + extra + `
  - id: billing
    topic_prefix: bl
    source_tables: [public.billing]
    connectors: [billing-src]` + conns("B") + `
layers:
  postgres: alice
`
}

func TestStagingLoad(t *testing.T) {
	o, err := Load(writeTemp(t, stagingDoc(`
    staging:
      connections:
        connect_url: {value: "http://connect-staging:8083"}
        schema_registry_url: {env: STAGING_SR_URL}
        connect_password: {env: STAGING_CONNECT_PASSWORD}
        kafka_brokers: {env: STAGING_KAFKA}
    require_staging: [connect_update_connector_config]`)))
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	p, _ := o.Pipeline("orders")
	if !p.HasStaging() || p.HasConnections() {
		t.Fatalf("HasStaging=%v HasConnections=%v", p.HasStaging(), p.HasConnections())
	}
	if got := p.RequireStagingList(); !slices.Equal(got, []string{"connect_update_connector_config"}) {
		t.Fatalf("RequireStagingList = %v", got)
	}
	if got := p.stagingRefs(); !slices.Equal(got, []string{"env:STAGING_CONNECT_PASSWORD", "env:STAGING_KAFKA", "env:STAGING_SR_URL"}) {
		t.Fatalf("stagingRefs = %v (the literal URL must not count)", got)
	}
	b, _ := o.Pipeline("billing")
	if b.HasStaging() || len(b.RequireStagingList()) != 0 || b.RequireStagingList() == nil {
		t.Fatalf("billing: HasStaging=%v list=%v", b.HasStaging(), b.RequireStagingList())
	}
}

func TestStagingLoadErrors(t *testing.T) {
	cases := []struct {
		name, extra, want string
		prod              bool // wraps ErrStagingIsProduction
	}{
		{"require without staging", `
    require_staging: [connect_update_connector_config]`, "require_staging: needs a staging: block", false},
		{"bad action name", `
    staging: {connections: {connect_url: {env: S_CONNECT}}}
    require_staging: [Connect-Update]`, "require_staging: \"Connect-Update\" is not an action name", false},
		{"duplicate action", `
    staging: {connections: {connect_url: {env: S_CONNECT}}}
    require_staging: [a, a]`, `require_staging lists "a" twice`, false},
		{"null require entry", `
    staging: {connections: {connect_url: {env: S_CONNECT}}}
    require_staging:
      -
      - a`, "require_staging: has an empty (null) entry", false},
		{"null staging", `
    staging:`, "staging: is empty (null)", false},
		{"unknown staging field", `
    staging: {connection: {connect_url: {env: S_CONNECT}}}`, "staging: unknown field \"connection\"", false},
		{"no connections", `
    staging: {connections: }`, "staging: needs a connections: block", false},
		{"source key", `
    staging: {connections: {connect_url: {env: S_CONNECT}, source_db_url: {env: S_DB}}}`, "key \"source_db_url\" is not allowed", false},
		{"warehouse key", `
    staging: {connections: {connect_url: {env: S_CONNECT}, warehouse_db_url: {env: S_WH}}}`, "key \"warehouse_db_url\" is not allowed", false},
		{"kafka only", `
    staging: {connections: {kafka_brokers: {env: S_KAFKA}}}`, "set connect_url or schema_registry_url", false},
		{"password literal", `
    staging: {connections: {connect_url: {env: S_CONNECT}, connect_password: {value: hunter2}}}`, "staging.connections.connect_password: value: is allowed only for non-secret settings", false},
		{"url with login", `
    staging: {connections: {connect_url: {value: "http://u:hunter2@connect-staging:8083"}}}`, "must not carry a login", false},
		{"url not http", `
    staging: {connections: {schema_registry_url: {value: "connect-staging:8081"}}}`, "must be an http or https URL", false},
		{"two sources", `
    staging: {connections: {connect_url: {env: S_CONNECT, file: /run/s}}}`, "set exactly one of env, file or value", false},
		{"relative file", `
    staging: {connections: {connect_url: {file: run/s}}}`, "must be an absolute path", false},
		{"login without url", `
    staging: {connections: {connect_url: {env: S_CONNECT}, schema_registry_password: {env: S_SR_PW}}}`, "schema_registry_password needs staging.connections.schema_registry_url", false},
		{"kafka login without brokers", `
    staging: {connections: {connect_url: {env: S_CONNECT}, kafka_sasl_password: {env: S_K_PW}}}`, "kafka_sasl_password needs staging.connections.kafka_brokers", false},
		{"process connect ref", `
    staging: {connections: {connect_url: {env: CONNECT_URL}}}`, "is the same reference as the process-wide CONNECT_URL", true},
		{"process registry ref, other key", `
    staging: {connections: {connect_url: {env: schema_registry_url}}}`, "is the same reference as the process-wide SCHEMA_REGISTRY_URL", true},
		{"other pipeline's ref", `
    staging: {connections: {schema_registry_url: {env: B_SR_URL}}}`, `is the same reference as pipeline "billing" connections.schema_registry_url`, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := Load(writeTemp(t, stagingDoc(tc.extra)))
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("Load error = %v, want %q", err, tc.want)
			}
			if got := errors.Is(err, ErrStagingIsProduction); got != tc.prod {
				t.Fatalf("errors.Is(ErrStagingIsProduction) = %v, want %v", got, tc.prod)
			}
			if strings.Contains(err.Error(), "hunter2") {
				t.Fatalf("error quotes a secret: %v", err)
			}
		})
	}
}

// TestStagingBatchRefused: a batch pipeline has no Connect to stage on.
func TestStagingBatchRefused(t *testing.T) {
	_, err := Load(writeTemp(t, `
pipelines:
  - id: nightly
    kind: batch
    dags: [load_orders]
    staging: {connections: {connect_url: {env: S_CONNECT}}}
`))
	if err == nil || !strings.Contains(err.Error(), "staging: is not supported for kind batch") {
		t.Fatalf("Load error = %v", err)
	}
}

// TestStagingSharedRefs: a staging reference counts for the shared-reference
// rule like a connections: one.
func TestStagingSharedRefs(t *testing.T) {
	doc := `
tenants:
  a: {}
  b: {}
pipelines:
  - id: pa
    tenant: a
    topic_prefix: pa
    source_tables: [public.a]
    connectors: [pa-connector]` + conns("A") + `
    staging: {connections: {connect_url: {env: STAGING_CONNECT_URL}, connect_password: {env: SHARED_STAGING_PW}}}
  - id: pb
    tenant: b
    topic_prefix: pb
    source_tables: [public.b]
    connectors: [pb-connector]` + conns("B") + `
    staging: {connections: {connect_url: {env: STAGING_CONNECT_URL_B}, connect_password: {env: SHARED_STAGING_PW}}}
`
	_, err := Load(writeTemp(t, doc))
	if err == nil || !strings.Contains(err.Error(), "env:SHARED_STAGING_PW is used by tenants a, b") {
		t.Fatalf("Load error = %v", err)
	}
}

func TestValidateRequireStaging(t *testing.T) {
	o, err := Load(writeTemp(t, stagingDoc(`
    staging: {connections: {connect_url: {env: S_CONNECT}}}
    require_staging: [connect_update_connector_config]`)))
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if err := o.ValidateRequireStaging([]string{"connect_update_connector_config", "schema_register"}); err != nil {
		t.Fatalf("ValidateRequireStaging: %v", err)
	}
	for _, names := range [][]string{nil, {"schema_register"}} {
		err := o.ValidateRequireStaging(names)
		if err == nil || !strings.Contains(err.Error(), `pipeline "orders": require_staging: "connect_update_connector_config" is not an action that can be staged`) {
			t.Fatalf("ValidateRequireStaging(%v) = %v", names, err)
		}
	}
	var nilOwners *Owners
	if err := nilOwners.ValidateRequireStaging(nil); err != nil {
		t.Fatalf("nil Owners: %v", err)
	}
}

// TestCheckStagingOverlap is TC1: a staging URL that resolves to a
// production endpoint, also through scheme default ports, is refused, and
// the error names both settings and no value.
func TestCheckStagingOverlap(t *testing.T) {
	o, err := Load(writeTemp(t, stagingDoc(`
    staging:
      connections:
        connect_url: {env: S_CONNECT}
        schema_registry_url: {value: "http://registry-staging:8081"}
        kafka_brokers: {env: S_KAFKA}`)))
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	process := map[string]string{"connect_url": "http://connect:8083", "schema_registry_url": "http://registry:8081"}
	billing := map[string]map[string]string{"billing": {"connect_url": "https://connect.billing.example", "schema_registry_url": "http://sr.billing.example:8081"}}
	cases := []struct {
		name    string
		staging map[string]string
		want    string // "" = no error
	}{
		{"distinct", map[string]string{"connect_url": "http://connect-staging:8083"}, ""},
		{"same as process", map[string]string{"connect_url": "http://CONNECT:8083/"}, "the process-wide CONNECT_URL"},
		{"scheme default port https", map[string]string{"connect_url": "https://connect.billing.example:443/x"}, `pipeline "billing" connections.connect_url`},
		{"scheme default port http", map[string]string{"connect_url": "http://user:pw@sr.billing.example:8081"}, `pipeline "billing" connections.schema_registry_url`},
		{"registry literal vs process", nil, ""},
		{"kafka may be shared", map[string]string{"connect_url": "http://connect-staging:8083", "kafka_brokers": "kafka:9092"}, ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := o.CheckStagingOverlap(StagingEndpoints{
				Process: process, Production: billing,
				Staging: map[string]map[string]string{"orders": tc.staging},
			})
			if tc.want == "" {
				if err != nil {
					t.Fatalf("CheckStagingOverlap: %v", err)
				}
				return
			}
			if err == nil || !errors.Is(err, ErrStagingIsProduction) || !strings.Contains(err.Error(), tc.want) ||
				!strings.Contains(err.Error(), `pipeline "orders": staging.connections.connect_url`) {
				t.Fatalf("CheckStagingOverlap = %v, want %q", err, tc.want)
			}
			for _, v := range []string{"CONNECT:8083", "connect.billing.example", "pw@"} {
				if strings.Contains(err.Error(), v) {
					t.Fatalf("error quotes a value (%s): %v", v, err)
				}
			}
		})
	}
	// The {value:} literal is compared when no resolved staging value is given.
	err = o.CheckStagingOverlap(StagingEndpoints{Process: map[string]string{"schema_registry_url": "http://REGISTRY-staging:8081/x"}})
	if err == nil || !strings.Contains(err.Error(), "staging.connections.schema_registry_url") {
		t.Fatalf("literal vs process = %v", err)
	}
	// http://registry-staging is port 80, the literal says 8081: distinct.
	if err := o.CheckStagingOverlap(StagingEndpoints{Process: map[string]string{"schema_registry_url": "http://registry-staging"}}); err != nil {
		t.Fatalf("other port refused: %v", err)
	}
}

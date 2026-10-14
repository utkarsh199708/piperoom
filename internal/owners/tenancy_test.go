package owners

import (
	"slices"
	"strings"
	"testing"
)

// conns is a full, valid connections: block whose references all start with
// prefix, so two pipelines with different prefixes share nothing.
func conns(prefix string) string {
	return `
    connections:
      source_db_url: {env: ` + prefix + `_SOURCE_DB_URL}
      warehouse_db_url: {file: /run/secrets/` + strings.ToLower(prefix) + `/warehouse}
      kafka_brokers: {env: ` + prefix + `_KAFKA_BROKERS}
      connect_url: {env: ` + prefix + `_CONNECT_URL}
      schema_registry_url: {env: ` + prefix + `_SR_URL}
      kafka_security_protocol: {value: SASL_SSL}`
}

// twoTenants is the A/B fixture: pipeline pa in tenant a (owned by alice) and
// pb in tenant b (owned by bob), each with its own connections. Top-level
// layers: is empty, so no owner is in both tenants.
func twoTenants() string {
	return `
tenants:
  a: {members: [user:ann, group:team-a]}
  b: {members: [user:ben]}
pipelines:
  - id: pa
    tenant: a
    topic_prefix: pa
    source_tables: [public.a]
    connectors: [pa-connector]` + conns("A") + `
    owners:
      postgres: {approvers: [user:alice], backup: [group:a-backup]}
  - id: pb
    tenant: b
    topic_prefix: pb
    source_tables: [public.b]
    connectors: [pb-connector]` + conns("B") + `
    owners:
      kafka: {approvers: [user:bob]}
auditors: [user:audrey]
`
}

func TestLoadTwoTenants(t *testing.T) {
	o, err := Load(writeTemp(t, twoTenants()))
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if !o.TenancyOn() {
		t.Fatal("TenancyOn = false with a tenants: block")
	}
	if got := o.TenantNames(); !slices.Equal(got, []string{"a", "b"}) {
		t.Fatalf("TenantNames = %v", got)
	}
	pa, _ := o.Pipeline("pa")
	if pa.TenantName() != "a" || !pa.HasConnections() {
		t.Fatalf("pa tenant=%q has connections=%v", pa.TenantName(), pa.HasConnections())
	}
	if r := pa.Connections["warehouse_db_url"]; r.File != "/run/secrets/a/warehouse" {
		t.Fatalf("warehouse ref = %+v", r)
	}
	if got := pa.ConnRefs(); !slices.Contains(got, "env:A_SOURCE_DB_URL") || !slices.Contains(got, "file:/run/secrets/a/warehouse") || len(got) != 5 {
		t.Fatalf("pa refs = %v (the literal must not count)", got)
	}
}

func TestMembership(t *testing.T) {
	o, err := Load(writeTemp(t, twoTenants()))
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	cases := []struct {
		name   string
		user   string
		groups []string
		a, b   bool
	}{
		{"listed user", "ann", nil, true, false},
		{"listed group", "zed", []string{"team-a"}, true, false},
		{"pipeline approver", "alice", nil, true, false},
		{"pipeline backup group", "zed", []string{"a-backup"}, true, false},
		{"other tenant's owner", "bob", nil, false, true},
		{"other tenant's member", "ben", nil, false, true},
		{"auditor is not a member", "audrey", nil, false, false},
		{"stranger", "mallory", []string{"team-b"}, false, false},
		{"empty user", "", nil, false, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := o.CanSeePipeline("pa", tc.user, tc.groups); got != tc.a {
				t.Errorf("CanSeePipeline(pa) = %v, want %v", got, tc.a)
			}
			if got := o.CanSeePipeline("pb", tc.user, tc.groups); got != tc.b {
				t.Errorf("CanSeePipeline(pb) = %v, want %v", got, tc.b)
			}
			set, all := o.VisiblePipelines(tc.user, tc.groups)
			if all || set["pa"] != tc.a || set["pb"] != tc.b {
				t.Errorf("VisiblePipelines = %v all=%v", set, all)
			}
		})
	}
	if o.CanSeePipeline("unknown", "ann", nil) || o.CanSeePipeline("", "alice", nil) {
		t.Fatal("a pipeline owners.yaml does not define must be in no tenant")
	}
}

// TestInheritedLayersJoinEveryTenant pins the documented rule: a top-level
// layers: owner owns that layer in every pipeline that does not override it.
func TestInheritedLayersJoinEveryTenant(t *testing.T) {
	y := strings.Replace(twoTenants(), "auditors: [user:audrey]", "auditors: [user:audrey]\nlayers:\n  warehouse: user:wendy", 1)
	o, err := Load(writeTemp(t, y))
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if !o.CanSeePipeline("pa", "wendy", nil) || !o.CanSeePipeline("pb", "wendy", nil) {
		t.Fatal("a top-level layers: owner must be in every tenant that inherits the layer")
	}
	if got := o.InheritedLayers("pa"); !slices.Equal(got, []string{"warehouse"}) {
		t.Fatalf("InheritedLayers(pa) = %v", got)
	}
	if got := o.InheritedLayers("nope"); got != nil {
		t.Fatalf("InheritedLayers(unknown) = %v", got)
	}
}

// TestNoTenantsBackCompat: an owners.yaml from before tenancy loads, the filter
// is off and every pipeline uses the process-wide settings.
func TestNoTenantsBackCompat(t *testing.T) {
	o, err := Load(writeTemp(t, validYAML))
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if o.TenancyOn() {
		t.Fatal("TenancyOn without a tenants: block")
	}
	for _, who := range []string{"priya", "mallory", ""} {
		if !o.CanSeePipeline("orders", who, nil) || !o.CanSeePipeline("unknown", who, nil) {
			t.Fatalf("filter off: %q must see everything", who)
		}
	}
	if set, all := o.VisiblePipelines("mallory", nil); !all || set != nil {
		t.Fatalf("VisiblePipelines = %v, %v; want nil, true", set, all)
	}
	p, _ := o.Pipeline("orders")
	if p.TenantName() != DefaultTenant || p.HasConnections() {
		t.Fatalf("tenant %q, connections %v", p.TenantName(), p.HasConnections())
	}
	if got := p.ConnRefs(); !slices.Equal(got, ProcessRefs()) || !slices.Contains(got, "env:SOURCE_DB_URL") {
		t.Fatalf("refs = %v, want the process-wide variables", got)
	}
	// Two pipelines with no connections in the one default tenant share the
	// process-wide settings freely.
	two := strings.Replace(validYAML, "layers:", `  - {id: refunds, topic_prefix: rf, source_tables: [public.refunds], connectors: [rf]}
layers:`, 1)
	if _, err := Load(writeTemp(t, two)); err != nil {
		t.Fatalf("two default-tenant pipelines: %v", err)
	}
}

func TestTenancyValidation(t *testing.T) {
	head := "pipelines:\n  - {id: p1, topic_prefix: pg, source_tables: [public.t], connectors: [c1]"
	cases := []struct {
		name    string
		yaml    string
		wantErr string
	}{
		{"tenants is a list", head + "}\ntenants: [a, b]\n", "cannot unmarshal"},
		{"tenants empty", head + "}\ntenants: {}\n", "tenants: is empty"},
		{"tenant unknown key", head + ", tenant: a}\ntenants:\n  a: {member: [user:x]}\n", `unknown field "member"`},
		{"tenant members not principals", head + ", tenant: a}\ntenants:\n  a: {members: [grp:x]}\n", "bad prefix"},
		{"tenant name bad", head + "}\ntenants:\n  'a b': {}\n", "must match"},
		{"pipeline tenant undeclared", head + ", tenant: c}\ntenants:\n  a: {}\n", `tenant "c" is not declared`},
		{"pipeline tenant without block", head + ", tenant: a}\n", "needs a tenants: block"},
		{"connections not a mapping", head + ", connections: [x]}\n", "want a mapping"},
		{"connections empty", head + ", connections: {}}\n", "connections: is empty"},
		{"connections unknown key", head + ", connections: {source_db: {env: X}}}\n", `unknown key "source_db"`},
		{"connections ref unknown field", head + ", connections: {source_db_url: {envv: X}}}\n", `unknown field "envv"`},
		{"connections two refs", head + ", connections: {source_db_url: {env: X, file: /x}}}\n", "exactly one"},
		{"connections no ref", head + ", connections: {source_db_url: {}}}\n", "exactly one"},
		{"connections literal secret", head + ", connections: {kafka_sasl_password: {value: hunter2}}}\n", "only for non-secret"},
		{"connections relative file", head + ", connections: {source_db_url: {file: secrets/dsn}}}\n", "absolute path"},
		{"connections bad env name", head + ", connections: {source_db_url: {env: 1BAD}}}\n", "not a variable name"},
		{"connections missing required", head + ", connections: {source_db_url: {env: X}}}\n", "is required"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := Load(writeTemp(t, tc.yaml))
			if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("err = %v, want %q", err, tc.wantErr)
			}
		})
	}
}

// TestConnectionsNeverEchoSecrets: a DSN or password pasted into owners.yaml
// is refused, and the error does not carry it into the start-up log.
func TestConnectionsNeverEchoSecrets(t *testing.T) {
	const secret = "s3cr3t-Pa55"
	head := "pipelines:\n  - {id: p1, topic_prefix: pg, source_tables: [public.t], connectors: [c1]"
	for name, y := range map[string]string{
		"plain dsn":        head + ", connections: {source_db_url: 'postgres://u:" + secret + "@h/db'}}\n",
		"block as string":  head + ", connections: 'postgres://u:" + secret + "@h/db'}\n",
		"secret in env":    head + ", connections: {source_db_url: {env: 'postgres://u:" + secret + "@h'}}}\n",
		"secret in file":   head + ", connections: {source_db_url: {file: 'u:" + secret + "@h'}}}\n",
		"secret as value":  head + ", connections: {connect_password: {value: " + secret + "}}}\n",
		"nested ref value": head + ", connections: {source_db_url: {env: [" + secret + "]}}}\n",
		// A short plain scalar: yaml.v3's own error quotes values of up to
		// 10 characters whole (and the first 7 of longer ones).
		"short plain password": "pipelines:\n  - id: p1\n    topic_prefix: pg\n    source_tables: [public.t]\n    connectors: [c1]\n    connections:\n      kafka_sasl_password: hunter2\n",
		// A DSN pasted where a name belongs.
		"dsn as a connections key": head + ", connections: {'postgres://u:" + secret + "@h': {env: X}}}\n",
		"dsn as a reference key":   head + ", connections: {source_db_url: {'postgres://u:" + secret + "@h': x}}}\n",
		"dsn as a duplicate key":   head + ", connections: {'u:" + secret + "': {env: X}, 'u:" + secret + "': {env: Y}}}\n",
		"dsn as a tenant":          head + ", tenant: 'postgres://u:" + secret + "@h'}\ntenants:\n  a: {}\n",
		"dsn as a tenant name":     head + "}\ntenants:\n  'postgres://u:" + secret + "@h': {}\n",
		"dsn as a tenants key":     head + "}\ntenants:\n  a: {'u:" + secret + "': x}\n",
		"dsn as a watch key":       head + ", watch: {'u:" + secret + "': x}}\n",
	} {
		t.Run(name, func(t *testing.T) {
			_, err := Load(writeTemp(t, y))
			if err == nil {
				t.Fatal("want an error")
			}
			// Not even yaml.v3's 7-character prefix of a longer value.
			for _, leak := range []string{secret[:7], "hunter2"} {
				if strings.Contains(err.Error(), leak) {
					t.Fatalf("error echoes the secret: %v", err)
				}
			}
		})
	}
}

func TestSharedConnectionRefusal(t *testing.T) {
	base := twoTenants()
	if _, err := Load(writeTemp(t, base)); err != nil {
		t.Fatalf("separate connections: %v", err)
	}
	// pb reads its source DSN from A's variable (different case: Windows
	// variable names ignore case).
	sharedEnv := strings.Replace(base, "{env: B_SOURCE_DB_URL}", "{env: a_source_db_url}", 1)
	_, err := Load(writeTemp(t, sharedEnv))
	if err == nil || !strings.Contains(err.Error(), "env:A_SOURCE_DB_URL") || !strings.Contains(err.Error(), "a, b") {
		t.Fatalf("shared env: err = %v", err)
	}
	// Same file through a path that only cleans to the same name.
	sharedFile := strings.Replace(base, "{file: /run/secrets/b/warehouse}", "{file: /run/secrets/x/../a/warehouse}", 1)
	if _, err := Load(writeTemp(t, sharedFile)); err == nil || !strings.Contains(err.Error(), "file:/run/secrets/a/warehouse") {
		t.Fatalf("shared file: err = %v", err)
	}
	// shared_connections on one pipeline only is not enough.
	oneSide := strings.Replace(sharedEnv, "    tenant: a\n", "    tenant: a\n    shared_connections: true\n", 1)
	if _, err := Load(writeTemp(t, oneSide)); err == nil || !strings.Contains(err.Error(), "shared_connections") {
		t.Fatalf("shared on one side: err = %v", err)
	}
	both := strings.Replace(oneSide, "    tenant: b\n", "    tenant: b\n    shared_connections: true\n", 1)
	if _, err := Load(writeTemp(t, both)); err != nil {
		t.Fatalf("shared_connections on both: %v", err)
	}
}

// TestSharedProcessWideRefusal: two tenants whose pipelines both fall back to
// the process-wide settings share one credential set; that is refused too.
func TestSharedProcessWideRefusal(t *testing.T) {
	y := `
tenants:
  a: {}
  b: {}
pipelines:
  - {id: pa, tenant: a, topic_prefix: pa, source_tables: [public.a], connectors: [ca]}
  - {id: pb, tenant: b, topic_prefix: pb, source_tables: [public.b], connectors: [cb]}
`
	if _, err := Load(writeTemp(t, y)); err == nil || !strings.Contains(err.Error(), "env:") || !strings.Contains(err.Error(), "pa, pb") {
		t.Fatalf("err = %v, want a shared process-wide reference", err)
	}
	// One process-wide pipeline and one with its own connections: nothing shared.
	own := strings.Replace(y, "connectors: [cb]}", "connectors: [cb], connections: {source_db_url: {env: B_SRC}, warehouse_db_url: {env: B_WH}, kafka_brokers: {env: B_K}, connect_url: {env: B_C}, schema_registry_url: {env: B_SR}}}", 1)
	if _, err := Load(writeTemp(t, own)); err != nil {
		t.Fatalf("process-wide + own connections: %v", err)
	}
	// Unless B names a process-wide variable itself.
	named := strings.Replace(own, "{env: B_SRC}", "{env: SOURCE_DB_URL}", 1)
	if _, err := Load(writeTemp(t, named)); err == nil || !strings.Contains(err.Error(), "env:SOURCE_DB_URL") {
		t.Fatalf("own connection naming a process-wide variable: err = %v", err)
	}
}

func TestCheckSharedRefsExtra(t *testing.T) {
	o, err := Load(writeTemp(t, twoTenants()))
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	extra := map[string][]string{"pa": {"aws:process-identity"}, "pb": {"aws:process-identity"}}
	if err := o.CheckSharedRefs(extra); err == nil || !strings.Contains(err.Error(), "aws:process-identity") {
		t.Fatalf("err = %v", err)
	}
	if err := o.CheckSharedRefs(map[string][]string{"pa": {"aws:process-identity"}}); err != nil {
		t.Fatalf("one tenant only: %v", err)
	}
}

func TestOnlyView(t *testing.T) {
	o, err := Load(writeTemp(t, twoTenants()))
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	v := o.Only("pb", "nope")
	if _, ok := v.Pipeline("pa"); ok {
		t.Fatal("the view must not resolve pa")
	}
	if p, ok := v.Pipeline("pb"); !ok || p.Watch().Enabled != o.Pipelines[1].Watch().Enabled {
		t.Fatal("the view must resolve pb with its validated watch settings")
	}
	if len(o.Pipelines) != 2 {
		t.Fatal("Only changed the original")
	}
	if (*Owners)(nil).Only("x") != nil {
		t.Fatal("nil Only")
	}
}

func TestNamedPrincipalsIncludeTenantMembers(t *testing.T) {
	o, err := Load(writeTemp(t, twoTenants()))
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	var names []string
	for _, p := range o.NamedPrincipals() {
		names = append(names, p.String())
	}
	for _, want := range []string{"user:ann", "group:team-a", "user:ben"} {
		if !slices.Contains(names, want) {
			t.Fatalf("NamedPrincipals = %v, missing %s (solo mode must see tenant members)", names, want)
		}
	}
}

func TestConnectionKeys(t *testing.T) {
	keys := ConnectionKeys()
	if !slices.IsSorted(keys) || len(keys) != len(connKeys) {
		t.Fatalf("keys = %v", keys)
	}
	if !IsPathKey("kafka_tls_ca_file") || IsPathKey("kafka_sasl_password") || IsPathKey("nope") {
		t.Fatal("IsPathKey")
	}
}

// TestNullBlocksRefused: a tenants:, connections: or tenant: key written with
// no value (empty, ~, or every entry commented out) is refused. yaml.v3 skips
// UnmarshalYAML for null, so without the check a lost connections: block
// would silently use the process-wide credentials, and a lost tenants: block
// would turn the filter off.
func TestNullBlocksRefused(t *testing.T) {
	pipe := "pipelines:\n  - id: pa\n    topic_prefix: pa\n    source_tables: [public.a]\n    connectors: [c1]\n"
	cases := map[string]struct{ yaml, want string }{
		"tenants empty":     {"tenants:\n" + pipe, "tenants: is empty (null)"},
		"tenants tilde":     {"tenants: ~\n" + pipe, "tenants: is empty (null)"},
		"tenants null":      {"tenants: null\n" + pipe, "tenants: is empty (null)"},
		"tenants commented": {"tenants:\n#  a: {members: [user:ann]}\n#  b: {}\n" + pipe, "tenants: is empty (null)"},
		"connections empty": {"tenants:\n  a: {}\n" + pipe + "    tenant: a\n    connections:\n", `pipeline "pa": connections: is empty (null)`},
		"connections tilde": {pipe + "    connections: ~\n", `pipeline "pa": connections: is empty (null)`},
		"connections commented": {"tenants:\n  a: {}\n" + pipe + "    tenant: a\n    connections:\n#      source_db_url: {env: A_SRC}\n#      kafka_brokers: {env: A_K}\n",
			`pipeline "pa": connections: is empty (null)`},
		"connections flow, no id": {"pipelines:\n  - {topic_prefix: pa, source_tables: [public.a], connectors: [c1], connections: }\n", "pipelines[0]: connections: is empty (null)"},
		"tenant empty":            {"tenants:\n  a: {}\n" + pipe + "    tenant:\n", `pipeline "pa": tenant: is empty (null)`},
		"tenant commented":        {"tenants:\n  a: {}\n" + pipe + "    tenant: # a\n", `pipeline "pa": tenant: is empty (null)`},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			_, err := Load(writeTemp(t, tc.yaml))
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("err = %v, want %q", err, tc.want)
			}
		})
	}
	// An absent key is fine: one tenant, process-wide settings.
	if _, err := Load(writeTemp(t, pipe)); err != nil {
		t.Fatalf("no blocks at all: %v", err)
	}
	// An empty file is still an empty config.
	if _, err := Load(writeTemp(t, "")); err != nil {
		t.Fatalf("empty file: %v", err)
	}
}

// TestDLQTopicNamesPerCluster: a DLQ topic name is unique per Kafka cluster,
// so two tenants on their own clusters may both declare "dlq"; on one
// cluster (one kafka_brokers reference) the old rule stands.
func TestDLQTopicNamesPerCluster(t *testing.T) {
	dlq := func(y, pipe, topic string) string {
		return strings.Replace(y, "    connectors: ["+pipe+"-connector]",
			"    connectors: ["+pipe+"-connector]\n    consumer_groups: ["+pipe+"-sync]\n    watch:\n      dlq_topics:\n        - {topic: "+topic+", written_by: "+pipe+"-sync}", 1)
	}
	two := dlq(dlq(twoTenants(), "pa", "dlq"), "pb", "dlq")
	if _, err := Load(writeTemp(t, two)); err != nil {
		t.Fatalf("one DLQ name on two clusters: %v", err)
	}
	// pb's DLQ named like pa's CDC topic, on another cluster: fine.
	if _, err := Load(writeTemp(t, dlq(twoTenants(), "pb", "pa.public.a"))); err != nil {
		t.Fatalf("another cluster's CDC topic name: %v", err)
	}
	// One cluster: pb reads its brokers from A's variable (shared on purpose).
	one := strings.Replace(two, "{env: B_KAFKA_BROKERS}", "{env: A_KAFKA_BROKERS}", 1)
	one = strings.ReplaceAll(one, "    tenant: a\n", "    tenant: a\n    shared_connections: true\n")
	one = strings.ReplaceAll(one, "    tenant: b\n", "    tenant: b\n    shared_connections: true\n")
	if _, err := Load(writeTemp(t, one)); err == nil || !strings.Contains(err.Error(), `"dlq" is declared by "pa" and "pb"`) {
		t.Fatalf("one DLQ name on one cluster: err = %v", err)
	}
}

// TestCABundlesAreNotSharedRefs: a CA bundle is public and one corporate CA
// for every team is normal, so it is no connection reference.
func TestCABundlesAreNotSharedRefs(t *testing.T) {
	y := strings.ReplaceAll(twoTenants(), "      kafka_security_protocol: {value: SASL_SSL}",
		"      kafka_security_protocol: {value: SASL_SSL}\n      kafka_tls_ca_file: {file: /etc/ssl/corp-ca.pem}\n      connect_tls_ca_file: {env: CORP_CA}")
	o, err := Load(writeTemp(t, y))
	if err != nil {
		t.Fatalf("two tenants on one CA: %v", err)
	}
	pa, _ := o.Pipeline("pa")
	for _, r := range pa.ConnRefs() {
		if strings.Contains(r, "corp-ca") || strings.Contains(r, "CORP_CA") {
			t.Fatalf("ConnRefs has the CA: %v", pa.ConnRefs())
		}
	}
	for _, r := range ProcessRefs() {
		if strings.Contains(r, "_CA_FILE") {
			t.Fatalf("ProcessRefs has a CA: %v", ProcessRefs())
		}
	}
	// A client key file is a credential: shared, it is refused.
	key := strings.ReplaceAll(twoTenants(), "      kafka_security_protocol: {value: SASL_SSL}",
		"      kafka_security_protocol: {value: SASL_SSL}\n      kafka_tls_key_file: {file: /etc/ssl/client.key}")
	if _, err := Load(writeTemp(t, key)); err == nil || !strings.Contains(err.Error(), "file:/etc/ssl/client.key") {
		t.Fatalf("shared key file: err = %v", err)
	}
	if !IsCAKey("schema_registry_tls_ca_file") || IsCAKey("kafka_tls_key_file") || !IsLiteralKey("kafka_tls_server_name") || IsLiteralKey("connect_password") {
		t.Fatal("IsCAKey / IsLiteralKey")
	}
}

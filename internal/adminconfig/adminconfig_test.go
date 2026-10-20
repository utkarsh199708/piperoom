package adminconfig

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"piperoom/internal/config"
	"piperoom/internal/connprobe"
	"piperoom/internal/owners"
	"piperoom/internal/room"
)

const testUsersYAML = `users:
  - id: arjun
    name: Arjun
    layers: [kafka-connect, kafka]
  - id: priya
    name: Priya
    layers: [postgres, schema-registry]
  - id: meera
    name: Meera
    layers: [warehouse]
  - id: kabir
    name: Kabir
    layers: [consumer]
  - id: ada
    name: Ada
    admin: true
`

// orders and legacy use the process-wide settings; payments has its own,
// with env, file and value references.
const testOwnersYAML = `pipelines:
  - id: orders
    title: Orders CDC
    topic_prefix: pg
    source_tables: [public.orders]
    connectors: [pg-orders-connector]
    consumer_groups: [order-sync]
    sink_tables: [analytics.orders]
    sink_ts_column: updated_at
  - id: payments
    title: Payments
    topic_prefix: pay
    source_tables: [public.payments]
    connectors: [pg-payments]
    owners:
      kafka-connect: { approvers: [group:dataeng-oncall], backup: [user:meera] }
    shadow: { enabled: true }
    connections:
      source_db_url: {env: PAY_SOURCE_DB_URL}
      warehouse_db_url: {file: /secrets/pay-wh-dsn}
      warehouse_kind: {value: postgres}
      kafka_brokers: {env: PAY_KAFKA_BROKERS}
      kafka_security_protocol: {value: SASL_SSL}
      kafka_sasl_mechanism: {value: SCRAM-SHA-512}
      kafka_sasl_username: {env: PAY_KAFKA_USER}
      kafka_sasl_password: {file: /secrets/pay-kafka-pw}
      kafka_tls_ca_file: {file: /certs/ca.pem}
      connect_url: {env: PAY_CONNECT_URL}
      schema_registry_url: {env: PAY_SR_URL}
  - id: legacy
    topic_prefix: leg
    source_tables: [public.legacy]
    connectors: [pg-legacy]
layers:
  postgres: priya
  kafka-connect: arjun
  schema-registry: priya
  kafka: arjun
  consumer: kabir
  warehouse: meera
`

// fakeSrc stands in for the process environment and files.
type fakeSrc struct {
	env   map[string]string
	files map[string]string
}

func (f fakeSrc) lookupEnv(name string) (string, bool) {
	v, ok := f.env[name]
	return v, ok
}

type fakeInfo struct{ size int64 }

func (i fakeInfo) Name() string       { return "f" }
func (i fakeInfo) Size() int64        { return i.size }
func (i fakeInfo) Mode() fs.FileMode  { return 0o600 }
func (i fakeInfo) ModTime() time.Time { return time.Time{} }
func (i fakeInfo) IsDir() bool        { return false }
func (i fakeInfo) Sys() any           { return nil }

func (f fakeSrc) stat(path string) (fs.FileInfo, error) {
	v, ok := f.files[path]
	if !ok {
		return nil, fs.ErrNotExist
	}
	return fakeInfo{size: int64(len(v))}, nil
}

// resolve mirrors wiring.PipelineConfig over the fakes: errors name the key
// and the reference only.
func (f fakeSrc) resolve(pl owners.Pipeline) (config.Config, error) {
	var c config.Config
	for k, r := range pl.Connections {
		var v string
		switch {
		case r.Value != "":
			v = r.Value
		case r.Env != "":
			x, ok := f.env[r.Env]
			if !ok || x == "" {
				return config.Config{}, fmt.Errorf("pipeline %q: connections.%s: environment variable %s is not set", pl.ID, k, r.Env)
			}
			v = x
		case r.File != "":
			x, ok := f.files[r.File]
			if !ok {
				return config.Config{}, fmt.Errorf("pipeline %q: connections.%s: read file %s: not found", pl.ID, k, r.File)
			}
			v = x
			if owners.IsPathKey(k) {
				v = r.File
			}
		}
		*keys[k].field(&c) = v
	}
	return c, nil
}

func writeFile(t *testing.T, dir, name, body string) string {
	t.Helper()
	p := filepath.Join(dir, name)
	if err := os.WriteFile(p, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	return p
}

// newProvider builds a Provider over the YAML texts. With src set, it reads
// references through src instead of the process.
func newProvider(t *testing.T, cfg config.Config, ownersYAML, usersYAML string, src *fakeSrc) *Provider {
	t.Helper()
	dir := t.TempDir()
	op := writeFile(t, dir, "owners.yaml", ownersYAML)
	up := writeFile(t, dir, "users.yaml", usersYAML)
	own, err := owners.Load(op)
	if err != nil {
		t.Fatal(err)
	}
	users, err := room.ParseUsers([]byte(usersYAML))
	if err != nil {
		t.Fatal(err)
	}
	p, err := New(cfg, own, users, op, up)
	if err != nil {
		t.Fatal(err)
	}
	if src != nil {
		p.resolve, p.lookupEnv, p.stat = src.resolve, src.lookupEnv, src.stat
	}
	return p
}

// devConfig is config.Load's defaults without reading the environment.
func devConfig() config.Config {
	return config.Config{
		SourceDBURL:       "postgres://piperoom:piperoom-dev@localhost:5432/orders?sslmode=disable",
		WarehouseDBURL:    "postgres://piperoom:piperoom-dev@localhost:5432/warehouse?sslmode=disable",
		KafkaBrokers:      "localhost:29092",
		ConnectURL:        "http://localhost:8083",
		SchemaRegistryURL: "http://localhost:8081",
		WarehouseKind:     "postgres",
	}
}

func paySrc() *fakeSrc {
	return &fakeSrc{
		env: map[string]string{
			"PAY_SOURCE_DB_URL": "postgres://pay:pw@paydb:5432/pay",
			"PAY_KAFKA_BROKERS": "b-1.pay:9096,b-2.pay:9096",
			"PAY_KAFKA_USER":    "pay-user",
			"PAY_CONNECT_URL":   "https://cu:cpw@connect.pay:8443/api?x=1",
			"PAY_SR_URL":        "https://sr.pay:8081",
		},
		files: map[string]string{
			"/secrets/pay-wh-dsn": "postgres://wh:pw@wh:5432/wh",
			"/certs/ca.pem":       "PEM",
			// /secrets/pay-kafka-pw is missing: set false.
		},
	}
}

func findConn(t *testing.T, ac AdminConfig, id string) Connection {
	t.Helper()
	for _, c := range ac.Connections {
		if c.ID == id {
			return c
		}
	}
	t.Fatalf("connection %s not in the view; have %v", id, connIDs(ac))
	return Connection{}
}

func connIDs(ac AdminConfig) []string {
	var out []string
	for _, c := range ac.Connections {
		out = append(out, c.ID)
	}
	return out
}

func findSetting(t *testing.T, c Connection, key string) Setting {
	t.Helper()
	for _, s := range c.Settings {
		if s.Key == key {
			return s
		}
	}
	t.Fatalf("%s: no setting %s", c.ID, key)
	return Setting{}
}

func boolp(b bool) *bool { return &b }

func TestConnectionList(t *testing.T) {
	cfg := devConfig()
	cfg.SchemaRegistryURL = "" // off: the kind is left out
	cfg.KafkaSASLPassword = "pw"
	p := newProvider(t, cfg, testOwnersYAML, testUsersYAML, paySrc())
	ac := p.Config()

	want := []string{
		"process-wide.source_db", "process-wide.kafka", "process-wide.connect", "process-wide.warehouse",
		"pipeline.payments.source_db", "pipeline.payments.kafka", "pipeline.payments.connect",
		"pipeline.payments.schema_registry", "pipeline.payments.warehouse",
	}
	if got := connIDs(ac); !slices.Equal(got, want) {
		t.Fatalf("ids = %v\nwant %v", got, want)
	}
	pw := findConn(t, ac, "process-wide.kafka")
	if pw.Scope != ScopeProcessWide || pw.PipelineID != "" || pw.Kind != connprobe.KindKafka {
		t.Errorf("process-wide.kafka = %+v", pw)
	}
	if !slices.Equal(pw.UsedBy, []string{"orders", "legacy"}) {
		t.Errorf("process-wide used_by = %v, want [orders legacy] (owners order, own-connection pipelines left out)", pw.UsedBy)
	}
	pc := findConn(t, ac, "pipeline.payments.kafka")
	if pc.Scope != ScopePipeline || pc.PipelineID != "payments" || !slices.Equal(pc.UsedBy, []string{"payments"}) {
		t.Errorf("pipeline.payments.kafka = %+v", pc)
	}
	// Set or required only: the unset TLS and mechanism keys have no row.
	var keysShown []string
	for _, s := range pw.Settings {
		keysShown = append(keysShown, s.Key)
	}
	if !slices.Equal(keysShown, []string{"kafka_brokers", "kafka_sasl_password"}) {
		t.Errorf("process-wide.kafka rows = %v", keysShown)
	}
}

// Without pipelines on the process-wide settings, used_by is an empty list.
func TestProcessWideUnused(t *testing.T) {
	own := strings.Replace(testOwnersYAML, `  - id: orders
    title: Orders CDC`, `  - id: orders
    title: Orders CDC
    connections:
      source_db_url: {env: O_DB}
      warehouse_db_url: {env: O_WH}
      kafka_brokers: {env: O_K}
      connect_url: {env: O_C}
      schema_registry_url: {env: O_SR}`, 1)
	own = strings.Replace(own, `  - id: legacy
    topic_prefix: leg
    source_tables: [public.legacy]
    connectors: [pg-legacy]
`, "", 1)
	p := newProvider(t, devConfig(), own, testUsersYAML, paySrc())
	b, _ := json.Marshal(findConn(t, p.Config(), "process-wide.kafka"))
	if !strings.Contains(string(b), `"used_by":[]`) {
		t.Errorf("unused process-wide connection: %s, want used_by []", b)
	}
}

func TestSettingsDisplay(t *testing.T) {
	cfg := devConfig()
	cfg.SourceDBURL = "postgres://u:pw@db:5432/x"
	cfg.ConnectURL = "https://cu:cpw@connect:8443/base?token=t"
	cfg.KafkaBrokers = "u:pw@b-1:9092,u:pw@b-2:9092"
	cfg.KafkaSecurityProtocol = "SASL_SSL"
	cfg.KafkaSASLUsername = "kuser"
	cfg.KafkaTLSCAFile = "/etc/ssl/ca.pem"
	p := newProvider(t, cfg, testOwnersYAML, testUsersYAML, paySrc())
	ac := p.Config()
	pay := func(kind, key string) Setting { return findSetting(t, findConn(t, ac, "pipeline.payments."+kind), key) }
	pw := func(kind, key string) Setting { return findSetting(t, findConn(t, ac, "process-wide."+kind), key) }

	for _, c := range []struct {
		name string
		got  Setting
		want Setting
	}{
		// Process-wide: from is the variable of config.go.
		{"pw dsn", pw("source_db", "source_db_url"), Setting{Key: "source_db_url", Secret: true, Set: boolp(true), From: "env SOURCE_DB_URL"}},
		{"pw url", pw("connect", "connect_url"), Setting{Key: "connect_url", Display: "https://connect:8443/base", From: "env CONNECT_URL"}},
		{"pw brokers", pw("kafka", "kafka_brokers"), Setting{Key: "kafka_brokers", Display: "b-1:9092,b-2:9092", From: "env KAFKA_BROKERS"}},
		{"pw literal", pw("kafka", "kafka_security_protocol"), Setting{Key: "kafka_security_protocol", Display: "SASL_SSL", From: "env KAFKA_SECURITY_PROTOCOL"}},
		{"pw user", pw("kafka", "kafka_sasl_username"), Setting{Key: "kafka_sasl_username", Secret: true, Set: boolp(true), From: "env KAFKA_SASL_USERNAME"}},
		{"pw tls path", pw("kafka", "kafka_tls_ca_file"), Setting{Key: "kafka_tls_ca_file", Display: "/etc/ssl/ca.pem", From: "env KAFKA_TLS_CA_FILE"}},
		{"pw warehouse kind", pw("warehouse", "warehouse_kind"), Setting{Key: "warehouse_kind", Display: "postgres", From: "env WAREHOUSE_KIND"}},
		// Pipeline: from is the reference; secrets by presence only.
		{"pay dsn env", pay("source_db", "source_db_url"), Setting{Key: "source_db_url", Secret: true, Set: boolp(true), From: "env PAY_SOURCE_DB_URL"}},
		{"pay dsn file", pay("warehouse", "warehouse_db_url"), Setting{Key: "warehouse_db_url", Secret: true, Set: boolp(true), From: "file /secrets/pay-wh-dsn"}},
		{"pay password unset", pay("kafka", "kafka_sasl_password"), Setting{Key: "kafka_sasl_password", Secret: true, Set: boolp(false), From: "file /secrets/pay-kafka-pw"}},
		{"pay user", pay("kafka", "kafka_sasl_username"), Setting{Key: "kafka_sasl_username", Secret: true, Set: boolp(true), From: "env PAY_KAFKA_USER"}},
		{"pay literal value", pay("kafka", "kafka_sasl_mechanism"), Setting{Key: "kafka_sasl_mechanism", Display: "SCRAM-SHA-512", From: "value"}},
		{"pay tls path file", pay("kafka", "kafka_tls_ca_file"), Setting{Key: "kafka_tls_ca_file", Display: "/certs/ca.pem", From: "file /certs/ca.pem"}},
		{"pay brokers env", pay("kafka", "kafka_brokers"), Setting{Key: "kafka_brokers", Display: "b-1.pay:9096,b-2.pay:9096", From: "env PAY_KAFKA_BROKERS"}},
		{"pay url userinfo", pay("connect", "connect_url"), Setting{Key: "connect_url", Display: "https://connect.pay:8443/api", From: "env PAY_CONNECT_URL"}},
	} {
		if fmt.Sprint(c.got.Set != nil && *c.got.Set) != fmt.Sprint(c.want.Set != nil && *c.want.Set) || (c.got.Set == nil) != (c.want.Set == nil) {
			t.Errorf("%s: set = %v, want %v", c.name, c.got.Set, c.want.Set)
		}
		c.got.Set, c.want.Set = nil, nil
		if c.got != c.want {
			t.Errorf("%s:\n got %+v\nwant %+v", c.name, c.got, c.want)
		}
	}
}

// A non-secret pipeline reference that does not resolve shows nothing; an
// unparseable URL shows nothing and is marked secret.
func TestSettingsUnresolvedAndUnparseable(t *testing.T) {
	src := paySrc()
	delete(src.env, "PAY_KAFKA_BROKERS")
	src.env["PAY_CONNECT_URL"] = "not a url"
	p := newProvider(t, devConfig(), testOwnersYAML, testUsersYAML, src)
	ac := p.Config()
	b := findSetting(t, findConn(t, ac, "pipeline.payments.kafka"), "kafka_brokers")
	if b.Display != "" || b.Secret || b.Set != nil || b.From != "env PAY_KAFKA_BROKERS" {
		t.Errorf("unresolved brokers = %+v", b)
	}
	u := findSetting(t, findConn(t, ac, "pipeline.payments.connect"), "connect_url")
	if u.Display != "" || !u.Secret {
		t.Errorf("unparseable url = %+v, want display \"\" and secret", u)
	}
}

func TestPipelinesAndPeople(t *testing.T) {
	p := newProvider(t, devConfig(), testOwnersYAML, testUsersYAML, paySrc())
	ac := p.Config()
	if ac.Source != "file" || ac.OwnersFile != "owners.yaml" || ac.UsersFile != "users.yaml" {
		t.Errorf("header = %q %q %q", ac.Source, ac.OwnersFile, ac.UsersFile)
	}
	if len(ac.Pipelines) != 3 {
		t.Fatalf("pipelines = %+v", ac.Pipelines)
	}
	o := ac.Pipelines[0]
	if o.ID != "orders" || o.Title != "Orders CDC" || o.Tenant != "default" || o.Connections != "process-wide" || o.Shadow ||
		!slices.Equal(o.Connectors, []string{"pg-orders-connector"}) || !slices.Equal(o.ConsumerGroups, []string{"order-sync"}) ||
		!slices.Equal(o.SourceTables, []string{"public.orders"}) || !slices.Equal(o.SinkTables, []string{"analytics.orders"}) {
		t.Errorf("orders = %+v", o)
	}
	if len(o.Owners) != 6 || o.Owners[0].Layer != "postgres" || !slices.Equal(o.Owners[0].Principals, []string{"user:priya"}) {
		t.Errorf("orders owners = %+v", o.Owners)
	}
	pay := ac.Pipelines[1]
	if pay.Connections != "own" || !pay.Shadow {
		t.Errorf("payments = %+v", pay)
	}
	if pay.Owners[1].Layer != "kafka-connect" || !slices.Equal(pay.Owners[1].Principals, []string{"group:dataeng-oncall"}) {
		t.Errorf("payments kafka-connect owners = %+v, want the pipeline's own approvers", pay.Owners[1])
	}
	leg := ac.Pipelines[2]
	b, _ := json.Marshal(leg)
	if !strings.Contains(string(b), `"consumer_groups":[]`) || !strings.Contains(string(b), `"sink_tables":[]`) {
		t.Errorf("legacy lists must be [] not null: %s", b)
	}
	if len(ac.People) != 5 || ac.People[0].ID != "arjun" || !slices.Equal(ac.People[0].Layers, []string{"kafka-connect", "kafka"}) || ac.People[0].Admin {
		t.Errorf("people = %+v", ac.People)
	}
	if ada := ac.People[4]; ada.ID != "ada" || !ada.Admin || ada.Layers == nil {
		t.Errorf("ada = %+v", ada)
	}
}

func TestTestUnknownID(t *testing.T) {
	cfg := devConfig()
	cfg.SchemaRegistryURL = ""
	p := newProvider(t, cfg, testOwnersYAML, testUsersYAML, paySrc())
	p.run = func(context.Context, connprobe.Target, *connprobe.Guard) connprobe.Result {
		t.Fatal("probe ran for an unknown id")
		return connprobe.Result{}
	}
	for _, id := range []string{"", "nope", "process-wide.schema_registry", "pipeline.orders.kafka", "pipeline.payments.nope", "process-wide.kafka.x"} {
		if _, err := p.Test(context.Background(), id); !errors.Is(err, ErrUnknownConnection) {
			t.Errorf("Test(%q) err = %v, want ErrUnknownConnection", id, err)
		}
	}
}

// A process-wide test gets only its kind's settings and the pipelines on the
// process-wide settings; a pipeline test gets the resolved settings of its
// kind and the pipeline. Both run with the phase-1 guard.
func TestTestTargets(t *testing.T) {
	cfg := devConfig()
	cfg.KafkaSASLPassword = "pw"
	p := newProvider(t, cfg, testOwnersYAML, testUsersYAML, paySrc())
	var got connprobe.Target
	var guard *connprobe.Guard
	p.run = func(_ context.Context, tg connprobe.Target, g *connprobe.Guard) connprobe.Result {
		got, guard = tg, g
		return connprobe.Result{Kind: tg.Kind, OK: true, Checks: []connprobe.Check{}}
	}
	res, err := p.Test(context.Background(), "process-wide.kafka")
	if err != nil || !res.OK {
		t.Fatalf("Test = %+v, %v", res, err)
	}
	if got.Kind != connprobe.KindKafka || got.Config.KafkaBrokers != "localhost:29092" || got.Config.KafkaSASLPassword != "pw" {
		t.Errorf("target config = %+v", got.Config)
	}
	if got.Config.SourceDBURL != "" || got.Config.ConnectURL != "" {
		t.Errorf("a kafka probe got another kind's settings: %+v", got.Config)
	}
	if len(got.Pipelines) != 2 || got.Pipelines[0].ID != "orders" || got.Pipelines[1].ID != "legacy" {
		t.Errorf("target pipelines = %v", got.Pipelines)
	}
	if guard == nil || !guard.AllowLoopback || !guard.AllowPrivate {
		t.Errorf("guard = %+v", guard)
	}

	if _, err := p.Test(context.Background(), "pipeline.payments.connect"); err != nil {
		t.Fatal(err)
	}
	if got.Kind != connprobe.KindConnect || got.Config.ConnectURL != "https://cu:cpw@connect.pay:8443/api?x=1" || got.Config.SourceDBURL != "" {
		t.Errorf("pipeline target config = %+v", got.Config)
	}
	if len(got.Pipelines) != 1 || got.Pipelines[0].ID != "payments" {
		t.Errorf("pipeline target pipelines = %v", got.Pipelines)
	}
}

// A reference that does not resolve is one failed config check naming it;
// the probe does not run. Other kinds' references do not matter.
func TestTestResolutionFailure(t *testing.T) {
	p := newProvider(t, devConfig(), testOwnersYAML, testUsersYAML, paySrc())
	ran := 0
	p.run = func(_ context.Context, tg connprobe.Target, _ *connprobe.Guard) connprobe.Result {
		ran++
		return connprobe.Result{Kind: tg.Kind, OK: true}
	}
	// /secrets/pay-kafka-pw is missing.
	res, err := p.Test(context.Background(), "pipeline.payments.kafka")
	if err != nil {
		t.Fatal(err)
	}
	if ran != 0 || res.OK || res.Kind != connprobe.KindKafka || len(res.Checks) != 1 {
		t.Fatalf("result = %+v (probe ran %d times)", res, ran)
	}
	c := res.Checks[0]
	if c.ID != "config" || c.Status != connprobe.StatusFail ||
		c.Problem != "pipeline payments: connections.kafka_sasl_password from file /secrets/pay-kafka-pw cannot be resolved" ||
		!strings.Contains(c.Fix, "/secrets/pay-kafka-pw") {
		t.Errorf("check = %+v", c)
	}
	// The missing Kafka password does not stop the Connect test.
	if _, err := p.Test(context.Background(), "pipeline.payments.connect"); err != nil || ran != 1 {
		t.Errorf("connect test: err %v, probe ran %d times, want 1", err, ran)
	}
}

func TestExport(t *testing.T) {
	p := newProvider(t, devConfig(), testOwnersYAML, testUsersYAML, nil)
	b, err := p.Export()
	if err != nil {
		t.Fatal(err)
	}
	if want := testOwnersYAML + "\n---\n" + testUsersYAML; string(b) != want {
		t.Fatalf("export =\n%s\nwant\n%s", b, want)
	}
	// The export is a copy: changing it changes nothing.
	b[0] = 'X'
	if again, _ := p.Export(); again[0] == 'X' {
		t.Fatal("Export returns the provider's own slice")
	}
}

// A file that cannot be read never stops the server (--quickstart runs with a
// users file elsewhere): New succeeds, the view works, and only Export fails.
func TestNewMissingFile(t *testing.T) {
	dir := t.TempDir()
	op := writeFile(t, dir, "owners.yaml", "x")
	up := writeFile(t, dir, "users.yaml", testUsersYAML)
	for name, paths := range map[string][2]string{
		"owners": {filepath.Join(dir, "missing.yaml"), up},
		"users":  {op, filepath.Join(dir, "missing.yaml")},
	} {
		p, err := New(devConfig(), nil, nil, paths[0], paths[1])
		if err != nil {
			t.Fatalf("%s missing: New: %v", name, err)
		}
		if got := p.Config(); got.Source != "file" {
			t.Fatalf("%s missing: view source %q", name, got.Source)
		}
		if b, err := p.Export(); err == nil || b != nil || !strings.Contains(err.Error(), name+" file") {
			t.Fatalf("%s missing: Export = %q, %v", name, b, err)
		}
	}
}

const marker = "S3CRET-MARKER"

// No response of the view or of a failed test contains a secret: every DSN,
// user name, password and URL user info below carries the marker, read
// through the real process environment and files (wiring.PipelineConfig).
func TestLeak(t *testing.T) {
	dir := t.TempDir()
	pwFile := writeFile(t, dir, "kafka-pw", marker+"-file-password")
	whFile := writeFile(t, dir, "wh-dsn", "postgres://wh:"+marker+"@wh:5432/wh")
	t.Setenv("LEAK_SOURCE_DB_URL", "postgres://u:"+marker+"@db:5432/x?password="+marker)
	t.Setenv("LEAK_BROKERS", "user:"+marker+"@b-1:9092,user:"+marker+"@b-2:9092")
	t.Setenv("LEAK_USER", marker+"-user")
	t.Setenv("LEAK_CONNECT_URL", "https://cu:"+marker+"@connect:8443/api?token="+marker+"#"+marker)
	t.Setenv("LEAK_SR_URL", "https://"+marker+"@sr:8081")
	// A literal key whose variable holds a secret by mistake.
	t.Setenv("LEAK_PROTOCOL", marker+"-not-a-protocol")
	ownersYAML := strings.Replace(testOwnersYAML, `    connections:
      source_db_url: {env: PAY_SOURCE_DB_URL}
      warehouse_db_url: {file: /secrets/pay-wh-dsn}
      warehouse_kind: {value: postgres}
      kafka_brokers: {env: PAY_KAFKA_BROKERS}
      kafka_security_protocol: {value: SASL_SSL}
      kafka_sasl_mechanism: {value: SCRAM-SHA-512}
      kafka_sasl_username: {env: PAY_KAFKA_USER}
      kafka_sasl_password: {file: /secrets/pay-kafka-pw}
      kafka_tls_ca_file: {file: /certs/ca.pem}
      connect_url: {env: PAY_CONNECT_URL}
      schema_registry_url: {env: PAY_SR_URL}`, `    connections:
      source_db_url: {env: LEAK_SOURCE_DB_URL}
      warehouse_db_url: {file: '`+whFile+`'}
      kafka_brokers: {env: LEAK_BROKERS}
      kafka_security_protocol: {env: LEAK_PROTOCOL}
      kafka_sasl_username: {env: LEAK_USER}
      kafka_sasl_password: {file: '`+pwFile+`'}
      connect_url: {env: LEAK_CONNECT_URL}
      connect_password: {env: LEAK_UNSET}
      schema_registry_url: {env: LEAK_SR_URL}`, 1)
	if !strings.Contains(ownersYAML, "LEAK_BROKERS") {
		t.Fatal("test owners file not rewritten")
	}
	cfg := config.Config{
		SourceDBURL:            "postgres://piperoom:" + marker + "@localhost:5432/orders?sslmode=disable&password=" + marker,
		WarehouseDBURL:         "snowflake://" + marker + ":" + marker + "@acct/db",
		WarehouseKind:          "snowflake",
		KafkaBrokers:           marker + ":" + marker + "@localhost:29092",
		KafkaSecurityProtocol:  "SASL_SSL",
		KafkaSASLMechanism:     marker + " PLAIN", // not a plain token
		KafkaSASLUsername:      marker + "-user",
		KafkaSASLPassword:      marker + "-password",
		KafkaTLSServerName:     "postgres://x:" + marker + "@h",
		ConnectURL:             "http://admin:" + marker + "@localhost:8083?apikey=" + marker,
		ConnectUsername:        marker,
		ConnectPassword:        marker,
		SchemaRegistryURL:      "http://localhost:8081/" + "#" + marker,
		SchemaRegistryUsername: marker,
		SchemaRegistryPassword: marker,
	}
	p := newProvider(t, cfg, ownersYAML, testUsersYAML, nil)
	b, err := json.Marshal(p.Config())
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(b), marker) {
		t.Fatalf("the view leaks a secret:\n%s", b)
	}
	// The URLs still show their host.
	if !strings.Contains(string(b), `"display":"https://connect:8443/api"`) || !strings.Contains(string(b), `"display":"b-1:9092,b-2:9092"`) {
		t.Errorf("the view lost the hosts:\n%s", b)
	}

	// Kafka: the protocol variable holds a value wiring refuses (literal
	// check); the test names the reference, never the value.
	p.run = func(context.Context, connprobe.Target, *connprobe.Guard) connprobe.Result {
		t.Fatal("probe ran with an unresolvable reference")
		return connprobe.Result{}
	}
	for _, id := range []string{"pipeline.payments.kafka", "pipeline.payments.connect"} {
		res, err := p.Test(context.Background(), id)
		if err != nil {
			t.Fatal(err)
		}
		rb, _ := json.Marshal(res)
		if res.OK || strings.Contains(string(rb), marker) {
			t.Fatalf("%s: result leaks or passes: %s", id, rb)
		}
		if !strings.Contains(string(rb), `"id":"config"`) {
			t.Errorf("%s: want a failed config check: %s", id, rb)
		}
	}
}

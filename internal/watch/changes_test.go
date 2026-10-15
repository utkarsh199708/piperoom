package watch

import (
	"context"
	"encoding/json"
	"errors"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"piperoom/internal/clients"
	"piperoom/internal/tools"
)

// ---- fakes of the change feed ----

type fakeChanges struct {
	mu   sync.Mutex
	rows []clients.PipelineChange
	seen map[string]bool
	err  error
	// attempts counts every RecordChange call, inserted or not.
	attempts int
}

func (f *fakeChanges) RecordChange(_ context.Context, c clients.PipelineChange) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.attempts++
	if f.err != nil {
		return false, f.err
	}
	if f.seen == nil {
		f.seen = map[string]bool{}
	}
	key := c.PipelineID + "|" + c.Kind + "|" + c.Object + "|" + c.Fingerprint
	if f.seen[key] {
		return false, nil
	}
	f.seen[key] = true
	f.rows = append(f.rows, c)
	return true, nil
}

func (f *fakeChanges) all() []clients.PipelineChange {
	f.mu.Lock()
	defer f.mu.Unlock()
	return slices.Clone(f.rows)
}

func (f *fakeChanges) setErr(err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.err = err
}

type fakeRegistry struct {
	mu       sync.Mutex
	versions map[string][]int
	err      map[string]error
}

func (r *fakeRegistry) Subjects(context.Context) ([]string, error) { return nil, nil }
func (r *fakeRegistry) Versions(_ context.Context, s string) ([]int, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return slices.Clone(r.versions[s]), r.err[s]
}
func (r *fakeRegistry) Schema(context.Context, string, int) (clients.SchemaVersion, error) {
	return clients.SchemaVersion{}, errors.New("fake: not used")
}
func (r *fakeRegistry) Compatibility(context.Context, string) (string, bool, error) {
	return "", false, errors.New("fake: not used")
}
func (r *fakeRegistry) set(s string, vs ...int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.versions[s] = vs
	delete(r.err, s)
}

type fakeDDL struct {
	mu    sync.Mutex
	rows  []clients.DDLChange
	err   error
	since []time.Time
}

func (d *fakeDDL) RecentDDL(_ context.Context, since time.Time, _ int) ([]clients.DDLChange, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.since = append(d.since, since)
	if d.err != nil {
		return nil, d.err
	}
	var out []clients.DDLChange
	for _, r := range d.rows {
		if !r.At.Before(since) {
			out = append(out, r)
		}
	}
	return out, nil
}

type changesHarness struct {
	*harness
	ch  *fakeChanges
	reg *fakeRegistry
	ddl *fakeDDL
}

// newChangesHarness is the usual one-pipeline harness with the change feed on.
// Pipeline "orders" has the connector pg-orders, the table public.t and the
// subjects orders.public.t-key and orders.public.t-value.
func newChangesHarness(t *testing.T) *changesHarness {
	t.Helper()
	h := newHarness(t, opts{})
	c := &changesHarness{harness: h, ch: &fakeChanges{}, reg: &fakeRegistry{versions: map[string][]int{}, err: map[string]error{}}, ddl: &fakeDDL{}}
	c.reg.set("orders.public.t-key", 1)
	c.reg.set("orders.public.t-value", 1, 2)
	d := h.deps
	d.Changes, d.Registry, d.DDL = c.ch, c.reg, c.ddl
	w, err := New(d)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	h.w = w
	return c
}

func (c *changesHarness) setCfg(k, v string) {
	c.conn.mu.Lock()
	defer c.conn.mu.Unlock()
	c.conn.cfg["pg-orders"][k] = v
}

func kinds(rows []clients.PipelineChange) []string {
	var out []string
	for _, r := range rows {
		out = append(out, r.Kind+" "+r.Object)
	}
	return out
}

func jsonMap(t *testing.T, b json.RawMessage) map[string]any {
	t.Helper()
	var m map[string]any
	if err := json.Unmarshal(b, &m); err != nil {
		t.Fatalf("bad json %q: %v", b, err)
	}
	return m
}

// ---- tests ----

func TestFirstTickRecordsNoDiff(t *testing.T) {
	h := newChangesHarness(t)
	h.step(0)
	if rows := h.ch.all(); len(rows) != 0 {
		t.Fatalf("first tick recorded %v", kinds(rows))
	}
	// A quiet second tick records nothing either.
	h.step(time.Minute)
	if rows := h.ch.all(); len(rows) != 0 {
		t.Fatalf("quiet tick recorded %v", kinds(rows))
	}
}

func TestFirstTickRecordsDDLWithItsOwnTime(t *testing.T) {
	h := newChangesHarness(t)
	at := t0.Add(-time.Minute)
	h.ddl.rows = []clients.DDLChange{
		{At: at, CommandTag: "ALTER TABLE", ObjectIdentity: "public.t", Query: "ALTER TABLE public.t ADD COLUMN x int", Username: "piperoom"},
		{At: at, CommandTag: "ALTER TABLE", ObjectIdentity: "public.other", Query: "ALTER TABLE public.other ADD COLUMN y int", Username: "x"},
	}
	h.step(0)
	rows := h.ch.all()
	if len(rows) != 1 {
		t.Fatalf("rows = %v, want only the pipeline's table", kinds(rows))
	}
	r := rows[0]
	if r.Kind != clients.ChangeDDL || !r.At.Equal(at) || r.Actor != "piperoom" || r.Source != "ddl_audit" || r.Object != "public.t" {
		t.Fatalf("row = %+v", r)
	}
	if !strings.HasPrefix(r.Summary, "ALTER TABLE public.t | ALTER TABLE public.t ADD COLUMN x int") {
		t.Fatalf("summary = %q", r.Summary)
	}
}

// A column rename is logged under schema.table.column and a mixed-case table
// quoted (public."Name"); both are the pipeline's tables (#330).
func TestDDLRecordsColumnAndMixedCaseTables(t *testing.T) {
	h := newHarness(t, opts{pipes: []pipeSpec{{id: "orders", tables: "[public.t, public.Mixed]"}}})
	ddl := &fakeDDL{}
	d := h.deps
	d.Changes, d.Registry, d.DDL = &fakeChanges{}, &fakeRegistry{versions: map[string][]int{}, err: map[string]error{}}, ddl
	w, err := New(d)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	h.w = w
	at := t0.Add(-time.Minute)
	for _, id := range []string{"public.t.b", `public."Mixed"`, `public."Mixed".note`, "public.other.b", "public.tx", "other.public.t.b", "public.t."} {
		ddl.rows = append(ddl.rows, clients.DDLChange{At: at, CommandTag: "ALTER TABLE", ObjectIdentity: id, Query: "q " + id})
	}
	h.step(0)
	var got []string
	for _, r := range d.Changes.(*fakeChanges).all() {
		got = append(got, r.Object)
	}
	slices.Sort(got)
	want := []string{`public."Mixed"`, `public."Mixed".note`, "public.t.b"}
	if !slices.Equal(got, want) {
		t.Fatalf("recorded %v, want %v", got, want)
	}
}

// An owners entry in the quoted form keeps matching, as it did on main, and
// malformed identities fall back to the exact match only.
func TestDDLInScopeQuotedOwnersAndMalformed(t *testing.T) {
	tests := []struct {
		name   string
		tables string
		id     string
		want   bool
	}{
		{"quoted owners entry, table", `['public."Mixed"']`, `public."Mixed"`, true},
		{"quoted owners entry, column", `['public."Mixed"']`, `public."Mixed".col`, true},
		{"quoted owners entry, other table", `['public."Mixed"']`, `public."Other"`, false},
		{"catalog owners entry, quoted column", "[public.Mixed]", `public."Mixed"."Col"`, true},
		{"unterminated quote", "[public.Mixed]", `public."Mix`, false},
		{"empty quoted part", "[public.Mixed]", `public."".x`, false},
		{"trailing dot", "[public.t]", "public.t.", false},
		{"too many parts", "[public.t]", "other.public.t.b", false},
		{"exact match of an odd identity", `['public."Mix']`, `public."Mix`, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := newHarness(t, opts{pipes: []pipeSpec{{id: "orders", tables: tt.tables}}})
			p, ok := h.deps.Owners.Pipeline("orders")
			if !ok {
				t.Fatal("pipeline orders missing")
			}
			sc, err := tools.ScopeFor(tools.WithPipeline(context.Background(), p), h.deps.Owners)
			if err != nil {
				t.Fatalf("ScopeFor: %v", err)
			}
			if got := ddlInScope(sc, tt.id); got != tt.want {
				t.Fatalf("ddlInScope(%q) with %s = %v, want %v", tt.id, tt.tables, got, tt.want)
			}
		})
	}
}

func TestDDLRowsCarryAuditTime(t *testing.T) {
	h := newChangesHarness(t)
	h.step(0)
	at := t0.Add(30 * time.Second)
	h.ddl.rows = []clients.DDLChange{{At: at, CommandTag: "ALTER TABLE", ObjectIdentity: "public.t", Query: "ALTER TABLE public.t DROP COLUMN z", Username: "app_migrator"}}
	h.step(time.Minute) // tick time is t0+1m, the row is older
	h.step(time.Minute) // the same row is read again: nothing new
	rows := h.ch.all()
	if len(rows) != 1 || !rows[0].At.Equal(at) {
		t.Fatalf("rows = %+v, want one at the audit time %v", rows, at)
	}
	if h.ch.attempts != 2 {
		t.Fatalf("attempts = %d: the row is offered twice and deduplicated by the writer", h.ch.attempts)
	}
	// The window reaches back two intervals from the last good tick.
	since := h.ddl.since
	if got, want := since[len(since)-1], t0.Add(time.Minute).Add(-2*time.Minute); !got.Equal(want) {
		t.Fatalf("last DDL window start = %v, want %v", got, want)
	}
}

func TestDDLAuditMissingTurnsKindOff(t *testing.T) {
	h := newChangesHarness(t)
	h.ddl.err = errors.New("postgres: DDL audit trigger is not installed")
	h.step(0)
	h.step(time.Minute)
	h.step(time.Minute)
	if n := strings.Count(h.logs.String(), "cannot read the DDL audit table"); n != 1 {
		t.Fatalf("warn lines = %d, want 1\n%s", n, h.logs.String())
	}
	h.conn.setStatus("pg-orders", failed("pg-orders", "boom"))
	h.step(time.Minute)
	if rows := h.ch.all(); len(rows) == 0 || rows[0].Kind != clients.ChangeConnectorState {
		t.Fatalf("other kinds must keep working: %v", kinds(rows))
	}
}

func TestStateTransitionRows(t *testing.T) {
	h := newChangesHarness(t)
	h.step(0)
	h.conn.setStatus("pg-orders", failed("pg-orders", "org.apache.kafka.connect.errors.ConnectException: bad\n\tat Foo.bar(Foo.java:1)"))
	h.step(time.Minute)
	rows := h.ch.all()
	if got := kinds(rows); !slices.Equal(got, []string{"connector_state pg-orders", "connector_state pg-orders/0"}) {
		t.Fatalf("rows = %v", got)
	}
	task := rows[1]
	if b, a := jsonMap(t, task.Before), jsonMap(t, task.After); b["state"] != "RUNNING" || a["state"] != "FAILED" ||
		a["trace"] != "org.apache.kafka.connect.errors.ConnectException: bad" {
		t.Fatalf("task before/after = %s / %s", task.Before, task.After)
	}
	if !strings.Contains(task.Summary, "RUNNING → FAILED") || task.Source != "watcher" || !task.At.Equal(t0.Add(time.Minute)) {
		t.Fatalf("task row = %+v", task)
	}
	// The same state again writes nothing; the fix writes the way back.
	h.step(time.Minute)
	if len(h.ch.all()) != 2 {
		t.Fatalf("a repeated state wrote rows: %v", kinds(h.ch.all()))
	}
	h.conn.setStatus("pg-orders", running("pg-orders"))
	h.step(time.Minute)
	rows = h.ch.all()
	if len(rows) != 4 || !strings.Contains(rows[3].Summary, "FAILED → RUNNING") {
		t.Fatalf("rows after the fix = %v", kinds(rows))
	}
	// A status error says nothing: no row, baseline kept.
	h.conn.setStatusErr("pg-orders", errors.New("timeout"))
	h.step(time.Minute)
	h.conn.setStatusErr("pg-orders", nil)
	h.step(time.Minute)
	if len(h.ch.all()) != 4 {
		t.Fatalf("an error tick produced a row: %v", kinds(h.ch.all()))
	}
}

func TestStateTransitionTraceIsRedacted(t *testing.T) {
	h := newChangesHarness(t)
	h.step(0)
	h.conn.setStatus("pg-orders", failed("pg-orders", "ConnectException: password=hunter2secret rejected"))
	h.step(time.Minute)
	for _, r := range h.ch.all() {
		if strings.Contains(string(r.After)+r.Summary, "hunter2secret") {
			t.Fatalf("a secret reached a change row: %+v", r)
		}
	}
}

func TestConfigDiffMasks(t *testing.T) {
	h := newChangesHarness(t)
	h.setCfg("database.password", "old-secret-value")
	h.setCfg("heartbeat.interval.ms", "10000")
	h.step(0)
	h.setCfg("database.password", "new-secret-value")
	h.setCfg("heartbeat.interval.ms", "5000")
	h.setCfg("publication.name", "pub2")
	h.step(time.Minute)
	rows := h.ch.all()
	if len(rows) != 1 || rows[0].Kind != clients.ChangeConnectorConfig {
		t.Fatalf("rows = %v", kinds(rows))
	}
	r := rows[0]
	b, a := jsonMap(t, r.Before), jsonMap(t, r.After)
	if b["database.password"] != "***" || a["database.password"] != "***" {
		t.Fatalf("password not masked on both sides: %s / %s", r.Before, r.After)
	}
	if b["heartbeat.interval.ms"] != "10000" || a["heartbeat.interval.ms"] != "5000" {
		t.Fatalf("plain key diff = %s / %s", r.Before, r.After)
	}
	if _, had := b["publication.name"]; had || a["publication.name"] != "pub2" {
		t.Fatalf("an added key has no before: %s / %s", r.Before, r.After)
	}
	whole := string(r.Before) + string(r.After) + r.Summary + r.Fingerprint
	for _, secret := range []string{"old-secret-value", "new-secret-value"} {
		if strings.Contains(whole, secret) {
			t.Fatalf("secret %q reached the row: %+v", secret, r)
		}
	}
	if !strings.Contains(r.Summary, "3 keys changed (database.password, heartbeat.interval.ms, publication.name)") {
		t.Fatalf("summary = %q", r.Summary)
	}
	// Another password change is a different row, though both sides read "***".
	h.setCfg("database.password", "third-secret-value")
	h.step(time.Minute)
	if len(h.ch.all()) != 2 {
		t.Fatalf("a second password change must be recorded: %v", kinds(h.ch.all()))
	}
	// A removed key has no after.
	h.conn.mu.Lock()
	delete(h.conn.cfg["pg-orders"], "publication.name")
	h.conn.mu.Unlock()
	h.step(time.Minute)
	rows = h.ch.all()
	last := rows[len(rows)-1]
	if _, has := jsonMap(t, last.After)["publication.name"]; has || jsonMap(t, last.Before)["publication.name"] != "pub2" {
		t.Fatalf("removed key = %s / %s", last.Before, last.After)
	}
}

func TestSchemaVersionUpAndDown(t *testing.T) {
	h := newChangesHarness(t)
	h.step(0)
	h.reg.set("orders.public.t-value", 1, 2, 3)
	h.step(time.Minute)
	rows := h.ch.all()
	if len(rows) != 1 || rows[0].Kind != clients.ChangeSchemaVersion || rows[0].Object != "orders.public.t-value" {
		t.Fatalf("rows = %v", kinds(rows))
	}
	if b, a := jsonMap(t, rows[0].Before), jsonMap(t, rows[0].After); b["version"] != float64(2) || a["version"] != float64(3) ||
		!strings.Contains(rows[0].Summary, "registered") {
		t.Fatalf("row = %+v", rows[0])
	}
	h.reg.set("orders.public.t-value", 1, 2)
	h.step(time.Minute)
	rows = h.ch.all()
	if len(rows) != 2 || !strings.Contains(rows[1].Summary, "deleted") {
		t.Fatalf("rows = %v", kinds(rows))
	}
	// An unreachable registry records nothing and keeps the baseline.
	h.reg.mu.Lock()
	h.reg.err["orders.public.t-value"] = errors.New("registry down")
	h.reg.mu.Unlock()
	h.step(time.Minute)
	h.reg.set("orders.public.t-value", 1, 2)
	h.step(time.Minute)
	if len(h.ch.all()) != 2 {
		t.Fatalf("an unreachable registry produced rows: %v", kinds(h.ch.all()))
	}
	// A subject outside the pipeline is never read.
	h.reg.set("other.public.t-value", 9)
	h.step(time.Minute)
	if len(h.ch.all()) != 2 {
		t.Fatalf("rows = %v", kinds(h.ch.all()))
	}
}

func TestWriteErrorKeepsBaseline(t *testing.T) {
	h := newChangesHarness(t)
	h.step(0)
	h.ch.setErr(errors.New("db down"))
	h.conn.setStatus("pg-orders", failed("pg-orders", "boom"))
	h.setCfg("heartbeat.interval.ms", "1")
	h.reg.set("orders.public.t-value", 1, 2, 3)
	h.step(time.Minute)
	h.step(time.Minute)
	if n := len(h.ch.all()); n != 0 {
		t.Fatalf("rows written while the store failed: %d", n)
	}
	if n := strings.Count(h.logs.String(), "cannot record a pipeline change"); n != 1 {
		t.Fatalf("write failure lines = %d, want one per episode\n%s", n, h.logs.String())
	}
	// The store comes back: the same diffs are found again and written.
	h.ch.setErr(nil)
	h.step(time.Minute)
	got := kinds(h.ch.all())
	for _, want := range []string{"connector_state pg-orders", "connector_state pg-orders/0", "connector_config pg-orders", "schema_version orders.public.t-value"} {
		if !slices.Contains(got, want) {
			t.Fatalf("missing %q after recovery: %v", want, got)
		}
	}
	n := len(got)
	h.step(time.Minute)
	if len(h.ch.all()) != n {
		t.Fatalf("recovery wrote twice: %v", kinds(h.ch.all()))
	}
}

func TestChangeFeedOffWithoutWriter(t *testing.T) {
	h := newHarness(t, opts{})
	h.step(0)
	h.conn.setStatus("pg-orders", failed("pg-orders", "boom"))
	h.step(time.Minute) // must not panic and must not need a config read
}

func TestSensitiveConfigKey(t *testing.T) {
	for _, tc := range []struct {
		key  string
		want bool
	}{
		{"database.password", true},
		{"database.PASSWORD", true},
		{"snowflake.private.key.passphrase", true},
		{"ssl.key.password", true},
		{"database.passwd", true},
		{"sink.pwd", true},
		{"apikey", true},
		{"api_key", true},
		{"API-KEY", true},
		{"aws.secret.access.key", true},
		{"consumer.override.sasl.jaas.config", true},
		{"credentials", true},
		{"gcp.credentials.json", true},
		{"http.authorization", true},
		{"oauth.token.endpoint", true},
		{"key.converter", false},
		{"value.converter", false},
		{"heartbeat.interval.ms", false},
		{"connection.url", false},
		{"database.hostname", false},
		{"cwd", false},
	} {
		if got := sensitiveConfigKey(tc.key); got != tc.want {
			t.Errorf("sensitiveConfigKey(%q) = %v, want %v", tc.key, got, tc.want)
		}
	}
}

func TestConfigDiffMasksWidenedKeys(t *testing.T) {
	h := newChangesHarness(t)
	keys := []string{"apikey", "api_key", "credentials", "sink.pwd", "sasl.jaas.config", "private.key.passphrase"}
	for _, k := range keys {
		h.setCfg(k, "old-"+k)
	}
	h.step(0)
	for _, k := range keys {
		h.setCfg(k, "new-"+k)
	}
	h.step(time.Minute)
	rows := h.ch.all()
	if len(rows) != 1 {
		t.Fatalf("rows = %v", kinds(rows))
	}
	b, a := jsonMap(t, rows[0].Before), jsonMap(t, rows[0].After)
	whole := string(rows[0].Before) + string(rows[0].After) + rows[0].Summary + rows[0].Fingerprint
	for _, k := range keys {
		if b[k] != "***" || a[k] != "***" {
			t.Errorf("%s not masked: %v / %v", k, b[k], a[k])
		}
		if strings.Contains(whole, "old-"+k) || strings.Contains(whole, "new-"+k) {
			t.Errorf("%s value reached the row", k)
		}
	}
}

func TestConfigDiffRedactsValues(t *testing.T) {
	for _, tc := range []struct {
		name, key, before, after string
		secrets                  []string
	}{
		{"url password", "connection.url", "jdbc:postgresql://app:hunter2old@db:5432/orders", "jdbc:postgresql://app:hunter2new@db:5432/orders", []string{"hunter2old", "hunter2new"}},
		{"url password and host", "database.url", "postgres://app:s3cretold@db1/x", "postgres://app:s3cretnew@db2/x", []string{"s3cretold", "s3cretnew"}},
		{"query password", "connection.url", "http://h/x?password=abc123old&a=b", "http://h/x?password=abc123new&a=b", []string{"abc123old", "abc123new"}},
		{"jaas in a plain key", "sasl.config", `x password="oldpassw0rd";`, `x password="newpassw0rd";`, []string{"oldpassw0rd", "newpassw0rd"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := newChangesHarness(t)
			h.setCfg(tc.key, tc.before)
			h.step(0)
			h.setCfg(tc.key, tc.after)
			h.step(time.Minute)
			rows := h.ch.all()
			if len(rows) != 1 || rows[0].Kind != clients.ChangeConnectorConfig {
				t.Fatalf("a secret-only change must still be recorded once: %v", kinds(rows))
			}
			whole := string(rows[0].Before) + string(rows[0].After) + rows[0].Summary + rows[0].Fingerprint
			for _, s := range tc.secrets {
				if strings.Contains(whole, s) {
					t.Fatalf("secret %q reached the row: %+v", s, rows[0])
				}
			}
			if !strings.Contains(string(rows[0].After), "[redacted") {
				t.Fatalf("after should show the mask: %s", rows[0].After)
			}
		})
	}
}

func TestConfigDiffKeepsPlainValuesAndSeesSecretOnlyChange(t *testing.T) {
	h := newChangesHarness(t)
	h.setCfg("connection.url", "jdbc:postgresql://app:pw-one-1234@db:5432/orders")
	h.step(0)
	h.setCfg("connection.url", "jdbc:postgresql://app:pw-two-5678@db:5432/orders")
	h.step(time.Minute)
	h.setCfg("connection.url", "jdbc:postgresql://app:pw-three-9@db:5432/orders")
	h.step(time.Minute)
	if rows := h.ch.all(); len(rows) != 2 || rows[0].Fingerprint == rows[1].Fingerprint {
		t.Fatalf("two different password changes must be two rows: %v", kinds(rows))
	}
	h.setCfg("heartbeat.interval.ms", "5000")
	h.step(time.Minute)
	last := h.ch.all()[2]
	if jsonMap(t, last.After)["heartbeat.interval.ms"] != "5000" {
		t.Fatalf("a plain value must stay readable: %s", last.After)
	}
}

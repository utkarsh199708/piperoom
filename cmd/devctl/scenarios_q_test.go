package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"gopkg.in/yaml.v3"

	"piperoom/internal/owners"
)

// The Q2f and Q5e breaks run against qWorld (scenarios_q_fake_test.go). What
// no fake shows is Debezium itself: the live checks in dev/scenarios.md
// §20-§24 are not run here (no Docker).

var qScenarios = []string{"no-replica-identity", "missing-grant", "blocked-slot", "idle-wal-growth", "stale-source-offsets"}

func TestQScenariosRegistered(t *testing.T) {
	root, err := repoRoot()
	if err != nil {
		t.Fatal(err)
	}
	b, err := os.ReadFile(filepath.Join(root, "dev", "scenarios.md"))
	if err != nil {
		t.Fatal(err)
	}
	doc := string(b)
	want := map[string]struct {
		signals []string
		alert   bool
		owners  string
	}{
		"no-replica-identity":  {alert: true},
		"missing-grant":        {signals: []string{owners.SignalConnectorDown, owners.SignalSlotInactive}},
		"blocked-slot":         {alert: true},
		"idle-wal-growth":      {signals: []string{owners.SignalSlotWAL}, owners: q2OwnersFile},
		"stale-source-offsets": {signals: []string{owners.SignalConnectorDown}},
	}
	for _, name := range qScenarios {
		sc, ok := scenarios[name]
		if !ok || sc.breakIt == nil || sc.fix == nil {
			t.Fatalf("scenario %q not registered with a break and a fix", name)
		}
		w := want[name]
		if !slices.Equal(sc.signals, w.signals) || (sc.alert != "") != w.alert || sc.owners != w.owners {
			t.Errorf("%s: signals %v alert %v owners %q, want %v %v %q", name, sc.signals, sc.alert != "", sc.owners, w.signals, w.alert, w.owners)
		}
		if sc.alert != "" {
			var a struct {
				Title, Severity, Source, Description string
				PipelineID                           string `json:"pipeline_id"`
				DedupKey                             string `json:"dedup_key"`
			}
			if err := json.Unmarshal([]byte(sc.alert), &a); err != nil {
				t.Fatalf("alert of %q is not JSON: %v", name, err)
			}
			if a.Title == "" || a.Severity == "" || a.Source == "" || a.Description == "" || a.PipelineID != "orders" || a.DedupKey != "devctl-"+name {
				t.Errorf("alert of %q = %+v", name, a)
			}
		}
		if !strings.Contains(doc, "`"+name+"` —") {
			t.Errorf("dev/scenarios.md does not document %s", name)
		}
	}
}

// dev/owners.q2.yaml loads, carries the slot_wal thresholds the break and fix
// use, and does NOT list the heartbeat table: source_create_heartbeat_table
// refuses a heartbeat table that is one of the pipeline's source_tables, so
// listing it would make the scenario's documented fix action unreachable.
func TestOwnersQ2File(t *testing.T) {
	root, err := repoRoot()
	if err != nil {
		t.Fatal(err)
	}
	own, err := owners.Load(filepath.Join(root, q2OwnersFile))
	if err != nil {
		t.Fatal(err)
	}
	p, ok := own.Pipeline("orders")
	if !ok {
		t.Fatal("pipeline orders missing")
	}
	sw := p.Watch().SlotWAL
	if !sw.Enabled || sw.FireBytes != idleWALFire || sw.ClearBytes != idleWALClear || sw.For != 30*time.Second {
		t.Errorf("slot_wal = %+v, want enabled, fire %d, clear %d, for 30s", sw, idleWALFire, idleWALClear)
	}
	if !slices.Equal(p.SourceTables, []string{"public.orders"}) {
		t.Errorf("source_tables = %v, want [public.orders] (the heartbeat table must not be listed)", p.SourceTables)
	}
	if slices.Contains(p.SourceTables, heartbeatTable) {
		t.Errorf("source_tables lists %s, which source_create_heartbeat_table refuses", heartbeatTable)
	}
}

// dev/owners.q2.yaml is the baseline plus the Q2 additions, nothing else.
func TestOwnersQ2MatchesBaseline(t *testing.T) {
	root, err := repoRoot()
	if err != nil {
		t.Fatal(err)
	}
	read := func(name string) map[string]any {
		b, err := os.ReadFile(filepath.Join(root, "dev", name))
		if err != nil {
			t.Fatal(err)
		}
		var m map[string]any
		if err := yaml.Unmarshal(b, &m); err != nil {
			t.Fatalf("dev/%s: %v", name, err)
		}
		return m
	}
	base, q2 := read("owners.yaml"), read(filepath.Base(q2OwnersFile))
	pipe := func(m map[string]any) map[string]any { return m["pipelines"].([]any)[0].(map[string]any) }
	bp, qp := pipe(base), pipe(q2)
	if _, ok := bp["watch"].(map[string]any)["signals"].(map[string]any)["slot_wal"]; ok {
		t.Errorf("baseline sets slot_wal: the small threshold belongs in %s only", q2OwnersFile)
	}
	delete(qp["watch"].(map[string]any)["signals"].(map[string]any), "slot_wal")
	if fmt.Sprint(base) != fmt.Sprint(q2) {
		t.Errorf("%s drifted from dev/owners.yaml beyond the Q2 additions:\nbase %v\nq2   %v", q2OwnersFile, base, q2)
	}
}

// ------------------------------------------------------------ no-replica-identity

func TestNoReplicaIdentityBreakAndFix(t *testing.T) {
	w := newQWorld(t)
	s, logs := w.stack()
	ctx := w.ctx()
	if err := breakNoReplicaIdentity(ctx, s); err != nil {
		t.Fatalf("break: %v\n%s", err, logs)
	}
	w.with(func() {
		if w.ident != "n" {
			t.Errorf("replica identity after break = %q, want n", w.ident)
		}
		if !w.loaded[w.nextID] {
			t.Error("the marker insert did not stream: inserts must keep flowing")
		}
	})
	if err := breakNoReplicaIdentity(ctx, s); err == nil || !strings.Contains(err.Error(), "already has REPLICA IDENTITY NOTHING") {
		t.Errorf("second break = %v, want a refusal", err)
	}
	for i := 0; i < 2; i++ {
		if err := fixNoReplicaIdentity(ctx, s); err != nil {
			t.Fatalf("fix %d: %v\n%s", i+1, err, logs)
		}
	}
	w.with(func() {
		if w.ident != "f" {
			t.Errorf("replica identity after fix = %q, want f (the dev baseline)", w.ident)
		}
	})
	if !strings.Contains(logs.String(), "skipped: public.orders already has REPLICA IDENTITY FULL") {
		t.Errorf("second fix did not skip: %s", logs)
	}
}

// An update that fails for another reason is not the break.
func TestNoReplicaIdentityBreakOtherUpdateError(t *testing.T) {
	w := newQWorld(t)
	s, _ := w.stack()
	inner := s.sqlFn
	s.sqlFn = func(ctx context.Context, db string, sql ...string) (string, error) {
		if len(sql) == 1 && sql[0] == sqlAppUpdate {
			return "", errors.New("devctl: psql: ERROR: canceling statement due to lock timeout")
		}
		return inner(ctx, db, sql...)
	}
	err := breakNoReplicaIdentity(w.ctx(), s)
	if err == nil || !strings.Contains(err.Error(), "another reason") {
		t.Fatalf("break = %v, want 'another reason'", err)
	}
}

// ------------------------------------------------------------------ missing-grant

func TestMissingGrantBreakAndFix(t *testing.T) {
	w := newQWorld(t)
	s, logs := w.stack()
	ctx := w.ctx()
	if err := breakMissingGrant(ctx, s); err != nil {
		t.Fatalf("break: %v\n%s", err, logs)
	}
	w.with(func() {
		if !w.roleExists || w.roleSelect {
			t.Errorf("role after break: exists %v select %v, want a role without SELECT", w.roleExists, w.roleSelect)
		}
		for k, v := range missingGrantConnector {
			if w.cfg(k) != v {
				t.Errorf("connector %s = %q, want %q", k, w.cfg(k), v)
			}
		}
		if w.cfg("database.hostname") != "postgres" {
			t.Errorf("break lost the base connector keys: %v", w.connCfg)
		}
		if w.taskState != "FAILED" || w.slotActive {
			t.Errorf("task %s slot active %v, want FAILED and inactive", w.taskState, w.slotActive)
		}
	})
	if !strings.Contains(logs.String(), "permission denied for table orders") {
		t.Errorf("break log has no trace: %s", logs)
	}
	if err := breakMissingGrant(ctx, s); err == nil || !strings.Contains(err.Error(), "already exists") {
		t.Errorf("second break = %v, want a refusal", err)
	}
	for i := 0; i < 2; i++ {
		if err := fixMissingGrant(ctx, s); err != nil {
			t.Fatalf("fix %d: %v\n%s", i+1, err, logs)
		}
	}
	w.with(func() {
		if w.roleExists {
			t.Error("role devctl_cdc left behind")
		}
		if w.cfg("database.user") != "piperoom" || w.cfg("publication.autocreate.mode") != "filtered" || w.cfg("snapshot.mode") != "" {
			t.Errorf("connector config after fix = %v, want dev/connectors", w.connCfg)
		}
		if w.taskState != "RUNNING" || !w.slotActive {
			t.Errorf("task %s slot active %v after fix", w.taskState, w.slotActive)
		}
	})
	if !strings.Contains(logs.String(), "granted SELECT on public.orders to devctl_cdc") {
		t.Errorf("fix did not grant: %s", logs)
	}
}

// The grant is the fix: a fix whose restart still fails stops there and
// leaves the role for the next try.
func TestMissingGrantFixFailsWhileTaskFails(t *testing.T) {
	w := newQWorld(t)
	s, _ := w.stack()
	if err := breakMissingGrant(w.ctx(), s); err != nil {
		t.Fatal(err)
	}
	inner := s.sqlFn
	s.sqlFn = func(ctx context.Context, db string, sql ...string) (string, error) {
		if len(sql) == 1 && sql[0] == sqlGrantCDCSelect {
			return "", nil // a grant that did not take
		}
		return inner(ctx, db, sql...)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	if err := fixMissingGrant(ctx, s); err == nil || !strings.Contains(err.Error(), "did not recover after the grant") {
		t.Fatalf("fix = %v, want 'did not recover after the grant'", err)
	}
	w.with(func() {
		if !w.roleExists || w.cfg("database.user") != cdcRole {
			t.Error("failed fix went on to the baseline")
		}
	})
}

// ------------------------------------------------------------------- blocked-slot

func TestBlockedSlotBreakAndFix(t *testing.T) {
	w := newQWorld(t)
	s, logs := w.stack()
	ctx := w.ctx()
	if err := breakBlockedSlot(ctx, s); err != nil {
		t.Fatalf("break: %v\n%s", err, logs)
	}
	w.with(func() {
		if !w.blockerOpen || !w.slotWaiting || w.slotExists {
			t.Errorf("after break: blocker %v waiting %v slot %v", w.blockerOpen, w.slotWaiting, w.slotExists)
		}
		if w.offsetLSN != 0 {
			t.Error("offsets not cleared")
		}
		if !slices.Equal(w.ops, []string{"stop", "drop-slot", "delete-offsets", "resume"}) {
			t.Errorf("ops = %v", w.ops)
		}
		if len(w.detached) != 1 || !strings.Contains(strings.Join(w.detached[0], " "), "-d -e PGAPPNAME="+blockerApp+" "+pgContainer+" psql -U "+blockerRole+" ") {
			t.Errorf("detached = %v", w.detached)
		}
		// The connector's database.user is piperoom: a blocker running as it
		// would be refused by source_end_session, which never ends the
		// connector's own sessions.
		if !w.blockerRole || (len(w.detached) == 1 && strings.Contains(strings.Join(w.detached[0], " "), "-U piperoom")) {
			t.Errorf("blocker role %v, detached %v: the blocker must run as %s", w.blockerRole, w.detached, blockerRole)
		}
	})
	if err := breakBlockedSlot(ctx, s); err == nil || !strings.Contains(err.Error(), "missing or inactive") {
		t.Errorf("second break = %v, want a refusal", err)
	}
	for i := 0; i < 2; i++ {
		if err := fixBlockedSlot(ctx, s); err != nil {
			t.Fatalf("fix %d: %v\n%s", i+1, err, logs)
		}
	}
	w.with(func() {
		if w.blockerOpen || w.blockerRole || !w.slotExists || !w.slotActive || w.taskState != "RUNNING" {
			t.Errorf("after fix: blocker %v role %v slot %v/%v task %s", w.blockerOpen, w.blockerRole, w.slotExists, w.slotActive, w.taskState)
		}
		for id := int64(1); id <= w.nextID; id++ {
			if !w.loaded[id] {
				t.Fatalf("order %d missing from the warehouse after the fix", id)
			}
		}
	})
}

// A blocker that never starts stops the break before the slot is touched.
func TestBlockedSlotBreakBlockerDoesNotStart(t *testing.T) {
	w := newQWorld(t)
	s, _ := w.stack()
	s.detachFn = func(context.Context, string, ...string) error { return nil }
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	if err := breakBlockedSlot(ctx, s); err == nil || !strings.Contains(err.Error(), "did not start") {
		t.Fatalf("break = %v, want 'did not start'", err)
	}
	w.with(func() {
		if len(w.ops) != 0 || !w.slotExists {
			t.Errorf("break touched the connector or slot: ops %v slot %v", w.ops, w.slotExists)
		}
	})
}

// --------------------------------------------------------------- idle-wal-growth

func TestIdleWALGrowthBreakAndFix(t *testing.T) {
	w := newQWorld(t)
	s, logs := w.stack()
	ctx := w.ctx()
	if err := breakIdleWALGrowth(ctx, s); err != nil {
		t.Fatalf("break: %v\n%s", err, logs)
	}
	w.with(func() {
		if !w.batchDB || !w.slotActive || w.retained < idleWALFire {
			t.Errorf("after break: batch db %v active %v retained %d", w.batchDB, w.slotActive, w.retained)
		}
	})
	w.createTopics(heartbeatTopic, heartbeatRecordsTopic)
	for i := 0; i < 2; i++ {
		if err := fixIdleWALGrowth(ctx, s); err != nil {
			t.Fatalf("fix %d: %v\n%s", i+1, err, logs)
		}
	}
	w.with(func() {
		if w.retained >= idleWALClear || w.batchDB || w.heartbeat {
			t.Errorf("after fix: retained %d batch db %v heartbeat table %v", w.retained, w.batchDB, w.heartbeat)
		}
		if w.cfg("heartbeat.action.query") != "" || w.cfg("table.include.list") != "public.orders" {
			t.Errorf("connector config after fix = %v, want dev/connectors", w.connCfg)
		}
		if !slices.Contains(w.ops, "pub-add-heartbeat") {
			t.Errorf("ops = %v: the heartbeat table was not published", w.ops)
		}
		if len(w.subjectsDel) == 0 {
			t.Error("heartbeat subjects not deleted")
		}
	})
	for _, topic := range []string{heartbeatTopic, heartbeatRecordsTopic} {
		if _, ok, err := s.topicSpan(ctx, topic); err != nil || ok {
			t.Errorf("topic %s still exists after fix (err %v)", topic, err)
		}
	}
	if !strings.Contains(logs.String(), "heartbeat released the WAL") || !strings.Contains(logs.String(), "skipped: heartbeat") {
		t.Errorf("logs: %s", logs)
	}
}

// The break refuses an inactive slot (that is slot-inactive), and fails
// honestly when the WAL does not grow.
func TestIdleWALGrowthBreakErrors(t *testing.T) {
	w := newQWorld(t)
	w.slotActive = false
	s, _ := w.stack()
	if err := breakIdleWALGrowth(w.ctx(), s); err == nil || !strings.Contains(err.Error(), "missing or inactive") {
		t.Errorf("break on an inactive slot = %v", err)
	}
	w2 := newQWorld(t)
	s2, _ := w2.stack()
	inner := s2.sqlFn
	s2.sqlFn = func(ctx context.Context, db string, sql ...string) (string, error) {
		if len(sql) == 1 && sql[0] == sqlFillWAL {
			return "", nil // a batch that writes no WAL the slot keeps
		}
		return inner(ctx, db, sql...)
	}
	if err := breakIdleWALGrowth(w2.ctx(), s2); err == nil || !strings.Contains(err.Error(), "did not hold") {
		t.Errorf("break without growth = %v, want 'did not hold'", err)
	}
}

// ----------------------------------------------------------- stale-source-offsets

func TestStaleSourceOffsetsBreakAndFix(t *testing.T) {
	w := newQWorld(t)
	s, logs := w.stack()
	ctx := w.ctx()
	var stale uint64
	w.with(func() { stale = w.offsetLSN })
	if err := breakStaleSourceOffsets(ctx, s); err != nil {
		t.Fatalf("break: %v\n%s", err, logs)
	}
	w.with(func() {
		if !w.freshDB || w.freshRows != 10 {
			t.Errorf("fresh db %v rows %d", w.freshDB, w.freshRows)
		}
		if w.cfg("database.dbname") != freshDB || w.cfg("slot.name") != freshSlot || w.cfg("topic.prefix") != "pg" {
			t.Errorf("connector config after break = %v", w.connCfg)
		}
		if w.taskState != "FAILED" || w.offsetLSN != stale {
			t.Errorf("task %s offset %x, want FAILED with the old offset %x", w.taskState, w.offsetLSN, stale)
		}
	})
	if !strings.Contains(logs.String(), fmt.Sprintf("stored_lsn_now=%d", stale)) {
		t.Errorf("break log does not show the stale LSN: %s", logs)
	}
	if err := breakStaleSourceOffsets(ctx, s); err == nil || !strings.Contains(err.Error(), "already exists") {
		t.Errorf("second break = %v, want a refusal", err)
	}
	for i := 0; i < 2; i++ {
		if err := fixStaleSourceOffsets(ctx, s); err != nil {
			t.Fatalf("fix %d: %v\n%s", i+1, err, logs)
		}
	}
	w.with(func() {
		if w.freshDB || w.freshSlot {
			t.Errorf("fresh db %v slot %v left behind", w.freshDB, w.freshSlot)
		}
		if w.cfg("database.dbname") != "orders" || w.cfg("slot.name") != slotName {
			t.Errorf("connector config after fix = %v", w.connCfg)
		}
		if w.taskState != "RUNNING" || !w.slotActive || w.offsetLSN == 0 || w.offsetLSN == stale {
			t.Errorf("after fix: task %s slot active %v offset %x", w.taskState, w.slotActive, w.offsetLSN)
		}
		i := slices.Index(w.ops, "delete-offsets")
		if i < 1 || w.ops[i-1] != "stop" {
			t.Errorf("ops = %v: the offsets must be deleted on a stopped connector", w.ops)
		}
	})
	if !strings.Contains(logs.String(), "skipped: no "+freshDB) {
		t.Errorf("second fix did not skip: %s", logs)
	}
}

// Without stored offsets nothing can go stale: the break creates nothing.
func TestStaleSourceOffsetsBreakNeedsOffsets(t *testing.T) {
	w := newQWorld(t)
	w.offsetLSN = 0
	s, _ := w.stack()
	if err := breakStaleSourceOffsets(w.ctx(), s); err == nil || !strings.Contains(err.Error(), "no stored offsets") {
		t.Fatalf("break = %v", err)
	}
	w.with(func() {
		if w.freshDB || len(w.ops) != 0 {
			t.Errorf("break changed the stack: fresh db %v ops %v", w.freshDB, w.ops)
		}
	})
}

func TestStoredOffsetsLSN(t *testing.T) {
	tests := []struct {
		body string
		want string
	}{
		{`{"offsets":[]}`, ""},
		{`{"offsets":[{"partition":{"server":"pg"},"offset":{"lsn":36028797018963971,"txId":5}}]}`, "36028797018963971"},
		{`{"offsets":[{"partition":{"server":"pg"},"offset":{"txId":5}}]}`, ""},
	}
	for _, tt := range tests {
		var o storedOffsets
		dec := json.NewDecoder(strings.NewReader(tt.body))
		dec.UseNumber()
		if err := dec.Decode(&o); err != nil {
			t.Fatal(err)
		}
		if got := o.lsn(); got != tt.want {
			t.Errorf("lsn(%s) = %q, want %q", tt.body, got, tt.want)
		}
	}
}

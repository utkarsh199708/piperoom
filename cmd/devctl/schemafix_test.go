package main

import (
	"context"
	"errors"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// Tests of the QA G17 changes to the schema-change fix (fakeLayers is in
// scenarios_test.go).

// shapedSchema is the captured int schema with amount changed to another Avro
// type, the shape another decimal.handling.mode registers (double, string).
func shapedSchema(t testing.TB, typ string) string {
	t.Helper()
	s := capturedSchema(t, intSchemaFile)
	const intAmount = `{"name":"amount","type":"int"}`
	if strings.Count(s, intAmount) != 1 {
		t.Fatalf("capture no longer has exactly one %s; update shapedSchema", intAmount)
	}
	return strings.Replace(s, intAmount, `{"name":"amount","type":"`+typ+`"}`, 1)
}

// An approved agent fix set decimal.handling.mode=double: the migrated schema
// (amount double) is registered and the task RUNNING. QA saw the old fix wait 60 s
// for a decimal version that never came; now it only restores BACKWARD.
func TestFixSchemaChangeAfterAgentFix(t *testing.T) {
	for _, typ := range []string{"double", "string"} {
		t.Run(typ, func(t *testing.T) {
			f := brokenLayers(t)
			f.schemas = append(f.schemas, shapedSchema(t, typ))
			f.taskState, f.compat = "RUNNING", "NONE"
			s, logs := newTestStack(t, f)
			if err := fixSchemaChange(context.Background(), s); err != nil {
				t.Fatalf("fixSchemaChange after the agent's fix: %v", err)
			}
			compat, task, versions, restarts := f.snapshot()
			if compat != "BACKWARD" || task != "RUNNING" || versions != 2 || restarts != 0 {
				t.Errorf("end state compat=%q task=%q versions=%d restarts=%d, want BACKWARD RUNNING 2 0", compat, task, versions, restarts)
			}
			if !strings.Contains(logs.String(), "amount="+typ) {
				t.Errorf("log does not name the registered amount type %q: %s", typ, logs.String())
			}
		})
	}
}

// A connector still on publication.autocreate.mode=filtered cannot get past the
// ALTER after a restart (G17). The fix moves it to disabled, keeping its other
// keys, and then recovers.
func TestFixSchemaChangeMovesFilteredConnectorToDisabled(t *testing.T) {
	f := brokenLayers(t)
	f.config = map[string]string{"name": connectorName, "publication.autocreate.mode": "filtered", "decimal.handling.mode": "precise"}
	f.corruptLog, f.corruptMode, f.t = filepath.Join(t.TempDir(), "connect.log"), "filtered", t
	s, logs := newTestStack(t, f)
	if err := fixSchemaChange(context.Background(), s); err != nil {
		t.Fatalf("fixSchemaChange with a filtered connector: %v", err)
	}
	mode, puts := f.configSnapshot()
	if mode != "disabled" || puts != 1 {
		t.Errorf("publication mode = %q after %d config PUTs, want disabled after 1", mode, puts)
	}
	f.mu.Lock()
	kept := f.config["decimal.handling.mode"]
	f.mu.Unlock()
	if kept != "precise" {
		t.Errorf("decimal.handling.mode = %q, want the live value kept", kept)
	}
	compat, task, versions, _ := f.snapshot()
	if compat != "BACKWARD" || task != "RUNNING" || versions != 2 {
		t.Errorf("end state compat=%q task=%q versions=%d, want BACKWARD RUNNING 2", compat, task, versions)
	}
	if !strings.Contains(logs.String(), "switching it to disabled") {
		t.Errorf("the switch was not logged: %s", logs.String())
	}
	f.mu.Lock()
	inits := f.cdcInits
	f.mu.Unlock()
	if inits != 1 {
		t.Errorf("CDC init script ran %d times, want 1 (a filtered start drops the heartbeat table from the publication)", inits)
	}
}

// Error path: the CDC init script fails. The connector stays on filtered and no
// task is restarted (a restart there would loop on the decode error).
func TestFixSchemaChangeCDCInitFails(t *testing.T) {
	f := brokenLayers(t)
	f.config = map[string]string{"name": connectorName, "publication.autocreate.mode": "filtered"}
	f.cdcErr = errors.New("psql: connection refused")
	s, _ := newTestStack(t, f)
	err := fixSchemaChange(context.Background(), s)
	if err == nil || !strings.Contains(err.Error(), "connection refused") {
		t.Fatalf("fixSchemaChange = %v, want the CDC init error", err)
	}
	mode, puts := f.configSnapshot()
	if _, _, _, restarts := f.snapshot(); mode != "filtered" || puts != 0 || restarts != 0 {
		t.Errorf("after a failed CDC init: mode=%q puts=%d restarts=%d, want filtered 0 0", mode, puts, restarts)
	}
}

// A connector already on disabled is left alone: no config PUT, no CDC init.
func TestFixSchemaChangeDisabledConnectorUntouched(t *testing.T) {
	f := brokenLayers(t)
	s, _ := newTestStack(t, f)
	if err := fixSchemaChange(context.Background(), s); err != nil {
		t.Fatal(err)
	}
	f.mu.Lock()
	inits := f.cdcInits
	f.mu.Unlock()
	if _, puts := f.configSnapshot(); puts != 0 || inits != 0 {
		t.Errorf("config PUTs = %d, CDC inits = %d; want 0 and 0 for a disabled connector", puts, inits)
	}
}

// Error path: the task shows RUNNING but the worker log has the decode error.
// The fix stops at once (no 3-minute wait), says why, and leaves NONE in place.
// An ERROR written before the fix started is not counted.
func TestFixSchemaChangeStopsOnDecodeError(t *testing.T) {
	f := brokenLayers(t)
	f.corruptLog, f.corruptMode, f.t = filepath.Join(t.TempDir(), "connect.log"), "disabled", t
	appendFile(t, f.corruptLog, "[2026-10-15 00:20:00,000] ERROR an older error, before the fix ran (x.Y)\n")
	s, _ := newTestStack(t, f)
	start := time.Now()
	err := fixSchemaChange(context.Background(), s)
	if !errors.Is(err, errDecodeStuck) {
		t.Fatalf("fixSchemaChange = %v, want errDecodeStuck", err)
	}
	if time.Since(start) > 10*time.Second {
		t.Errorf("fix took %s; it should stop at the first sight of the decode error", time.Since(start))
	}
	for _, want := range []string{"new schema version was not registered", "compressed lz4 data is corrupt", "Known issues (G17)", "1 ERROR record(s)"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error does not contain %q: %v", want, err)
		}
	}
	if compat, _, _, _ := f.snapshot(); compat != "NONE" {
		t.Errorf("compatibility = %q after a failed fix, want NONE (not restored)", compat)
	}
}

// Right after a config change Connect answers 409 to a restart while the worker
// group rebalances; the restart is retried.
func TestFixSchemaChangeRetriesRestartOnConflict(t *testing.T) {
	f := brokenLayers(t)
	f.conflicts = 2
	s, _ := newTestStack(t, f)
	if err := fixSchemaChange(context.Background(), s); err != nil {
		t.Fatalf("fixSchemaChange with two 409 answers: %v", err)
	}
	if compat, task, versions, restarts := f.snapshot(); compat != "BACKWARD" || task != "RUNNING" || versions != 2 || restarts != 1 {
		t.Errorf("end state compat=%q task=%q versions=%d restarts=%d, want BACKWARD RUNNING 2 1", compat, task, versions, restarts)
	}
}

// Error path: Connect keeps answering 409. The restart gives up and says so.
func TestRestartTaskSettledGivesUp(t *testing.T) {
	f := brokenLayers(t)
	f.conflicts = 1 << 30
	s, _ := newTestStack(t, f)
	err := s.restartTaskSettled(context.Background(), 0, 200*time.Millisecond)
	var he *httpStatusError
	if err == nil || !errors.As(err, &he) || he.Code != 409 {
		t.Fatalf("restartTaskSettled = %v, want an error carrying the 409", err)
	}
}

// Error path: Connect cannot return the live config. The fix stops before it
// restarts anything.
func TestFixSchemaChangeConfigUnreadable(t *testing.T) {
	f := brokenLayers(t)
	s, _ := newTestStack(t, f)
	s.cfg.ConnectURL += "/nowhere" // status and config answer 404
	if err := fixSchemaChange(context.Background(), s); err == nil {
		t.Fatal("fixSchemaChange with Connect answering 404 = nil, want error")
	}
	if _, _, _, restarts := f.snapshot(); restarts != 0 {
		t.Errorf("task restarts = %d, want 0", restarts)
	}
}

func TestAmountShape(t *testing.T) {
	tests := []struct {
		name   string
		schema string
		want   string
	}{
		{"captured int (version 1)", capturedSchema(t, intSchemaFile), "int"},
		{"captured decimal (version 2)", capturedSchema(t, decimalSchemaFile), "decimal"},
		{"decimal.handling.mode=double", shapedSchema(t, "double"), "double"},
		{"decimal.handling.mode=string", shapedSchema(t, "string"), "string"},
		{"nullable double", strings.Replace(shapedSchema(t, "double"), `"type":"double"`, `"type":["null","double"]`, 1), "double"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := amountShape(tt.schema)
			if err != nil {
				t.Fatalf("amountShape: %v", err)
			}
			if got != tt.want {
				t.Errorf("amountShape = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestPublicationMode(t *testing.T) {
	tests := []struct {
		cfg  map[string]string
		want string
	}{
		{map[string]string{"publication.autocreate.mode": "disabled"}, "disabled"},
		{map[string]string{"publication.autocreate.mode": "filtered"}, "filtered"},
		{map[string]string{}, "all_tables"}, // Debezium's default
	}
	for _, tt := range tests {
		if got := publicationMode(tt.cfg); got != tt.want {
			t.Errorf("publicationMode(%v) = %q, want %q", tt.cfg, got, tt.want)
		}
	}
}

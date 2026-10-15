package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"piperoom/internal/config"
)

// The two value schemas are real Schema Registry responses captured from the dev
// stack (copies of cmd/devconsumer/testdata): version 1 has amount as int, version
// 2 has amount as bytes with logicalType decimal. Debezium defines the row record
// once, in before, and refers to it from after as "Value".
const (
	intSchemaFile     = "schema-2.json"
	decimalSchemaFile = "schema-3.json"
)

// capturedSchema returns the schema text of a captured registry response.
func capturedSchema(t testing.TB, name string) string {
	t.Helper()
	b, err := os.ReadFile(filepath.Join("testdata", name))
	if err != nil {
		t.Fatalf("read capture: %v", err)
	}
	var v struct {
		Schema string `json:"schema"`
	}
	if err := json.Unmarshal(b, &v); err != nil || v.Schema == "" {
		t.Fatalf("capture %s has no schema: %v", name, err)
	}
	return v.Schema
}

// fakeLayers plays Kafka Connect and Schema Registry for fixSchemaChange on one
// server. Restarting task 0 registers nextSchema, as Debezium does, but only while
// the subject allows it (compatibility NONE).
type fakeLayers struct {
	mu         sync.Mutex
	connState  string
	taskState  string
	compat     string
	schemas    []string // value schema per version; schemas[0] is version 1
	nextSchema string   // what a task restart registers
	restarts   int

	noRegister bool // a restart does not register the new schema (timeout path)
	failLatest bool // GET .../versions/latest returns 500 (registry down path)

	config     map[string]string // the live connector config; nil means the dev default (disabled)
	configPuts int
	conflicts  int // the next restarts answer 409 (rebalance after a config change)

	// corruptLog, when set, is a worker log file: a task start in corruptMode
	// appends the G17 decode error there and registers nothing, as Debezium does
	// in filtered mode.
	corruptLog  string
	corruptMode string
	t           testing.TB

	cdcInits int   // runs of the CDC init script (stack.cdcInit, never docker in tests)
	cdcErr   error // what the CDC init script returns
}

// liveConfig returns the connector config, the dev default when none is set.
func (f *fakeLayers) liveConfig() map[string]string {
	if f.config == nil {
		f.config = map[string]string{"name": connectorName, "publication.autocreate.mode": "disabled"}
	}
	return f.config
}

// startTask is what a task start does: it streams, and registers the new schema
// when the subject allows it, unless filtered mode makes it fail to decode.
func (f *fakeLayers) startTask() {
	f.taskState = "RUNNING"
	if f.corruptLog != "" && publicationMode(f.liveConfig()) == f.corruptMode {
		appendFile(f.t, f.corruptLog, g17LogExcerpt)
		return
	}
	if f.compat == "NONE" && !f.noRegister && f.latestIsInt() {
		f.schemas = append(f.schemas, f.nextSchema)
	}
}

func (f *fakeLayers) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	defer f.mu.Unlock()
	path := r.URL.Path
	switch {
	case r.Method == http.MethodGet && path == "/connectors/"+connectorName+"/status":
		writeJSON(w, http.StatusOK, fmt.Sprintf(`{"connector":{"state":%q},"tasks":[{"id":0,"state":%q}]}`, f.connState, f.taskState))
	case r.Method == http.MethodGet && path == "/connectors/"+connectorName+"/config":
		b, _ := json.Marshal(f.liveConfig())
		writeJSON(w, http.StatusOK, string(b))
	case r.Method == http.MethodPut && path == "/connectors/"+connectorName+"/config":
		var cfg map[string]string
		if err := json.NewDecoder(r.Body).Decode(&cfg); err != nil {
			writeJSON(w, http.StatusBadRequest, `{"error_code":400}`)
			return
		}
		f.config = cfg
		f.configPuts++
		f.startTask() // Connect restarts the tasks for a new config
		writeJSON(w, http.StatusOK, `{}`)
	case r.Method == http.MethodPost && path == "/connectors/"+connectorName+"/tasks/0/restart":
		if f.conflicts > 0 {
			f.conflicts--
			writeJSON(w, http.StatusConflict, `{"error_code":409,"message":"Cannot complete request because of a conflicting operation (e.g. worker rebalance)"}`)
			return
		}
		f.restarts++
		f.startTask()
		w.WriteHeader(http.StatusNoContent)
	case r.Method == http.MethodPut && path == "/config/"+valueSubject:
		var body struct {
			Compatibility string `json:"compatibility"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			writeJSON(w, http.StatusUnprocessableEntity, `{"error_code":42203}`)
			return
		}
		f.compat = body.Compatibility
		writeJSON(w, http.StatusOK, fmt.Sprintf(`{"compatibility":%q}`, f.compat))
	case r.Method == http.MethodGet && path == "/subjects/"+valueSubject+"/versions":
		ids := make([]string, 0, len(f.schemas))
		for i := range f.schemas {
			ids = append(ids, fmt.Sprint(i+1))
		}
		writeJSON(w, http.StatusOK, "["+strings.Join(ids, ",")+"]")
	case r.Method == http.MethodGet && path == "/subjects/"+valueSubject+"/versions/latest":
		if f.failLatest {
			writeJSON(w, http.StatusInternalServerError, `{"error_code":50001}`)
			return
		}
		n := len(f.schemas)
		b, _ := json.Marshal(map[string]any{"subject": valueSubject, "version": n, "id": n, "schema": f.schemas[n-1]})
		writeJSON(w, http.StatusOK, string(b))
	default:
		writeJSON(w, http.StatusNotFound, `{"error_code":40401}`)
	}
}

func (f *fakeLayers) latestIsInt() bool {
	shape, err := amountShape(f.schemas[len(f.schemas)-1])
	return err == nil && shape == "int"
}

// g17LogExcerpt is the worker log of a task that cannot decode the WAL from
// before the ALTER, shortened from the dev stack (Kafka layout of the stand-in).
const g17LogExcerpt = `[2026-10-15 00:22:16,917] INFO Processing messages (io.debezium.connector.postgresql.PostgresStreamingChangeEventSource)
[2026-10-15 00:22:16,938] ERROR Producer failure (io.debezium.pipeline.ErrorHandler)
org.postgresql.util.PSQLException: ERROR: compressed lz4 data is corrupt
	at org.postgresql.core.v3.QueryExecutorImpl.receiveErrorResponse(QueryExecutorImpl.java:2734)
	at org.postgresql.core.v3.CopyDualImpl.receiveCopyData(CopyDualImpl.java:31)
[2026-10-15 00:22:16,939] INFO Connected metrics set to 'false' (io.debezium.pipeline.ChangeEventSourceCoordinator)
`

// appendFile appends text to path. It runs on the fake server's goroutine, so it
// reports with Errorf (safe there), not Fatal.
func appendFile(t testing.TB, path, text string) {
	f, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		t.Errorf("append to %s: %v", path, err)
		return
	}
	defer f.Close()
	if _, err := f.WriteString(text); err != nil {
		t.Errorf("append to %s: %v", path, err)
	}
}

// snapshot returns the state the tests assert on, under the lock the handler uses.
func (f *fakeLayers) snapshot() (compat, taskState string, versions, restarts int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.compat, f.taskState, len(f.schemas), f.restarts
}

// configSnapshot returns the live publication mode and how often the config was PUT.
func (f *fakeLayers) configSnapshot() (mode string, puts int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return publicationMode(f.liveConfig()), f.configPuts
}

func writeJSON(w http.ResponseWriter, code int, body string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_, _ = io.WriteString(w, body)
}

// newTestStack points a stack at one httptest server that plays both services.
func newTestStack(t *testing.T, f *fakeLayers) (*stack, *bytes.Buffer) {
	t.Helper()
	srv := httptest.NewServer(f)
	t.Cleanup(srv.Close)
	var logs bytes.Buffer
	return &stack{
		cfg:  config.Config{ConnectURL: srv.URL, SchemaRegistryURL: srv.URL, ConnectWorkerLogFile: f.corruptLog},
		http: &http.Client{Timeout: 5 * time.Second},
		log:  slog.New(slog.NewTextHandler(&logs, nil)),
		poll: 10 * time.Millisecond,
		cdcInit: func(context.Context) error {
			f.mu.Lock()
			defer f.mu.Unlock()
			f.cdcInits++
			return f.cdcErr
		},
	}, &logs
}

// brokenLayers is the stack right after the migration: version 1 has int, the
// subject is BACKWARD, and task 0 has FAILED.
func brokenLayers(t testing.TB) *fakeLayers {
	return &fakeLayers{
		connState:  "RUNNING",
		taskState:  "FAILED",
		compat:     "BACKWARD",
		schemas:    []string{capturedSchema(t, intSchemaFile)},
		nextSchema: capturedSchema(t, decimalSchemaFile),
	}
}

// fixedLayers is the stack after a complete fix: version 2 has decimal, the
// connector and task are RUNNING, and the subject is on the given compatibility.
func fixedLayers(t testing.TB, compat string) *fakeLayers {
	f := brokenLayers(t)
	f.schemas = append(f.schemas, f.nextSchema)
	f.taskState = "RUNNING"
	f.compat = compat
	return f
}

func TestFixSchemaChangeOnBrokenStack(t *testing.T) {
	f := brokenLayers(t)
	s, logs := newTestStack(t, f)
	if err := fixSchemaChange(context.Background(), s); err != nil {
		t.Fatalf("fixSchemaChange on broken stack: %v", err)
	}
	compat, task, versions, restarts := f.snapshot()
	if compat != "BACKWARD" {
		t.Errorf("compatibility = %q, want BACKWARD", compat)
	}
	if task != "RUNNING" {
		t.Errorf("task 0 state = %q, want RUNNING", task)
	}
	if versions != 2 {
		t.Errorf("registered versions = %d, want 2 (the new decimal schema)", versions)
	}
	if restarts != 1 {
		t.Errorf("task restarts = %d, want 1", restarts)
	}
	if strings.Contains(logs.String(), "skipped") {
		t.Errorf("broken stack logged a skip: %s", logs.String())
	}
}

// Already fixed (schema registered, task RUNNING): succeed without a restart and
// end on BACKWARD, whatever compatibility the stack was left on.
func TestFixSchemaChangeAlreadyFixed(t *testing.T) {
	for _, compat := range []string{"NONE", "BACKWARD"} {
		t.Run("compat "+compat, func(t *testing.T) {
			f := fixedLayers(t, compat)
			s, logs := newTestStack(t, f)
			if err := fixSchemaChange(context.Background(), s); err != nil {
				t.Fatalf("fixSchemaChange on fixed stack: %v", err)
			}
			gotCompat, task, versions, restarts := f.snapshot()
			if gotCompat != "BACKWARD" {
				t.Errorf("compatibility = %q, want BACKWARD", gotCompat)
			}
			if task != "RUNNING" {
				t.Errorf("task 0 state = %q, want RUNNING", task)
			}
			if restarts != 0 {
				t.Errorf("task restarts = %d, want 0 (schema registered and task RUNNING)", restarts)
			}
			if versions != 2 {
				t.Errorf("registered versions = %d, want 2 (nothing new registered)", versions)
			}
			if !strings.Contains(logs.String(), "skipped: task 0 already RUNNING") {
				t.Errorf("log does not say the restart was skipped: %s", logs.String())
			}
			if !strings.Contains(logs.String(), "skipped: new schema version already registered") {
				t.Errorf("log does not say the wait was skipped: %s", logs.String())
			}
		})
	}
}

// The schema is registered but the task is FAILED (someone registered it and did
// not restart). The fix restarts the task once and does not wait.
func TestFixSchemaChangeRegisteredButTaskFailed(t *testing.T) {
	f := fixedLayers(t, "NONE")
	f.taskState = "FAILED"
	s, logs := newTestStack(t, f)
	if err := fixSchemaChange(context.Background(), s); err != nil {
		t.Fatalf("fixSchemaChange: %v", err)
	}
	compat, task, versions, restarts := f.snapshot()
	if compat != "BACKWARD" || task != "RUNNING" {
		t.Errorf("end state compat=%q task=%q, want BACKWARD and RUNNING", compat, task)
	}
	if restarts != 1 {
		t.Errorf("task restarts = %d, want 1", restarts)
	}
	if versions != 2 {
		t.Errorf("registered versions = %d, want 2", versions)
	}
	if strings.Contains(logs.String(), "skipped: task 0 already RUNNING") {
		t.Errorf("failed task was logged as already RUNNING: %s", logs.String())
	}
}

// The schema is not registered yet, and task 0 shows RUNNING. The task can still
// fail again on the 409, so the fix restarts it anyway, then registers the schema.
func TestFixSchemaChangeNotRegisteredTaskRunning(t *testing.T) {
	f := brokenLayers(t)
	f.taskState = "RUNNING"
	f.compat = "NONE"
	s, _ := newTestStack(t, f)
	if err := fixSchemaChange(context.Background(), s); err != nil {
		t.Fatalf("fixSchemaChange: %v", err)
	}
	compat, task, versions, restarts := f.snapshot()
	if compat != "BACKWARD" || task != "RUNNING" || versions != 2 {
		t.Errorf("end state compat=%q task=%q versions=%d, want BACKWARD RUNNING 2", compat, task, versions)
	}
	if restarts != 1 {
		t.Errorf("task restarts = %d, want 1 (not registered: always restart)", restarts)
	}
}

// Partly fixed: compatibility is NONE and the schema is not registered yet. The
// fix finishes the job: restart, wait for the new version, restore BACKWARD.
func TestFixSchemaChangePartlyFixed(t *testing.T) {
	f := brokenLayers(t)
	f.compat = "NONE"
	s, _ := newTestStack(t, f)
	if err := fixSchemaChange(context.Background(), s); err != nil {
		t.Fatalf("fixSchemaChange on partly fixed stack: %v", err)
	}
	compat, task, versions, restarts := f.snapshot()
	if compat != "BACKWARD" || task != "RUNNING" || versions != 2 || restarts != 1 {
		t.Errorf("end state compat=%q task=%q versions=%d restarts=%d, want BACKWARD RUNNING 2 1",
			compat, task, versions, restarts)
	}
}

// Running the fix twice: the second run sees the fixed stack and changes nothing.
func TestFixSchemaChangeRunTwice(t *testing.T) {
	f := brokenLayers(t)
	s, _ := newTestStack(t, f)
	ctx := context.Background()
	for i := 1; i <= 2; i++ {
		if err := fixSchemaChange(ctx, s); err != nil {
			t.Fatalf("run %d: fixSchemaChange: %v", i, err)
		}
	}
	compat, task, versions, restarts := f.snapshot()
	if compat != "BACKWARD" || task != "RUNNING" {
		t.Errorf("end state compat=%q task=%q, want BACKWARD and RUNNING", compat, task)
	}
	if versions != 2 {
		t.Errorf("registered versions = %d, want 2", versions)
	}
	if restarts != 1 {
		t.Errorf("task restarts over two runs = %d, want 1", restarts)
	}
}

// Error path: the new schema never registers. The fix must fail and must not
// restore BACKWARD, which would make the task fail again on the next record.
func TestFixSchemaChangeNewSchemaNeverRegistered(t *testing.T) {
	f := brokenLayers(t)
	f.noRegister = true
	s, _ := newTestStack(t, f)
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	err := fixSchemaChange(ctx, s)
	if err == nil || !strings.Contains(err.Error(), "new schema version was not registered") {
		t.Fatalf("fixSchemaChange = %v, want 'new schema version was not registered'", err)
	}
	compat, _, _, _ := f.snapshot()
	if compat != "NONE" {
		t.Errorf("compatibility = %q after failed fix, want NONE (not restored)", compat)
	}
}

// Error path: the registry cannot answer the latest-version read. The fix must
// stop before it changes anything.
func TestFixSchemaChangeRegistryDown(t *testing.T) {
	f := brokenLayers(t)
	f.failLatest = true
	s, _ := newTestStack(t, f)
	err := fixSchemaChange(context.Background(), s)
	if err == nil {
		t.Fatal("fixSchemaChange with registry failing = nil, want error")
	}
	compat, _, _, restarts := f.snapshot()
	if compat != "BACKWARD" || restarts != 0 {
		t.Errorf("registry error still changed state: compat=%q restarts=%d", compat, restarts)
	}
}

// TestAmountIsDecimal runs on the real captured schemas. The "after" reference is
// changed in the text to cover the fully qualified name and an unknown name.
func TestAmountIsDecimal(t *testing.T) {
	intSchema := capturedSchema(t, intSchemaFile)
	decSchema := capturedSchema(t, decimalSchemaFile)
	const bareRef = `["null","Value"]`
	if strings.Count(decSchema, bareRef) != 1 {
		t.Fatalf("capture no longer has exactly one %s; update the fixture edits", bareRef)
	}
	tests := []struct {
		name    string
		schema  string
		want    bool
		wantErr bool
	}{
		{"captured int (version 1)", intSchema, false, false},
		{"captured decimal (version 2)", decSchema, true, false},
		{"decimal, after by fully qualified name", strings.Replace(decSchema, bareRef, `["null","pg.public.orders.Value"]`, 1), true, false},
		{"int, after by fully qualified name", strings.Replace(intSchema, bareRef, `["null","pg.public.orders.Value"]`, 1), false, false},
		{"after names an undefined record", strings.Replace(decSchema, bareRef, `["null","Missing"]`, 1), false, true},
		{"not JSON", "not-json", false, true},
		{"not a record", `"string"`, false, true},
		{"no after field", `{"type":"record","name":"E","fields":[{"name":"op","type":"string"}]}`, false, true},
		{"after has no amount", `{"type":"record","name":"E","fields":[{"name":"after","type":["null",{"type":"record","name":"V","fields":[{"name":"id","type":"long"}]}]}]}`, false, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := amountIsDecimal(tt.schema)
			if (err != nil) != tt.wantErr {
				t.Fatalf("amountIsDecimal error = %v, wantErr %v", err, tt.wantErr)
			}
			if got != tt.want {
				t.Errorf("amountIsDecimal = %v, want %v", got, tt.want)
			}
		})
	}
}

// TestAmountIsDecimalNoStaleNamedType checks that a record name defined by an
// earlier call does not satisfy a later schema. The first call defines
// pg.public.orders.Value; the second refers to Value without defining it and
// must fail rather than reuse the earlier definition.
func TestAmountIsDecimalNoStaleNamedType(t *testing.T) {
	decSchema := capturedSchema(t, decimalSchemaFile)
	if got, err := amountIsDecimal(decSchema); err != nil || !got {
		t.Fatalf("setup: amountIsDecimal(captured decimal) = %v, %v; want true, nil", got, err)
	}
	const dangling = `{"type":"record","name":"Envelope","namespace":"pg.public.orders","fields":[{"name":"after","type":["null","Value"]}]}`
	got, err := amountIsDecimal(dangling)
	if err == nil {
		t.Fatalf("amountIsDecimal(dangling Value) = %v, nil; want an error for an undefined name", got)
	}
}

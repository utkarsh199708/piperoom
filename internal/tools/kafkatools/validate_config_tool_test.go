package kafkatools

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"piperoom/internal/clients"
	"piperoom/internal/clients/connect"
	"piperoom/internal/owners"
	"piperoom/internal/tools"
)

const validateClass = "io.debezium.connector.postgresql.PostgresConnector"

// validateAPI is an httptest Kafka Connect for connect_validate_config: the
// plugin config defs, config/validate, and the base/pipeline connectors'
// config and status. Every request is counted so a refusal can be shown to
// happen before the validate call.
type validateAPI struct {
	t *testing.T

	mu          sync.Mutex
	defsCalls   int
	statusCalls int
	configCalls int
	putCalls    int
	putBody     map[string]string

	defsStatus     int
	defs           []connect.ConfigKeyDef
	validateStatus int
	validateBody   string
	baseConfig     map[string]string
	sourceStatus   clients.ConnectorStatus
	sourceConfig   map[string]string
	delay          time.Duration
}

func newValidateAPI(t *testing.T) *validateAPI {
	return &validateAPI{
		t:              t,
		defsStatus:     http.StatusOK,
		validateStatus: http.StatusOK,
		validateBody:   `{"error_count":0,"configs":[]}`,
		defs: []connect.ConfigKeyDef{
			{Name: "connector.class", Required: true},
			{Name: "database.hostname", Required: true},
			{Name: "database.port"},
			{Name: "database.user"},
			{Name: "database.password"},
			{Name: "database.dbname"},
			{Name: "topic.prefix", Required: true},
			{Name: "decimal.handling.mode"},
			{Name: "key.converter"},
		},
		sourceStatus: clients.ConnectorStatus{Name: "pg-orders-connector", Type: "source", State: "RUNNING"},
	}
}

func (a *validateAPI) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	a.mu.Lock()
	a.requestLocked(r)
	a.mu.Unlock()
	switch {
	case r.Method == http.MethodGet && strings.HasPrefix(r.URL.Path, "/connector-plugins/") && strings.HasSuffix(r.URL.Path, "/config"):
		a.writeJSON(w, a.defsStatus, a.defs) // KIP-769: a top-level array
	case r.Method == http.MethodPut && strings.HasPrefix(r.URL.Path, "/connector-plugins/") && strings.HasSuffix(r.URL.Path, "/config/validate"):
		var body map[string]string
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			a.t.Errorf("validate body: %v", err)
		}
		a.mu.Lock()
		a.putBody = body
		a.mu.Unlock()
		if a.delay > 0 {
			time.Sleep(a.delay)
		}
		a.writeJSON(w, a.validateStatus, json.RawMessage(a.validateBody))
	case r.Method == http.MethodGet && strings.HasSuffix(r.URL.Path, "/config"):
		cfg := a.baseConfig
		if a.sourceConfig != nil {
			cfg = a.sourceConfig
		}
		a.writeJSON(w, http.StatusOK, cfg)
	case r.Method == http.MethodGet && strings.HasSuffix(r.URL.Path, "/status"):
		a.writeJSON(w, http.StatusOK, a.sourceStatus)
	default:
		a.t.Errorf("unexpected %s %s", r.Method, r.URL.Path)
		http.NotFound(w, r)
	}
}

func (a *validateAPI) requestLocked(r *http.Request) {
	switch {
	case r.Method == http.MethodGet && strings.HasPrefix(r.URL.Path, "/connector-plugins/"):
		a.defsCalls++
	case r.Method == http.MethodPut:
		a.putCalls++
	case strings.HasSuffix(r.URL.Path, "/status"):
		a.statusCalls++
	case strings.HasSuffix(r.URL.Path, "/config"):
		a.configCalls++
	}
}

func (a *validateAPI) writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	switch b := v.(type) {
	case json.RawMessage:
		_, _ = w.Write(b)
	default:
		if err := json.NewEncoder(w).Encode(b); err != nil {
			a.t.Errorf("encode response: %v", err)
		}
	}
}

func (a *validateAPI) counts() (defs, status, config, put int) {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.defsCalls, a.statusCalls, a.configCalls, a.putCalls
}

func (a *validateAPI) body() map[string]string {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.putBody
}

// validateOwners is the orders pipeline. Its connections.source_db_url is a
// literal (the provisional/setup form), so no environment or file is read.
func validateOwners(connectors ...string) *owners.Owners {
	return &owners.Owners{Pipelines: []owners.Pipeline{{
		ID:           "orders",
		TopicPrefix:  "pg",
		SourceTables: []string{"public.orders"},
		Connectors:   connectors,
		Connections: owners.Connections{
			"source_db_url": {Value: "postgres://piperoom:secret@db.acme:5432/orders?sslmode=disable"},
		},
	}}}
}

func validateOwnersNoHost(connectors ...string) *owners.Owners {
	o := validateOwners(connectors...)
	o.Pipelines[0].Connections = nil
	return o
}

func newValidateTool(t *testing.T, c clients.ConnectClient, o *owners.Owners, renamed func(string) (string, string, bool)) tools.Tool {
	t.Helper()
	reg := tools.NewRegistry()
	if err := Register(reg, Deps{Connect: c, Owners: o, RenamedKey: renamed}); err != nil {
		t.Fatalf("Register: %v", err)
	}
	tl, ok := reg.Get("connect_validate_config")
	if !ok {
		t.Fatal("connect_validate_config not registered")
	}
	if tl.Kind() != tools.KindRead {
		t.Fatalf("kind = %s, want read", tl.Kind())
	}
	return tl
}

func runValidate(t *testing.T, tl tools.Tool, pipeline, input string) (tools.Result, error) {
	t.Helper()
	return tl.Run(tools.WithPipelineID(context.Background(), pipeline), json.RawMessage(input))
}

func mustRunValidate(t *testing.T, tl tools.Tool, input string) tools.Result {
	t.Helper()
	r, err := runValidate(t, tl, "orders", input)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	return r
}

func TestConnectValidateConfigHappyPath(t *testing.T) {
	api := newValidateAPI(t)
	api.validateBody = `{"error_count":1,"configs":[
		{"value":{"name":"decimal.handling.mode","errors":["Invalid value bad; allowed: precise, string"]}},
		{"value":{"name":"database.hostname","errors":[]}}]}`
	srv := httptest.NewServer(api)
	defer srv.Close()
	tl := newValidateTool(t, connect.New(srv.URL, srv.Client()), validateOwners(), nil)

	r := mustRunValidate(t, tl, `{"class":"`+validateClass+`","config":{"connector.class":"`+validateClass+`","database.hostname":"db.acme","decimal.handling.mode":"bad"}}`)
	if !strings.Contains(r.Summary, "1 error in "+validateClass) {
		t.Fatalf("summary = %q", r.Summary)
	}
	for _, want := range []string{"decimal.handling.mode", "Invalid value bad", "allowed: precise, string"} {
		if !strings.Contains(r.Summary, want) {
			t.Errorf("summary missing %q: %q", want, r.Summary)
		}
	}
	if got := api.body()["database.hostname"]; got != "db.acme" {
		t.Errorf("validate body host = %q", got)
	}
	data, ok := r.Data.(validateData)
	if !ok {
		t.Fatalf("data type %T", r.Data)
	}
	if data.ErrorCount != 1 || len(data.Errors["decimal.handling.mode"]) != 1 {
		t.Fatalf("data = %+v", data)
	}
	if _, status, _, put := api.counts(); status != 0 || put != 1 {
		t.Fatalf("calls: status=%d put=%d, want 0 and 1", status, put)
	}
}

func TestConnectValidateConfigRefusesPlaintextSecret(t *testing.T) {
	var called atomic.Int32
	fc := &fakeConnect{
		validateFn: func(context.Context, string, map[string]string) (connect.ValidateResult, error) {
			called.Add(1)
			return connect.ValidateResult{}, nil
		},
		defsFn: func(context.Context, string) ([]connect.ConfigKeyDef, error) {
			called.Add(1)
			return nil, nil
		},
	}
	tl := newValidateTool(t, fc, validateOwners(), nil)

	for _, in := range []string{
		`{"class":"` + validateClass + `","config":{"database.password":"hunter2"}}`,
		`{"class":"` + validateClass + `","config":{"database.user":"u","ssl.key.password":"%"}}`,
	} {
		_, err := runValidate(t, tl, "orders", in)
		if !errors.Is(err, tools.ErrBadInput) || !strings.Contains(err.Error(), "looks like a credential") {
			t.Fatalf("err = %v, want ErrBadInput naming the credential", err)
		}
	}
	if called.Load() != 0 {
		t.Fatalf("a client was called %d time(s) before the input was refused", called.Load())
	}

	// ${secret:...} is a non-secret value (U0), and a converter class contains a
	// bare "key" segment but is not a credential.
	api := newValidateAPI(t)
	srv := httptest.NewServer(api)
	defer srv.Close()
	tl = newValidateTool(t, connect.New(srv.URL, srv.Client()), validateOwners(), nil)
	mustRunValidate(t, tl, `{"class":"`+validateClass+`","config":{"database.password":"${secret:pg_password}","key.converter":"org.apache.kafka.connect.json.JsonConverter"}}`)
	if got := api.body()["database.password"]; got != "${secret:pg_password}" {
		t.Errorf("password sent = %q, want the reference", got)
	}
}

func TestConnectValidateConfigHostGuard(t *testing.T) {
	t.Run("mismatch is refused before any HTTP", func(t *testing.T) {
		var called atomic.Int32
		fc := &fakeConnect{validateFn: func(context.Context, string, map[string]string) (connect.ValidateResult, error) {
			called.Add(1)
			return connect.ValidateResult{}, nil
		}, defsFn: func(context.Context, string) ([]connect.ConfigKeyDef, error) {
			called.Add(1)
			return nil, nil
		}}
		tl := newValidateTool(t, fc, validateOwners(), nil)
		_, err := runValidate(t, tl, "orders", `{"class":"`+validateClass+`","config":{"database.hostname":"db.evil"}}`)
		if !errors.Is(err, tools.ErrOutOfScope) || !strings.Contains(err.Error(), "db.evil") {
			t.Fatalf("err = %v, want ErrOutOfScope naming db.evil", err)
		}
		if called.Load() != 0 {
			t.Fatalf("client called %d time(s) before the host guard", called.Load())
		}
	})

	t.Run("base connector host is allowed", func(t *testing.T) {
		api := newValidateAPI(t)
		api.baseConfig = map[string]string{"connector.class": validateClass, "database.hostname": "db.base", "database.password": "s3cr3t"}
		srv := httptest.NewServer(api)
		defer srv.Close()
		tl := newValidateTool(t, connect.New(srv.URL, srv.Client()), validateOwners("pg-orders-connector"), nil)
		mustRunValidate(t, tl, `{"class":"`+validateClass+`","config":{"database.hostname":"db.base"},"base_connector":"pg-orders-connector"}`)
		if api.body()["database.hostname"] != "db.base" {
			t.Fatalf("validate body = %+v", api.body())
		}
	})

	t.Run("pipeline source connector host is allowed", func(t *testing.T) {
		api := newValidateAPI(t)
		api.sourceConfig = map[string]string{"connector.class": validateClass, "database.hostname": "db.acme"}
		srv := httptest.NewServer(api)
		defer srv.Close()
		tl := newValidateTool(t, connect.New(srv.URL, srv.Client()), validateOwnersNoHost("pg-orders-connector"), nil)
		mustRunValidate(t, tl, `{"class":"`+validateClass+`","config":{"database.hostname":"db.acme"}}`)
		if _, _, _, put := api.counts(); put != 1 {
			t.Fatalf("put calls = %d, want 1", put)
		}
	})

	t.Run("a sink connector host is not a source host", func(t *testing.T) {
		api := newValidateAPI(t)
		api.sourceStatus = clients.ConnectorStatus{Name: "pg-orders-sink", Type: "sink", State: "RUNNING"}
		api.sourceConfig = map[string]string{"connection.url": "jdbc:postgresql://warehouse:5432/wh"}
		srv := httptest.NewServer(api)
		defer srv.Close()
		tl := newValidateTool(t, connect.New(srv.URL, srv.Client()), validateOwnersNoHost("pg-orders-sink"), nil)
		_, err := runValidate(t, tl, "orders", `{"class":"`+validateClass+`","config":{"database.hostname":"warehouse"}}`)
		if !errors.Is(err, tools.ErrOutOfScope) {
			t.Fatalf("err = %v, want ErrOutOfScope", err)
		}
		if _, _, _, put := api.counts(); put != 0 {
			t.Fatalf("validate called %d time(s) for a sink host", put)
		}
	})

	t.Run("no known host fails closed", func(t *testing.T) {
		fc := &fakeConnect{}
		tl := newValidateTool(t, fc, validateOwnersNoHost(), nil)
		_, err := runValidate(t, tl, "orders", `{"class":"`+validateClass+`","config":{"database.hostname":"db.acme"}}`)
		if !errors.Is(err, tools.ErrOutOfScope) || !strings.Contains(err.Error(), "no known source host") {
			t.Fatalf("err = %v, want a fail-closed refusal", err)
		}
	})

	t.Run("a host reference is refused", func(t *testing.T) {
		fc := &fakeConnect{}
		tl := newValidateTool(t, fc, validateOwners(), nil)
		// A ${secret:...} reference passes the input gate (a host key accepts
		// a reference form) and the host guard then refuses it: a host must
		// be a literal.
		_, err := runValidate(t, tl, "orders", `{"class":"`+validateClass+`","config":{"database.hostname":"${secret:db-host}"}}`)
		if !errors.Is(err, tools.ErrBadInput) || !strings.Contains(err.Error(), "literal host") {
			t.Fatalf("err = %v, want ErrBadInput", err)
		}
		// A ${env:...} reference is refused at the input gate, whatever the
		// key: Connect could resolve it and echo the value.
		_, err = runValidate(t, tl, "orders", `{"class":"`+validateClass+`","config":{"database.hostname":"${env:DB_HOST}"}}`)
		if !errors.Is(err, tools.ErrBadInput) || !strings.Contains(err.Error(), "only ${secret:...} is accepted") {
			t.Fatalf("err = %v, want ErrBadInput", err)
		}
	})
}

func TestConnectValidateConfigBaseConnectorMerge(t *testing.T) {
	api := newValidateAPI(t)
	api.baseConfig = map[string]string{
		"connector.class":   validateClass,
		"database.hostname": "db.acme",
		"database.password": "sup3r-s3cr3t",
		"topic.prefix":      "pg",
	}
	// Connect echoes the credential in an error message: neither Summary nor
	// Data may carry it.
	api.validateBody = `{"error_count":1,"configs":[{"value":{"name":"database.password","errors":["connection failed with password sup3r-s3cr3t"]}}]}`
	srv := httptest.NewServer(api)
	defer srv.Close()
	tl := newValidateTool(t, connect.New(srv.URL, srv.Client()), validateOwners("pg-orders-connector"), nil)

	r := mustRunValidate(t, tl, `{"class":"`+validateClass+`","config":{"database.hostname":"db.acme","database.password":"${secret:pg_password}","database.dbname":"orders"},"base_connector":"pg-orders-connector"}`)
	body := api.body()
	for k, want := range map[string]string{
		"database.password": "sup3r-s3cr3t", // server-side value, not the model's reference
		"database.hostname": "db.acme",
		"database.dbname":   "orders",
		"topic.prefix":      "pg",
	} {
		if body[k] != want {
			t.Errorf("validate body %s = %q, want %q", k, body[k], want)
		}
	}
	if strings.Contains(r.Summary, "sup3r-s3cr3t") || strings.Contains(dataJSON(t, r), "sup3r-s3cr3t") {
		t.Fatalf("credential leaked: summary=%q data=%s", r.Summary, dataJSON(t, r))
	}
	if !strings.Contains(r.Summary, "invalid value (details redacted)") {
		t.Fatalf("summary did not show the redacted message: %q", r.Summary)
	}
	if data := r.Data.(validateData); data.BaseConnector != "pg-orders-connector" {
		t.Fatalf("base connector = %q", data.BaseConnector)
	}
}

func TestConnectValidateConfigUnknownKeys(t *testing.T) {
	api := newValidateAPI(t)
	srv := httptest.NewServer(api)
	defer srv.Close()
	renamed := func(k string) (string, string, bool) {
		if k == "database.server.name" {
			return "topic.prefix", "DBZ-1234", true
		}
		return "", "", false
	}
	tl := newValidateTool(t, connect.New(srv.URL, srv.Client()), validateOwners(), renamed)
	r := mustRunValidate(t, tl, `{"class":"`+validateClass+`","config":{"decimal.handling.mod":"precise","database.server.name":"pg"}}`)
	for _, want := range []string{
		`unknown key "decimal.handling.mod": did you mean "decimal.handling.mode"?`,
		`unknown key "database.server.name": renamed to "topic.prefix" (known issue DBZ-1234)`,
	} {
		if !strings.Contains(r.Summary, want) {
			t.Errorf("summary missing %q:\n%s", want, r.Summary)
		}
	}

	// An error on a renamed key names the issue id too.
	api.mu.Lock()
	api.validateBody = `{"error_count":1,"configs":[{"value":{"name":"database.server.name","errors":["unknown property"]}}]}`
	api.mu.Unlock()
	r = mustRunValidate(t, tl, `{"class":"`+validateClass+`","config":{"database.server.name":"pg"}}`)
	if !strings.Contains(r.Summary, "known issue DBZ-1234") {
		t.Fatalf("summary does not name the known issue: %q", r.Summary)
	}

	// An older Connect without the defs endpoint still validates; it says the
	// suggestions are unavailable instead of failing the call.
	api.mu.Lock()
	api.defsStatus = http.StatusNotFound
	api.mu.Unlock()
	r = mustRunValidate(t, tl, `{"class":"`+validateClass+`","config":{"database.hostname":"db.acme"}}`)
	if !strings.Contains(r.Summary, "plugin config definitions unavailable") {
		t.Fatalf("summary = %q", r.Summary)
	}
	if _, _, _, put := api.counts(); put < 3 {
		t.Fatalf("validate calls = %d, want 3", put)
	}
}

func TestConnectValidateConfigConnectErrors(t *testing.T) {
	tests := []struct {
		status   int
		wantText string
	}{
		{http.StatusBadRequest, "status 400"},
		{http.StatusNotFound, "not found"},
		{http.StatusMethodNotAllowed, "status 405"},
		{http.StatusInternalServerError, "status 500"},
	}
	for _, tc := range tests {
		t.Run(fmt.Sprintf("%d", tc.status), func(t *testing.T) {
			api := newValidateAPI(t)
			api.validateStatus = tc.status
			api.validateBody = `{"error":"nope"}`
			srv := httptest.NewServer(api)
			defer srv.Close()
			tl := newValidateTool(t, connect.New(srv.URL, srv.Client()), validateOwners(), nil)
			_, err := runValidate(t, tl, "orders", `{"class":"`+validateClass+`","config":{"database.hostname":"db.acme"}}`)
			if err == nil {
				t.Fatal("want an error")
			}
			if tc.status == http.StatusNotFound {
				if !errors.Is(err, clients.ErrNotFound) {
					t.Fatalf("err = %v, want ErrNotFound", err)
				}
				return
			}
			if !strings.Contains(err.Error(), tc.wantText) {
				t.Fatalf("err = %v, want %q", err, tc.wantText)
			}
		})
	}
}

func TestConnectValidateConfigTimeout(t *testing.T) {
	api := newValidateAPI(t)
	api.delay = 300 * time.Millisecond
	srv := httptest.NewServer(api)
	defer srv.Close()
	c := connect.New(srv.URL, &http.Client{Timeout: 50 * time.Millisecond})
	tl := newValidateTool(t, c, validateOwners(), nil)
	start := time.Now()
	_, err := runValidate(t, tl, "orders", `{"class":"`+validateClass+`","config":{"database.hostname":"db.acme"}}`)
	if err == nil {
		t.Fatal("want a timeout error")
	}
	if time.Since(start) > 2*time.Second {
		t.Fatalf("timeout took %s", time.Since(start))
	}
}

func TestConnectValidateConfigCapsResult(t *testing.T) {
	api := newValidateAPI(t)
	api.baseConfig = map[string]string{"connector.class": validateClass, "database.password": "sup3r-s3cr3t"}
	const long = "the value at the end of this long message is not the secret; padding padding padding padding padding padding padding padding padding padding"
	configs := make([]map[string]any, 0, 100)
	for i := 0; i < 100; i++ {
		msgs := make([]string, 10)
		for j := range msgs {
			msgs[j] = long
		}
		msgs[0] = "failed with password sup3r-s3cr3t"
		configs = append(configs, map[string]any{"value": map[string]any{"name": fmt.Sprintf("key.%02d", i), "errors": msgs}})
	}
	b, err := json.Marshal(map[string]any{"error_count": 1000, "configs": configs})
	if err != nil {
		t.Fatal(err)
	}
	api.validateBody = string(b)
	srv := httptest.NewServer(api)
	defer srv.Close()
	tl := newValidateTool(t, connect.New(srv.URL, srv.Client()), validateOwners("pg-orders-connector"), nil)

	r := mustRunValidate(t, tl, `{"class":"`+validateClass+`","config":{"database.hostname":"db.acme"},"base_connector":"pg-orders-connector"}`)
	if len(r.Summary) > tools.MaxSummaryBytes {
		t.Fatalf("summary is %d bytes, want at most %d", len(r.Summary), tools.MaxSummaryBytes)
	}
	if strings.Contains(r.Summary, "sup3r-s3cr3t") || strings.Contains(dataJSON(t, r), "sup3r-s3cr3t") {
		t.Fatal("credential leaked into the capped result")
	}
	data := r.Data.(validateData)
	if len(data.Errors) > maxValidateErrorKeys {
		t.Fatalf("data has %d error keys, want at most %d", len(data.Errors), maxValidateErrorKeys)
	}
	total := 0
	for k, msgs := range data.Errors {
		if len(msgs) > maxValidateKeyMessages {
			t.Errorf("%s has %d messages, want at most %d", k, len(msgs), maxValidateKeyMessages)
		}
		for _, m := range msgs {
			if len(m) > maxValidateMessageSize {
				t.Errorf("%s message is %d bytes, want at most %d", k, len(m), maxValidateMessageSize)
			}
			total += len(m)
		}
	}
	if total > maxValidateMessageSum {
		t.Fatalf("messages total %d bytes, want at most %d", total, maxValidateMessageSum)
	}
	if data.ErrorCount != 1000 {
		t.Fatalf("error_count = %d, want the full 1000", data.ErrorCount)
	}
}

func TestConnectValidateConfigScope(t *testing.T) {
	var called atomic.Int32
	fc := &fakeConnect{
		configFn: func(context.Context, string) (map[string]string, error) {
			called.Add(1)
			return nil, nil
		},
	}
	tl := newValidateTool(t, fc, validateOwners("pg-orders-connector"), nil)

	if _, err := runValidate(t, tl, "orders", `{"class":"`+validateClass+`","config":{},"base_connector":"other-pipeline-connector"}`); !errors.Is(err, tools.ErrOutOfScope) {
		t.Fatalf("base connector outside the pipeline: err = %v, want ErrOutOfScope", err)
	}
	if called.Load() != 0 {
		t.Fatal("the base connector's config was read before the scope check")
	}

	if _, err := tl.Run(context.Background(), json.RawMessage(`{"class":"`+validateClass+`","config":{}}`)); !errors.Is(err, tools.ErrNoScope) {
		t.Fatalf("no scope: err = %v, want ErrNoScope", err)
	}

	if _, err := runValidate(t, tl, "not-a-pipeline", `{"class":"`+validateClass+`","config":{}}`); !errors.Is(err, tools.ErrOutOfScope) {
		t.Fatalf("unknown room pipeline: err = %v, want ErrOutOfScope", err)
	}
}

func TestConnectValidateConfigBadInput(t *testing.T) {
	fc := &fakeConnect{}
	tl := newValidateTool(t, fc, validateOwners(), nil)
	tests := []struct {
		name  string
		input string
	}{
		{"missing class", `{"config":{}}`},
		{"newline in class", `{"class":"a\nb","config":{}}`},
		{"bad key", `{"class":"c","config":{"not a key":"x"}}`},
		{"oversized value", `{"class":"c","config":{"k":"` + strings.Repeat("x", maxValidateValueBytes+1) + `"}}`},
		{"non-string value", `{"class":"c","config":{"k":1}}`},
		{"oversized class", `{"class":"` + strings.Repeat("c", maxValidateClassBytes+1) + `","config":{}}`},
		{"oversized well-formed key", `{"class":"c","config":{"` + strings.Repeat("k", maxValidateKeyBytes+1) + `":"x"}}`},
		{"too many keys", tooManyKeysInput()},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := runValidate(t, tl, "orders", tc.input)
			if !errors.Is(err, tools.ErrBadInput) {
				t.Fatalf("err = %v, want ErrBadInput", err)
			}
		})
	}
}

// The real client is the ConfigValidator the tool asserts; a fake without the
// pair is not registered.
func TestConnectValidateConfigRegistration(t *testing.T) {
	reg := tools.NewRegistry()
	if err := Register(reg, Deps{Connect: &fakeConnect{}, Owners: validateOwners()}); err != nil {
		t.Fatal(err)
	}
	if _, ok := reg.Get("connect_validate_config"); !ok {
		t.Fatal("a client with the validator pair did not register the tool")
	}
	var _ ConfigValidator = (*connect.Client)(nil)
}

// Host-guard reductions: a URL whose path or query holds '@' must reduce to
// the real host, every host of a multi-host value must be returned, and a
// property segment must not be mistaken for (or hidden behind) a host.
func TestConfigHosts(t *testing.T) {
	tests := []struct {
		in   string
		want []string
	}{
		{"db.acme", []string{"db.acme"}},
		{"db.acme:5432", []string{"db.acme"}},
		{"DB.ACME.", []string{"db.acme"}},
		{"[::1]:5432", []string{"::1"}},
		{"postgres://u:p@db.acme:5432/orders?sslmode=disable", []string{"db.acme"}},
		{"jdbc:postgresql://db.evil/x@db.acme", []string{"db.evil"}},
		{"jdbc:postgresql://db.evil?@db.acme", []string{"db.evil"}},
		{"jdbc:postgresql://db.evil#x@db.acme", []string{"db.evil"}},
		{"db.evil#frag@db.acme", []string{"db.evil"}},
		{"jdbc:mysql://db.acme,db.evil/db", []string{"db.acme", "db.evil"}},
		{"jdbc:mysql:loadbalance://db.acme,db.evil:3306/db", []string{"db.acme", "db.evil"}},
		{"jdbc:sqlserver://db.acme;serverName=db.evil", []string{"db.acme", "servername=db.evil"}},
		{"k1:9092, k2:9092", []string{"k1", "k2"}},
		{"${env:DB_HOST}", nil},
		{"", nil},
	}
	for _, tc := range tests {
		got := configHosts(tc.in)
		if fmt.Sprint(got) != fmt.Sprint(tc.want) {
			t.Errorf("configHosts(%q) = %q, want %q", tc.in, got, tc.want)
		}
	}
}

// Every reduction trick and every host-carrying key must be refused before any
// HTTP when it names a host outside the pipeline (db.acme is the only allowed
// host).
func TestConnectValidateConfigHostGuardBypasses(t *testing.T) {
	var called atomic.Int32
	fc := &fakeConnect{
		validateFn: func(context.Context, string, map[string]string) (connect.ValidateResult, error) {
			called.Add(1)
			return connect.ValidateResult{}, nil
		},
		defsFn: func(context.Context, string) ([]connect.ConfigKeyDef, error) {
			called.Add(1)
			return nil, nil
		},
	}
	tl := newValidateTool(t, fc, validateOwners(), nil)
	tests := []struct {
		name, key, value string
		wantErr          error
	}{
		{"userinfo in path", "connection.url", "jdbc:postgresql://db.evil/x@db.acme", tools.ErrOutOfScope},
		{"userinfo in query", "connection.url", "jdbc:postgresql://db.evil?@db.acme", tools.ErrOutOfScope},
		{"userinfo in fragment", "connection.url", "jdbc:postgresql://db.evil#x@db.acme", tools.ErrOutOfScope},
		{"failover host list", "connection.url", "jdbc:mysql://db.acme,db.evil/db", tools.ErrOutOfScope},
		{"loadbalance host list", "connection.url", "jdbc:mysql:loadbalance://db.acme,db.evil:3306/db", tools.ErrOutOfScope},
		{"sqlserver property segment", "connection.url", "jdbc:sqlserver://db.acme;serverName=db.evil", tools.ErrBadInput},
		{"hostname list", "database.hostname", "db.acme,db.evil", tools.ErrOutOfScope},
		{"hostname list with space", "database.hostname", "db.acme, db.evil", tools.ErrOutOfScope},
		{"mongodb connection.uri", "connection.uri", "mongodb://db.evil:27017/db", tools.ErrOutOfScope},
		{"mongodb.hosts", "mongodb.hosts", "db.evil:27017", tools.ErrOutOfScope},
		{"schema registry url", "value.converter.schema.registry.url", "http://db.evil:8081", tools.ErrOutOfScope},
		{"history bootstrap servers", "database.history.kafka.bootstrap.servers", "db.evil:9092", tools.ErrOutOfScope},
		{"http api url", "http.api.url", "https://db.evil/webhook", tools.ErrOutOfScope},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			in := fmt.Sprintf(`{"class":%q,"config":{%q:%q}}`, validateClass, tc.key, tc.value)
			_, err := runValidate(t, tl, "orders", in)
			if !errors.Is(err, tc.wantErr) {
				t.Fatalf("err = %v, want %v", err, tc.wantErr)
			}
			if strings.Contains(fmt.Sprint(err), "db.evil") == (tc.wantErr == tools.ErrBadInput) {
				// an out-of-scope refusal names the model's own host; a
				// bad-input one names no host at all
				t.Fatalf("err names the wrong things: %v", err)
			}
		})
	}
	if called.Load() != 0 {
		t.Fatalf("client called %d time(s) before the host guard", called.Load())
	}
}

// A multi-host value whose every host the base connector already reaches is
// allowed.
func TestConnectValidateConfigMultiHostAllowed(t *testing.T) {
	api := newValidateAPI(t)
	api.baseConfig = map[string]string{
		"connector.class":                          validateClass,
		"database.hostname":                        "db.acme",
		"database.history.kafka.bootstrap.servers": "k1:9092,k2:9092",
	}
	srv := httptest.NewServer(api)
	defer srv.Close()
	tl := newValidateTool(t, connect.New(srv.URL, srv.Client()), validateOwners("pg-orders-connector"), nil)
	mustRunValidate(t, tl, `{"class":"`+validateClass+`","config":{"database.history.kafka.bootstrap.servers":"k2:9092, k1:9092"},"base_connector":"pg-orders-connector"}`)
	if _, _, _, put := api.counts(); put != 1 {
		t.Fatalf("put calls = %d, want 1", put)
	}
}

// A ${...} reference under a non-credential key is refused before any HTTP:
// Connect could resolve it server-side and echo the resolved value in a
// validation message. A converter credential key still takes one.
func TestConnectValidateConfigReferenceOnlyOnCredentialKeys(t *testing.T) {
	var called atomic.Int32
	fc := &fakeConnect{
		validateFn: func(context.Context, string, map[string]string) (connect.ValidateResult, error) {
			called.Add(1)
			return connect.ValidateResult{}, nil
		},
		defsFn: func(context.Context, string) ([]connect.ConfigKeyDef, error) {
			called.Add(1)
			return nil, nil
		},
	}
	tl := newValidateTool(t, fc, validateOwners(), nil)
	// ${file:...}/${env:...} are refused outright (they could read an
	// arbitrary server file or environment variable back through an echoed
	// validation message); a ${secret:...} is refused on a non-credential key.
	for in, want := range map[string]string{
		`{"class":"` + validateClass + `","config":{"tasks.max":"${file:/etc/kafka/secret.properties:password}"}}`: "only ${secret:...} is accepted",
		`{"class":"` + validateClass + `","config":{"tasks.max":"${file:/etc/passwd:0}"}}`:                         "only ${secret:...} is accepted",
		`{"class":"` + validateClass + `","config":{"decimal.handling.mode":"${env:ANYTHING}"}}`:                   "only ${secret:...} is accepted",
		`{"class":"` + validateClass + `","config":{"database.password":"${file:/etc/passwd:0}"}}`:                 "only ${secret:...} is accepted",
		`{"class":"` + validateClass + `","config":{"value.converter.basic.auth.user.info":"${env:SECRET}"}}`:      "only ${secret:...} is accepted",
		`{"class":"` + validateClass + `","config":{"tasks.max":"${secret:name}"}}`:                                "not a credential key",
	} {
		_, err := runValidate(t, tl, "orders", in)
		if !errors.Is(err, tools.ErrBadInput) || !strings.Contains(err.Error(), want) {
			t.Fatalf("input %s: err = %v, want ErrBadInput with %q", in, err, want)
		}
	}
	if called.Load() != 0 {
		t.Fatalf("client called %d time(s) before the reference was refused", called.Load())
	}

	api := newValidateAPI(t)
	srv := httptest.NewServer(api)
	defer srv.Close()
	tl = newValidateTool(t, connect.New(srv.URL, srv.Client()), validateOwners(), nil)
	mustRunValidate(t, tl, `{"class":"`+validateClass+`","config":{"value.converter.basic.auth.user.info":"${secret:sr_auth}","database.hostname":"db.acme"}}`)
	if got := api.body()["value.converter.basic.auth.user.info"]; got != "${secret:sr_auth}" {
		t.Errorf("converter credential sent = %q, want the reference", got)
	}
}

// A key that is both host-looking and credential-looking takes its value from
// the base connector; the host guard must neither refuse it nor echo any
// fragment of the server-side value.
func TestConnectValidateConfigSecretHostKeyNeverEchoed(t *testing.T) {
	api := newValidateAPI(t)
	api.baseConfig = map[string]string{
		"connector.class":   validateClass,
		"database.hostname": "db.acme",
		"jdbc.password":     "s3cr3t-fragment.example",
	}
	srv := httptest.NewServer(api)
	defer srv.Close()
	tl := newValidateTool(t, connect.New(srv.URL, srv.Client()), validateOwners("pg-orders-connector"), nil)
	r := mustRunValidate(t, tl, `{"class":"`+validateClass+`","config":{"jdbc.password":"${secret:x}"},"base_connector":"pg-orders-connector"}`)
	if strings.Contains(r.Summary, "s3cr3t-fragment") || strings.Contains(dataJSON(t, r), "s3cr3t-fragment") {
		t.Fatalf("server-side value leaked: %q", r.Summary)
	}
	if api.body()["jdbc.password"] != "s3cr3t-fragment.example" {
		t.Fatalf("validate body = %+v, want the base value", api.body())
	}
}

// A message that carries the base credential URL-encoded is dropped whole,
// like one that carries it literally.
func TestConnectValidateConfigDropsEncodedSecretEcho(t *testing.T) {
	api := newValidateAPI(t)
	api.baseConfig = map[string]string{
		"connector.class":   validateClass,
		"database.password": "p@ss w0rd",
	}
	api.validateBody = `{"error_count":1,"configs":[{"value":{"name":"database.password","errors":["could not reach jdbc:postgresql://u:p%40ss+w0rd@db.acme/orders"]}}]}`
	srv := httptest.NewServer(api)
	defer srv.Close()
	tl := newValidateTool(t, connect.New(srv.URL, srv.Client()), validateOwners("pg-orders-connector"), nil)
	r := mustRunValidate(t, tl, `{"class":"`+validateClass+`","config":{"database.hostname":"db.acme"},"base_connector":"pg-orders-connector"}`)
	out := r.Summary + dataJSON(t, r)
	if strings.Contains(out, "p%40ss+w0rd") || strings.Contains(out, "p@ss w0rd") {
		t.Fatalf("encoded credential leaked: %q", out)
	}
	if !strings.Contains(r.Summary, "invalid value (details redacted)") {
		t.Fatalf("summary did not show the redacted message: %q", r.Summary)
	}
}

// A key that is both unknown (renamed) and error-reported appears once, not
// twice, in unknown_keys.
func TestConnectValidateConfigRenamedKeyListedOnce(t *testing.T) {
	api := newValidateAPI(t)
	api.validateBody = `{"error_count":1,"configs":[{"value":{"name":"database.server.name","errors":["unknown property"]}}]}`
	srv := httptest.NewServer(api)
	defer srv.Close()
	renamed := func(k string) (string, string, bool) {
		if k == "database.server.name" {
			return "topic.prefix", "DBZ-1234", true
		}
		return "", "", false
	}
	tl := newValidateTool(t, connect.New(srv.URL, srv.Client()), validateOwners(), renamed)
	r := mustRunValidate(t, tl, `{"class":"`+validateClass+`","config":{"database.server.name":"pg"}}`)
	data := r.Data.(validateData)
	n := 0
	for _, u := range data.UnknownKeys {
		if u.Key == "database.server.name" {
			n++
		}
	}
	if n != 1 {
		t.Fatalf("database.server.name listed %d times in unknown_keys: %+v", n, data.UnknownKeys)
	}
}

// tooManyKeysInput builds a config one key over the cap; each key is
// well-formed, so only the count refuses it.
func tooManyKeysInput() string {
	var sb strings.Builder
	sb.WriteString(`{"class":"c","config":{`)
	for i := 0; i <= maxValidateConfigKeys; i++ {
		if i > 0 {
			sb.WriteString(",")
		}
		fmt.Fprintf(&sb, `"k%d":"v"`, i)
	}
	sb.WriteString("}}")
	return sb.String()
}

// A message carrying a known credential in any form is dropped whole:
// literal, query-escaped (space becomes '+') and path-escaped (space becomes
// "%20") differ, and a Connect message quoting a connection URL can carry any
// of them.
func TestRedactMessageEncodedForms(t *testing.T) {
	const secret = "p@ss w0rd"
	for _, m := range []string{
		"could not log in with p@ss w0rd",
		"could not reach jdbc:postgresql://u:p%40ss+w0rd@db/x", // QueryEscape
		"could not open path /creds/p@ss%20w0rd/pgpass",        // PathEscape
	} {
		if got := redactMessage(m, []string{secret}); got != "invalid value (details redacted)" {
			t.Errorf("redactMessage(%q) = %q, want the message dropped", m, got)
		}
	}
	if got := redactMessage("value must be a number", []string{secret}); got != "value must be a number" {
		t.Errorf("clean message changed: %q", got)
	}
}

// The base credential overrides the model's value, but an empty base
// credential must not clobber the model's ${secret:...} reference (Connect
// would then report a spurious missing credential), and "" never enters the
// never-echo list, where it would drop every message.
func TestMergeBaseConfigEmptyCredential(t *testing.T) {
	merged := map[string]string{
		"database.password": "${secret:db_pass}",
		"jdbc.password":     "${secret:jdbc}",
		"tasks.max":         "2",
	}
	base := map[string]string{
		"database.password": "",
		"jdbc.password":     "server-side-pw",
		"database.hostname": "db.acme",
		"tasks.max":         "1",
	}
	secrets := mergeBaseConfig(merged, base)
	if merged["database.password"] != "${secret:db_pass}" {
		t.Errorf("empty base credential clobbered the reference: %q", merged["database.password"])
	}
	if merged["jdbc.password"] != "server-side-pw" {
		t.Errorf("base credential did not win: %q", merged["jdbc.password"])
	}
	if merged["database.hostname"] != "db.acme" || merged["tasks.max"] != "2" {
		t.Errorf("merge wrong: %+v", merged)
	}
	if len(secrets) != 1 || secrets[0] != "server-side-pw" {
		t.Errorf("secrets = %q, want only the non-empty base credential", secrets)
	}
}

// With no plugin definitions (an older Connect) unknown-key detection cannot
// run, but a known renamed key is still reported through the lookup.
func TestUnknownConfigKeysRenamedWithoutDefs(t *testing.T) {
	lookup := func(k string) (string, string, bool) {
		if k == "database.server.name" {
			return "topic.prefix", "DBZ-1234", true
		}
		return "", "", false
	}
	out := unknownConfigKeys(map[string]string{
		"database.server.name": "pg",
		"made.up.key":          "x",
	}, nil, lookup)
	if len(out) != 1 || out[0].Key != "database.server.name" || out[0].NewKey != "topic.prefix" || out[0].IssueID != "DBZ-1234" {
		t.Fatalf("out = %+v, want only the renamed key", out)
	}
	if out := unknownConfigKeys(map[string]string{"made.up.key": "x"}, nil, nil); out != nil {
		t.Fatalf("no defs and no lookup: out = %+v, want nil", out)
	}
}

// An out-of-scope host refusal names only the model's own key and host: the
// allowed set (the pipeline's source host, the base connector's internal
// hosts) must not be handed out by probing with a bad host.
func TestConnectValidateConfigOutOfScopeNamesNoAllowedHost(t *testing.T) {
	api := newValidateAPI(t)
	api.baseConfig = map[string]string{
		"connector.class":   validateClass,
		"database.hostname": "internal-db.corp.example",
	}
	srv := httptest.NewServer(api)
	defer srv.Close()
	tl := newValidateTool(t, connect.New(srv.URL, srv.Client()), validateOwners("pg-orders-connector"), nil)
	_, err := runValidate(t, tl, "orders", `{"class":"`+validateClass+`","config":{"database.hostname":"db.evil"},"base_connector":"pg-orders-connector"}`)
	if !errors.Is(err, tools.ErrOutOfScope) {
		t.Fatalf("err = %v, want ErrOutOfScope", err)
	}
	msg := fmt.Sprint(err)
	if !strings.Contains(msg, "db.evil") {
		t.Fatalf("refusal does not name the model's own host: %v", err)
	}
	for _, h := range []string{"internal-db", "corp.example", "db.acme"} {
		if strings.Contains(msg, h) {
			t.Fatalf("refusal echoes allowed host %q: %v", h, err)
		}
	}
	if _, _, _, put := api.counts(); put != 0 {
		t.Fatalf("validate was called despite the refusal")
	}
}

// An older Connect without the plugin-config endpoint (404/405) must not fail
// the validation itself: the result says the definitions are unavailable and
// names the version that adds the endpoint.
func TestConnectValidateConfigDefsUnsupported(t *testing.T) {
	fc := &fakeConnect{
		validateFn: func(context.Context, string, map[string]string) (connect.ValidateResult, error) {
			return connect.ValidateResult{}, nil
		},
		defsFn: func(context.Context, string) ([]connect.ConfigKeyDef, error) {
			return nil, fmt.Errorf("connect: plugin config defs: %w", clients.ErrNotSupported)
		},
	}
	tl := newValidateTool(t, fc, validateOwners(), nil)
	r := mustRunValidate(t, tl, `{"class":"`+validateClass+`","config":{"tasks.max":"1"}}`)
	data := r.Data.(validateData)
	if !strings.Contains(data.DefsUnavailable, "Kafka Connect 3.2+") {
		t.Fatalf("DefsUnavailable = %q, want the 3.2+ note", data.DefsUnavailable)
	}
	if !strings.Contains(r.Summary, "plugin config definitions unavailable") {
		t.Fatalf("summary missed the note: %q", r.Summary)
	}
}

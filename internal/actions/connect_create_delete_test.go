package actions

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"piperoom/internal/clients"
	"piperoom/internal/clients/connect"
	"piperoom/internal/tools"
)

func createAction(t *testing.T, c clients.ConnectClient) *createConnector {
	t.Helper()
	a := &createConnector{d: LifecycleDeps{Connect: c}}
	if w, ok := c.(writeCounter); ok {
		guardWrites(t, w)
	}
	return a
}

func deleteAction(t *testing.T, c clients.ConnectClient) *deleteConnector {
	t.Helper()
	a := &deleteConnector{d: LifecycleDeps{Connect: c}}
	if w, ok := c.(writeCounter); ok {
		guardWrites(t, w)
	}
	return a
}

const createIn = `{"connector":"pg-orders-connector","config":{
	"connector.class":"io.debezium.connector.postgresql.PostgresConnector",
	"database.hostname":"postgres",
	"database.password":"${secret:source_password}",
	"topic.prefix":"pg"}}`

func TestCreateConnectorValidate(t *testing.T) {
	f := newLifecycleFake()
	f.names = nil // the planned connector is not registered yet
	a := createAction(t, f)
	if err := a.Validate(scoped(), json.RawMessage(createIn)); err != nil {
		t.Fatalf("Validate: %v", err)
	}
	if err := a.CheckScope(testPipeline(), json.RawMessage(createIn)); err != nil {
		t.Fatalf("CheckScope: %v", err)
	}
	if err := a.Validate(otherPipeline(), json.RawMessage(createIn)); !errors.Is(err, tools.ErrOutOfScope) {
		t.Fatalf("out of scope: %v", err)
	}
	if err := a.Validate(scoped(), json.RawMessage(`{"connector":"pg-other-connector","config":{"connector.class":"c"}}`)); !errors.Is(err, tools.ErrOutOfScope) {
		t.Fatalf("a name outside owners.yaml: %v", err)
	}

	// Already registered: refused unless a delete step is promised.
	f2 := newLifecycleFake() // lists the connector
	a2 := createAction(t, f2)
	if err := a2.Validate(scoped(), json.RawMessage(createIn)); !errors.Is(err, connect.ErrConnectorExists) {
		t.Fatalf("already registered: %v, want connect.ErrConnectorExists", err)
	}
	repl := strings.Replace(createIn, `"connector":"pg-orders-connector",`, `"connector":"pg-orders-connector","replace":true,`, 1)
	if err := a2.Validate(scoped(), json.RawMessage(repl)); err != nil {
		t.Fatalf("replace with the delete step promised: %v", err)
	}
}

func TestCreateConnectorConfigGuards(t *testing.T) {
	tests := []struct {
		name string
		cfg  string
		want error
		text string
	}{
		{"plaintext password", `{"connector.class":"c","database.password":"hunter2"}`, ErrPlaintextSecret, "database.password"},
		{"password inside a URL", `{"connector.class":"c","database.url":"jdbc:postgresql://db/x?password=hunter2"}`, ErrPlaintextSecret, "database.url"},
		{"jaas config", `{"connector.class":"c","database.history.kafka.sasl.jaas.config":"org.apache.kafka.common.security.plain.PlainLoginModule required password=\"hunter2\";"}`, ErrPlaintextSecret, "jaas"},
		{"no class", `{"database.hostname":"postgres"}`, nil, "connector.class is required"},
		{"empty config", `{}`, nil, "config is required"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			f := newLifecycleFake()
			f.names = nil
			a := createAction(t, f)
			in := `{"connector":"pg-orders-connector","config":` + tc.cfg + `}`
			err := a.Validate(scoped(), json.RawMessage(in))
			if err == nil {
				t.Fatal("Validate accepted the config")
			}
			if tc.want != nil && !errors.Is(err, tc.want) {
				t.Fatalf("err = %v, want %v", err, tc.want)
			}
			if tc.text != "" && !strings.Contains(err.Error(), tc.text) {
				t.Fatalf("err %q does not name %q", err, tc.text)
			}
			if strings.Contains(err.Error(), "hunter2") {
				t.Fatalf("err leaks the value: %v", err)
			}
		})
	}
}

// Only the ${secret:...} form is accepted from a proposal, and only on a
// credential or converter key: Connect resolves ${file:}/${env:} server-side
// during Preview's validate round trip and can echo the resolved value in a
// validation message, before anyone approves (the Q4a validate tool's rule).
func TestCreateConnectorReferenceForms(t *testing.T) {
	validateRef := func(t *testing.T, key, ref string) error {
		t.Helper()
		f := newLifecycleFake()
		f.names = nil
		a := createAction(t, f)
		in, _ := json.Marshal(map[string]any{
			"connector": "pg-orders-connector",
			"config":    map[string]string{"connector.class": "c", key: ref},
		})
		return a.Validate(scoped(), json.RawMessage(in))
	}
	if err := validateRef(t, "database.password", "${secret:source_password}"); err != nil {
		t.Fatalf("${secret:...} on a credential key refused: %v", err)
	}
	for _, ref := range []string{"${file:/secrets/pg.properties:password}", "${env:PG_PASSWORD}"} {
		err := validateRef(t, "database.password", ref)
		if err == nil {
			t.Fatalf("reference %s accepted; Connect can resolve and echo it server-side", ref)
		}
		if !strings.Contains(err.Error(), "only ${secret:...}") {
			t.Fatalf("err = %v", err)
		}
	}
	// A reference on a non-credential key is refused: a validation message
	// could echo the resolved value whatever key it sits under.
	if err := validateRef(t, "tasks.max", "${secret:source_password}"); err == nil || !strings.Contains(err.Error(), "not a credential key") {
		t.Fatalf("reference on a non-credential key: %v", err)
	}
	// A converter key may carry a reference (schema-registry credentials such
	// as basic.auth.user.info live under it).
	if err := validateRef(t, "value.converter.basic.auth.user.info", "${secret:sr_auth}"); err != nil {
		t.Fatalf("reference on a converter key refused: %v", err)
	}

	// A converter class is not a credential although its key has a "key" segment.
	f := newLifecycleFake()
	f.names = nil
	a := createAction(t, f)
	in := `{"connector":"pg-orders-connector","config":{"connector.class":"c","key.converter":"org.apache.kafka.connect.storage.StringConverter"}}`
	if err := a.Validate(scoped(), json.RawMessage(in)); err != nil {
		t.Fatalf("converter class refused: %v", err)
	}
}

func TestCreateConnectorPreviewRunsValidate(t *testing.T) {
	f := newLifecycleFake()
	f.names = nil
	f.vr = connect.ValidateResult{ErrorCount: 2, Errors: map[string][]string{
		"database.hostname": {"Invalid value postgres for configuration database.hostname"},
		"slot.name":         {"invalid slot name"},
	}}
	a := createAction(t, f)
	_, err := a.Preview(scoped(), json.RawMessage(createIn))
	if err == nil {
		t.Fatal("Preview accepted a config Connect refused")
	}
	if !strings.Contains(err.Error(), "database.hostname") || !strings.Contains(err.Error(), "Invalid value") {
		t.Fatalf("err = %v", err)
	}
	if f.validateCalls != 1 || f.validateClass != "io.debezium.connector.postgresql.PostgresConnector" {
		t.Fatalf("validate calls=%d class=%q", f.validateCalls, f.validateClass)
	}
	if f.refusedWrites() != 0 || len(f.created) != 0 {
		t.Fatal("Preview wrote")
	}

	f2 := newLifecycleFake()
	f2.names = nil
	a2 := createAction(t, f2)
	res, err := a2.Preview(scoped(), json.RawMessage(createIn))
	if err != nil {
		t.Fatalf("Preview: %v", err)
	}
	if res.DryRun == nil || !res.DryRun.OK || res.DryRun.Source != sourceValidate || res.DryRun.Text != "0 errors" {
		t.Fatalf("dry run = %+v", res.DryRun)
	}
	if !res.Reversible || res.Rollback != `connect_delete_connector {"connector":"pg-orders-connector"}` {
		t.Fatalf("rollback = %q", res.Rollback)
	}
	if f2.refusedWrites() != 0 || len(f2.created) != 0 {
		t.Fatal("Preview wrote")
	}
	// The config was sent to Connect whole, references included, and with the
	// connector's name under "name": Connect's validate endpoint reports a
	// missing required name otherwise, which would fail every create.
	if f2.validated["database.password"] != "${secret:source_password}" {
		t.Fatalf("validated config = %+v", f2.validated)
	}
	if f2.validated["name"] != "pg-orders-connector" {
		t.Fatalf("validated config carries name %q, want the connector's", f2.validated["name"])
	}
	// The approver sees where the connector points.
	if !strings.Contains(res.Current, "database.hostname -> postgres") {
		t.Fatalf("preview does not name the hosts: %q", res.Current)
	}
}

func TestCreateConnectorPreviewRefusals(t *testing.T) {
	f := newLifecycleFake() // the name is registered
	a := createAction(t, f)
	if _, err := a.Preview(scoped(), json.RawMessage(createIn)); !errors.Is(err, connect.ErrConnectorExists) {
		t.Fatalf("present without replace: %v", err)
	}
	if f.validateCalls != 0 {
		t.Fatal("validate ran for a name that is registered")
	}
	repl := strings.Replace(createIn, `"connector":"pg-orders-connector",`, `"connector":"pg-orders-connector","replace":true,`, 1)
	res, err := a.Preview(scoped(), json.RawMessage(repl))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(res.Expected, "re-registered") || !strings.Contains(res.Current, DeleteConnectorName) {
		t.Fatalf("replace preview = %+v", res)
	}

	// A client without the validate endpoint refuses: the dry-run is mandatory.
	f3 := newLifecycleFake()
	f3.names = nil
	a3 := &createConnector{d: LifecycleDeps{Connect: noExtras{f3}}}
	if _, err := a3.Preview(scoped(), json.RawMessage(createIn)); !errors.Is(err, clients.ErrNotSupported) {
		t.Fatalf("no validator: %v, want ErrNotSupported", err)
	}
	// A transport failure is surfaced, never treated as 0 errors.
	f4 := newLifecycleFake()
	f4.names = nil
	f4.validateErr = errors.New("connect 500")
	if _, err := createAction(t, f4).Preview(scoped(), json.RawMessage(createIn)); err == nil || !strings.Contains(err.Error(), "could not validate") {
		t.Fatalf("validate transport error: %v", err)
	}
}

func TestCreateConnectorExecute(t *testing.T) {
	// Connect refuses the config: nothing is created.
	f := newLifecycleFake()
	f.names = nil
	f.allowWrites = true
	f.vr = connect.ValidateResult{ErrorCount: 1, Errors: map[string][]string{"connector.class": {"bad"}}}
	if _, err := createAction(t, f).Execute(scoped(), json.RawMessage(createIn)); err == nil || !strings.Contains(err.Error(), "Connect refused") {
		t.Fatalf("invalid config: %v", err)
	}
	if len(f.created) != 0 {
		t.Fatal("created a connector Connect refused")
	}

	// The promised delete did not run.
	f2 := newLifecycleFake() // registered
	f2.allowWrites = true
	repl := strings.Replace(createIn, `"connector":"pg-orders-connector",`, `"connector":"pg-orders-connector","replace":true,`, 1)
	if _, err := createAction(t, f2).Execute(scoped(), json.RawMessage(repl)); !errors.Is(err, connect.ErrConnectorExists) {
		t.Fatalf("execute with the old registration still there: %v", err)
	}
	if len(f2.created) != 0 {
		t.Fatal("created over a live registration")
	}

	// Happy path: validate then create, with the whole config.
	f3 := newLifecycleFake()
	f3.names = nil
	f3.allowWrites = true
	out, err := createAction(t, f3).Execute(scoped(), json.RawMessage(createIn))
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if f3.validateCalls != 1 || len(f3.created) != 1 || !strings.Contains(out, "created connector pg-orders-connector") {
		t.Fatalf("validate=%d created=%v out=%q", f3.validateCalls, f3.created, out)
	}
	if strings.Contains(out, "source_password") {
		t.Fatalf("result leaks a config value: %q", out)
	}

	// A racing create answers the typed conflict.
	f4 := newLifecycleFake()
	f4.names = nil
	f4.allowWrites = true
	f4.createErr = connect.ErrConnectorExists
	if _, err := createAction(t, f4).Execute(scoped(), json.RawMessage(createIn)); !errors.Is(err, connect.ErrConnectorExists) {
		t.Fatalf("racing create: %v", err)
	}
}

func TestCreateConnectorWithoutWriter(t *testing.T) {
	f := newLifecycleFake()
	f.names = nil
	a := &createConnector{d: LifecycleDeps{Connect: noExtras{f}}}
	if _, err := a.Execute(scoped(), json.RawMessage(createIn)); err == nil || !strings.Contains(err.Error(), "write mode") {
		t.Fatalf("Execute without a writer: %v", err)
	}
}

const deleteIn = `{"connector":"pg-orders-connector"}`

func TestDeleteConnectorValidate(t *testing.T) {
	f := newLifecycleFake()
	a := deleteAction(t, f)
	if err := a.Validate(scoped(), json.RawMessage(deleteIn)); err != nil {
		t.Fatalf("Validate: %v", err)
	}
	if err := a.CheckScope(testPipeline(), json.RawMessage(deleteIn)); err != nil {
		t.Fatalf("CheckScope: %v", err)
	}
	before := f.configCalls
	if err := a.Validate(otherPipeline(), json.RawMessage(deleteIn)); !errors.Is(err, tools.ErrOutOfScope) {
		t.Fatalf("out of scope: %v", err)
	}
	if f.configCalls != before {
		t.Fatal("read the config before the scope refusal")
	}
	if err := a.Validate(scoped(), json.RawMessage(`{"connector":"nope"}`)); err == nil {
		t.Fatal("accepted a missing connector")
	}

	f2 := newLifecycleFake()
	f2.configs[lifecycleConnector]["database.password"] = "hunter2"
	err := deleteAction(t, f2).Validate(scoped(), json.RawMessage(deleteIn))
	if !errors.Is(err, ErrPlaintextSecret) {
		t.Fatalf("plaintext secret: %v, want ErrPlaintextSecret", err)
	}
	if strings.Contains(err.Error(), "hunter2") {
		t.Fatalf("err leaks the value: %v", err)
	}

	// A provider reference is not a plaintext secret.
	f3 := newLifecycleFake()
	f3.configs[lifecycleConnector]["database.password"] = "${file:/secrets/pg.properties:password}"
	if err := deleteAction(t, f3).Validate(scoped(), json.RawMessage(deleteIn)); err != nil {
		t.Fatalf("reference refused: %v", err)
	}
}

func TestDeleteConnectorPreview(t *testing.T) {
	f := newLifecycleFake()
	f.setState("RUNNING")
	a := deleteAction(t, f)
	res, err := a.Preview(scoped(), json.RawMessage(deleteIn))
	if err != nil {
		t.Fatalf("Preview: %v", err)
	}
	if !strings.Contains(res.Current, "RUNNING") || !strings.Contains(res.Expected, "stored offsets under the name") {
		t.Fatalf("preview = %+v", res)
	}
	if res.Reversible || f.refusedWrites() != 0 || len(f.deleted) != 0 {
		t.Fatalf("reversible=%v writes=%v deleted=%v", res.Reversible, f.refusals.Load(), f.deleted)
	}

	f2 := newLifecycleFake()
	f2.configs[lifecycleConnector]["database.password"] = "hunter2"
	if _, err := deleteAction(t, f2).Preview(scoped(), json.RawMessage(deleteIn)); !errors.Is(err, ErrPlaintextSecret) {
		t.Fatalf("Preview with a plaintext secret: %v", err)
	}
}

func TestDeleteConnectorExecute(t *testing.T) {
	f := newLifecycleFake()
	f.allowWrites = true
	out, err := deleteAction(t, f).Execute(scoped(), json.RawMessage(deleteIn))
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if len(f.deleted) != 1 || !strings.Contains(out, "stored offsets stay under the name") {
		t.Fatalf("deleted=%v out=%q", f.deleted, out)
	}

	// The config changed to a plaintext secret after the approval.
	f2 := newLifecycleFake()
	f2.allowWrites = true
	f2.configs[lifecycleConnector]["database.password"] = "hunter2"
	if _, err := deleteAction(t, f2).Execute(scoped(), json.RawMessage(deleteIn)); !errors.Is(err, ErrPlaintextSecret) {
		t.Fatalf("re-check at Execute: %v", err)
	}
	if len(f2.deleted) != 0 {
		t.Fatal("deleted a connector with a plaintext secret")
	}

	f3 := newLifecycleFake()
	a3 := &deleteConnector{d: LifecycleDeps{Connect: noExtras{f3}}}
	if _, err := a3.Execute(scoped(), json.RawMessage(deleteIn)); err == nil || !strings.Contains(err.Error(), "write mode") {
		t.Fatalf("Execute without a writer: %v", err)
	}
}

func TestCreateDeleteRetryOnTimeoutAndRebalance(t *testing.T) {
	timeout := context.DeadlineExceeded

	t.Run("create times out, then succeeds", func(t *testing.T) {
		f := &retryCreateFake{lifecycleFake: newLifecycleFake(), errs: []error{timeout, nil}}
		f.names = nil
		f.allowWrites = true
		if _, err := createAction(t, f).Execute(scoped(), json.RawMessage(createIn)); err != nil {
			t.Fatalf("Execute: %v", err)
		}
		if len(f.created) != 1 || f.attempts != 2 {
			t.Fatalf("created=%v attempts=%d", f.created, f.attempts)
		}
	})
	t.Run("create times out but landed", func(t *testing.T) {
		f := &retryCreateFake{lifecycleFake: newLifecycleFake(), errs: []error{timeout}, landed: true}
		f.names = nil
		f.allowWrites = true
		if _, err := createAction(t, f).Execute(scoped(), json.RawMessage(createIn)); err != nil {
			t.Fatalf("Execute: %v", err)
		}
		if f.attempts != 1 {
			t.Fatalf("attempts=%d, want 1 (the landed create is not repeated)", f.attempts)
		}
	})
	t.Run("create times out, another create won", func(t *testing.T) {
		// The name turns up registered after the timeout, but with a config
		// that is not the approved one: a concurrent actor won the race, and
		// claiming "created connector X" would put a false result in the room
		// log.
		f := &retryCreateFake{
			lifecycleFake: newLifecycleFake(),
			errs:          []error{timeout, timeout, timeout},
			landed:        true,
			landedConfig:  map[string]string{"connector.class": "other.Class"},
		}
		f.names = nil
		f.allowWrites = true
		_, err := createAction(t, f).Execute(scoped(), json.RawMessage(createIn))
		if err == nil || !strings.Contains(err.Error(), "not claimed") {
			t.Fatalf("Execute: %v, want the create not claimed", err)
		}
		if strings.Contains(err.Error(), "other.Class") || strings.Contains(err.Error(), "source_password") {
			t.Fatalf("err leaks a config value: %v", err)
		}
	})
	t.Run("create rebalance then succeeds", func(t *testing.T) {
		f := &retryCreateFake{lifecycleFake: newLifecycleFake(), errs: []error{connect.ErrRebalancing, nil}}
		f.names = nil
		f.allowWrites = true
		if _, err := createAction(t, f).Execute(scoped(), json.RawMessage(createIn)); err != nil {
			t.Fatalf("Execute: %v", err)
		}
		if f.attempts != 2 {
			t.Fatalf("attempts=%d, want 2", f.attempts)
		}
	})
	t.Run("delete times out but landed", func(t *testing.T) {
		f := &retryDeleteFake{lifecycleFake: newLifecycleFake(), errs: []error{timeout}, landed: true}
		f.allowWrites = true
		if _, err := deleteAction(t, f).Execute(scoped(), json.RawMessage(deleteIn)); err != nil {
			t.Fatalf("Execute: %v", err)
		}
		if f.attempts != 1 {
			t.Fatalf("attempts=%d, want 1", f.attempts)
		}
	})
	t.Run("delete rebalance then succeeds", func(t *testing.T) {
		f := &retryDeleteFake{lifecycleFake: newLifecycleFake(), errs: []error{connect.ErrRebalancing, nil}}
		f.allowWrites = true
		if _, err := deleteAction(t, f).Execute(scoped(), json.RawMessage(deleteIn)); err != nil {
			t.Fatalf("Execute: %v", err)
		}
		if f.attempts != 2 {
			t.Fatalf("attempts=%d, want 2", f.attempts)
		}
	})
}

// retryCreateFake returns queued create errors, one per attempt. landed makes
// a timed-out attempt append the name, as Connect does when the create was
// applied but the response was lost; landedConfig registers that config
// instead of the request's own (a concurrent create of the same name).
type retryCreateFake struct {
	*lifecycleFake
	errs         []error
	landed       bool
	landedConfig map[string]string
	attempts     int
}

func (f *retryCreateFake) CreateConnector(ctx context.Context, name string, cfg map[string]string) error {
	f.attempts++
	if len(f.errs) == 0 {
		return f.lifecycleFake.CreateConnector(ctx, name, cfg)
	}
	err := f.errs[0]
	f.errs = f.errs[1:]
	if err == nil {
		return f.lifecycleFake.CreateConnector(ctx, name, cfg)
	}
	if f.landed {
		f.lifecycleFake.names = append(f.lifecycleFake.names, name)
		// A landed create stores the config too; Execute's landed detection
		// reads it back before claiming the create.
		stored := f.landedConfig
		if stored == nil {
			stored = make(map[string]string, len(cfg))
			for k, v := range cfg {
				stored[k] = v
			}
		}
		f.lifecycleFake.configs[name] = stored
		f.landed = false
	}
	return err
}

// retryDeleteFake is retryCreateFake for deletes.
type retryDeleteFake struct {
	*lifecycleFake
	errs     []error
	landed   bool
	attempts int
}

func (f *retryDeleteFake) DeleteConnector(ctx context.Context, name string) error {
	f.attempts++
	if len(f.errs) == 0 {
		return f.lifecycleFake.DeleteConnector(ctx, name)
	}
	err := f.errs[0]
	f.errs = f.errs[1:]
	if err == nil {
		return f.lifecycleFake.DeleteConnector(ctx, name)
	}
	if f.landed {
		var names []string
		for _, n := range f.lifecycleFake.names {
			if n != name {
				names = append(names, n)
			}
		}
		f.lifecycleFake.names = names
		f.landed = false
	}
	return err
}

func TestCreateDeleteConfigReferenceForms(t *testing.T) {
	for ref, want := range map[string]bool{
		"${file:/a:key}":       true,
		"${env:NAME}":          true,
		"${secret:name}":       true,
		"${file:}":             false,
		"${secret:}":           false,
		"${unknown:x}":         false,
		"plain":                false,
		"$${secret:name}":      false,
		"${secret:name}suffix": false,
		"${secret:${x}}":       false,
	} {
		if got := lifecycleConfigReference(ref); got != want {
			t.Errorf("lifecycleConfigReference(%q) = %v, want %v", ref, got, want)
		}
	}
}

func TestConfigSecretKeyExemptions(t *testing.T) {
	tests := map[string]bool{
		"database.password":                   true,
		"database.history.kafka.sasl.jaas":    true,
		"key.converter":                       false,
		"value.converter.schema.registry.url": false,
		"message.key.columns":                 false,
		"transforms.unwrap.key.format":        false,
		"task.max":                            false,
		// The converter exemption covers only the bare "key" segment: a
		// credential substring under a converter prefix is still a credential
		// (a schema-registry keystore password is as much a secret as a
		// database password).
		"value.converter.schema.registry.ssl.keystore.password": true,
		"key.converter.basic.auth.user.secret":                  true,
		"db.password.key.format":                                true,
	}
	for key, want := range tests {
		if got := lifecycleConfigSecretKey(key); got != want {
			t.Errorf("lifecycleConfigSecretKey(%q) = %v, want %v", key, got, want)
		}
	}
}

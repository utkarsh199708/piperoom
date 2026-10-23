package actions

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"time"

	"piperoom/internal/clients"
	"piperoom/internal/clients/connect"
	"piperoom/internal/owners"
	"piperoom/internal/tools"
	"piperoom/internal/tools/kafkatools"
)

// Q5c: connect_create_connector and connect_delete_connector.
//
// A create registers a connector the pipeline's owners.yaml already names, and
// only when Connect's own config validation accepts the config. A credential
// value may only be a ${secret:...} config-provider reference, so a secret
// never travels through a proposal, the room log or an error — and never a
// ${file:...} or ${env:...} reference, which Connect resolves server-side and
// can echo in a validation message (the same rule as the Q4a validate tool).
// Its host keys may point only at hosts the pipeline already reaches. A
// delete is refused when the stored config holds a plaintext credential: its
// only undo would be a create with that value, which this action must not
// carry (a stored, server-side config may hold any provider form).
var (
	// ErrPlaintextSecret refuses a create whose credential value is not a
	// config-provider reference, and a delete whose stored config holds a
	// plaintext credential (a rollback could not restore it).
	ErrPlaintextSecret = errors.New("actions: the connector config holds a credential in plain text")
)

const (
	// maxLifecycleConfigKeys caps one created config.
	maxLifecycleConfigKeys = 256
	// maxLifecycleKeyBytes caps one config key.
	maxLifecycleKeyBytes = 128
	// maxLifecycleValueBytes caps one config value.
	maxLifecycleValueBytes = 8192
	// maxLifecycleClassBytes caps a connector plugin class.
	maxLifecycleClassBytes = 300
)

// lifecycleConfigKeyRe is the shape of a Connect config key (the validate tool
// accepts the same form).
var lifecycleConfigKeyRe = regexp.MustCompile(`^[A-Za-z0-9_$][A-Za-z0-9._$-]*$`)

// connectorClassNameRe is the shape of a plugin class name. A class is never
// a config-provider reference: Connect needs the literal class to look up the
// plugin.
var connectorClassNameRe = regexp.MustCompile(`^[A-Za-z0-9_.$]+$`)

// ---- connect_create_connector ----

// createConnector registers a new connector (POST /connectors).
type createConnector struct{ d LifecycleDeps }

var (
	_ Action  = (*createConnector)(nil)
	_ Scoped  = (*createConnector)(nil)
	_ Preview = (*createConnector)(nil)
)

func (a *createConnector) Name() string  { return CreateConnectorName }
func (a *createConnector) Layer() string { return "kafka-connect" }
func (a *createConnector) Describe() string {
	return "Registers a Kafka Connect connector whose name the pipeline's owners.yaml already lists and which is not registered now (POST /connectors). Preview asks Connect to validate the config (PUT /connector-plugins/{class}/config/validate) and refuses when it reports errors. Every credential value must be a ${secret:...} reference (plaintext credentials and ${file:...}/${env:...} references are refused), and every host key must name a host the pipeline already reaches. Undo with connect_delete_connector. To re-register an existing connector, put connect_delete_connector before this step in the same proposal and set replace: true."
}
func (a *createConnector) ParamsSchema() map[string]any {
	return objectSchema(map[string]any{
		"connector": strProp("connector name; it must be one of the pipeline's connectors in owners.yaml"),
		"config": map[string]any{
			"type":                 "object",
			"description":          "the whole connector config, key -> value; a credential value must be a ${secret:...} reference, and a host key a host the pipeline already reaches",
			"additionalProperties": map[string]any{"type": "string"},
		},
		"replace": map[string]any{
			"type":        "boolean",
			"description": "set only when a connect_delete_connector step for the same name precedes this step in the same proposal (re-register)",
		},
	}, "connector", "config")
}

type createConnectorParams struct {
	Connector string            `json:"connector"`
	Config    map[string]string `json:"config"`
	Replace   bool              `json:"replace"`
}

func parseCreateParams(raw json.RawMessage) (createConnectorParams, error) {
	var p createConnectorParams
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&p); err != nil {
		return p, fmt.Errorf("actions: %s: bad params: %w", CreateConnectorName, err)
	}
	if p.Connector == "" {
		return p, fmt.Errorf("actions: %s: connector is required", CreateConnectorName)
	}
	if len(p.Connector) > maxConnectorNameLen {
		return p, fmt.Errorf("actions: %s: connector name is longer than %d bytes", CreateConnectorName, maxConnectorNameLen)
	}
	if err := checkCreateConfig(p.Config); err != nil {
		return p, err
	}
	// Connect's POST body carries the config's own "name" key verbatim and
	// refuses a mismatch at create time; refuse it here instead, like
	// checkPutConfig does for PUT config, so Preview cannot pass a config
	// Execute is sure to fail with.
	if n, ok := p.Config["name"]; ok && n != p.Connector {
		return p, fmt.Errorf("actions: %s: the config's \"name\" key (%s) does not equal the connector param %q; omit it or make them equal",
			CreateConnectorName, shortText(n), p.Connector)
	}
	return p, nil
}

func (a *createConnector) CheckScope(p owners.Pipeline, raw json.RawMessage) error {
	var params struct {
		Connector string `json:"connector"`
	}
	if err := json.Unmarshal(raw, &params); err != nil {
		return fmt.Errorf("actions: bad params: %w", err)
	}
	return inScopeConnector(p, params.Connector)
}

// Validate checks the params and that the name is absent without asking
// Connect to validate the config (Preview and Execute do that): the name must
// be one of the pipeline's planned connectors, and replace lets a proposal
// whose earlier step deletes the old registration past the absence check.
func (a *createConnector) Validate(ctx context.Context, raw json.RawMessage) error {
	p, err := parseCreateParams(raw)
	if err != nil {
		return err
	}
	if err := ctxConnectorScope(ctx, p.Connector); err != nil {
		return err
	}
	ctx, cancel := a.d.ctx(ctx)
	defer cancel()
	if err := a.checkCreateHosts(ctx, p.Config); err != nil {
		return err
	}
	present, err := a.present(ctx, p.Connector)
	if err != nil {
		return err
	}
	if present && !p.Replace {
		return fmt.Errorf("actions: %s: %w: connector %q is already registered; put connect_delete_connector before this step and set replace: true to re-register it",
			CreateConnectorName, connect.ErrConnectorExists, p.Connector)
	}
	return nil
}

// present reports whether Connect lists name.
func (a *createConnector) present(ctx context.Context, name string) (bool, error) {
	present, err := a.d.listed(ctx, name)
	if err != nil {
		return false, fmt.Errorf("actions: %s: %w", CreateConnectorName, err)
	}
	return present, nil
}

// Preview runs Connect's config validation and refuses when it reports errors.
// It reads only: the validate endpoint creates nothing.
func (a *createConnector) Preview(ctx context.Context, raw json.RawMessage) (PreviewResult, error) {
	p, err := parseCreateParams(raw)
	if err != nil {
		return PreviewResult{}, previewErr(a.Name(), err)
	}
	if err := ctxConnectorScope(ctx, p.Connector); err != nil {
		return PreviewResult{}, previewErr(a.Name(), err)
	}
	ctx, cancel := a.d.ctx(ctx)
	defer cancel()
	present, err := a.present(ctx, p.Connector)
	if err != nil {
		return PreviewResult{}, previewErr(a.Name(), err)
	}
	if present && !p.Replace {
		return PreviewResult{}, previewErr(a.Name(), fmt.Errorf("%w: connector %q is already registered", connect.ErrConnectorExists, p.Connector))
	}
	// The host guard runs before the validate round trip, so Connect is never
	// asked to reach an unapproved host.
	if err := a.checkCreateHosts(ctx, p.Config); err != nil {
		return PreviewResult{}, previewErr(a.Name(), err)
	}
	dry, err := a.validate(ctx, p.Connector, p.Config)
	if err != nil {
		return PreviewResult{}, previewErr(a.Name(), err)
	}
	class := strings.TrimSpace(p.Config["connector.class"])
	res := PreviewResult{
		Target: "connector " + p.Connector,
		// The approver decides on this text, so it names where the connector
		// will point, not just how many keys the config has.
		Current:    fmt.Sprintf("not registered (GET /connectors); class %s, %d config keys; %s", class, len(p.Config), createHostsText(p.Config)),
		Expected:   "created from the validated config; it starts its tasks and begins emitting/writing to its topics",
		Reversible: true,
		Rollback:   rollbackText(DeleteConnectorName, map[string]string{"connector": p.Connector}),
		DryRun:     dry,
	}
	if present {
		res.Current = fmt.Sprintf("registered now; a preceding %s step removes it (GET /connectors); %s", DeleteConnectorName, createHostsText(p.Config))
		res.Expected = fmt.Sprintf("re-registered: the delete step removes the current registration, then this step creates it from the validated config; Connect keeps the stored offsets under the name, so the new registration resumes from them")
	}
	return res, nil
}

// validate asks Connect to validate cfg, refusing an error count above zero.
// Connect's validate endpoint checks a connector config and requires the
// connector's name as the "name" key, which is not part of the config that
// POST /connectors carries separately.
func (a *createConnector) validate(ctx context.Context, name string, cfg map[string]string) (*DryRunResult, error) {
	v, ok := a.d.Connect.(configValidator)
	if !ok {
		return nil, fmt.Errorf("this Connect client cannot validate a config (PUT /connector-plugins/{class}/config/validate): %w", clients.ErrNotSupported)
	}
	class := strings.TrimSpace(cfg["connector.class"])
	sent := make(map[string]string, len(cfg))
	for k, val := range cfg {
		sent[k] = val
	}
	sent["name"] = name
	vr, err := v.ValidateConfig(ctx, class, sent)
	if err != nil {
		return nil, fmt.Errorf("Connect could not validate the config: %w", err)
	}
	if vr.ErrorCount > 0 {
		return nil, fmt.Errorf("Connect refused the config: %s", validateText(vr))
	}
	return &DryRunResult{OK: true, Source: sourceValidate, Text: validateText(vr)}, nil
}

func (a *createConnector) Execute(ctx context.Context, raw json.RawMessage) (string, error) {
	p, err := parseCreateParams(raw)
	if err != nil {
		return "", err
	}
	if err := ctxConnectorScope(ctx, p.Connector); err != nil {
		return "", err
	}
	ctx, cancel := a.d.writeCtx(ctx)
	defer cancel()
	lw, ok := a.d.Connect.(connect.ConnectLifecycleWriter)
	if !ok {
		return "", fmt.Errorf("actions: %s: connect client does not support CreateConnector (connect.ConnectLifecycleWriter); is write mode on?", CreateConnectorName)
	}
	present, err := a.present(ctx, p.Connector)
	if err != nil {
		return "", err
	}
	if present {
		// The promised delete step did not run (or failed): never overwrite a
		// registration that may be someone else's.
		return "", fmt.Errorf("actions: %s: %w: connector %q is still registered at execution; the %s step before this one must remove it",
			CreateConnectorName, connect.ErrConnectorExists, p.Connector, DeleteConnectorName)
	}
	// Validate again right before the write: the config is the same approved
	// one, but Connect's plugin set or an older worker may have changed.
	if _, err := a.validate(ctx, p.Connector, p.Config); err != nil {
		return "", fmt.Errorf("actions: %s: %w", CreateConnectorName, err)
	}
	if err := a.createWithRetry(ctx, lw, p.Connector, p.Config); err != nil {
		return "", fmt.Errorf("actions: %s: %w", CreateConnectorName, err)
	}
	// The write is done. Every later problem is a non-error result: an error
	// would tell the agent the create failed and invite a retry.
	class := strings.TrimSpace(p.Config["connector.class"])
	out := fmt.Sprintf("created connector %s (class %s, %d config keys)", p.Connector, class, len(p.Config))
	st, err := a.d.Connect.Status(ctx, p.Connector)
	if err != nil {
		return out + "; the status read failed (Connect starts it asynchronously)", nil
	}
	return fmt.Sprintf("%s; state %s at first read (Connect starts it asynchronously)", out, orUnknown(st.State)), nil
}

// createWithRetry creates name and copes with Connect's worker rebalance: the
// HTTP client bounds one request at 10 s, so a create that waits on a
// rebalance (common right after a delete of the same name) can time out. When
// a timed-out attempt did land, the list check sees it; otherwise the create
// is retried, and a 409 rebalance is retried directly. A landed detection is
// only believed after the registered config reads back as the approved one:
// without that, a timed-out POST that never landed plus a concurrent create
// of the same name would be reported as this create's success.
func (a *createConnector) createWithRetry(ctx context.Context, lw connect.ConnectLifecycleWriter, name string, cfg map[string]string) error {
	var last error
	timedOut := false
	for attempt := 0; attempt < 3; attempt++ {
		err := lw.CreateConnector(ctx, name, cfg)
		switch {
		case err == nil:
			return nil
		case errors.Is(err, connect.ErrConnectorExists):
			if timedOut {
				// The attempt that timed out may have created it.
				return a.verifyCreated(ctx, name, cfg)
			}
			return err
		case isTimeoutErr(err):
			timedOut, last = true, err
			if present, perr := a.d.listed(ctx, name); perr == nil && present {
				return a.verifyCreated(ctx, name, cfg)
			}
		case errors.Is(err, connect.ErrRebalancing):
			last = err
		default:
			return err
		}
		select {
		case <-ctx.Done():
			return last
		case <-time.After(500 * time.Millisecond):
		}
	}
	return last
}

// verifyCreated confirms a landed detection: the registered config must carry
// every approved key with its approved value (Connect stores provider
// references verbatim, so they compare as text). The error names only a key,
// never a value.
func (a *createConnector) verifyCreated(ctx context.Context, name string, cfg map[string]string) error {
	got, err := a.d.Connect.Config(ctx, name)
	if err != nil {
		return fmt.Errorf("a timed-out create attempt may have registered %s, but the config read-back failed, so this create is not claimed: %w", name, err)
	}
	for _, k := range sortedStringKeys(cfg) {
		if got[k] != cfg[k] {
			return fmt.Errorf("%s is registered, but its config differs from the approved one at key %s (a concurrent create may have won); this create is not claimed", name, k)
		}
	}
	return nil
}

// ---- connect_delete_connector ----

// deleteConnector deletes a connector (DELETE /connectors/{name}).
type deleteConnector struct{ d LifecycleDeps }

var (
	_ Action  = (*deleteConnector)(nil)
	_ Scoped  = (*deleteConnector)(nil)
	_ Preview = (*deleteConnector)(nil)
)

func (a *deleteConnector) Name() string  { return DeleteConnectorName }
func (a *deleteConnector) Layer() string { return "kafka-connect" }
func (a *deleteConnector) Describe() string {
	return "Deletes a Kafka Connect connector and stops its tasks (DELETE /connectors/{name}). Refused when the stored config holds a credential in plain text, because the delete's only undo (re-creating the connector) could not restore it. Connect keeps the connector's stored offsets under the name: a later connect_create_connector with the same name resumes from them. To re-register, put this step before the create step in the same proposal."
}
func (a *deleteConnector) ParamsSchema() map[string]any {
	return objectSchema(map[string]any{"connector": strProp("connector name")}, "connector")
}

func (a *deleteConnector) CheckScope(p owners.Pipeline, raw json.RawMessage) error {
	name, err := parseConnectorParam(raw)
	if err != nil {
		return err
	}
	return inScopeConnector(p, name)
}

// readConfig reads the connector config and refuses one with a plaintext
// credential. The config never leaves this function.
func (a *deleteConnector) readConfig(ctx context.Context, name string) error {
	cfg, err := a.d.Connect.Config(ctx, name)
	if err != nil {
		return fmt.Errorf("actions: %s: read config of %s: %w", DeleteConnectorName, name, err)
	}
	if err := checkNoPlaintextSecrets(cfg); err != nil {
		return fmt.Errorf("actions: %s: connector %s: %w", DeleteConnectorName, name, err)
	}
	return nil
}

func (a *deleteConnector) Validate(ctx context.Context, raw json.RawMessage) error {
	name, err := parseConnectorParam(raw)
	if err != nil {
		return err
	}
	if err := ctxConnectorScope(ctx, name); err != nil {
		return err
	}
	ctx, cancel := a.d.ctx(ctx)
	defer cancel()
	if err := a.d.connectorExists(ctx, name); err != nil {
		return err
	}
	return a.readConfig(ctx, name)
}

// Preview reads the status and the config only.
func (a *deleteConnector) Preview(ctx context.Context, raw json.RawMessage) (PreviewResult, error) {
	name, err := parseConnectorParam(raw)
	if err != nil {
		return PreviewResult{}, previewErr(a.Name(), err)
	}
	if err := ctxConnectorScope(ctx, name); err != nil {
		return PreviewResult{}, previewErr(a.Name(), err)
	}
	ctx, cancel := a.d.ctx(ctx)
	defer cancel()
	st, err := a.d.Connect.Status(ctx, name)
	if err != nil {
		return PreviewResult{}, previewErr(a.Name(), err)
	}
	if err := a.readConfig(ctx, name); err != nil {
		return PreviewResult{}, previewErr(a.Name(), err)
	}
	res := PreviewResult{
		Target: "connector " + name,
		Current: fmt.Sprintf("%s (GET /connectors/%s/status); its config holds no plaintext credential",
			connectorAndTasksText(st), name),
		Expected: "deleted: the connector and its tasks stop; Connect keeps the stored offsets under the name, so a later create with this name resumes from them",
		Downstream: []string{
			"records already in its topics stay there and consumers keep reading them; deleting a source connector does not clean up its source-side replication slot/publication",
		},
		Reversible: false, // re-registering is delete then create in one proposal, not this step's undo
	}
	return res, nil
}

// connectorAndTasksText describes a connector status.
func connectorAndTasksText(st clients.ConnectorStatus) string {
	desc, _, _ := connectorAndTasks(st)
	return desc
}

func (a *deleteConnector) Execute(ctx context.Context, raw json.RawMessage) (string, error) {
	name, err := parseConnectorParam(raw)
	if err != nil {
		return "", err
	}
	if err := ctxConnectorScope(ctx, name); err != nil {
		return "", err
	}
	ctx, cancel := a.d.writeCtx(ctx)
	defer cancel()
	lw, ok := a.d.Connect.(connect.ConnectLifecycleWriter)
	if !ok {
		return "", fmt.Errorf("actions: %s: connect client does not support DeleteConnector (connect.ConnectLifecycleWriter); is write mode on?", DeleteConnectorName)
	}
	if err := a.d.connectorExists(ctx, name); err != nil {
		return "", err
	}
	// Re-read the config: it may have changed since the approval, and a
	// plaintext credential must still block the delete.
	if err := a.readConfig(ctx, name); err != nil {
		return "", err
	}
	if err := a.deleteWithRetry(ctx, lw, name); err != nil {
		return "", fmt.Errorf("actions: %s: %w", DeleteConnectorName, err)
	}
	return fmt.Sprintf("deleted connector %s; its tasks stop and the stored offsets stay under the name (a later create with this name resumes from them)", name), nil
}

// deleteWithRetry deletes name, retrying a rebalance and treating a timed-out
// request that did remove the connector (or a 404) as done.
func (a *deleteConnector) deleteWithRetry(ctx context.Context, lw connect.ConnectLifecycleWriter, name string) error {
	var last error
	for attempt := 0; attempt < 3; attempt++ {
		err := lw.DeleteConnector(ctx, name)
		switch {
		case err == nil:
			return nil
		case errors.Is(err, clients.ErrNotFound):
			// Gone: removed by this loop's earlier timed-out attempt, or by
			// someone else between Execute's exists check and the DELETE.
			// Either way the approved end state holds; failing here would only
			// invite a pointless retry.
			return nil
		case isTimeoutErr(err):
			last = err
			if present, perr := a.d.listed(ctx, name); perr == nil && !present {
				return nil
			}
		case errors.Is(err, connect.ErrRebalancing):
			last = err
		default:
			return err
		}
		select {
		case <-ctx.Done():
			return last
		case <-time.After(500 * time.Millisecond):
		}
	}
	return last
}

// ---- config guards ----

// checkCreateConfig refuses a config whose credential values are not
// ${secret:...} references, any ${file:...} or ${env:...} reference, and a
// reference on a non-credential key. It never echoes a value.
//
// Only the named-secret-store form is accepted from a proposal, on a
// credential or converter key only: Connect resolves a provider reference
// server-side during Preview's validate round trip and can echo the resolved
// value in a validation message, so a proposal-chosen ${file:path} or
// ${env:NAME} would read an arbitrary server file or environment variable
// back into the room before anyone approves (the Q4a validate tool applies
// the same rule, checkValidateInput). A stored config read back from Connect
// (the delete guard) may hold any provider form.
func checkCreateConfig(cfg map[string]string) error {
	if len(cfg) == 0 {
		return fmt.Errorf("actions: %s: config is required", CreateConnectorName)
	}
	if len(cfg) > maxLifecycleConfigKeys {
		return fmt.Errorf("actions: %s: config has %d keys; at most %d", CreateConnectorName, len(cfg), maxLifecycleConfigKeys)
	}
	class := strings.TrimSpace(cfg["connector.class"])
	if class == "" {
		return fmt.Errorf("actions: %s: config.connector.class is required (the connector plugin class)", CreateConnectorName)
	}
	if len(class) > maxLifecycleClassBytes || !connectorClassNameRe.MatchString(class) {
		return fmt.Errorf("actions: %s: config.connector.class is not a connector plugin class name", CreateConnectorName)
	}
	for _, k := range sortedStringKeys(cfg) {
		if len(k) > maxLifecycleKeyBytes || !lifecycleConfigKeyRe.MatchString(k) {
			return fmt.Errorf("actions: %s: config key %q is not a connector config key name (letters, digits, '.', '_', '-', '$'; at most %d bytes)",
				CreateConnectorName, shortText(k), maxLifecycleKeyBytes)
		}
		v := cfg[k]
		if len(v) > maxLifecycleValueBytes {
			return fmt.Errorf("actions: %s: the value of %s is longer than %d bytes", CreateConnectorName, k, maxLifecycleValueBytes)
		}
		if lifecycleConfigReference(v) {
			if !lifecycleSecretReference(v) {
				return fmt.Errorf("actions: %s: %s uses a ${file:...} or ${env:...} reference, which Connect could resolve server-side and echo in a validation message; only ${secret:...} is accepted in a proposal",
					CreateConnectorName, k)
			}
			if !lifecycleReferenceAllowedKey(k) {
				return fmt.Errorf("actions: %s: %s is not a credential key; a ${secret:...} reference is only accepted on a credential or converter key, so a validation message cannot echo a server-side value",
					CreateConnectorName, k)
			}
			continue
		}
		if lifecycleConfigSecretKey(k) {
			return fmt.Errorf("actions: %s: %w: %s is a credential key; its value must be a ${secret:...} reference",
				CreateConnectorName, ErrPlaintextSecret, k)
		}
		if valueLooksSecret(v) {
			return fmt.Errorf("actions: %s: %w: the value of %s looks like a credential; use a ${secret:...} reference",
				CreateConnectorName, ErrPlaintextSecret, k)
		}
	}
	return nil
}

// checkNoPlaintextSecrets refuses a stored config with a credential value that
// is not a config-provider reference. Its rollback (a create with the same
// config) could not restore a plaintext secret, so the delete is refused.
func checkNoPlaintextSecrets(cfg map[string]string) error {
	for _, k := range sortedStringKeys(cfg) {
		v := cfg[k]
		if v == "" || lifecycleConfigReference(v) {
			continue
		}
		if lifecycleConfigSecretKey(k) {
			return fmt.Errorf("%w: config key %s; its value is not a config-provider reference, so a re-create could not restore it", ErrPlaintextSecret, k)
		}
		if valueLooksSecret(v) {
			return fmt.Errorf("%w: the value of config key %s is not a config-provider reference, so a re-create could not restore it", ErrPlaintextSecret, k)
		}
	}
	return nil
}

// lifecycleConfigSecretKey is the shared LooksSecret rule with exemptions for
// the bare "key" segment ONLY: a converter class and the record-key settings
// are not credentials although their names hold "key" (key.converter,
// key.format). A key that carries one of the credential substrings
// (password, secret, token, ...) is a credential under ANY prefix: a
// converter's schema-registry keystore password
// (value.converter.schema.registry.ssl.keystore.password) is as much a secret
// as a database password, and the exemption must not launder it.
func lifecycleConfigSecretKey(k string) bool {
	lk := strings.ToLower(k)
	for _, s := range secretKeySubstrings {
		if strings.Contains(lk, s) {
			return true
		}
	}
	if !LooksSecret(k) {
		return false
	}
	// LooksSecret fired on the bare "key" segment alone; exempt the known
	// record-key and converter settings.
	for _, prefix := range []string{"key.converter", "value.converter", "header.converter"} {
		if lk == prefix || strings.HasPrefix(lk, prefix+".") {
			return false
		}
	}
	for _, suffix := range []string{"key.ignore", "message.key.columns", "key.format", "key.fields"} {
		if lk == suffix || strings.HasSuffix(lk, "."+suffix) {
			return false
		}
	}
	return true
}

// lifecycleSecretReference reports whether v is one whole ${secret:...}
// reference: the only provider form a proposal may carry (see
// checkCreateConfig).
func lifecycleSecretReference(v string) bool {
	return lifecycleConfigReference(v) && strings.HasPrefix(v[2:], "secret:")
}

// lifecycleReferenceAllowedKey reports whether a ${secret:...} reference may
// sit under key k: a credential-looking key (where a reference is the only
// accepted form) or a converter key (key./value./header.converter.* carry
// schema-registry credentials such as basic.auth.user.info). A host key is
// not allowed: a host must be a literal the host guard can check.
func lifecycleReferenceAllowedKey(k string) bool {
	if LooksSecret(k) {
		return true
	}
	lk := strings.ToLower(k)
	for _, prefix := range []string{"key.converter", "value.converter", "header.converter"} {
		if lk == prefix || strings.HasPrefix(lk, prefix+".") {
			return true
		}
	}
	return false
}

// checkCreateHosts refuses a config whose host keys name any host the
// pipeline does not already reach: the hosts of the pipeline's owners.yaml
// connections and of its currently registered connectors' configs (the Q4a
// reduction rules, kafkatools.ConfigHosts, so a failover URL or bootstrap
// list cannot hide a second host behind an allowed first one). The scope
// check constrains only the connector NAME; without this guard an in-scope
// name could be configured to read another database or write to an attacker
// host, and the approver would not see it. It needs the pipeline from ctx:
// Validate also runs before the proposal path scopes ctx and passes then
// (like ctxConnectorScope); Preview runs scoped, so every proposal is guarded
// before approval. Execute does not re-run it: its params are byte-identical
// to the approved ones, and in a replace proposal the delete step has already
// removed the old registration, whose config may have been the allowed set's
// only source for its own host. The refusal names only the proposal's own key
// and host, never the allowed set.
func (a *createConnector) checkCreateHosts(ctx context.Context, cfg map[string]string) error {
	p, ok := tools.PipelineFrom(ctx)
	if !ok {
		return nil
	}
	allowed := map[string]bool{}
	for _, r := range p.Connections {
		for _, h := range kafkatools.ConfigHosts(r.Value) {
			if h != "" && kafkatools.ValidHost(h) {
				allowed[h] = true
			}
		}
	}
	for _, name := range p.Connectors {
		other, err := a.d.Connect.Config(ctx, name)
		if err != nil {
			continue // an unreadable config adds no hosts: fail closed
		}
		kafkatools.AddConfigHosts(allowed, other)
	}
	for _, k := range sortedStringKeys(cfg) {
		if !kafkatools.IsHostConfigKey(k) {
			continue
		}
		if lifecycleConfigSecretKey(k) {
			continue // its value is a ${secret:...} reference (checkCreateConfig), never a host
		}
		v := strings.TrimSpace(cfg[k])
		if v == "" {
			continue // Connect's validation reports the missing required value
		}
		if lifecycleConfigReference(v) {
			return fmt.Errorf("actions: %s: host key %s must be a literal host, not a config reference", CreateConnectorName, k)
		}
		hosts := kafkatools.ConfigHosts(v)
		if len(hosts) == 0 {
			return fmt.Errorf("actions: %s: host key %s does not name a host", CreateConnectorName, k)
		}
		for _, h := range hosts {
			if !kafkatools.ValidHost(h) {
				return fmt.Errorf("actions: %s: host key %s does not reduce to host names (every host of a multi-host value must be a plain host[:port])", CreateConnectorName, k)
			}
			if !allowed[h] {
				return fmt.Errorf("actions: %s: %w: host key %s points at %s, which none of the pipeline's connections or registered connectors reach",
					CreateConnectorName, tools.ErrOutOfScope, k, h)
			}
		}
	}
	return nil
}

// createHostsText names the hosts a to-be-created config points at, for the
// approver: the reduced host names per host key, never raw values (a DSN may
// carry userinfo). Hosts passed checkCreateHosts, so they are plain names.
func createHostsText(cfg map[string]string) string {
	var parts []string
	for _, k := range sortedStringKeys(cfg) {
		if !kafkatools.IsHostConfigKey(k) || lifecycleConfigSecretKey(k) {
			continue
		}
		v := strings.TrimSpace(cfg[k])
		if v == "" || lifecycleConfigReference(v) {
			continue
		}
		if hosts := kafkatools.ConfigHosts(v); len(hosts) > 0 {
			parts = append(parts, k+" -> "+strings.Join(hosts, ","))
		}
	}
	if len(parts) == 0 {
		return "no host keys in the config"
	}
	return "points at: " + strings.Join(parts, "; ")
}

// lifecycleConfigReference reports whether v is one whole Connect config
// provider reference of the three documented forms. It is checked before any
// secret-looking value rule, because ${secret:...} itself contains "secret".
func lifecycleConfigReference(v string) bool {
	if !strings.HasPrefix(v, "${") || !strings.HasSuffix(v, "}") || len(v) < len("${x:}") {
		return false
	}
	inner := v[2 : len(v)-1]
	if strings.Contains(inner, "}") {
		return false
	}
	for _, p := range []string{"file:", "env:", "secret:"} {
		if strings.HasPrefix(inner, p) && len(inner) > len(p) {
			return true
		}
	}
	return false
}

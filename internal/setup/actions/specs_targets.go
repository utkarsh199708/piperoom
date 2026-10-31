package setupactions

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"slices"
	"strings"

	"piperoom/internal/actions"
	"piperoom/internal/owners"
	"piperoom/internal/tools"
)

// The writes of the Strimzi target (internal/setup/targets/strimzi, design
// docs/design/setup-strimzi.md §5–§8) and of the managed targets
// (internal/setup/targets/managed, docs/design/setup-managed.md).
//
// This file mirrors the drivers' params rules instead of importing them: the
// drivers reach the agent through ops, connprobe and wiring, and importing the
// managed driver would link the runner's AWS, Confluent and Aiven clients
// into the server. Every schema here is the op's ParamsSchema (except the
// card-only field named below), and every rule is a no-I/O subset of the
// op's Validate; the runner re-validates every param against the driver's
// own rules, including the ones that need its state (an ARN's account and
// region, the MSK worker property values, a template's own params). The
// external round-trip test (specs_targets_test.go) keeps the two in step.

// targetSpecs are the T2 writes of the Strimzi and managed drivers. A runner
// whose room has another target does not register these ops and refuses them
// as unknown.
func targetSpecs() []Spec {
	return []Spec{
		// ---- Strimzi (row U12a; rollback ops of §8) ----
		{
			Name:     "k8s_apply",
			Tier:     "T2",
			Layer:    "kafka-connect",
			Describe: "Write one chart-created Strimzi object from a template (kind and template: KafkaConnect connect, KafkaConnector connector, KafkaTopic canary-topic, KafkaUser connect-user or read-user, ConfigMap connect-logging) with the template's typed params; the runner replaces its spec under a resourceVersion test, reads it back and unpauses it last. The object name comes from the template, never a param.",
			Schema:   strimziApplySchema(),
			Scope:    strimziApplyScope,
			// preview_k8s_apply answers the dry run as an
			// events.PreviewResult with the rollback it records.
			PreviewOp:   "preview_k8s_apply",
			CheckParams: checkStrimziApply,
		},
		{
			Name:     "k8s_secret_apply",
			Tier:     "T2",
			Layer:    "kafka-connect",
			Describe: "Fill the chart's <pipeline>-db-credentials Secret: keys map a Secret key to a ${secret:name} reference the runner materialises from its own store; a value is never a literal and never leaves the runner.",
			Schema: objectSchema(map[string]any{
				"keys": map[string]any{"type": "object", "minProperties": 1, "maxProperties": 16,
					"additionalProperties": map[string]any{"type": "string", "pattern": strimziSecretRefPattern}},
			}, "keys"),
			Scope:       ScopeFree,
			PreviewOp:   "preview_k8s_secret_apply",
			CheckParams: checkStrimziSecretApply,
		},
		{
			Name:     "k8s_delete",
			Tier:     "T2",
			Layer:    "kafka-connect",
			Describe: "Roll back a KafkaConnector or the canary KafkaTopic: delete it by its chart-fixed name (the Topic Operator then deletes the topic). Any other kind or name is refused, and both kinds are refused on a production target (the topic is data; a connector is deleted by a person).",
			Schema:   strimziRollbackSchema(strimziDeleteKinds, true),
			Scope:    strimziDeleteScope,
			// No PreviewOp until the runner registers preview_k8s_delete:
			// Preview answers "no preview" without a runner round trip.
			CheckParams: checkStrimziRollback(strimziDeleteKinds, true),
		},
		{
			Name:     "k8s_unapply",
			Tier:     "T2",
			Layer:    "kafka-connect",
			Describe: "Roll back a k8s_apply or k8s_secret_apply: re-pause the KafkaConnect or a KafkaUser (name required) with the chart shell's spec, or patch the db-credentials Secret or the logging ConfigMap back to empty. The KafkaConnect and the Secret are refused on a production target.",
			Schema:   strimziRollbackSchema(strimziUnapplyKinds, false),
			Scope:    ScopeFree,
			// No PreviewOp until the runner registers preview_k8s_unapply:
			// Preview answers "no preview" without a runner round trip.
			CheckParams: checkStrimziRollback(strimziUnapplyKinds, false),
		},

		// ---- AWS MSK Connect (row U13b2) ----
		{
			Name:     "msk_create_plugin",
			Tier:     "T2",
			Layer:    "kafka-connect",
			Describe: "Create an MSK Connect custom plugin from the .zip or .jar archive the person uploaded to S3 in the target's account pinned to the object's S3 version id, required (versioning must be on for the bucket), so the object the runner checks is the object MSK builds; the runner checks the bucket's owner, that that version exists and that its size is expected_size, required, first; the digest is not verified; asynchronous, follow with msk_wait.",
			Schema: objectSchema(map[string]any{
				"name": mskString(), "bucket": mskString(), "key": mskString(), "version": mskString(),
				"content_type":  map[string]any{"type": "string", "enum": []string{"ZIP", "JAR"}},
				"expected_size": map[string]any{"type": "integer", "minimum": 1},
			}, "name", "bucket", "key", "version", "content_type", "expected_size"),
			Scope:       ScopeFree,
			CheckParams: checkMSKCreatePlugin,
			// No PreviewOp: the runner registers no preview_msk_* op.
		},
		{
			Name:     "msk_create_worker_config",
			Tier:     "T2",
			Layer:    "kafka-connect",
			Describe: "Create an MSK Connect worker configuration rendered from the runner's template; entries are allow-listed worker keys with enumerated or bounded values, never a credential.",
			Schema: objectSchema(map[string]any{
				"name": mskString(), "entries": stringMapSchema(),
			}, "name", "entries"),
			Scope:       ScopeFree,
			CheckParams: checkMSKWorker,
		},
		{
			Name:     "msk_create_connector",
			Tier:     "T2",
			Layer:    "kafka-connect",
			Describe: "Create the MSK Connect connector (1 MCU, 1 worker, IAM auth, TLS) in the target's account; a config value under a secret-looking key is a ${secret:name} reference the runner materialises. Asynchronous, follow with msk_wait.",
			Schema: objectSchema(map[string]any{
				"name": mskString(), "kafka_connect_version": mskString(), "cluster_arn": mskString(),
				"subnets": stringListSchema(), "security_groups": stringListSchema(), "plugin_arn": mskString(),
				"plugin_revision":        map[string]any{"type": "integer", "minimum": 1},
				"worker_config_arn":      mskString(),
				"worker_config_revision": map[string]any{"type": "integer", "minimum": 1},
				"service_role_arn":       mskString(), "log_group": mskString(), "config": stringMapSchema(),
			}, "name", "kafka_connect_version", "subnets", "security_groups", "plugin_arn", "service_role_arn", "config"),
			Scope:       scopeConnectorName,
			CheckParams: checkMSKCreateConnector,
		},
		{
			Name:     "msk_delete_connector",
			Tier:     "T2",
			Layer:    "kafka-connect",
			Describe: "Delete an MSK Connect connector this room created (tagged with its room id); the runner refuses any other connector on every class.",
			Schema:   objectSchema(map[string]any{"name": mskString()}, "name"),
			Scope:    scopeConnectorName,
			CheckParams: func(raw json.RawMessage) error {
				var p struct {
					Name string `json:"name"`
				}
				if err := decodeStrict(raw, &p); err != nil {
					return err
				}
				return matchOrErr(mskNameRE, "name", p.Name)
			},
		},

		// ---- Confluent Cloud (row U13c) ----
		{
			Name:     "ccloud_create_connector",
			Tier:     "T2",
			Layer:    "kafka-connect",
			Describe: "Validate a managed connector's config against the Confluent Cloud plugin, then create it only if validation found no errors; a config value under a secret-looking key is a ${secret:name} reference the runner materialises.",
			Schema: objectSchema(map[string]any{
				"name": mskString(), "plugin": mskString(), "config": stringMapSchema(),
			}, "name", "plugin", "config"),
			Scope:       scopeConnectorName,
			CheckParams: checkCCloudCreate,
		},
		{
			Name:     "ccloud_acl_create",
			Tier:     "T2",
			Layer:    "kafka-connect",
			Describe: "Create a template's fixed ACL rows for one Confluent service account. watcher: read the topics, describe group (connect-<connector>, a consumer_groups entry, or connect-lcc-<id> with connector naming a managed sink, whose id the runner checks). connector: create, write, describe the topics. Topic rows are PREFIXED on <topic_prefix>. (the pipeline's). principal must be the account the person entered on the runner for the template; the runner checks it.",
			Schema: objectSchema(map[string]any{
				"template":  map[string]any{"type": "string", "enum": []string{"watcher", "connector"}},
				"principal": mskString(), "topic_prefix": mskString(), "group": mskString(), "connector": mskString(),
			}, "template", "principal", "topic_prefix"),
			Scope:       scopeCCloudACL,
			CheckParams: checkCCloudACL,
		},

		// ---- Aiven (row U13d) ----
		{
			Name:     "aiven_create_connector",
			Tier:     "T2",
			Layer:    "kafka-connect",
			Describe: "Create a connector on the Aiven Kafka Connect service after the runner checks its connector.class is an available plugin; a config value under a secret-looking key is a ${secret:name} reference the runner materialises.",
			Schema: objectSchema(map[string]any{
				"name": mskString(), "config": stringMapSchema(),
			}, "name", "config"),
			Scope:       scopeConnectorName,
			CheckParams: checkAivenCreate,
		},
		{
			Name:     "aiven_integration_create",
			Tier:     "T2",
			Layer:    "kafka-connect",
			Describe: "Join a Kafka service of the project to the Aiven Kafka Connect service (integration type kafka_connect); the runner refuses a source service that is not Kafka.",
			Schema:   objectSchema(map[string]any{"source_service": mskString()}, "source_service"),
			Scope:    ScopeFree,
			CheckParams: func(raw json.RawMessage) error {
				var p struct {
					SourceService string `json:"source_service"`
				}
				if err := decodeStrict(raw, &p); err != nil {
					return err
				}
				return matchOrErr(aivenServiceRE, "source_service", p.SourceService)
			},
		},
	}
}

// decodeStrict decodes params with no unknown field and no trailing JSON, as
// the drivers decode them.
func decodeStrict(raw json.RawMessage, dst any) error {
	if len(bytes.TrimSpace(raw)) == 0 {
		raw = json.RawMessage(`{}`)
	}
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()
	if err := dec.Decode(dst); err != nil {
		return fmt.Errorf("params: %v", err)
	}
	if dec.More() {
		return errors.New("params: trailing JSON")
	}
	return nil
}

func matchOrErr(re *regexp.Regexp, field, v string) error {
	if !re.MatchString(v) {
		return fmt.Errorf("%s %q does not match %s", field, clipText(v), re.String())
	}
	return nil
}

// ---- Strimzi mirrors ----

// strimziApplyKinds and strimziTemplates mirror k8s_apply's kinds and the
// template table (strimzi's applyKinds and templates): template -> kind.
var (
	strimziApplyKinds = []string{"ConfigMap", "KafkaConnect", "KafkaConnector", "KafkaTopic", "KafkaUser"}
	strimziTemplates  = map[string]string{
		"canary-topic":    "KafkaTopic",
		"connect":         "KafkaConnect",
		"connect-logging": "ConfigMap",
		"connect-user":    "KafkaUser",
		"connector":       "KafkaConnector",
		"read-user":       "KafkaUser",
	}
	strimziDeleteKinds  = []string{"KafkaConnector", "KafkaTopic"}
	strimziUnapplyKinds = []string{"ConfigMap", "KafkaConnect", "KafkaUser", "Secret"}
)

// strimziNamePattern mirrors the driver's nameRE (a DNS-1123 label as the
// chart names objects).
const strimziNamePattern = `^[a-z0-9]([a-z0-9-]{0,51}[a-z0-9])?$`

var strimziNameRE = regexp.MustCompile(strimziNamePattern)

// strimziSecretKeyRE and strimziSecretRefPattern mirror k8s_secret_apply's
// key and reference rules.
var strimziSecretKeyRE = regexp.MustCompile(`^[A-Za-z0-9._-]{1,63}$`)

const strimziSecretRefPattern = `^\$\{secret:([A-Za-z_][A-Za-z0-9_.-]{0,127})\}$`

var strimziSecretRefRE = regexp.MustCompile(strimziSecretRefPattern)

// strimziCardOnly is the field only the server's approval card may set (D11:
// the person confirms the admission policy is bound when the runner's Role
// cannot read it). The model never sees it in a schema and a step carrying it
// is refused: a confirmation in model text is not a person's confirmation.
const strimziCardOnly = "admission_policy_confirmed"

func templateNamesSorted() []string {
	out := make([]string, 0, len(strimziTemplates))
	for n := range strimziTemplates {
		out = append(out, n)
	}
	slices.Sort(out)
	return out
}

func strimziApplySchema() map[string]any {
	return objectSchema(map[string]any{
		"kind":     map[string]any{"type": "string", "enum": append([]string(nil), strimziApplyKinds...)},
		"template": map[string]any{"type": "string", "enum": templateNamesSorted()},
		"params":   map[string]any{"type": "object"},
	}, "kind", "template", "params")
}

func strimziRollbackSchema(kinds []string, nameRequired bool) map[string]any {
	required := []string{"kind"}
	if nameRequired {
		required = append(required, "name")
	}
	return objectSchema(map[string]any{
		"kind": map[string]any{"type": "string", "enum": append([]string(nil), kinds...)},
		"name": map[string]any{"type": "string", "pattern": strimziNamePattern},
	}, required...)
}

// refuseCardOnly refuses params that carry the card-only field.
func refuseCardOnly(raw json.RawMessage) error {
	var obj map[string]json.RawMessage
	if err := json.Unmarshal(paramsOrEmpty(raw), &obj); err != nil {
		return fmt.Errorf("params must be a JSON object: %v", err)
	}
	if _, ok := obj[strimziCardOnly]; ok {
		return fmt.Errorf("%s is set by the approval card, never by a proposal", strimziCardOnly)
	}
	return nil
}

type strimziApplyParams struct {
	Kind     string          `json:"kind"`
	Template string          `json:"template"`
	Params   json.RawMessage `json:"params"`
}

func checkStrimziApply(raw json.RawMessage) error {
	if err := refuseCardOnly(raw); err != nil {
		return err
	}
	var p strimziApplyParams
	if err := decodeStrict(raw, &p); err != nil {
		return err
	}
	if !slices.Contains(strimziApplyKinds, p.Kind) {
		return fmt.Errorf("kind %q is not one of %s", clipText(p.Kind), strings.Join(strimziApplyKinds, ", "))
	}
	kind, ok := strimziTemplates[p.Template]
	if !ok {
		return fmt.Errorf("template %q is not one of %s", clipText(p.Template), strings.Join(templateNamesSorted(), ", "))
	}
	if kind != p.Kind {
		return fmt.Errorf("template %s renders a %s, not a %s", p.Template, kind, p.Kind)
	}
	if t := bytes.TrimSpace(p.Params); len(t) > 0 && t[0] != '{' {
		return errors.New("params must be the template's params object")
	}
	return nil
}

func checkStrimziSecretApply(raw json.RawMessage) error {
	if err := refuseCardOnly(raw); err != nil {
		return err
	}
	var p struct {
		Keys map[string]string `json:"keys"`
	}
	if err := decodeStrict(raw, &p); err != nil {
		return err
	}
	if len(p.Keys) == 0 || len(p.Keys) > 16 {
		return errors.New("keys must hold 1 to 16 entries")
	}
	for k, v := range p.Keys {
		if !strimziSecretKeyRE.MatchString(k) {
			return fmt.Errorf("key %q is not a Secret key", clipText(k))
		}
		if !strimziSecretRefRE.MatchString(v) {
			// The value is never echoed: it may be a credential typed by
			// mistake.
			return fmt.Errorf("key %s: the value must be a ${secret:name} reference, never a literal", k)
		}
	}
	return nil
}

type strimziRollbackParams struct {
	Kind string `json:"kind"`
	Name string `json:"name,omitempty"`
}

// checkStrimziRollback mirrors the shape rules of k8s_delete and k8s_unapply;
// which chart name a kind may take, and the production refusals, are the
// runner's (it knows the chart's pipeline and the target's class).
func checkStrimziRollback(kinds []string, nameRequired bool) func(json.RawMessage) error {
	return func(raw json.RawMessage) error {
		if err := refuseCardOnly(raw); err != nil {
			return err
		}
		var p strimziRollbackParams
		if err := decodeStrict(raw, &p); err != nil {
			return err
		}
		if !slices.Contains(kinds, p.Kind) {
			return fmt.Errorf("kind %q is not one of %s", clipText(p.Kind), strings.Join(kinds, ", "))
		}
		if p.Name == "" {
			if nameRequired || p.Kind == "KafkaUser" {
				return fmt.Errorf("a %s needs its name", p.Kind)
			}
			return nil
		}
		return matchOrErr(strimziNameRE, "name", p.Name)
	}
}

// strimziApplyScope binds what a template's params name to the pipeline:
// the connector is one of its connectors, and every topic prefix a render
// uses is the pipeline's own (owners.Pipeline.TopicPrefix), so a KafkaUser's
// ACLs, the canary topic and the connector's topics never reach another
// pipeline's or an arbitrary prefix. The canary topic's schema is the schema
// of one of the pipeline's source tables (the room creates the canary table
// there), or public when none names a schema.
func strimziApplyScope(pl owners.Pipeline, raw json.RawMessage) error {
	var p strimziApplyParams
	if err := json.Unmarshal(paramsOrEmpty(raw), &p); err != nil {
		return fmt.Errorf("%w: scope: params must be a JSON object", tools.ErrBadInput)
	}
	params := paramsOrEmpty(p.Params)
	switch p.Template {
	case "connector":
		if err := ScopeConnector()(pl, params); err != nil {
			return err
		}
		return strimziConnectorScope(pl, params)
	case "canary-topic":
		var tp struct {
			TopicPrefix string `json:"topic_prefix"`
			Schema      string `json:"schema"`
		}
		if err := json.Unmarshal(params, &tp); err != nil {
			return fmt.Errorf("%w: scope: params must be the template's params object", tools.ErrBadInput)
		}
		if err := strimziPrefixScope(pl, tp.TopicPrefix); err != nil {
			return err
		}
		if !slices.Contains(strimziCanarySchemas(pl), tp.Schema) {
			return fmt.Errorf("%w: schema %q is not the schema of a source table of pipeline %q", tools.ErrOutOfScope, clipText(tp.Schema), clipText(pl.ID))
		}
	case "connect-user", "read-user":
		var up struct {
			TopicPrefix string `json:"topic_prefix"`
		}
		if err := json.Unmarshal(params, &up); err != nil {
			return fmt.Errorf("%w: scope: params must be the template's params object", tools.ErrBadInput)
		}
		return strimziPrefixScope(pl, up.TopicPrefix)
	}
	return nil
}

// strimziPrefixScope refuses a topic prefix other than the pipeline's.
func strimziPrefixScope(pl owners.Pipeline, prefix string) error {
	if pl.TopicPrefix == "" {
		return fmt.Errorf("%w: pipeline %q has no topic_prefix", tools.ErrOutOfScope, clipText(pl.ID))
	}
	if prefix != pl.TopicPrefix {
		return fmt.Errorf("%w: topic_prefix %q is not pipeline %q's (%s)", tools.ErrOutOfScope, clipText(prefix), clipText(pl.ID), clipText(pl.TopicPrefix))
	}
	return nil
}

// strimziCanarySchemas are the schemas the canary table may be in: those of
// the pipeline's source tables, or public when none names one.
func strimziCanarySchemas(pl owners.Pipeline) []string {
	var out []string
	for _, t := range pl.SourceTables {
		if i := strings.LastIndex(t, "."); i > 0 && !slices.Contains(out, t[:i]) {
			out = append(out, t[:i])
		}
	}
	if len(out) == 0 {
		out = []string{"public"}
	}
	return out
}

// strimziConnectorScope binds a connector config's topics to the pipeline:
// a source's topic.prefix is the pipeline's (and required), and a JDBC sink's
// topics each carry it (topics.regex is refused by the runner's allow-list).
func strimziConnectorScope(pl owners.Pipeline, params json.RawMessage) error {
	var cp struct {
		Plugin string            `json:"plugin"`
		Config map[string]string `json:"config"`
	}
	if err := json.Unmarshal(params, &cp); err != nil {
		return fmt.Errorf("%w: scope: params must be the template's params object", tools.ErrBadInput)
	}
	if cp.Plugin == "jdbc" {
		topics := strings.Split(cp.Config["topics"], ",")
		for _, t := range topics {
			t = strings.TrimSpace(t)
			if pl.TopicPrefix == "" || !strings.HasPrefix(t, pl.TopicPrefix+".") {
				return fmt.Errorf("%w: sink topic %q is not a topic of pipeline %q", tools.ErrOutOfScope, clipText(t), clipText(pl.ID))
			}
		}
		return nil
	}
	return strimziPrefixScope(pl, cp.Config["topic.prefix"])
}

// strimziDeleteScope: a KafkaConnector deleted is one of the pipeline's
// connectors; the canary topic is the pipeline's by its chart name.
func strimziDeleteScope(pl owners.Pipeline, raw json.RawMessage) error {
	var p strimziRollbackParams
	if err := json.Unmarshal(paramsOrEmpty(raw), &p); err != nil {
		return fmt.Errorf("%w: scope: params must be a JSON object", tools.ErrBadInput)
	}
	if p.Kind != "KafkaConnector" {
		return nil
	}
	return ScopeField("name", func(p owners.Pipeline) []string { return p.Connectors })(pl, raw)
}

// ---- managed mirrors ----

// The client packages' shape rules (mskconnect, confluentcloud, aiven
// validate.go and client.go). The account and region an ARN must be in are
// the runner's check: they come from the room's target, which this table
// does not hold.
var (
	mskNameRE          = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9_.-]{0,127}$`)
	mskResourceARNRE   = regexp.MustCompile(`^arn:aws[a-z-]{0,12}:kafkaconnect:[a-z]{2}(-[a-z]+)+-[0-9]:[0-9]{12}:(connector|custom-plugin|worker-configuration)/[A-Za-z0-9_.-]{1,128}/[A-Za-z0-9-]{1,64}$`)
	mskClusterARNRE    = regexp.MustCompile(`^arn:aws[a-z-]{0,12}:kafka:[a-z]{2}(-[a-z]+)+-[0-9]:[0-9]{12}:cluster/[A-Za-z0-9-]{1,64}/[A-Za-z0-9-]{1,64}$`)
	mskRoleARNRE       = regexp.MustCompile(`^arn:aws[a-z-]{0,12}:iam::[0-9]{12}:role/([A-Za-z0-9+=,.@_-]{1,64}/){0,4}(?i:piperoom)[A-Za-z0-9+=,.@_-]{0,56}$`) // the role's name starts with piperoom (any case), the runner's managed.RolePrefix
	mskBucketRE        = regexp.MustCompile(`^[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]$`)
	mskObjectKeyRE     = regexp.MustCompile(`^([A-Za-z0-9_.-]{1,100}/){0,8}[A-Za-z0-9_.-]{1,128}\.(zip|jar)$`)
	mskObjectVersionRE = regexp.MustCompile(`^[A-Za-z0-9._-]{1,256}$`)
	mskSubnetRE        = regexp.MustCompile(`^subnet-[0-9a-f]{8,17}$`)
	mskGroupRE         = regexp.MustCompile(`^sg-[0-9a-f]{8,17}$`)
	mskLogGroupRE      = regexp.MustCompile(`^[A-Za-z0-9_/.#-]{1,512}$`)
	mskConnectVerRE    = regexp.MustCompile(`^[0-9]+\.[0-9]+(\.[0-9x]+)?$`)
	mskWorkerKeyRE     = regexp.MustCompile(`^[a-z][a-z0-9.]{0,63}$`)

	ccNameRE      = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9_.-]{0,63}$`)
	ccPluginRE    = regexp.MustCompile(`^[A-Za-z][A-Za-z0-9_]{0,63}$`)
	ccPrincipalRE = regexp.MustCompile(`^User:sa-[a-z0-9]{3,16}$`)
	ccResourceRE  = regexp.MustCompile(`^[A-Za-z0-9._-]{1,200}$`)

	aivenServiceRE = regexp.MustCompile(`^[a-z][a-z0-9-]{0,63}$`)
	aivenNameRE    = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9_.-]{0,63}$`)

	// managedSecretRefRE and managedConfigKeyRE mirror the managed driver's
	// secretRef and configKey.
	managedSecretRefRE = regexp.MustCompile(`^\$\{secret:[a-zA-Z_][a-zA-Z0-9_]*\}$`)
	managedConfigKeyRE = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9_.-]{0,127}$`)
)

// maxManagedConfigValue mirrors the managed driver's cap on one config value.
const maxManagedConfigValue = 4096

// mskString is the managed driver's unconstrained string schema; its rules
// are in Validate (and CheckParams here), not the schema.
func mskString() map[string]any { return map[string]any{"type": "string"} }

func stringMapSchema() map[string]any {
	return map[string]any{"type": "object", "additionalProperties": map[string]any{"type": "string"}}
}

func stringListSchema() map[string]any {
	return map[string]any{"type": "array", "items": mskString(), "minItems": 1, "maxItems": 16}
}

// scopeConnectorName: the connector a managed op creates or deletes is one
// of the pipeline's connectors.
var scopeConnectorName = ScopeField("name", func(p owners.Pipeline) []string { return p.Connectors })

// isCloudSetupSecretName mirrors runner.IsCloudSetupSecret (the server does
// not link the runner): ccloud_*, aws_* and aiven_token, case-insensitive
// (docs/design/setup-managed.md §2).
func isCloudSetupSecretName(name string) bool {
	n := strings.ToLower(name)
	return strings.HasPrefix(n, "ccloud_") || strings.HasPrefix(n, "aws_") || n == "aiven_token"
}

// checkManagedConfig mirrors the managed driver's checkConnectorConfig: keys
// by pattern, a value under a secret-looking key a whole ${secret:}
// reference, no other value carrying one, no line break.
func checkManagedConfig(cfg map[string]string) error {
	if len(cfg) == 0 || len(cfg) > 256 {
		return fmt.Errorf("config has %d keys, want 1 to 256", len(cfg))
	}
	for k, v := range cfg {
		if !managedConfigKeyRE.MatchString(k) {
			return fmt.Errorf("config key %q", clipText(k))
		}
		if len(v) > maxManagedConfigValue || strings.ContainsAny(v, "\r\n\x00") {
			return fmt.Errorf("config value of %s is too long or has a line break", k)
		}
		if actions.LooksSecret(k) {
			if !managedSecretRefRE.MatchString(v) {
				return fmt.Errorf("%s must be a ${secret:name} reference", k)
			}
			if name := v[len("${secret:") : len(v)-1]; isCloudSetupSecretName(name) {
				// The runner refuses it too (runner.Store.Materialise); the
				// server does not propose a step that would copy a cloud
				// setup key into a connector config.
				return fmt.Errorf("%s references %s, a cloud setup credential: those stay on the runner and are never a connector config value", k, name)
			}
			continue
		}
		if strings.Contains(v, "${secret:") {
			return fmt.Errorf("%s is not a secret-named key and cannot take a ${secret:} reference", k)
		}
	}
	return nil
}

func checkMSKObject(bucket, key, version string) error {
	if err := matchOrErr(mskBucketRE, "bucket", bucket); err != nil {
		return err
	}
	for _, seg := range strings.Split(key, "/") {
		if seg == "." || seg == ".." {
			return fmt.Errorf("key %q has a dot segment", clipText(key))
		}
	}
	if err := matchOrErr(mskObjectKeyRE, "key", key); err != nil {
		return err
	}
	if version != "" {
		return matchOrErr(mskObjectVersionRE, "version", version)
	}
	return nil
}

func checkMSKCreatePlugin(raw json.RawMessage) error {
	var p struct {
		Name         string `json:"name"`
		Bucket       string `json:"bucket"`
		Key          string `json:"key"`
		Version      string `json:"version,omitempty"`
		ContentType  string `json:"content_type"`
		ExpectedSize int64  `json:"expected_size,omitempty"`
	}
	if err := decodeStrict(raw, &p); err != nil {
		return err
	}
	if err := matchOrErr(mskNameRE, "name", p.Name); err != nil {
		return err
	}
	if p.ContentType != "ZIP" && p.ContentType != "JAR" {
		return fmt.Errorf("content_type %q, want ZIP or JAR", clipText(p.ContentType))
	}
	if p.Version == "" {
		return errors.New("version is required: the archive's S3 version id, which pins the object the runner checks to the object MSK builds")
	}
	if p.ExpectedSize <= 0 {
		return errors.New("expected_size is required: the archive's size in bytes")
	}
	return checkMSKObject(p.Bucket, p.Key, p.Version)
}

// checkMSKWorker mirrors the worker key shape; the key allow-list and the
// value rules are the runner's (RenderWorkerProperties).
func checkMSKWorker(raw json.RawMessage) error {
	var p struct {
		Name    string            `json:"name"`
		Entries map[string]string `json:"entries"`
	}
	if err := decodeStrict(raw, &p); err != nil {
		return err
	}
	if err := matchOrErr(mskNameRE, "name", p.Name); err != nil {
		return err
	}
	if len(p.Entries) == 0 {
		return errors.New("no worker properties")
	}
	for k := range p.Entries {
		if !mskWorkerKeyRE.MatchString(k) {
			return fmt.Errorf("worker key %q", clipText(k))
		}
	}
	return nil
}

func checkMSKCreateConnector(raw json.RawMessage) error {
	var p struct {
		Name                 string            `json:"name"`
		KafkaConnectVersion  string            `json:"kafka_connect_version"`
		ClusterArn           string            `json:"cluster_arn,omitempty"`
		Subnets              []string          `json:"subnets"`
		SecurityGroups       []string          `json:"security_groups"`
		PluginArn            string            `json:"plugin_arn"`
		PluginRevision       int64             `json:"plugin_revision,omitempty"`
		WorkerConfigArn      string            `json:"worker_config_arn,omitempty"`
		WorkerConfigRevision int64             `json:"worker_config_revision,omitempty"`
		ServiceRoleArn       string            `json:"service_role_arn"`
		LogGroup             string            `json:"log_group,omitempty"`
		Config               map[string]string `json:"config"`
	}
	if err := decodeStrict(raw, &p); err != nil {
		return err
	}
	if err := matchOrErr(mskNameRE, "name", p.Name); err != nil {
		return err
	}
	if err := matchOrErr(mskConnectVerRE, "kafka_connect_version", p.KafkaConnectVersion); err != nil {
		return err
	}
	// An empty cluster_arn means the target's cluster; whether the target
	// names one is the runner's check.
	if p.ClusterArn != "" {
		if err := matchOrErr(mskClusterARNRE, "cluster_arn", p.ClusterArn); err != nil {
			return err
		}
	}
	if len(p.Subnets) == 0 || len(p.Subnets) > 16 || len(p.SecurityGroups) == 0 || len(p.SecurityGroups) > 16 {
		return errors.New("1 to 16 subnets and 1 to 16 security groups")
	}
	for _, s := range p.Subnets {
		if err := matchOrErr(mskSubnetRE, "subnet", s); err != nil {
			return err
		}
	}
	for _, g := range p.SecurityGroups {
		if err := matchOrErr(mskGroupRE, "security group", g); err != nil {
			return err
		}
	}
	if err := matchOrErr(mskResourceARNRE, "plugin_arn", p.PluginArn); err != nil {
		return err
	}
	if p.WorkerConfigArn != "" {
		if err := matchOrErr(mskResourceARNRE, "worker_config_arn", p.WorkerConfigArn); err != nil {
			return err
		}
	}
	if err := matchOrErr(mskRoleARNRE, "service_role_arn", p.ServiceRoleArn); err != nil {
		return err
	}
	if p.LogGroup != "" {
		if err := matchOrErr(mskLogGroupRE, "log_group", p.LogGroup); err != nil {
			return err
		}
	}
	return checkManagedConfig(p.Config)
}

func checkCCloudCreate(raw json.RawMessage) error {
	var p struct {
		Name   string            `json:"name"`
		Plugin string            `json:"plugin"`
		Config map[string]string `json:"config"`
	}
	if err := decodeStrict(raw, &p); err != nil {
		return err
	}
	if err := matchOrErr(ccNameRE, "name", p.Name); err != nil {
		return err
	}
	if err := matchOrErr(ccPluginRE, "plugin", p.Plugin); err != nil {
		return err
	}
	if v, ok := p.Config["connector.class"]; ok && v != p.Plugin {
		return fmt.Errorf("config connector.class %q is not the plugin %q", clipText(v), p.Plugin)
	}
	if v, ok := p.Config["name"]; ok && v != p.Name {
		return fmt.Errorf("config name %q is not the connector name %q", clipText(v), p.Name)
	}
	return checkManagedConfig(p.Config)
}

// minCCloudACLPrefix mirrors the managed driver's minACLPrefix.
const minCCloudACLPrefix = 3

// ccManagedGroupRE is a Confluent Cloud managed sink's consumer group.
var ccManagedGroupRE = regexp.MustCompile(`^connect-lcc-[a-z0-9]{1,32}$`)

// scopeCCloudACL binds ccloud_acl_create to the pipeline: topic_prefix is
// the pipeline's own topic_prefix (a pipeline without one gets no ACL), and
// the watcher's group is the consumer group of one of the pipeline's
// connectors (connect-<connector>, a self-managed sink's group naming), one
// of its declared consumer_groups, or, with connector naming one of its
// connectors, a managed sink's connect-lcc-<id>. The principal is not the server's to bind:
// the server holds no record of the person's service accounts, so the
// runner refuses any principal other than the one the person entered on its
// terminal for the template (managed.ConfluentOptions.Principal).
func scopeCCloudACL(p owners.Pipeline, raw json.RawMessage) error {
	var q struct {
		Template    string `json:"template"`
		TopicPrefix string `json:"topic_prefix"`
		Group       string `json:"group"`
		Connector   string `json:"connector"`
	}
	if err := json.Unmarshal(paramsOrEmpty(raw), &q); err != nil {
		return fmt.Errorf("%w: scope: params must be a JSON object", tools.ErrBadInput)
	}
	if p.TopicPrefix == "" || q.TopicPrefix != p.TopicPrefix {
		return fmt.Errorf("%w: topic_prefix %q is not pipeline %q's topic_prefix", tools.ErrOutOfScope, clipText(q.TopicPrefix), clipText(p.ID))
	}
	if q.Template != "watcher" {
		return nil
	}
	if q.Connector != "" {
		// A managed sink's group is connect-lcc-<its id>. The server holds
		// no record of Confluent's ids; it binds the connector to the
		// pipeline and the runner reads that connector's id and refuses a
		// group that is not connect-<id> (managed.ACLParams).
		if !slices.Contains(p.Connectors, q.Connector) {
			return fmt.Errorf("%w: connector %q is not a connector of pipeline %q", tools.ErrOutOfScope, clipText(q.Connector), clipText(p.ID))
		}
		if !ccManagedGroupRE.MatchString(q.Group) {
			return fmt.Errorf("%w: group %q is not a managed connector's connect-lcc-<id> group", tools.ErrOutOfScope, clipText(q.Group))
		}
		return nil
	}
	groups := append([]string{}, p.ConsumerGroups...)
	for _, c := range p.Connectors {
		groups = append(groups, "connect-"+c)
	}
	if !slices.Contains(groups, q.Group) {
		return fmt.Errorf("%w: group %q is not a consumer group of pipeline %q", tools.ErrOutOfScope, clipText(q.Group), clipText(p.ID))
	}
	return nil
}

func checkCCloudACL(raw json.RawMessage) error {
	var p struct {
		Template    string `json:"template"`
		Principal   string `json:"principal"`
		TopicPrefix string `json:"topic_prefix"`
		Group       string `json:"group,omitempty"`
		Connector   string `json:"connector,omitempty"`
	}
	if err := decodeStrict(raw, &p); err != nil {
		return err
	}
	if p.Connector != "" {
		if p.Template != "watcher" {
			return errors.New("only the watcher template takes a connector")
		}
		if err := matchOrErr(ccNameRE, "connector", p.Connector); err != nil {
			return err
		}
	}
	if err := matchOrErr(ccPrincipalRE, "principal", p.Principal); err != nil {
		return err
	}
	if err := matchOrErr(ccResourceRE, "topic_prefix", p.TopicPrefix); err != nil {
		return err
	}
	if len(p.TopicPrefix) < minCCloudACLPrefix || strings.HasPrefix(p.TopicPrefix, "_") {
		return fmt.Errorf("topic_prefix %q must be at least %d characters and must not start with _", clipText(p.TopicPrefix), minCCloudACLPrefix)
	}
	if strings.HasSuffix(p.TopicPrefix, ".") {
		// The runner grants on "<topic_prefix>.", the separator included.
		return fmt.Errorf("topic_prefix %q is the pipeline's topic_prefix, without the trailing .", clipText(p.TopicPrefix))
	}
	switch p.Template {
	case "watcher":
		return matchOrErr(ccResourceRE, "group (the watcher template needs the connector's group)", p.Group)
	case "connector":
		if p.Group != "" {
			return errors.New("the connector template takes no group")
		}
		return nil
	}
	return fmt.Errorf("template %q, want watcher or connector", clipText(p.Template))
}

func checkAivenCreate(raw json.RawMessage) error {
	var p struct {
		Name   string            `json:"name"`
		Config map[string]string `json:"config"`
	}
	if err := decodeStrict(raw, &p); err != nil {
		return err
	}
	if err := matchOrErr(aivenNameRE, "name", p.Name); err != nil {
		return err
	}
	if v, ok := p.Config["name"]; ok && v != p.Name {
		return fmt.Errorf("config name %q is not the connector name %q", clipText(v), p.Name)
	}
	if p.Config["connector.class"] == "" {
		return errors.New("config needs connector.class")
	}
	return checkManagedConfig(p.Config)
}

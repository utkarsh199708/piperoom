package setupactions_test

// Round trip of the Strimzi and managed writes: the server's specs
// (specs_targets.go) mirror the drivers' schemas and rules, which the package
// must not import (the drivers reach the agent through ops, connprobe and
// wiring, and the managed one would link the runner's cloud clients into the
// server). This external test registers the real drivers and checks, op by
// op, that the floors and schemas are the runner's and that the spec and the
// driver accept and refuse the same params.

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"piperoom/internal/events"
	"piperoom/internal/owners"
	setupactions "piperoom/internal/setup/actions"
	"piperoom/internal/setup/ops"
	"piperoom/internal/setup/protocol"
	"piperoom/internal/setup/targets/managed"
	"piperoom/internal/setup/targets/strimzi"
	"piperoom/internal/tools"
)

// The managed drivers' Validate does no I/O, so a client whose methods are
// never called is enough to register them.
type (
	mskStub       struct{ managed.MSKAPI }
	confluentStub struct{ managed.ConfluentAPI }
	aivenStub     struct{ managed.AivenAPI }
)

var testTargets = []events.SetupTarget{
	{Kind: "kubernetes", Address: "kind-test/data", Class: protocol.ClassShared},
	{Kind: "kafka", Address: "strimzi://prod-kafka", Class: protocol.ClassShared},
	{Kind: managed.KindAWS, Address: "123456789012/eu-west-1", Class: protocol.ClassShared},
	{Kind: managed.KindConfluent, Address: "env-abc123/lkc-abc123", Class: protocol.ClassShared},
	{Kind: managed.KindAiven, Address: "orders-proj/orders-connect", Class: protocol.ClassShared},
}

// targetCatalogue is a runner catalogue with the Strimzi driver and the three
// managed drivers registered (a runner registers the one its room names; the
// op names do not collide, so one registry holds all four here).
func targetCatalogue(t *testing.T) *ops.Registry {
	t.Helper()
	reg, err := ops.New(ops.Config{ProjectDir: t.TempDir(), Targets: testTargets})
	if err != nil {
		t.Fatalf("ops.New: %v", err)
	}
	t.Cleanup(func() { _ = reg.Close() })
	api, err := strimzi.NewAPI(strimzi.APIConfig{BaseURL: "https://k8s.example:443", Token: func() (string, error) { return "t", nil }})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := strimzi.Register(reg, strimzi.Options{Pipeline: "orders", Connectors: []string{"orders-src"}, API: api}); err != nil {
		t.Fatalf("strimzi.Register: %v", err)
	}
	if _, err := managed.RegisterMSK(reg, managed.MSKOptions{Client: mskStub{}, Target: testTargets[2], RoomID: "r_setup"}); err != nil {
		t.Fatalf("RegisterMSK: %v", err)
	}
	if _, err := managed.RegisterConfluent(reg, managed.ConfluentOptions{Client: confluentStub{}, Target: testTargets[3]}); err != nil {
		t.Fatalf("RegisterConfluent: %v", err)
	}
	if _, err := managed.RegisterAiven(reg, managed.AivenOptions{Client: aivenStub{}, Target: testTargets[4]}); err != nil {
		t.Fatalf("RegisterAiven: %v", err)
	}
	return reg
}

// targetCases are, per write op of the two drivers, params both sides accept
// and params both refuse. Every T1+ op the drivers register must be here.
var targetCases = map[string]struct{ good, bad []string }{
	"k8s_apply": {
		good: []string{`{"kind":"KafkaTopic","template":"canary-topic","params":{"topic_prefix":"pg","schema":"public","table":"piperoom_canary"}}`},
		bad:  []string{`{"kind":"KafkaUser","template":"canary-topic","params":{}}`, `{"kind":"Role","template":"connect","params":{}}`, `{"kind":"KafkaTopic","template":"any-topic","params":{}}`},
	},
	"k8s_secret_apply": {
		good: []string{`{"keys":{"password":"${secret:source_password}"}}`},
		bad:  []string{`{"keys":{"password":"hunter2"}}`, `{"keys":{}}`, `{"keys":{"bad key":"${secret:x}"}}`},
	},
	"k8s_delete": {
		good: []string{`{"kind":"KafkaConnector","name":"orders-src"}`, `{"kind":"KafkaTopic","name":"orders-canary"}`},
		bad:  []string{`{"kind":"Kafka","name":"prod-kafka"}`, `{"kind":"KafkaConnector"}`, `{"kind":"KafkaTopic","name":"Orders"}`},
	},
	"k8s_unapply": {
		good: []string{`{"kind":"KafkaUser","name":"orders-connect"}`, `{"kind":"Secret"}`, `{"kind":"KafkaConnect"}`, `{"kind":"ConfigMap","name":"orders-connect-logging"}`},
		bad:  []string{`{"kind":"KafkaUser"}`, `{"kind":"KafkaTopic"}`, `{"kind":"Secret","data":{}}`},
	},
	"msk_create_plugin": {
		good: []string{`{"name":"debezium-pg","bucket":"orders-plugins","key":"debezium/pg-3.2.0.zip","content_type":"ZIP","expected_size":1024}`},
		bad:  []string{`{"name":"debezium-pg","bucket":"orders-plugins","key":"debezium/pg.zip","content_type":"TAR"}`, `{"name":"debezium-pg","bucket":"orders-plugins","key":"../pg.zip","content_type":"ZIP"}`},
	},
	"msk_create_worker_config": {
		good: []string{`{"name":"piperoom-worker","entries":{"key.converter.schemas.enable":"false"}}`},
		bad:  []string{`{"name":"piperoom-worker","entries":{}}`, `{"name":"piperoom worker","entries":{"key.converter.schemas.enable":"false"}}`, `{"name":"w","entries":{"Bad-Key":"x"}}`},
	},
	"msk_create_connector": {
		good: []string{`{"name":"orders-src","kafka_connect_version":"3.7.x","cluster_arn":"arn:aws:kafka:eu-west-1:123456789012:cluster/orders/abcd-1234",` +
			`"subnets":["subnet-0123456789abcdef0"],"security_groups":["sg-0123456789abcdef0"],` +
			`"plugin_arn":"arn:aws:kafkaconnect:eu-west-1:123456789012:custom-plugin/debezium-pg/abcd-1234",` +
			`"service_role_arn":"arn:aws:iam::123456789012:role/msk-connect",` +
			`"config":{"connector.class":"io.debezium.connector.postgresql.PostgresConnector","database.password":"${secret:pg_password}"}}`},
		bad: []string{`{"name":"orders-src","kafka_connect_version":"3.7.x","subnets":["subnet-xyz"],"security_groups":["sg-0123456789abcdef0"],` +
			`"plugin_arn":"arn:aws:kafkaconnect:eu-west-1:123456789012:custom-plugin/debezium-pg/abcd-1234",` +
			`"service_role_arn":"arn:aws:iam::123456789012:role/msk-connect","config":{"a":"b"}}`,
			`{"name":"orders-src","kafka_connect_version":"3.7.x","cluster_arn":"arn:aws:kafka:eu-west-1:123456789012:cluster/orders/abcd-1234",` +
				`"subnets":["subnet-0123456789abcdef0"],"security_groups":["sg-0123456789abcdef0"],` +
				`"plugin_arn":"arn:aws:kafkaconnect:eu-west-1:123456789012:custom-plugin/debezium-pg/abcd-1234",` +
				`"service_role_arn":"arn:aws:iam::123456789012:role/msk-connect","config":{"database.password":"hunter2"}}`},
	},
	"msk_delete_connector": {
		good: []string{`{"name":"orders-src"}`},
		bad:  []string{`{"name":"-orders"}`, `{}`},
	},
	"ccloud_create_connector": {
		good: []string{`{"name":"orders-src","plugin":"PostgresCdcSourceV2","config":{"database.hostname":"db.example","database.password":"${secret:pg_password}"}}`},
		bad: []string{`{"name":"orders-src","plugin":"PostgresCdcSourceV2","config":{"database.password":"hunter2"}}`, `{"name":"orders-src","plugin":"PostgresCdcSourceV2","config":{"connector.class":"Other"}}`,
			// A cloud setup credential is never a connector config value (H2).
			`{"name":"orders-src","plugin":"PostgresCdcSourceV2","config":{"kafka.api.secret":"${secret:ccloud_api_secret}"}}`,
			`{"name":"orders-src","plugin":"PostgresCdcSourceV2","config":{"database.password":"${secret:AWS_SECRET_ACCESS_KEY}"}}`},
	},
	"ccloud_acl_create": {
		good: []string{`{"template":"watcher","principal":"User:sa-abc123","topic_prefix":"pg.","group":"connect-orders-src"}`, `{"template":"connector","principal":"User:sa-abc123","topic_prefix":"pg."}`},
		bad: []string{`{"template":"connector","principal":"User:sa-abc123","topic_prefix":"pg.","group":"x"}`, `{"template":"watcher","principal":"User:sa-abc123","topic_prefix":"pg."}`, `{"template":"admin","principal":"User:sa-abc123","topic_prefix":"pg."}`,
			// A one- or two-character prefix covers most of a cluster; "_"
			// starts its internal topics (M2).
			`{"template":"connector","principal":"User:sa-abc123","topic_prefix":"o"}`,
			`{"template":"connector","principal":"User:sa-abc123","topic_prefix":"pg"}`,
			`{"template":"connector","principal":"User:sa-abc123","topic_prefix":"_confluent"}`},
	},
	"aiven_create_connector": {
		good: []string{`{"name":"orders-src","config":{"connector.class":"io.debezium.connector.postgresql.PostgresConnector"}}`},
		bad: []string{`{"name":"orders-src","config":{"database.hostname":"db"}}`, `{"name":"orders-src","config":{"connector.class":"x","name":"other"}}`,
			`{"name":"orders-src","config":{"connector.class":"x","aiven.password":"${secret:aiven_token}"}}`},
	},
	"aiven_integration_create": {
		good: []string{`{"source_service":"kafka-main"}`},
		bad:  []string{`{"source_service":"Kafka_Main"}`, `{}`},
	},
}

func specsByName() map[string]setupactions.Spec {
	out := map[string]setupactions.Spec{}
	for _, s := range setupactions.EnvironmentSpecs() {
		out[s.Name] = s
	}
	return out
}

// TestTargetSpecsMatchTheRunnerOps: every T1+ op of the two drivers has a
// spec, with the op's floor, its schema (less the card-only field) and a
// preview op that is the runner's T0 op or none at all.
func TestTargetSpecsMatchTheRunnerOps(t *testing.T) {
	reg := targetCatalogue(t)
	specs := specsByName()
	base, err := ops.New(ops.Config{ProjectDir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = base.Close() })
	for _, name := range reg.Names() {
		floor, _ := reg.Floor(name)
		if _, core := base.Get(name); core || floor == protocol.TierT0 {
			// The core catalogue's writes are checked by specs_test.go.
			continue
		}
		if _, ok := targetCases[name]; !ok {
			t.Errorf("%s (%s) has no round-trip case", name, floor)
		}
		if _, ok := specs[name]; !ok {
			t.Errorf("%s (%s): no server action spec", name, floor)
		}
	}
	for name := range targetCases {
		spec, ok := specs[name]
		if !ok {
			t.Errorf("%s: not in EnvironmentSpecs", name)
			continue
		}
		op, ok := reg.Get(name)
		if !ok {
			t.Errorf("%s: not in the runner catalogue", name)
			continue
		}
		if floor, _ := reg.Floor(name); floor != spec.Tier {
			t.Errorf("%s: spec tier %q, runner floor %q", name, spec.Tier, floor)
		}
		if spec.Layer != op.Layer() {
			t.Errorf("%s: spec layer %q, runner layer %q", name, spec.Layer, op.Layer())
		}
		want := op.ParamsSchema()
		if props, ok := want["properties"].(map[string]any); ok {
			// The D11 confirmation is the approval card's, never the model's.
			trimmed := map[string]any{}
			for k, v := range props {
				if k != "admission_policy_confirmed" {
					trimmed[k] = v
				}
			}
			cp := map[string]any{}
			for k, v := range want {
				cp[k] = v
			}
			cp["properties"] = trimmed
			want = cp
		}
		gotJSON, _ := json.Marshal(spec.Schema)
		wantJSON, _ := json.Marshal(want)
		if string(gotJSON) != string(wantJSON) {
			t.Errorf("%s: schema differs from the runner op's\n spec   %s\n runner %s", name, gotJSON, wantJSON)
		}
		preview := setupactions.PreviewOpName(name)
		_, registered := reg.Get(preview)
		switch {
		case spec.PreviewOp == "" && registered:
			t.Errorf("%s: the runner registers %s; set PreviewOp", name, preview)
		case spec.PreviewOp != "":
			if f, ok := reg.Floor(spec.PreviewOp); !ok || f != protocol.TierT0 {
				t.Errorf("%s: preview op %q floor %q (registered %v), want T0", name, spec.PreviewOp, f, ok)
			}
		}
	}
}

// TestTargetSpecParamsRoundTrip: the spec and the runner op accept the same
// good params and refuse the same bad ones.
func TestTargetSpecParamsRoundTrip(t *testing.T) {
	reg := targetCatalogue(t)
	env := ops.Env{Targets: testTargets}
	specs := specsByName()
	for name, tc := range targetCases {
		spec, ok := specs[name]
		op, ok2 := reg.Get(name)
		if !ok || !ok2 {
			t.Errorf("%s: spec %v, op %v", name, ok, ok2)
			continue
		}
		a, err := setupactions.New(spec, setupactions.Deps{})
		if err != nil {
			t.Fatalf("New(%s): %v", name, err)
		}
		for _, p := range tc.good {
			if err := a.Validate(context.Background(), json.RawMessage(p)); err != nil {
				t.Errorf("%s: spec refuses %s: %v", name, p, err)
			}
			if err := op.Validate(json.RawMessage(p), env); err != nil {
				t.Errorf("%s: runner refuses %s: %v", name, p, err)
			}
		}
		for _, p := range tc.bad {
			if err := a.Validate(context.Background(), json.RawMessage(p)); err == nil {
				t.Errorf("%s: spec accepts %s", name, p)
			}
			if err := op.Validate(json.RawMessage(p), env); err == nil {
				t.Errorf("%s: runner accepts %s", name, p)
			}
		}
	}
}

// TestStrimziCardOnlyFieldIsNotTheModels: the runner accepts the D11 card
// confirmation in params; the server refuses a proposal that carries it, so
// model text cannot stand in for the person's confirmation.
func TestStrimziCardOnlyFieldIsNotTheModels(t *testing.T) {
	reg := targetCatalogue(t)
	env := ops.Env{Targets: testTargets}
	specs := specsByName()
	for name, p := range map[string]string{
		"k8s_apply":   `{"kind":"KafkaTopic","template":"canary-topic","params":{"topic_prefix":"pg","schema":"public","table":"t"},"admission_policy_confirmed":true}`,
		"k8s_delete":  `{"kind":"KafkaConnector","name":"orders-src","admission_policy_confirmed":true}`,
		"k8s_unapply": `{"kind":"Secret","admission_policy_confirmed":true}`,
	} {
		op, _ := reg.Get(name)
		if err := op.Validate(json.RawMessage(p), env); err != nil {
			t.Errorf("%s: the runner refuses the card field: %v", name, err)
		}
		a, err := setupactions.New(specs[name], setupactions.Deps{})
		if err != nil {
			t.Fatal(err)
		}
		if err := a.Validate(context.Background(), json.RawMessage(p)); err == nil {
			t.Errorf("%s: the server accepts admission_policy_confirmed in a proposal", name)
		}
	}
}

// TestTargetSpecScopes: a connector a target write names is one of the
// pipeline's; the chart-fixed Strimzi objects need no scope.
func TestTargetSpecScopes(t *testing.T) {
	pl := owners.Pipeline{ID: "orders", TopicPrefix: "orders", Connectors: []string{"orders-src"}, ConsumerGroups: []string{"orders-sink"}}
	specs := specsByName()
	cases := []struct {
		op, params string
		inScope    bool
	}{
		{"k8s_apply", `{"kind":"KafkaConnector","template":"connector","params":{"connector":"orders-src","plugin":"postgres","config":{}}}`, true},
		{"k8s_apply", `{"kind":"KafkaConnector","template":"connector","params":{"connector":"payments-src","plugin":"postgres","config":{}}}`, false},
		{"k8s_apply", `{"kind":"KafkaTopic","template":"canary-topic","params":{"topic_prefix":"pg","schema":"public","table":"t"}}`, true},
		{"k8s_delete", `{"kind":"KafkaConnector","name":"orders-src"}`, true},
		{"k8s_delete", `{"kind":"KafkaConnector","name":"payments-src"}`, false},
		{"k8s_delete", `{"kind":"KafkaTopic","name":"orders-canary"}`, true},
		{"msk_delete_connector", `{"name":"payments-src"}`, false},
		{"ccloud_create_connector", `{"name":"orders-src","plugin":"PostgresCdcSourceV2","config":{"a":"b"}}`, true},
		{"aiven_create_connector", `{"name":"payments-src","config":{"connector.class":"x"}}`, false},
		// ccloud_acl_create grants on the pipeline's own prefix and groups only (M2).
		{"ccloud_acl_create", `{"template":"connector","principal":"User:sa-abc123","topic_prefix":"orders"}`, true},
		{"ccloud_acl_create", `{"template":"connector","principal":"User:sa-abc123","topic_prefix":"payments"}`, false},
		{"ccloud_acl_create", `{"template":"connector","principal":"User:sa-abc123","topic_prefix":"ord"}`, false},
		{"ccloud_acl_create", `{"template":"watcher","principal":"User:sa-abc123","topic_prefix":"orders","group":"connect-orders-src"}`, true},
		{"ccloud_acl_create", `{"template":"watcher","principal":"User:sa-abc123","topic_prefix":"orders","group":"orders-sink"}`, true},
		{"ccloud_acl_create", `{"template":"watcher","principal":"User:sa-abc123","topic_prefix":"orders","group":"connect-payments-src"}`, false},
	}
	for _, c := range cases {
		a, err := setupactions.New(specs[c.op], setupactions.Deps{})
		if err != nil {
			t.Fatal(err)
		}
		err = a.CheckScope(pl, json.RawMessage(c.params))
		if c.inScope && err != nil {
			t.Errorf("%s %s: %v", c.op, c.params, err)
		}
		if !c.inScope && !errors.Is(err, tools.ErrOutOfScope) {
			t.Errorf("%s %s: err = %v, want ErrOutOfScope", c.op, c.params, err)
		}
	}
}

// TestCCloudACLScopeNeedsATopicPrefix: a pipeline without a topic_prefix
// gets no ACL at all: there is no prefix to bind the rows to.
func TestCCloudACLScopeNeedsATopicPrefix(t *testing.T) {
	a, err := setupactions.New(specsByName()["ccloud_acl_create"], setupactions.Deps{})
	if err != nil {
		t.Fatal(err)
	}
	err = a.CheckScope(owners.Pipeline{ID: "orders"}, json.RawMessage(`{"template":"connector","principal":"User:sa-abc123","topic_prefix":"orders"}`))
	if !errors.Is(err, tools.ErrOutOfScope) {
		t.Fatalf("CheckScope = %v, want ErrOutOfScope", err)
	}
}

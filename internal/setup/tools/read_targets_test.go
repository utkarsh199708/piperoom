package setuptools_test

// Round trip of the Strimzi and managed read tools: every T0 op of the two
// drivers has a tool, the tool's schema is the op's, the params a tool
// forwards pass the driver's own Validate, and an input a tool refuses never
// reaches the runner. External, because the package itself must not import
// the drivers (see read_host_test.go).

import (
	"context"
	"encoding/json"
	"testing"

	"piperoom/internal/events"
	"piperoom/internal/setup/ops"
	"piperoom/internal/setup/protocol"
	"piperoom/internal/setup/targets/managed"
	"piperoom/internal/setup/targets/strimzi"
	setuptools "piperoom/internal/setup/tools"
	"piperoom/internal/tools"
)

// The managed drivers' Validate does no I/O: a client nothing calls is enough.
type (
	mskStub       struct{ managed.MSKAPI }
	confluentStub struct{ managed.ConfluentAPI }
	aivenStub     struct{ managed.AivenAPI }
)

var targetTargets = []events.SetupTarget{
	{Kind: "kubernetes", Address: "kind-test/data", Class: protocol.ClassShared},
	{Kind: "kafka", Address: "strimzi://prod-kafka", Class: protocol.ClassShared},
	{Kind: managed.KindAWS, Address: "123456789012/eu-west-1/arn:aws:kafka:eu-west-1:123456789012:cluster/orders/abcd-1234", Class: protocol.ClassShared},
	{Kind: managed.KindConfluent, Address: "env-abc123/lkc-abc123", Class: protocol.ClassShared},
	{Kind: managed.KindAiven, Address: "orders-proj/orders-connect", Class: protocol.ClassShared},
}

func targetsCatalogue(t *testing.T) *ops.Registry {
	t.Helper()
	reg, err := ops.New(ops.Config{ProjectDir: t.TempDir(), Targets: targetTargets})
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
	if _, err := managed.RegisterMSK(reg, managed.MSKOptions{Client: mskStub{}, Target: targetTargets[2], RoomID: "r_setup"}); err != nil {
		t.Fatalf("RegisterMSK: %v", err)
	}
	if _, err := managed.RegisterConfluent(reg, managed.ConfluentOptions{Client: confluentStub{}, Target: targetTargets[3]}); err != nil {
		t.Fatalf("RegisterConfluent: %v", err)
	}
	if _, err := managed.RegisterAiven(reg, managed.AivenOptions{Client: aivenStub{}, Target: targetTargets[4]}); err != nil {
		t.Fatalf("RegisterAiven: %v", err)
	}
	return reg
}

// targetReadCases: per read tool, inputs the tool forwards and the driver
// accepts, and inputs the tool refuses (and so does the driver).
var targetReadCases = map[string]struct{ good, bad []string }{
	// The ladder's rungs 1 and 2 read exactly this input (room setup_ladder).
	"k8s_get":              {good: []string{`{"kind":"KafkaConnector","name":"orders-src"}`, `{"kind":"Pod","name":"orders-connect-connect-0"}`}, bad: []string{`{"kind":"Secret","name":"orders-db-credentials"}`, `{"kind":"KafkaConnector","name":"Orders"}`}},
	"k8s_events":           {good: []string{`{"involved":"orders-connect"}`}, bad: []string{`{"involved":"a b"}`, `{}`}},
	"k8s_logs":             {good: []string{`{"pod":"orders-connect-connect-build"}`, `{"pod":"orders-connect-connect-0","container":"orders-connect-connect","tail":20}`}, bad: []string{`{"pod":"orders-connect-connect-0","tail":500}`, `{"pod":"Bad Pod"}`}},
	"strimzi_build_status": {good: []string{`{}`}, bad: []string{`{"name":"x"}`}},
	"preview_k8s_apply": {
		good: []string{`{"kind":"KafkaTopic","template":"canary-topic","params":{"topic_prefix":"pg","schema":"public","table":"piperoom_canary"}}`},
		bad:  []string{`{"kind":"KafkaUser","template":"canary-topic","params":{}}`, `{"kind":"Role","template":"connect","params":{}}`},
	},
	"msk_describe_cluster":    {good: []string{`{}`, `{"cluster_arn":"arn:aws:kafka:eu-west-1:123456789012:cluster/orders/abcd-1234"}`}, bad: []string{`{"cluster_arn":"orders"}`}},
	"msk_list_plugins":        {good: []string{`{}`, `{"name_prefix":"debezium"}`}, bad: []string{`{"name_prefix":"-x"}`}},
	"msk_describe_connector":  {good: []string{`{"name":"orders-src"}`}, bad: []string{`{"name":""}`, `{"name":"a b"}`}},
	"msk_vpc_check":           {good: []string{`{"subnets":["subnet-0123456789abcdef0"],"security_groups":["sg-0123456789abcdef0"],"database_ip":"10.0.1.5","database_port":5432}`}, bad: []string{`{"subnets":["subnet-0123456789abcdef0"],"security_groups":["sg-0123456789abcdef0"],"database_ip":"db.internal","database_port":5432}`}},
	"msk_role_check":          {good: []string{`{"role_arn":"arn:aws:iam::123456789012:role/msk-connect"}`}, bad: []string{`{"role_arn":"msk-connect"}`}},
	"msk_plugin_object_check": {good: []string{`{"bucket":"orders-plugins","key":"debezium/pg-3.2.0.zip"}`}, bad: []string{`{"bucket":"orders-plugins","key":"debezium/../pg.zip"}`, `{"bucket":"orders-plugins","key":"pg.tgz"}`}},
	"msk_wait":                {good: []string{`{"kind":"plugin","arn":"arn:aws:kafkaconnect:eu-west-1:123456789012:custom-plugin/debezium-pg/abcd-1234"}`}, bad: []string{`{"kind":"connector","arn":"arn:aws:kafkaconnect:eu-west-1:123456789012:custom-plugin/debezium-pg/abcd-1234"}`, `{"kind":"cluster","arn":"x"}`}},
	"ccloud_list_plugins":     {good: []string{`{}`}, bad: []string{`{"x":1}`}},
	"ccloud_connector_status": {good: []string{`{"name":"orders-src"}`}, bad: []string{`{"name":"-x"}`}},
	"ccloud_cluster":          {good: []string{`{}`}, bad: []string{`{"cluster":"lkc-x"}`}},
	"aiven_service_get":       {good: []string{`{}`, `{"service":"kafka-main"}`}, bad: []string{`{"service":"Kafka"}`}},
	"aiven_list_plugins":      {good: []string{`{}`}, bad: []string{`{"x":1}`}},
	"aiven_connector_status":  {good: []string{`{"name":"orders-src"}`}, bad: []string{`{"name":"a b"}`}},
}

// noReadTool are the drivers' T0 ops the room reaches another way:
// preview_k8s_secret_apply is the k8s_secret_apply action's preview.
var noReadTool = map[string]bool{"preview_k8s_secret_apply": true}

func TestTargetReadToolsMatchTheRunnerOps(t *testing.T) {
	cat := targetsCatalogue(t)
	base, err := ops.New(ops.Config{ProjectDir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = base.Close() })

	rn := &recordingRunner{}
	reg := tools.NewReadOnlyRegistry()
	if err := setuptools.Register(reg, setuptools.Deps{Runner: rn, RoomID: "r_setup", CallRoom: func(context.Context) string { return "r_setup" }}); err != nil {
		t.Fatalf("Register: %v", err)
	}
	for _, name := range cat.Names() {
		if _, core := base.Get(name); core {
			continue
		}
		if f, _ := cat.Floor(name); f != protocol.TierT0 || noReadTool[name] {
			continue
		}
		if _, ok := targetReadCases[name]; !ok {
			t.Errorf("%s: a T0 op of the drivers has no read tool case", name)
		}
	}
	env := ops.Env{Targets: targetTargets}
	for name, tc := range targetReadCases {
		tool, ok := reg.Get(name)
		if !ok {
			t.Errorf("%s: no read tool", name)
			continue
		}
		op, ok := cat.Get(name)
		if !ok {
			t.Errorf("%s: not in the runner catalogue", name)
			continue
		}
		if tool.Kind() != tools.KindRead {
			t.Errorf("%s: kind %q, want read", name, tool.Kind())
		}
		if f, _ := cat.Floor(name); f != protocol.TierT0 {
			t.Errorf("%s: runner floor %q, want T0", name, f)
		}
		want := op.ParamsSchema()
		cp := map[string]any{}
		for k, v := range want {
			cp[k] = v
		}
		if _, ok := cp["required"]; !ok {
			cp["required"] = []string{}
		}
		if props, ok := cp["properties"].(map[string]any); ok {
			// preview_k8s_apply shares k8s_apply's schema; the D11 card
			// confirmation means nothing to a preview and is not offered.
			trimmed := map[string]any{}
			for k, v := range props {
				if k != "admission_policy_confirmed" {
					trimmed[k] = v
				}
			}
			cp["properties"] = trimmed
		}
		got, _ := json.Marshal(tool.InputSchema())
		exp, _ := json.Marshal(cp)
		if string(got) != string(exp) {
			t.Errorf("%s: schema differs from the runner op's\n tool   %s\n runner %s", name, got, exp)
		}
		for _, in := range tc.good {
			res, err := tool.Run(context.Background(), json.RawMessage(in))
			if err != nil {
				t.Errorf("%s: Run(%s) = %v", name, in, err)
				continue
			}
			if d, _ := json.Marshal(res.Data); string(d) != `{"ok":true}` {
				t.Errorf("%s: Data = %s, want the runner's Data unchanged", name, res.Data)
			}
			req := rn.last(t)
			if req.Op != name || req.Tier != protocol.TierT0 {
				t.Errorf("%s: forwarded op %q at %q", name, req.Op, req.Tier)
			}
			if err := op.Validate(req.Params, env); err != nil {
				t.Errorf("%s: the driver refuses the forwarded params %s: %v", name, req.Params, err)
			}
		}
		for _, in := range tc.bad {
			rn.mu.Lock()
			before := len(rn.reqs)
			rn.mu.Unlock()
			if _, err := tool.Run(context.Background(), json.RawMessage(in)); err == nil {
				t.Errorf("%s: the tool accepts %s", name, in)
			}
			rn.mu.Lock()
			after := len(rn.reqs)
			rn.mu.Unlock()
			if after != before {
				t.Errorf("%s: a refused input reached the runner", name)
			}
			if err := op.Validate(json.RawMessage(in), env); err == nil {
				t.Errorf("%s: the driver accepts %s", name, in)
			}
		}
	}
}

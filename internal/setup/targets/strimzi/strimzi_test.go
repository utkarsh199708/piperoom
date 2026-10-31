package strimzi

import (
	"context"
	"encoding/json"
	"errors"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"piperoom/internal/setup/ops"
	"piperoom/internal/setup/protocol"
)

func run(t *testing.T, h *harness, op string, params any) (ops.Result, error) {
	t.Helper()
	o, ok := h.reg.Get(op)
	if !ok {
		t.Fatalf("%s is not registered", op)
	}
	raw, _ := json.Marshal(params)
	if err := o.Validate(raw, h.env); err != nil {
		return ops.Result{}, err
	}
	return o.Execute(context.Background(), raw, h.env)
}

func TestRegisterAndFloors(t *testing.T) {
	h := newHarness(t, "", nil)
	want := map[string]string{
		"k8s_get": protocol.TierT0, "k8s_events": protocol.TierT0, "k8s_logs": protocol.TierT0,
		"strimzi_build_status": protocol.TierT0, "preview_k8s_apply": protocol.TierT0,
		"k8s_apply": protocol.TierT2, "k8s_secret_apply": protocol.TierT2, "preview_k8s_secret_apply": protocol.TierT0,
		"k8s_delete": protocol.TierT2, "k8s_unapply": protocol.TierT2,
	}
	for op, tier := range want {
		if got, ok := h.reg.Floor(op); !ok || got != tier {
			t.Errorf("%s floor = %q (registered %v), want %q", op, got, ok, tier)
		}
	}
	reg, _ := ops.New(ops.Config{ProjectDir: t.TempDir()})
	t.Cleanup(func() { _ = reg.Close() })
	api, _ := NewAPI(APIConfig{BaseURL: "https://k8s.example:443", Token: func() (string, error) { return "x", nil }})
	for name, o := range map[string]Options{
		"local class":   {Pipeline: "orders", Connectors: []string{"c"}, Class: protocol.ClassLocal, API: api},
		"no api":        {Pipeline: "orders", Connectors: []string{"c"}},
		"bad pipeline":  {Pipeline: "Orders!", Connectors: []string{"c"}, API: api},
		"no connectors": {Pipeline: "orders", API: api},
		"bad connector": {Pipeline: "orders", Connectors: []string{"a b"}, API: api},
		"unknown class": {Pipeline: "orders", Connectors: []string{"c"}, Class: "prod", API: api},
	} {
		if _, err := Register(reg, o); err == nil {
			t.Errorf("%s: Register accepted", name)
		}
	}
	if _, err := NewAPI(APIConfig{BaseURL: "http://k8s.example"}); err == nil {
		t.Error("a plain-http API server was accepted")
	}
}

// The namespace and the Kafka cluster come from Env.Targets only.
func TestScopeFromTargetsOnly(t *testing.T) {
	h := newHarness(t, "", nil)
	h.shell("KafkaConnect", "orders-connect", map[string]any{})
	o, _ := h.reg.Get("k8s_get")
	raw := json.RawMessage(`{"kind":"KafkaConnect","name":"orders-connect"}`)
	if _, err := o.Execute(context.Background(), raw, ops.Env{}); !errors.Is(err, ErrTarget) {
		t.Fatalf("no targets: err = %v, want ErrTarget", err)
	}
	if len(h.api.requests) != 0 {
		t.Fatalf("a request was sent without targets: %v", h.api.requests)
	}
	if _, err := o.Execute(context.Background(), raw, h.env); err != nil {
		t.Fatalf("with targets: %v", err)
	}
}

func TestK8sGet(t *testing.T) {
	h := newHarness(t, "", nil)
	h.shell("KafkaConnector", "orders-src", map[string]any{"class": "x"})
	res, err := run(t, h, "k8s_get", map[string]any{"kind": "KafkaConnector", "name": "orders-src"})
	if err != nil {
		t.Fatal(err)
	}
	b, _ := json.Marshal(res.Data)
	if strings.Contains(string(b), "managedFields") {
		t.Fatal("managedFields reached Data")
	}
	if res.Receipt.Kind != "http" || !strings.HasPrefix(res.Receipt.Target, "GET /apis/kafka.strimzi.io/v1/namespaces/data/kafkaconnectors/orders-src") {
		t.Fatalf("receipt = %+v", res.Receipt)
	}
	n := len(h.api.requests)
	for name, p := range map[string]map[string]any{
		"another connector": {"kind": "KafkaConnector", "name": "payments-src"},
		"a Secret":          {"kind": "Secret", "name": "orders-db-credentials"},
		"a ConfigMap":       {"kind": "ConfigMap", "name": "orders-connect-logging"},
		"another Kafka":     {"kind": "Kafka", "name": "other-kafka"},
		"a foreign pod":     {"kind": "Pod", "name": "postgres-0"},
	} {
		if _, err := run(t, h, "k8s_get", p); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
	if len(h.api.requests) != n {
		t.Fatalf("refused reads reached the API: %v", h.api.requests[n:])
	}
}

func TestAPIStatusMapping(t *testing.T) {
	h := newHarness(t, "", nil)
	path := h.shell("KafkaConnect", "orders-connect", map[string]any{})
	for code, want := range map[int]error{401: ErrUnauthorized, 403: ErrForbidden, 404: ErrNotFound, 409: ErrConflict, 422: ErrInvalid, 500: ErrAPI} {
		h.api.status["GET "+path] = code
		if _, err := run(t, h, "k8s_get", map[string]any{"kind": "KafkaConnect", "name": "orders-connect"}); !errors.Is(err, want) {
			t.Errorf("HTTP %d: err = %v, want %v", code, err, want)
		}
	}
	// 429 is retried once after Retry-After.
	h.api.retryAfter = 1
	if _, err := run(t, h, "k8s_get", map[string]any{"kind": "KafkaConnect", "name": "orders-connect"}); err != nil {
		t.Fatalf("429 then 200: %v", err)
	}
	// A body over 4 MB is refused.
	h.api.logs["/api/v1/namespaces/data/pods/orders-connect-connect-0/log"] = strings.Repeat("x", maxResponseBytes+10)
	if _, err := run(t, h, "k8s_logs", map[string]any{"pod": "orders-connect-connect-0"}); !errors.Is(err, ErrTooLarge) {
		t.Fatalf("a 4 MB log: err = %v", err)
	}
}

func TestK8sLogsAndEvents(t *testing.T) {
	h := newHarness(t, "", nil)
	h.api.logs["/api/v1/namespaces/data/pods/orders-connect-connect-0/log"] = "line 1\npassword=hunter2\nline 3\n"
	res, err := run(t, h, "k8s_logs", map[string]any{"pod": "orders-connect-connect-0", "tail": 50})
	if err != nil {
		t.Fatal(err)
	}
	b, _ := json.Marshal(res.Data)
	if strings.Contains(string(b), "hunter2") {
		t.Fatal("a credential in a log line was not redacted")
	}
	if _, err := run(t, h, "k8s_logs", map[string]any{"pod": "postgres-0"}); !errors.Is(err, ErrName) {
		t.Fatalf("a foreign pod: %v", err)
	}
	if _, err := run(t, h, "k8s_logs", map[string]any{"pod": "orders-connect-connect-0", "tail": 5000}); err == nil {
		t.Fatal("tail above the cap accepted")
	}
	h.api.allowed["events"] = true
	h.api.objects["events"] = map[string]any{"items": []any{
		map[string]any{"type": "Warning", "reason": "BackOff", "message": "Back-off pulling image", "lastTimestamp": "2026-10-30T10:00:00Z"},
	}}
	res, err = run(t, h, "k8s_events", map[string]any{"involved": "orders-connect-connect-0"})
	if err != nil || !strings.Contains(res.Summary, "1 warnings") {
		t.Fatalf("events = %q, %v", res.Summary, err)
	}
	if _, err := run(t, h, "k8s_events", map[string]any{"involved": "someone-else"}); !errors.Is(err, ErrName) {
		t.Fatalf("events of a foreign object: %v", err)
	}
}

func TestBuildStatusFailedBuild(t *testing.T) {
	h := newHarness(t, "", nil)
	path := h.shell("KafkaConnect", "orders-connect", map[string]any{})
	obj := h.api.get(path)
	obj["status"] = map[string]any{"conditions": []any{map[string]any{"type": "NotReady", "status": "True", "reason": "BuildFailed", "message": "Build failed"}}}
	h.api.put(path, obj)
	h.api.put("/api/v1/namespaces/data/pods/orders-connect-connect-build", map[string]any{"status": map[string]any{"phase": "Failed"}})
	h.api.logs["/api/v1/namespaces/data/pods/orders-connect-connect-build/log"] = "error: sha512sum mismatch\n"
	res, err := run(t, h, "strimzi_build_status", map[string]any{})
	if err != nil {
		t.Fatal(err)
	}
	data := res.Data.(map[string]any)
	if data["state"] != "build failed" || data["build_pod_phase"] != "Failed" {
		t.Fatalf("data = %v", data)
	}
	if tail, _ := data["build_log_tail"].([]string); len(tail) != 1 || !strings.Contains(tail[0], "sha512sum") {
		t.Fatalf("build log tail = %v", data["build_log_tail"])
	}
}

// The allow-list per field (board U12a): every refused field is refused and
// the refusal names its path.
func TestConnectAllowListRefusesEachField(t *testing.T) {
	h := newHarness(t, "", nil)
	base := func() map[string]any {
		return map[string]any{"replicas": 1, "bootstrapServers": "prod-kafka-kafka-bootstrap:9093"}
	}
	cases := map[string]struct {
		mutate func(map[string]any)
		path   string
	}{
		"spec.template pod overrides": {func(s map[string]any) { s["template"] = map[string]any{"pod": map[string]any{}} }, "spec.template"},
		"hostPath": {func(s map[string]any) {
			s["logging"] = map[string]any{"type": "inline", "x": map[string]any{"hostPath": "/"}}
		}, "hostPath"},
		"hostNetwork":           {func(s map[string]any) { s["build"] = map[string]any{"hostNetwork": true} }, "hostNetwork"},
		"serviceAccountName":    {func(s map[string]any) { s["metricsConfig"] = map[string]any{"serviceAccountName": "admin"} }, "serviceAccountName"},
		"initContainers":        {func(s map[string]any) { s["resources"] = map[string]any{"initContainers": []any{}} }, "initContainers"},
		"volumes":               {func(s map[string]any) { s["jvmOptions"] = map[string]any{"volumes": []any{}} }, "volumes"},
		"image":                 {func(s map[string]any) { s["image"] = "evil/connect:latest" }, "spec.image"},
		"externalConfiguration": {func(s map[string]any) { s["externalConfiguration"] = map[string]any{} }, "spec.externalConfiguration"},
		"too many replicas":     {func(s map[string]any) { s["replicas"] = 5 }, "spec.replicas"},
		"a worker key":          {func(s map[string]any) { s["config"] = map[string]any{"plugin.path": "/tmp"} }, "spec.config.plugin.path"},
		"another provider class": {func(s map[string]any) {
			s["config"] = map[string]any{"config.providers.secrets.class": "x.FileProvider"}
		}, "spec.config.config.providers.secrets.class"},
		"another push secret": {func(s map[string]any) {
			s["build"] = map[string]any{"output": map[string]any{"type": "docker", "image": "reg.io/x:piperoom-orders-abc", "pushSecret": "admin-creds"}}
		}, "spec.build.output.pushSecret"},
		"a maven artifact": {func(s map[string]any) {
			s["build"] = map[string]any{"output": map[string]any{"type": "docker", "image": "reg.io/x:piperoom-orders-abc", "pushSecret": "orders-registry-push"},
				"plugins": []any{map[string]any{"name": "dbz", "artifacts": []any{map[string]any{"type": "maven", "url": "x"}}}}}
		}, "spec.build.plugins[0].artifacts[0].type"},
		"another TLS secret": {func(s map[string]any) {
			s["tls"] = map[string]any{"trustedCertificates": []any{map[string]any{"secretName": "admin-ca", "pattern": "*.crt"}}}
		}, "spec.tls.trustedCertificates[0].secretName"},
	}
	for name, tc := range cases {
		spec := base()
		tc.mutate(spec)
		err := h.d.ValidateConnect(spec, testKafka)
		if !errors.Is(err, ErrField) || !strings.Contains(err.Error(), tc.path) {
			t.Errorf("%s: err = %v, want a field refusal naming %s", name, err, tc.path)
		}
	}
	if err := h.d.ValidateConnect(base(), testKafka); err != nil {
		t.Fatalf("a minimal spec was refused: %v", err)
	}
}

func TestUserAndConnectorAllowList(t *testing.T) {
	h := newHarness(t, "", nil)
	ok := map[string]any{"authentication": map[string]any{"type": "scram-sha-512"}, "authorization": map[string]any{"type": "simple", "acls": []any{
		map[string]any{"resource": map[string]any{"type": "topic", "name": "pg.", "patternType": "prefix"}, "operations": []any{"Read"}},
	}}}
	if err := h.d.ValidateUser(ok, "connect-user", "pg"); err != nil {
		t.Fatalf("a table ACL was refused: %v", err)
	}
	for name, spec := range map[string]map[string]any{
		"password source": {"authentication": map[string]any{"type": "scram-sha-512", "password": map[string]any{"valueFrom": map[string]any{}}}, "authorization": map[string]any{"type": "simple"}},
		"cluster ACL": {"authentication": map[string]any{"type": "tls"}, "authorization": map[string]any{"type": "simple", "acls": []any{
			map[string]any{"resource": map[string]any{"type": "cluster"}, "operations": []any{"All"}}}}},
		"another topic": {"authentication": map[string]any{"type": "tls"}, "authorization": map[string]any{"type": "simple", "acls": []any{
			map[string]any{"resource": map[string]any{"type": "topic", "name": "payments.", "patternType": "prefix"}, "operations": []any{"Read"}}}}},
		"an operation outside the table": {"authentication": map[string]any{"type": "tls"}, "authorization": map[string]any{"type": "simple", "acls": []any{
			map[string]any{"resource": map[string]any{"type": "topic", "name": "pg.", "patternType": "prefix"}, "operations": []any{"Delete"}}}}},
	} {
		if err := h.d.ValidateUser(spec, "connect-user", "pg"); !errors.Is(err, ErrField) {
			t.Errorf("%s: err = %v", name, err)
		}
	}
	good := map[string]any{"class": "io.debezium.connector.postgresql.PostgresConnector", "tasksMax": 1, "config": map[string]any{
		"database.password": "${secrets:data/orders-db-credentials:password}", "topic.prefix": "pg"}}
	if err := h.d.ValidateConnector(good, testNS); err != nil {
		t.Fatalf("a good connector was refused: %v", err)
	}
	for name, cfg := range map[string]map[string]any{
		"file provider":    {"database.password": "${file:/etc/passwd:x}"},
		"another secret":   {"database.password": "${secrets:data/admin-creds:password}"},
		"env provider":     {"database.hostname": "${strimzienv:HOST}"},
		"literal password": {"database.password": "hunter2"},
		"class in config":  {"connector.class": "x"},
	} {
		spec := map[string]any{"class": "io.debezium.connector.postgresql.PostgresConnector", "config": cfg}
		if err := h.d.ValidateConnector(spec, testNS); !errors.Is(err, ErrField) {
			t.Errorf("%s: err = %v", name, err)
		}
	}
}

func topicApply() map[string]any {
	return map[string]any{"kind": "KafkaTopic", "template": "canary-topic", "params": map[string]any{"topic_prefix": "pg", "schema": "public", "table": "piperoom_canary"}}
}

// The three steps of §5 on a paused shell: the spec replaced whole, read
// back equal, and unpaused last.
func TestK8sApplyThreeSteps(t *testing.T) {
	h := newHarness(t, "", nil)
	path := h.shell("KafkaTopic", "orders-canary", map[string]any{"partitions": 1})
	res, err := run(t, h, "k8s_apply", topicApply())
	if err != nil {
		t.Fatalf("k8s_apply: %v (%v)", err, h.api.requests)
	}
	obj := h.api.get(path)
	if ann, _ := obj["metadata"].(map[string]any)["annotations"].(map[string]any); ann[PauseAnnotation] != nil {
		t.Fatal("the topic is still paused")
	}
	if spec := obj["spec"].(map[string]any); spec["topicName"] != "pg.public.piperoom_canary" {
		t.Fatalf("spec = %v", spec)
	}
	if !strings.Contains(res.Summary, "unpaused") {
		t.Fatalf("summary = %q", res.Summary)
	}
	var seq []string
	for _, r := range h.api.requests {
		seq = append(seq, strings.SplitN(r, " ", 2)[0]+strings.Repeat("(dry)", strings.Count(r, "dryRun=All")))
	}
	if got := strings.Join(seq, ","); got != "GET,PATCH(dry),PATCH,GET,PATCH" {
		t.Fatalf("request sequence = %s", got)
	}
}

// A field the stored spec gains that the rendered one lacks (a webhook
// re-adding spec.template) is caught by the read-back: the object stays
// paused.
func TestK8sApplyReadBackRefusesTamperedSpec(t *testing.T) {
	for name, tamper := range map[string]map[string]any{
		"spec.template":    {"template": map[string]any{"pod": map[string]any{}}},
		"spec.image":       {"image": "evil:latest"},
		"config.providers": {"config": map[string]any{"config.providers": "secrets,file"}},
	} {
		h := newHarness(t, "", nil)
		path := h.shell("KafkaTopic", "orders-canary", map[string]any{})
		h.api.tamperWrite = tamper
		if _, err := run(t, h, "k8s_apply", topicApply()); !errors.Is(err, ErrReadBack) {
			t.Errorf("%s: err = %v, want ErrReadBack", name, err)
		}
		ann := h.api.get(path)["metadata"].(map[string]any)["annotations"].(map[string]any)
		if ann[PauseAnnotation] != "true" {
			t.Errorf("%s: a tampered object was unpaused", name)
		}
	}
}

// A default the schema adds is not a difference: the dry run's normalised
// form is what the read-back compares.
func TestK8sApplyServerDefaults(t *testing.T) {
	h := newHarness(t, "", nil)
	h.shell("KafkaTopic", "orders-canary", map[string]any{})
	h.api.defaultField = map[string]any{"defaulted": "yes"}
	if _, err := run(t, h, "k8s_apply", topicApply()); err != nil {
		t.Fatalf("a schema default failed the read-back: %v", err)
	}
}

// A 409 on step 1 (the object changed since the GET) fails the step, and the
// shell stays paused.
func TestK8sApplyConflict(t *testing.T) {
	h := newHarness(t, "", nil)
	path := h.shell("KafkaTopic", "orders-canary", map[string]any{})
	h.api.status["PATCH "+path] = 409
	if _, err := run(t, h, "k8s_apply", topicApply()); !errors.Is(err, ErrConflict) {
		t.Fatalf("err = %v, want ErrConflict", err)
	}
	if ann := h.api.get(path)["metadata"].(map[string]any)["annotations"].(map[string]any); ann[PauseAnnotation] != "true" {
		t.Fatal("unpaused after a conflict")
	}
}

func TestK8sApplyRefusals(t *testing.T) {
	h := newHarness(t, "", nil)
	for name, p := range map[string]map[string]any{
		"a Role":                   {"kind": "Role", "template": "x", "params": map[string]any{}},
		"a Secret":                 {"kind": "Secret", "template": "db-credentials", "params": map[string]any{}},
		"an unknown template":      {"kind": "KafkaTopic", "template": "any-topic", "params": map[string]any{}},
		"template of another kind": {"kind": "KafkaUser", "template": "canary-topic", "params": map[string]any{}},
		"a name param":             {"kind": "KafkaTopic", "template": "canary-topic", "params": map[string]any{"topic_prefix": "pg", "schema": "public", "table": "t", "name": "other"}},
	} {
		if _, err := run(t, h, "k8s_apply", p); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
	if len(h.api.requests) != 0 {
		t.Fatalf("refused applies reached the API: %v", h.api.requests)
	}
}

func TestPreviewK8sApply(t *testing.T) {
	h := newHarness(t, "", nil)
	path := h.shell("KafkaTopic", "orders-canary", map[string]any{})
	res, err := run(t, h, "preview_k8s_apply", topicApply())
	if err != nil {
		t.Fatal(err)
	}
	b, _ := json.Marshal(res.Data)
	if !strings.Contains(string(b), "piperoom_canary") || !strings.Contains(string(b), `"ok":true`) {
		t.Fatalf("preview = %s", b)
	}
	if ann := h.api.get(path)["metadata"].(map[string]any)["annotations"].(map[string]any); ann[PauseAnnotation] != "true" {
		t.Fatal("a preview changed the object")
	}
	for _, r := range h.api.requests {
		if strings.HasPrefix(r, "PATCH") && !strings.Contains(r, "dryRun=All") {
			t.Fatalf("a preview sent a real write: %s", r)
		}
	}
}

// D11 on production: a bound policy passes, a 403 on the binding falls back
// to the card confirmation, a missing binding refuses (board U12a, test both).
func TestProductionGate(t *testing.T) {
	binding := "/apis/admissionregistration.k8s.io/v1/validatingadmissionpolicybindings/piperoom-pipeline-orders"
	h := newHarness(t, protocol.ClassProduction, nil)
	h.shell("KafkaTopic", "orders-canary", map[string]any{})
	// 403: the Role cannot read the binding.
	if _, err := run(t, h, "k8s_apply", topicApply()); err == nil || !strings.Contains(err.Error(), "card must confirm") {
		t.Fatalf("403 without the card confirmation: %v", err)
	}
	p := topicApply()
	p["admission_policy_confirmed"] = true
	if _, err := run(t, h, "k8s_apply", p); err != nil {
		t.Fatalf("403 with the card confirmation: %v", err)
	}

	policy := "/apis/admissionregistration.k8s.io/v1/validatingadmissionpolicies/piperoom-pipeline-orders"
	good := map[string]any{"kind": "ValidatingAdmissionPolicyBinding", "spec": map[string]any{"policyName": "piperoom-pipeline-orders", "validationActions": []any{"Deny"}}}
	h = newHarness(t, protocol.ClassProduction, nil)
	h.shell("KafkaTopic", "orders-canary", map[string]any{})
	h.api.put(binding, good)
	h.api.put(policy, map[string]any{"kind": "ValidatingAdmissionPolicy", "spec": map[string]any{"failurePolicy": "Fail"}})
	if _, err := run(t, h, "k8s_apply", topicApply()); err != nil {
		t.Fatalf("a bound policy: %v", err)
	}

	// A readable binding is checked: another policy, no Deny, a policy that
	// fails open or is missing refuse even with the card; a policy the Role
	// cannot read falls back to the card.
	for name, tc := range map[string]struct {
		binding, policy map[string]any
		policyAllowed   bool
		want            string
	}{
		"another policy": {binding: map[string]any{"spec": map[string]any{"policyName": "permissive", "validationActions": []any{"Deny"}}}, want: "binds policy"},
		"warn only":      {binding: map[string]any{"spec": map[string]any{"policyName": "piperoom-pipeline-orders", "validationActions": []any{"Warn", "Audit"}}}, want: "does not Deny"},
		"fails open":     {binding: good, policy: map[string]any{"spec": map[string]any{"failurePolicy": "Ignore"}}, want: "fails open"},
		"no policy":      {binding: good, policyAllowed: true, want: "not installed"},
	} {
		h := newHarness(t, protocol.ClassProduction, nil)
		h.shell("KafkaTopic", "orders-canary", map[string]any{})
		h.api.put(binding, tc.binding)
		if tc.policy != nil {
			h.api.put(policy, tc.policy)
		} else if tc.policyAllowed {
			h.api.allow(policy)
		}
		if _, err := run(t, h, "k8s_apply", p); err == nil || !strings.Contains(err.Error(), tc.want) {
			t.Errorf("%s: err = %v, want %q", name, err, tc.want)
		}
	}
	h = newHarness(t, protocol.ClassProduction, nil)
	h.shell("KafkaTopic", "orders-canary", map[string]any{})
	h.api.put(binding, good) // the policy itself answers 403
	if _, err := run(t, h, "k8s_apply", topicApply()); err == nil || !strings.Contains(err.Error(), "card must confirm") {
		t.Fatalf("an unreadable policy without the card: %v", err)
	}
	if _, err := run(t, h, "k8s_apply", p); err != nil {
		t.Fatalf("an unreadable policy with the card: %v", err)
	}

	h = newHarness(t, protocol.ClassProduction, nil)
	h.shell("KafkaTopic", "orders-canary", map[string]any{})
	h.api.allowed[binding] = true // readable, but absent: 404
	if _, err := run(t, h, "k8s_apply", p); err == nil || !strings.Contains(err.Error(), "not installed") {
		t.Fatalf("a missing binding: %v", err)
	}
}

func TestK8sSecretApply(t *testing.T) {
	h := newHarness(t, "", map[string]string{"source_password": "s3cr3t-value"})
	path := coreObjectPath(testNS, "secrets", "orders-db-credentials")
	h.api.put(path, map[string]any{"kind": "Secret", "type": "Opaque", "metadata": map[string]any{"name": "orders-db-credentials"}, "data": map[string]any{"stale": "eA=="}})
	res, err := run(t, h, "k8s_secret_apply", map[string]any{"keys": map[string]string{"password": "${secret:source_password}"}})
	if err != nil {
		t.Fatalf("k8s_secret_apply: %v", err)
	}
	b, _ := json.Marshal(res)
	if strings.Contains(string(b), "s3cr3t") || strings.Contains(res.Summary, "s3cr3t") {
		t.Fatal("a secret value reached the result")
	}
	if data := h.api.get(path)["data"].(map[string]any); len(data) != 1 || data["stale"] != nil {
		t.Fatalf("stored data = %v, want only the new key", data)
	}
	if _, err := run(t, h, "k8s_secret_apply", map[string]any{"keys": map[string]string{"password": "hunter2"}}); !errors.Is(err, ErrParams) || strings.Contains(err.Error(), "hunter2") {
		t.Fatalf("a literal value: %v", err)
	}
	if _, err := run(t, h, "k8s_secret_apply", map[string]any{"keys": map[string]string{"password": "${secret:missing}"}}); err == nil {
		t.Fatal("a missing secret was accepted")
	}
	none := newHarness(t, "", nil)
	if _, err := run(t, none, "k8s_secret_apply", map[string]any{"keys": map[string]string{"password": "${secret:x}"}}); !errors.Is(err, ErrNoSecrets) {
		t.Fatalf("no store: %v", err)
	}
}

// No op runs a program: the package imports no os/exec (design §5).
func TestNoExecImport(t *testing.T) {
	files, _ := filepath.Glob("*.go")
	fset := token.NewFileSet()
	for _, f := range files {
		if strings.HasSuffix(f, "_test.go") {
			continue
		}
		src, err := os.ReadFile(f)
		if err != nil {
			t.Fatal(err)
		}
		af, err := parser.ParseFile(fset, f, src, parser.ImportsOnly)
		if err != nil {
			t.Fatal(err)
		}
		for _, imp := range af.Imports {
			if imp.Path.Value == `"os/exec"` || strings.Contains(imp.Path.Value, "k8s.io/client-go") {
				t.Errorf("%s imports %s", f, imp.Path.Value)
			}
		}
	}
}

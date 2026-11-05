package strimzi

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"piperoom/internal/events"
	"piperoom/internal/setup/knowledge"
)

const testRelease = "3.2.0.Final"

func connectApply(mut func(p map[string]any)) map[string]any {
	p := map[string]any{
		"listener": "tls", "tls": true, "auth": "tls",
		"registry": "registry.example.com:5000", "repository": "data/connect",
		"release": testRelease, "plugins": []string{"postgres"},
	}
	if mut != nil {
		mut(p)
	}
	return map[string]any{"kind": "KafkaConnect", "template": "connect", "params": p}
}

// withConnectFacts gives the harness what a KafkaConnect write reads: an
// image_registry target and the Kafka CR's listener status.
func withConnectFacts(h *harness) {
	h.env.Targets = append(h.env.Targets, events.SetupTarget{Kind: targetImageRegistry, Address: "registry.example.com:5000"})
	h.api.put(objectPath(testNS, "Kafka", testKafka), map[string]any{
		"apiVersion": "kafka.strimzi.io/v1", "kind": "Kafka", "metadata": map[string]any{"name": testKafka},
		"status": map[string]any{"listeners": []any{
			map[string]any{"name": "plain", "bootstrapServers": "prod-kafka-kafka-bootstrap.data.svc:9092"},
			map[string]any{"name": "tls", "bootstrapServers": "prod-kafka-kafka-bootstrap.data.svc:9093"},
		}},
	})
}

func TestConnectTemplateRendersFromKnowledge(t *testing.T) {
	h := newHarness(t, "", nil)
	withConnectFacts(h)
	path := h.shell("KafkaConnect", h.d.connectName(), map[string]any{"replicas": 1})
	if _, err := run(t, h, "k8s_apply", connectApply(nil)); err != nil {
		t.Fatalf("k8s_apply: %v (%v)", err, h.api.requests)
	}
	spec := h.api.get(path)["spec"].(map[string]any)
	if spec["bootstrapServers"] != "prod-kafka-kafka-bootstrap.data.svc:9093" {
		t.Errorf("bootstrapServers = %v, want the tls listener's", spec["bootstrapServers"])
	}
	plugin, err := knowledge.ResolvePlugin(testRelease, "postgres")
	if err != nil {
		t.Fatal(err)
	}
	build := spec["build"].(map[string]any)
	art := build["plugins"].([]any)[0].(map[string]any)["artifacts"].([]any)[0].(map[string]any)
	if art["url"] != plugin.ArtifactURL() || "sha512:"+art["sha512sum"].(string) != plugin.Digest {
		t.Errorf("artifact = %v, want %s %s", art, plugin.ArtifactURL(), plugin.Digest)
	}
	img := build["output"].(map[string]any)["image"].(string)
	if !strings.HasPrefix(img, "registry.example.com:5000/data/connect:piperoom-"+testPipeline+"-") {
		t.Errorf("image = %s", img)
	}
	if cfg := spec["config"].(map[string]any); cfg["group.id"] != h.d.connectName() || cfg["config.providers.secrets.class"] != secretsProviderClass {
		t.Errorf("config = %v", cfg)
	}
}

// The image tag follows the plugin digests: a different plugin set is a
// different image, the same set (in any order) the same one.
func TestConnectImageTagFollowsDigests(t *testing.T) {
	h := newHarness(t, "", nil)
	tag := func(plugins ...string) string {
		raw, _ := json.Marshal(connectApply(func(p map[string]any) { p["plugins"] = plugins })["params"])
		r, err := renderConnect(h.d, testKafka, raw)
		if err != nil {
			t.Fatalf("%v: %v", plugins, err)
		}
		return r.value["build"].(map[string]any)["output"].(map[string]any)["image"].(string)
	}
	a, b, c := tag("postgres", "jdbc"), tag("jdbc", "postgres"), tag("postgres")
	if a != b || a == c {
		t.Errorf("tags: %s %s %s", a, b, c)
	}
}

func TestConnectTemplateRefusals(t *testing.T) {
	cases := map[string]func(map[string]any){
		"unknown release":  func(p map[string]any) { p["release"] = "9.9.9.Final" },
		"unknown plugin":   func(p map[string]any) { p["plugins"] = []string{"db2-custom"} },
		"duplicate plugin": func(p map[string]any) { p["plugins"] = []string{"postgres", "postgres"} },
		"no plugins":       func(p map[string]any) { p["plugins"] = []string{} },
		"bad registry":     func(p map[string]any) { p["registry"] = "Registry Example" },
		"bad listener":     func(p map[string]any) { p["listener"] = "tls; rm" },
		"bad auth":         func(p map[string]any) { p["auth"] = "plain" },
		"bad resources":    func(p map[string]any) { p["resources"] = "huge" },
		"too many":         func(p map[string]any) { p["replicas"] = 99 },
		"unknown field":    func(p map[string]any) { p["image"] = "evil/image:latest" },
	}
	h := newHarness(t, "", nil)
	o, _ := h.reg.Get("k8s_apply")
	ok, _ := json.Marshal(connectApply(nil))
	if err := o.Validate(ok, h.env); err != nil {
		t.Fatalf("the base params are refused: %v", err)
	}
	for name, mut := range cases {
		raw, _ := json.Marshal(connectApply(mut))
		if err := o.Validate(raw, h.env); err == nil {
			t.Errorf("%s: Validate accepted it", name)
		}
	}
}

// A plugin served by a host outside PIPEROOM_SETUP_PLUGIN_HOSTS is refused
// even with a pinned digest.
func TestConnectPluginHostAllowList(t *testing.T) {
	h := newHarness(t, "", nil)
	h.d.plugins = []string{"mirror.internal"}
	raw, _ := json.Marshal(connectApply(nil)["params"])
	if _, err := renderConnect(h.d, testKafka, raw); err == nil || !strings.Contains(err.Error(), "not an allowed plugin host") {
		t.Fatalf("err = %v", err)
	}
}

func TestConnectFactsRefusals(t *testing.T) {
	t.Run("registry not a target", func(t *testing.T) {
		h := newHarness(t, "", nil)
		withConnectFacts(h)
		h.env.Targets = h.env.Targets[:len(h.env.Targets)-1]
		h.shell("KafkaConnect", h.d.connectName(), map[string]any{})
		if _, err := run(t, h, "k8s_apply", connectApply(nil)); !errors.Is(err, ErrTarget) {
			t.Fatalf("err = %v, want ErrTarget", err)
		}
	})
	t.Run("listener missing", func(t *testing.T) {
		h := newHarness(t, "", nil)
		withConnectFacts(h)
		h.shell("KafkaConnect", h.d.connectName(), map[string]any{})
		_, err := run(t, h, "k8s_apply", connectApply(func(p map[string]any) { p["listener"] = "external" }))
		if !errors.Is(err, ErrTarget) {
			t.Fatalf("err = %v, want ErrTarget", err)
		}
	})
	t.Run("preview resolves too", func(t *testing.T) {
		h := newHarness(t, "", nil)
		withConnectFacts(h)
		h.shell("KafkaConnect", h.d.connectName(), map[string]any{})
		res, err := run(t, h, "preview_k8s_apply", connectApply(nil))
		if err != nil {
			t.Fatal(err)
		}
		if b, _ := json.Marshal(res.Data); !strings.Contains(string(b), "data.svc:9093") || strings.Contains(string(b), placeholderBootstrap(testKafka)) {
			t.Fatalf("preview = %s", b)
		}
	})
}

func TestConnectorTemplate(t *testing.T) {
	h := newHarness(t, "", nil)
	path := h.shell("KafkaConnector", "orders-src", map[string]any{})
	ref := "${secrets:" + testNS + "/" + h.d.dbSecretName() + ":password}"
	apply := func(plugin string, cfg map[string]string) error {
		_, err := run(t, h, "k8s_apply", map[string]any{"kind": "KafkaConnector", "template": "connector", "params": map[string]any{
			"topic_prefix": "pg", "connector": "orders-src", "plugin": plugin, "config": cfg,
		}})
		return err
	}
	if err := apply("postgres", map[string]string{"database.hostname": "pg", "database.port": "5432", "database.password": ref, "topic.prefix": "pg", "database.sslmode": "verify-full"}); err != nil {
		t.Fatalf("k8s_apply: %v", err)
	}
	spec := h.api.get(path)["spec"].(map[string]any)
	if spec["class"] != sourceClasses["postgres"] || spec["tasksMax"] != float64(1) {
		t.Errorf("spec = %v", spec)
	}
	for name, cfg := range map[string]map[string]string{
		"literal password": {"database.password": "hunter2"},
		"other secret":     {"database.password": "${secrets:" + testNS + "/other:password}"},
		"class in config":  {"connector.class": "x"},
	} {
		if err := apply("postgres", cfg); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
	if err := apply("db2", map[string]string{"a": "b"}); err == nil {
		t.Error("unknown plugin accepted")
	}
	if _, err := run(t, h, "k8s_apply", map[string]any{"kind": "KafkaConnector", "template": "connector", "params": map[string]any{
		"topic_prefix": "pg", "connector": "not-the-charts", "plugin": "postgres", "config": map[string]string{"a": "b"},
	}}); err == nil {
		t.Error("a connector name outside the chart was accepted")
	}
}

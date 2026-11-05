package strimzi

import (
	"errors"
	"testing"
)

// withSourceDB is a driver for a SQL Server source whose database the
// person typed in the room release.
func withSourceDB(db string) func(*Options) {
	return func(o *Options) { o.SourceDatabase = db }
}

// A SQL Server source's canary topic carries the person's database:
// <prefix>.<database>.<schema>.piperoom_canary, rendered from no param.
func TestSQLServerCanaryTopicCarriesTheDatabase(t *testing.T) {
	h := newHarnessWith(t, "", nil, withSourceDB("shop"))
	path := h.shell("KafkaTopic", "orders-canary", map[string]any{})
	if _, err := run(t, h, "k8s_apply", topicApply()); err != nil {
		t.Fatalf("k8s_apply: %v (%v)", err, h.api.requests)
	}
	spec, _ := h.api.get(path)["spec"].(map[string]any)
	if spec["topicName"] != "pg.shop.public.piperoom_canary" {
		t.Fatalf("topicName = %v, want pg.shop.public.piperoom_canary", spec["topicName"])
	}
	// The database is the driver's, never the model's: a params field is
	// refused by the template's strict decode.
	p := topicApply()
	p["params"].(map[string]any)["database"] = "other"
	if _, err := run(t, h, "k8s_apply", p); !errors.Is(err, ErrParams) {
		t.Fatalf("a database param: err = %v, want ErrParams", err)
	}
}

// ValidateTopic and the delete's canary check pin the database: the
// three-segment canary, another database and the dotted-prefix lookalike are
// refused.
func TestSQLServerCanaryIsPinnedToTheDatabase(t *testing.T) {
	h := newHarnessWith(t, "", nil, withSourceDB("shop"))
	for name, tc := range map[string]struct {
		topic string
		ok    bool
	}{
		"the database's canary":      {"pg.shop.public.piperoom_canary", true},
		"no database segment":        {"pg.public.piperoom_canary", false},
		"another database":           {"pg.other.public.piperoom_canary", false},
		"a dotted schema":            {"pg.shop.a.b.piperoom_canary", false},
		"the database as the schema": {"pg.shop.piperoom_canary", false},
	} {
		err := h.d.ValidateTopic(map[string]any{"topicName": tc.topic, "partitions": 1, "replicas": 1}, "pg")
		if tc.ok != (err == nil) {
			t.Errorf("ValidateTopic %s: err = %v, want ok %v", name, err, tc.ok)
		}
		obj := map[string]any{"spec": map[string]any{"topicName": tc.topic}, "metadata": map[string]any{"name": "orders-canary"}}
		if err := h.d.checkCanaryTopic(obj, "orders-canary", "pg"); tc.ok != (err == nil) {
			t.Errorf("delete check %s: err = %v, want ok %v", name, err, tc.ok)
		}
	}
	// Without a database, a four-segment name is still the lookalike of a
	// dotted prefix's canary and is refused.
	plain := newHarness(t, "", nil)
	if err := plain.d.ValidateTopic(map[string]any{"topicName": "pg.shop.public.piperoom_canary", "partitions": 1, "replicas": 1}, "pg"); err == nil {
		t.Error("no database: a four-segment canary was accepted")
	}
}

func TestSQLServerCanaryDeleteAndPreview(t *testing.T) {
	h := newHarnessWith(t, "", nil, withSourceDB("shop"))
	path := h.shell("KafkaTopic", "orders-canary", map[string]any{"topicName": "pg.shop.public.piperoom_canary"})
	pr, err := previewOf(t, h, "preview_k8s_delete", delParams("KafkaTopic", "orders-canary"))
	if err != nil || pr.Expected == "" {
		t.Fatalf("preview: %+v, %v", pr, err)
	}
	if _, err := run(t, h, "k8s_delete", delParams("KafkaTopic", "orders-canary")); err != nil {
		t.Fatalf("k8s_delete: %v", err)
	}
	if h.api.get(path) != nil {
		t.Fatal("the canary is still there")
	}
}

func TestRegisterRefusesABadSourceDatabase(t *testing.T) {
	h := newHarness(t, "", nil)
	for _, db := range []string{"a.b", "1shop", "sh op", "x;y"} {
		if _, err := Register(h.reg, Options{Pipeline: testPipeline, Connectors: []string{"orders-src"}, API: h.d.api, SourceDatabase: db}); !errors.Is(err, ErrParams) {
			t.Errorf("%q: err = %v, want ErrParams", db, err)
		}
	}
}

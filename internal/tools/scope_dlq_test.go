package tools

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"piperoom/internal/owners"
)

const dlqScopeYAML = `
pipelines:
  - id: orders
    topic_prefix: pg
    source_tables: [public.orders]
    connectors: [pg-orders-connector, pg-orders-sink]
    consumer_groups: [order-sync]
    watch:
      dlq_topics:
        - {topic: order-sync-dlq, written_by: order-sync}
        - {topic: orders-sink-dlq, written_by: pg-orders-sink}
  - id: payments
    topic_prefix: pay
    source_tables: [public.payments]
    connectors: [pay-connector]
    consumer_groups: [pay-sync]
    watch:
      dlq_topics:
        - {topic: pay-dlq, written_by: pay-sync}
layers:
  postgres: priya
  kafka-connect: arjun
  kafka: arjun
  consumer: kabir
`

func loadDLQOwners(t *testing.T) *owners.Owners {
	t.Helper()
	path := filepath.Join(t.TempDir(), "owners.yaml")
	if err := os.WriteFile(path, []byte(dlqScopeYAML), 0o600); err != nil {
		t.Fatal(err)
	}
	o, err := owners.Load(path)
	if err != nil {
		t.Fatalf("owners.Load: %v", err)
	}
	return o
}

func TestScopeDLQTopics(t *testing.T) {
	o := loadDLQOwners(t)
	sc, err := ScopeFor(WithPipelineID(context.Background(), "orders"), o)
	if err != nil {
		t.Fatal(err)
	}
	if got := sc.DLQTopics(); !slices.Equal(got, []string{"order-sync-dlq", "orders-sink-dlq"}) {
		t.Fatalf("DLQTopics = %v", got)
	}
	for _, topic := range []string{"order-sync-dlq", "orders-sink-dlq"} {
		if err := sc.CheckDLQTopic(topic); err != nil {
			t.Errorf("declared topic %q refused: %v", topic, err)
		}
	}
	refused := map[string]string{
		"another pipeline's DLQ":     "pay-dlq",
		"undeclared topic":           "orders-dlq",
		"CDC topic of the pipeline":  "pg.public.orders",
		"CDC topic of another":       "pay.public.payments",
		"empty":                      "",
		"prefix of a declared topic": "order-sync",
		"declared topic plus suffix": "order-sync-dlq2",
		"case differs":               "Order-Sync-DLQ",
		"internal topic":             "__consumer_offsets",
	}
	for name, topic := range refused {
		err := sc.CheckDLQTopic(topic)
		if !errors.Is(err, ErrOutOfScope) {
			t.Errorf("%s (%q): err = %v, want ErrOutOfScope", name, topic, err)
			continue
		}
		// The refusal tells the model what it may ask for.
		if !strings.Contains(err.Error(), "order-sync-dlq") || !strings.Contains(err.Error(), `"orders"`) {
			t.Errorf("%s: error %q should list the declared DLQ topics and name the pipeline", name, err)
		}
	}
	// A CDC topic is refused as a DLQ topic and a DLQ topic is refused as a CDC topic.
	if err := sc.CheckTopic("order-sync-dlq"); !errors.Is(err, ErrOutOfScope) {
		t.Errorf("CheckTopic(DLQ topic) = %v, want ErrOutOfScope: dlq_peek is the only road to a DLQ", err)
	}
	if err := sc.CheckTopic("pg.public.orders"); err != nil {
		t.Errorf("CheckTopic(CDC topic) = %v", err)
	}
}

func TestScopeDLQTopicsOtherPipeline(t *testing.T) {
	sc, err := ScopeFor(WithPipelineID(context.Background(), "payments"), loadDLQOwners(t))
	if err != nil {
		t.Fatal(err)
	}
	if err := sc.CheckDLQTopic("pay-dlq"); err != nil {
		t.Errorf("own DLQ refused: %v", err)
	}
	if err := sc.CheckDLQTopic("order-sync-dlq"); !errors.Is(err, ErrOutOfScope) {
		t.Errorf("another pipeline's DLQ: err = %v, want ErrOutOfScope", err)
	}
}

func TestScopeDLQTopicsUnscoped(t *testing.T) {
	sc, err := ScopeFor(WithUnscoped(context.Background()), nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := sc.CheckDLQTopic("anything"); err != nil {
		t.Errorf("unscoped must pass: %v", err)
	}
	if got := sc.DLQTopics(); len(got) != 0 {
		t.Errorf("unscoped DLQTopics = %v, want none", got)
	}
}

// A pipeline built in code has no watch block and so no declared DLQ topics:
// every topic is refused, never "allowed because nothing is declared".
func TestScopeDLQTopicsNoneDeclared(t *testing.T) {
	sc, err := ScopeFor(WithPipelineID(context.Background(), "orders"), scopeTestOwners())
	if err != nil {
		t.Fatal(err)
	}
	if got := sc.DLQTopics(); len(got) != 0 {
		t.Fatalf("DLQTopics = %v, want none", got)
	}
	err = sc.CheckDLQTopic("orders-dlq")
	if !errors.Is(err, ErrOutOfScope) {
		t.Fatalf("err = %v, want ErrOutOfScope", err)
	}
	if !strings.Contains(err.Error(), "none declared (use connector for a sink's DLQ)") {
		t.Fatalf("error %q should say none are declared and point at connector", err)
	}
	if err = sc.CheckDLQTopic(""); !errors.Is(err, ErrOutOfScope) {
		t.Fatalf("empty topic with none declared: err = %v, want ErrOutOfScope", err)
	}
}

// The model-supplied name is shortened before it is echoed in the refusal.
func TestScopeDLQTopicRefusalShortensName(t *testing.T) {
	sc, _ := ScopeFor(WithPipelineID(context.Background(), "orders"), loadDLQOwners(t))
	err := sc.CheckDLQTopic(strings.Repeat("x", 500))
	if !errors.Is(err, ErrOutOfScope) {
		t.Fatalf("err = %v", err)
	}
	if strings.Count(err.Error(), "x") > 100 {
		t.Fatalf("refusal echoes the whole name: %d chars", len(err.Error()))
	}
}

// The slice DLQTopics returns is a copy: the model-facing code cannot widen the scope.
func TestScopeDLQTopicsReturnsCopy(t *testing.T) {
	sc, _ := ScopeFor(WithPipelineID(context.Background(), "orders"), loadDLQOwners(t))
	got := sc.DLQTopics()
	got[0] = "tampered"
	if err := sc.CheckDLQTopic("tampered"); err == nil {
		t.Fatal("editing the returned slice changed the scope")
	}
	if err := sc.CheckDLQTopic("order-sync-dlq"); err != nil {
		t.Fatalf("declared topic lost: %v", err)
	}
}

package actions

import (
	"testing"

	"piperoom/internal/events"
)

// sourceLayeredAction is a source-neutral source action (Q8): Layer() answers
// "postgres", the approving layer comes from the pipeline's source_kind.
type sourceLayeredAction struct{ classAction }

func (sourceLayeredAction) SourceLayered() {}

// kafkaAction is a plain action on another layer.
type kafkaAction struct{ classAction }

func (kafkaAction) Layer() string { return "kafka" }

var _ SourceLayered = sourceLayeredAction{}

func TestLayerFor(t *testing.T) {
	cases := []struct {
		name       string
		a          Action
		sourceKind string
		want       string
	}{
		{"source layered, default source", sourceLayeredAction{classAction{"source_x"}}, "", "postgres"},
		{"source layered, postgres", sourceLayeredAction{classAction{"source_x"}}, events.SourceKindPostgres, "postgres"},
		{"source layered, mysql", sourceLayeredAction{classAction{"source_x"}}, events.SourceKindMySQL, events.LayerMySQL},
		{"source layered, unknown kind fails closed", sourceLayeredAction{classAction{"source_x"}}, "oracle", ""},
		{"plain postgres action ignores mysql", classAction{"pg_x"}, events.SourceKindMySQL, "postgres"},
		{"plain kafka action", kafkaAction{classAction{"kafka_x"}}, events.SourceKindMySQL, "kafka"},
		{"plain action, unknown kind", kafkaAction{classAction{"kafka_x"}}, "oracle", "kafka"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := LayerFor(c.a, c.sourceKind); got != c.want {
				t.Errorf("LayerFor(%s, %q) = %q, want %q", c.a.Name(), c.sourceKind, got, c.want)
			}
		})
	}
}

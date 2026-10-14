package kafkatools

import (
	"testing"

	"piperoom/internal/tools"
)

// With no Schema Registry configured (SCHEMA_REGISTRY_URL empty or off) the
// wiring passes no registry: each schema tool answers so, as a finding,
// instead of failing or dereferencing nil.
func TestSchemaToolsWithoutRegistry(t *testing.T) {
	reg := tools.NewRegistry()
	if err := Register(reg, Deps{}); err != nil {
		t.Fatal(err)
	}
	for name, input := range map[string]string{
		"schema_registry_subjects": `{}`,
		"schema_registry_versions": `{"subject":"pg.public.orders-value"}`,
		"schema_registry_diff":     `{"subject":"pg.public.orders-value","from_version":1}`,
	} {
		tool, ok := reg.Get(name)
		if !ok {
			t.Fatalf("%s not registered", name)
		}
		r := runTool(t, tool, input)
		if r.Summary != "Schema Registry not configured (SCHEMA_REGISTRY_URL is empty or off)" {
			t.Errorf("%s summary = %q", name, r.Summary)
		}
	}
}

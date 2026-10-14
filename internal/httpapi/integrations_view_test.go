package httpapi

import (
	"testing"

	"piperoom/internal/integrations/spec"
)

// catalogOnly is a spec.Service that answers only Catalog; sanitizeView uses
// nothing else.
type catalogOnly struct {
	spec.Service
	kinds []spec.Kind
}

func (c catalogOnly) Catalog() []spec.Kind { return c.kinds }

// A reference stored in an AllowRef text field (a Vault lease's username)
// must survive the view filter, or the drawer shows the field empty and the
// row cannot be saved again without retyping it.
func TestSanitizeViewKeepsAllowRefReference(t *testing.T) {
	kind := spec.Kind{ID: "pg", Fields: []spec.Field{
		{Name: "user", Type: spec.FieldText, AllowRef: true},
		{Name: "host", Type: spec.FieldText},
		{Name: "password", Type: spec.FieldSecret},
	}}
	s := &Server{ints: &Integrations{Service: catalogOnly{kinds: []spec.Kind{kind}}}}
	userRef := "secret://vault-corp/database/creds/ro#username"
	v := s.sanitizeView(spec.View{Kind: "pg", Config: map[string]any{
		"user":     map[string]any{spec.RefKey: userRef},
		"host":     map[string]any{spec.RefKey: "secret://vault-corp/x"}, // not AllowRef: dropped
		"password": "typed-in-value",                                     // a typed secret: dropped
	}})

	got, ok := refValue(v.Config["user"])
	if !ok || got != userRef {
		t.Fatalf("user = %#v, want the reference %q", v.Config["user"], userRef)
	}
	if _, ok := v.Config["host"]; ok {
		t.Errorf("host kept a reference although the field does not allow one: %#v", v.Config["host"])
	}
	if _, ok := v.Config["password"]; ok {
		t.Error("a typed-in secret value was returned")
	}
}

// A plain value in an AllowRef field is still returned as it is.
func TestSanitizeViewKeepsPlainAllowRefValue(t *testing.T) {
	kind := spec.Kind{ID: "pg", Fields: []spec.Field{{Name: "user", Type: spec.FieldText, AllowRef: true}}}
	s := &Server{ints: &Integrations{Service: catalogOnly{kinds: []spec.Kind{kind}}}}
	v := s.sanitizeView(spec.View{Kind: "pg", Config: map[string]any{"user": "piperoom_ro"}})
	if v.Config["user"] != "piperoom_ro" {
		t.Fatalf("user = %#v, want piperoom_ro", v.Config["user"])
	}
}

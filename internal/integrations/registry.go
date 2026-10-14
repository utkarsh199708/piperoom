package integrations

import (
	"fmt"
	"regexp"

	"piperoom/internal/integrations/spec"
)

var (
	kindIDRe    = regexp.MustCompile(`^[a-z0-9][a-z0-9-]*$`)
	fieldNameRe = regexp.MustCompile(`^[a-z][a-z0-9_]*$`)
)

// Registry holds the catalog: every kind, in registration order. Kinds from
// other packages (the secret managers in internal/secrets) register through
// Register before the Service is built. It is not safe for concurrent
// Register calls; register everything at start-up.
type Registry struct {
	order []string
	kinds map[string]spec.Kind
}

// NewRegistry returns an empty registry.
func NewRegistry() *Registry { return &Registry{kinds: map[string]spec.Kind{}} }

// Register adds k after checking its id, category, fields and patterns.
func (r *Registry) Register(k spec.Kind) error {
	if !kindIDRe.MatchString(k.ID) {
		return fmt.Errorf("integrations: kind id %q does not match %s", k.ID, kindIDRe)
	}
	if _, dup := r.kinds[k.ID]; dup {
		return fmt.Errorf("integrations: kind %q registered twice", k.ID)
	}
	if !k.Category.Valid() {
		return fmt.Errorf("integrations: kind %s: unknown category %q", k.ID, k.Category)
	}
	if k.Test == nil || k.Build == nil {
		return fmt.Errorf("integrations: kind %s: Test and Build must be set", k.ID)
	}
	seen := map[string]bool{}
	hasName := false
	for _, f := range k.Fields {
		if !fieldNameRe.MatchString(f.Name) {
			return fmt.Errorf("integrations: kind %s: field name %q does not match %s", k.ID, f.Name, fieldNameRe)
		}
		if seen[f.Name] {
			return fmt.Errorf("integrations: kind %s: field %s twice", k.ID, f.Name)
		}
		seen[f.Name] = true
		if f.Pattern != "" {
			if _, err := regexp.Compile(f.Pattern); err != nil {
				return fmt.Errorf("integrations: kind %s: field %s: bad pattern: %w", k.ID, f.Name, err)
			}
		}
		if f.Type.IsSecret() && f.Default != "" {
			return fmt.Errorf("integrations: kind %s: secret field %s has a default", k.ID, f.Name)
		}
		if f.Type == spec.FieldSelect && len(f.Options) == 0 {
			return fmt.Errorf("integrations: kind %s: select field %s has no options", k.ID, f.Name)
		}
		if f.Name == spec.ProviderNameField && f.Type == spec.FieldText && f.Required {
			hasName = true
		}
	}
	if k.Category == spec.CategorySecretManager && !hasName {
		return fmt.Errorf("integrations: kind %s: a secret_manager kind needs a required text field %q", k.ID, spec.ProviderNameField)
	}
	r.order = append(r.order, k.ID)
	r.kinds[k.ID] = k
	return nil
}

// Kind returns the kind with that id.
func (r *Registry) Kind(id string) (spec.Kind, bool) {
	k, ok := r.kinds[id]
	return k, ok
}

// Kinds returns every kind in catalog order: by spec.Categories, then by
// registration order within a category.
func (r *Registry) Kinds() []spec.Kind {
	out := make([]spec.Kind, 0, len(r.order))
	for _, c := range spec.Categories {
		for _, id := range r.order {
			if k := r.kinds[id]; k.Category == c {
				out = append(out, k)
			}
		}
	}
	return out
}

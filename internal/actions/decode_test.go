package actions

import (
	"encoding/json"
	"errors"
	"testing"
)

// TestValidateWrapsDecodeErrors: a malformed params body keeps the decoder's
// error in the chain (rule 3), for every action, instead of a generic message.
func TestValidateWrapsDecodeErrors(t *testing.T) {
	reg := NewRegistry()
	if err := Register(reg, testDeps()); err != nil {
		t.Fatal(err)
	}
	all := reg.All()
	if len(all) != 6 {
		t.Fatalf("registered %d actions, want 6", len(all))
	}
	for _, a := range all {
		t.Run(a.Name(), func(t *testing.T) {
			err := a.Validate(json.RawMessage(`{`))
			var syntaxErr *json.SyntaxError
			if !errors.As(err, &syntaxErr) {
				t.Fatalf("Validate(`{`) = %v, want a wrapped *json.SyntaxError", err)
			}
			err = a.Validate(json.RawMessage(`{"connector":1,"subject":1}`))
			var typeErr *json.UnmarshalTypeError
			if !errors.As(err, &typeErr) {
				t.Fatalf("Validate(wrong types) = %v, want a wrapped *json.UnmarshalTypeError", err)
			}
		})
	}
}

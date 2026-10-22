package clients_test

import (
	"errors"
	"strings"
	"testing"

	"piperoom/internal/clients"
)

// The writers' boundary check (X2): what X2a's grammar builds passes, and
// anything that could end the expression, start another statement or hide in
// a comment is refused, without echoing the condition.
func TestValidateSnapshotCondition(t *testing.T) {
	tests := []struct {
		name string
		cond string
		ok   bool
	}{
		{"key range", `"id" >= 100 AND "id" <= 199`, true},
		{"time bound", `"updated_at" >= '2026-10-22T10:00:00Z'`, true},
		{"sql-ish value bound as literal", `"id" = '1; drop table orders'`, true},
		{"doubled quote stays inside", `"name" = 'O''Brien; --'`, true},
		{"quoted identifier", `"weird""col" > 0`, true},
		{"exactly the cap", `"a" = '` + strings.Repeat("x", clients.MaxSnapshotConditionBytes-8) + `'`, true},
		{"empty", ``, false},
		{"over the cap", `"a" = '` + strings.Repeat("x", clients.MaxSnapshotConditionBytes-7) + `'`, false},
		{"second statement", `"id" > 0; DROP TABLE orders`, false},
		{"line comment", `"id" > 0 -- AND tenant = 1`, false},
		{"block comment", `"id" > 0 /* x */`, false},
		{"mysql comment", `id > 0 # x`, false},
		{"dollar quote", `"id" = $$'$$; drop`, false},
		{"positional param", `"id" = $1`, false},
		{"backtick", "`id` > 0", false},
		{"backslash escape", `"id" = E'\''; drop --'`, false},
		{"newline", "\"id\" > 0\nOR true", false},
		{"nul", "\"id\" > 0\x00", false},
		{"unterminated literal", `"id" = 'abc`, false},
		{"unterminated identifier", `"id = 1`, false},
		{"quote in identifier cannot hide a statement", `"x'" ; drop -- '`, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := clients.ValidateSnapshotCondition(tt.cond)
			if tt.ok {
				if err != nil {
					t.Fatalf("refused %q: %v", tt.cond, err)
				}
				return
			}
			if !errors.Is(err, clients.ErrBadSnapshotCondition) {
				t.Fatalf("accepted or wrong error for %q: %v", tt.cond, err)
			}
			if len(tt.cond) > 8 && strings.Contains(err.Error(), tt.cond) {
				t.Fatalf("error echoes the condition: %v", err)
			}
		})
	}
}

package store

import (
	"errors"
	"fmt"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"
)

// classify wraps the three SQLSTATEs that mean "the client's content cannot be
// stored". 22P02 (invalid text representation) is our own bug after the
// pre-checks, so it must come back unchanged and stay a logged 500.
func TestClassifyUnstorableCodes(t *testing.T) {
	for _, tc := range []struct {
		code string
		want bool
	}{
		{"22P05", true},
		{"22021", true},
		{"22003", true},
		{"22P02", false},
		{"23505", false},
		{"08006", false},
	} {
		pgErr := &pgconn.PgError{Code: tc.code, Message: "boom"}
		wrapped := fmt.Errorf("store: insert: %w", pgErr)
		got := classify(wrapped)
		if is := errors.Is(got, ErrUnstorable); is != tc.want {
			t.Errorf("classify(%s): ErrUnstorable = %v, want %v", tc.code, is, tc.want)
		}
		if IsUnstorablePGError(wrapped) != tc.want {
			t.Errorf("IsUnstorablePGError(%s) = %v, want %v", tc.code, !tc.want, tc.want)
		}
		var back *pgconn.PgError
		if !errors.As(got, &back) || back.Code != tc.code {
			t.Errorf("classify(%s): the PgError must stay in the chain", tc.code)
		}
		if !tc.want && got != wrapped {
			t.Errorf("classify(%s) must return the error unchanged", tc.code)
		}
	}
	plain := errors.New("plain")
	if got := classify(plain); got != plain {
		t.Error("a non-Postgres error must pass through")
	}
}

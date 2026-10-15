package room

import (
	"bytes"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"testing"
)

// TestMaskExecErrKeepsSecretsOutOfSlog logs wrapped and joined errors the way
// the executor does and checks the captured handler output.
func TestMaskExecErrKeepsSecretsOutOfSlog(t *testing.T) {
	const pw = "Zx9!hunter2-Qw7"
	errs := []error{
		fmt.Errorf("room: finish: %w", fmt.Errorf("dial postgres://app:%s@db/orders", pw)),
		errors.Join(errors.New("first"), fmt.Errorf(`{"password":"%s"}`, pw)),
		errors.New("Authorization: Bearer eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiIxMjM0NTY3ODkwIn0.dBjftJeZ4CVPmB92K27uhbUJU1p1r"),
	}
	var buf bytes.Buffer
	log := slog.New(slog.NewJSONHandler(&buf, nil))
	for _, e := range errs {
		log.Warn("room: executor stopped", "err", maskExecErr(e))
	}
	for _, s := range []string{pw, "dBjftJeZ4CVPmB92K27uhbUJU1p1r"} {
		if strings.Contains(buf.String(), s) {
			t.Fatalf("log holds %q:\n%s", s, buf.String())
		}
	}
	if maskExecErr(nil) != "" {
		t.Fatal("nil error must log as empty")
	}
}

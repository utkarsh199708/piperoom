package warehouse

import (
	"context"
	"errors"
	"testing"
	"time"

	"piperoom/internal/clients"
)

func TestLoadErrorsNotSupported(t *testing.T) {
	if _, err := (&DB{}).LoadErrors(context.Background(), "analytics.orders", time.Now(), 5); !errors.Is(err, clients.ErrNotSupported) {
		t.Fatalf("err = %v, want ErrNotSupported", err)
	}
}

func TestTableActivityBadInput(t *testing.T) {
	for _, tbl := range []string{"orders", "a.b;drop", "a.b.c"} {
		if _, err := (&DB{}).TableActivity(context.Background(), tbl); err == nil {
			t.Errorf("table %q: want error", tbl)
		}
	}
}

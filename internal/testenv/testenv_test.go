//go:build integration

package testenv

import (
	"strings"
	"testing"
	"time"
)

func TestRequireAndHelpers(t *testing.T) {
	cfg := Require(t)
	if cfg.ConnectURL == "" {
		t.Fatal("empty ConnectURL")
	}
	a, b := Name("x"), Name("x")
	if a == b || !strings.HasPrefix(a, "it_x_") {
		t.Fatalf("names %q %q", a, b)
	}
	n := 0
	WaitFor(t, 5*time.Second, func() (bool, error) { n++; return n == 3, nil })
}

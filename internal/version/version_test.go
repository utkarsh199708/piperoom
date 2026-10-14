package version

import "testing"

func TestString(t *testing.T) {
	if got := String(); got != Dev {
		t.Fatalf("String() = %q without -ldflags, want %q", got, Dev)
	}
	old := Version
	t.Cleanup(func() { Version = old })
	Version = ""
	if got := String(); got != Dev {
		t.Fatalf("String() = %q for an empty -X value, want %q", got, Dev)
	}
	Version = "v1.2.3-4-gabc"
	if got := String(); got != "v1.2.3-4-gabc" {
		t.Fatalf("String() = %q, want the linked value", got)
	}
}

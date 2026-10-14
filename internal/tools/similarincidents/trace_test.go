package similarincidents

import (
	"strings"
	"testing"
)

func TestExtractTrace(t *testing.T) {
	for _, tc := range []struct {
		name, in, want string
	}{
		{"none", "pg-orders-connector (source): RUNNING.\nTask 0: RUNNING", ""},
		{"empty", "", ""},
		{"status lines around a trace are cut",
			"conn: RUNNING on 10.0.0.1:8083.\nTask 0: FAILED:\norg.a.b.FooException: boom 1\nCaused by: org.a.b.BarException: root\n\tat org.a.Bar.run(Bar.java:10)\nRejected schema vs x v1:\nValue.amount: int",
			"org.a.b.FooException: boom 1\nCaused by: org.a.b.BarException: root\n\tat org.a.Bar.run(Bar.java:10)"},
		{"postgres error", "load failed\nERROR: duplicate key value violates unique constraint \"orders_pkey\"", "ERROR: duplicate key value violates unique constraint \"orders_pkey\""},
		{"plain error word is not a trace", "the error is that lag grew", ""},
		{"go style error is not a trace", "kafka: group g: not found", ""},
	} {
		if got := extractTrace(tc.in); got != tc.want {
			t.Errorf("%s: extractTrace = %q, want %q", tc.name, got, tc.want)
		}
	}
	long := "org.a.FooException: x\n" + strings.Repeat("\tat org.a.B.m(B.java:1)\n", 2000)
	if got := extractTrace(long); len(got) > maxTraceBytes {
		t.Errorf("block is %d bytes, cap %d", len(got), maxTraceBytes)
	}
}

func TestFingerprintIgnoresVolatileParts(t *testing.T) {
	a := "task: FAILED on 10.0.0.1:8083\norg.a.b.FooException: failed at offset 17 on host-1.internal\nCaused by: org.a.b.BarException: lsn 0/16B3748 bad"
	b := "org.a.b.FooException: failed at offset 99 on host-9.internal\nCaused by: org.a.b.BarException: lsn 1A/2B000028 bad\nnote: after"
	fa, oka := fingerprint(a)
	fb, okb := fingerprint(b)
	if !oka || !okb || fa != fb {
		t.Fatalf("fingerprints differ: %+v %+v", fa, fb)
	}
	if fa.Full == fa.Root {
		t.Error("full and root fingerprints must differ for a chain")
	}
	if _, ok := fingerprint("no trace here"); ok {
		t.Error("text without a trace got a fingerprint")
	}
}

func FuzzExtractAndFingerprint(f *testing.F) {
	f.Add("org.a.FooException: x\nCaused by: org.a.Bar: y\n\tat a.B.c(D.java:1)")
	f.Add("ERROR: x")
	f.Fuzz(func(t *testing.T, s string) {
		_ = extractTrace(s)
		_, _ = fingerprint(s)
	})
}

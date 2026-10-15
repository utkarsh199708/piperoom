package redact

import (
	"strings"
	"testing"
	"time"
)

// Cases from the #217 review: regressions (R*), leaks on paths the PR
// covers (L*) and next-line identifiers that must stay (N3).

func TestReviewIdempotent(t *testing.T) {
	for _, in := range []string{
		"token=sk-ant-api03-AbcDef123456GhiJkl789%26password%3D\"\nZx9abc12",
		"pass=AKIAIOSFODNN7EXAMPLE%pass:\"\nv2",
		"sk-1Ab1Ab1client_secret=pass='\nb1",
		"Authorization:pass':secret:\"\n\\\\\"",
	} {
		once, _ := Text(in)
		twice, st := Text(once)
		if twice != once || st.Total != 0 {
			t.Errorf("not idempotent:\n in:    %q\n once:  %q\n twice: %q", in, once, twice)
		}
	}
	// The first case's wrapped tail is masked on the first pass.
	if got, _ := Text("token=sk-ant-api03-AbcDef123456GhiJkl789%26password%3D\"\nZx9abc12"); strings.Contains(got, "Zx9abc12") {
		t.Errorf("tail survives the first pass: %q", got)
	}
}

func TestReviewNearMisses(t *testing.T) {
	for _, in := range []string{
		// R2: URLs with a port and a later '@', no credentials.
		"http://connect:8083/connectors?expand=status&user=ops@corp.com",
		"GET http://svc:8080/users/bob@example.com HTTP/1.1",
		"https://api.example.com:443/v1/users/alice@corp.com",
		"http://connect:8083/connectors/orders@v2/status",
		"git+https://github.com:443/org/repo@v1.2.3",
		"oci://registry.local:5000/app@v1.2",
		"kafka://broker:9092/topic@latest",
		"https://registry.example.com:443/@scope/pkg",
		// R3: an unclosed quote is not followed into an unrelated line.
		"password=\"[redacted 8]\n2026-10-14T09:41:07Z ERROR: relation \"orders\" does not exist",
		"{\"level\":\"error\",\"password\":\"[redacted 8]\n{\"level\":\"info\",\"msg\":\"retrying\"}",
		// N3: identifiers on the line after a bare secret at the end of a line.
		"password=[redacted 8]\norders-0@42 lag=17",
		"password=[redacted 8]\n4f3c2a9e-1b7d-4e2a-9c1e-2f6b8d0a3e5c joined",
		"password=[redacted 8]\nc0ffee12 committed",
		"password=[redacted 8]\nPostgreSQL15 ready",
		"password=[redacted 8]\ndbz2 connector restarted",
		"password=[redacted 8]\nWAL2 slot",
		"password=[redacted 8]\ntopic2 lag=4",
	} {
		got, _ := New(Options{}).Text(in)
		if got != in {
			t.Errorf("Text(%q)\n got  %q\n want it unchanged", in, got)
		}
	}
	// R3: the secret on the first line is masked, the next line kept.
	for _, tc := range []struct{ in, want string }{
		{"password=\"Zx9abc\n2026-10-14T09:41:07Z ERROR: relation \"orders\" does not exist",
			"password=\"[redacted 8]\n2026-10-14T09:41:07Z ERROR: relation \"orders\" does not exist"},
		{"{\"level\":\"error\",\"password\":\"Zx9…\n{\"level\":\"info\",\"msg\":\"retrying\"}",
			"{\"level\":\"error\",\"password\":\"[redacted 8]\n{\"level\":\"info\",\"msg\":\"retrying\"}"},
		{"password=Zx9…\nter2-Qw7", "password=[redacted 8]\nter2-Qw7"},
	} {
		if got, _ := New(Options{}).Text(tc.in); got != tc.want {
			t.Errorf("Text(%q)\n got  %q\n want %q", tc.in, got, tc.want)
		}
	}
}

func TestReviewLeaks(t *testing.T) {
	jaas := `{"a":"{\"sasl.jaas.config\":\"x required username=\\\"svc\\\" password=\\\"Pa55word`
	for _, tc := range []struct {
		name, in string
		secrets  []string
	}{
		{"L1 open double-escaped, truncated", jaas + "\n…[truncated]", []string{"Pa55word"}},
		{"L1 open double-escaped, wrapped", "password=\\\\\\\"Pa55\nword9X\\\\\\\";", []string{"Pa55", "word9X"}},
		{"L2 url password ending in /", "postgres://app:Zx9hunter2/@db:5432/orders", []string{"Zx9hunter2"}},
		{"L2 redis", "redis://default:Zx9hunter2Qw7/@redis-1.internal:6379", []string{"Zx9hunter2Qw7"}},
		{"L2 amqp", "amqp://svc:aB3+xQ9z/L7mK2/@rabbit:5672/vh", []string{"aB3+xQ9z", "L7mK2"}},
		{"L3 nine spaces", "password         = Zx9!hun\nter2-Qw7", []string{"Zx9", "ter2-Qw7"}},
		{"L3 kafka padding", "ssl.truststore.password                 = Zx9!hun\nter2-Qw7", []string{"Zx9", "ter2-Qw7"}},
		{"L3 tabs", "password\t\t\t\t\t\t\t\t\t= Zx9!hun\nter2-Qw7", []string{"Zx9", "ter2-Qw7"}},
		{"L4 quoted over three lines", "password=\"Zx9!hun\nter2-Qw7\nAb3xQ9zL\" ok", []string{"Zx9", "ter2-Qw7", "Ab3xQ9zL"}},
		{"L5 encoded json", "body=%7B%22username%22%3A%22bob%22%2C%22password%22%3A%22Zx9%21hunter2-Qw7%22%7D", []string{"Zx9", "hunter2", "Qw7"}},
		{"L5 encoded json lower", "body=%7b%22username%22%3a%22bob%22%2c%22password%22%3a%22Zx9%21hunter2-Qw7%22%7d", []string{"Zx9", "hunter2", "Qw7"}},
		{"L5 double encoded", "redirect=%2Flogin%3Fuser%3Dbob%26password%253DZx9%2521hunter2-Qw7", []string{"Zx9", "hunter2", "Qw7"}},
		{"L5 encoded spaces", "password%20%3D%20Zx9%21hunter2-Qw7", []string{"Zx9", "hunter2", "Qw7"}},
		{"L5 plus spaces", "password+%3D+Zx9%21hunter2-Qw7", []string{"Zx9", "hunter2", "Qw7"}},
		{"N2 azure user@server", "postgresql://myadmin@mydemoserver:Zx9hunter2Qw7@mydemoserver.postgres.database.azure.com:5432/postgres", []string{"Zx9hunter2Qw7"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, _ := Text(tc.in)
			for _, s := range tc.secrets {
				if strings.Contains(got, s) {
					t.Errorf("Text(%q) = %q: %q survives", tc.in, got, s)
				}
			}
			if again, _ := Text(got); again != got {
				t.Errorf("not idempotent: %q -> %q", got, again)
			}
		})
	}
	// Evidence around the leaks stays.
	for _, tc := range []struct{ in, want string }{
		{"postgres://app:Zx9hunter2/@db:5432/orders", "postgres://app:[redacted 16]@db:5432/orders"},
		{"body=%22password%22%3A%22Zx9%21hunter2%22%2C%22user%22%3A%22bob%22", "body=%22password%22%3A%22[redacted 16]%22%2C%22user%22%3A%22bob%22"},
	} {
		if got, _ := New(Options{}).Text(tc.in); got != tc.want {
			t.Errorf("Text(%q)\n got  %q\n want %q", tc.in, got, tc.want)
		}
	}
}

// slowLines are untrusted lines whose cost grew quadratically: one
// form-encoded or URL-like line at the scan limit.
var slowLines = map[string]string{
	"encoded-pairs": strings.Repeat("password%3Da%26", MaxScan/15),
	"token-a":       strings.Repeat("token=a", MaxScan/7),
	"url-prefixes":  strings.Repeat("x://u:", MaxScan/6),
	"sasl-list":     strings.Repeat("SASL_SSL://broker-1:9093,", MaxScan/25),
	"encoded-keys":  strings.Repeat("password%3Dx", MaxScan/12),
}

func TestReviewLinearTime(t *testing.T) {
	for name, s := range slowLines {
		start := time.Now()
		Text(s)
		if d := time.Since(start); d > 2*time.Second {
			t.Errorf("%s: %v for %d bytes", name, d, len(s))
		}
	}
}

func BenchmarkTextHostileLine(b *testing.B) {
	for _, name := range []string{"encoded-pairs", "url-prefixes"} {
		s := slowLines[name]
		b.Run(name, func(b *testing.B) {
			b.SetBytes(int64(len(s)))
			for b.Loop() {
				Text(s)
			}
		})
	}
}

func TestReviewPaddedKeys(t *testing.T) {
	in := "password" + strings.Repeat(" ", 300) + "= Zx9hunter2Qw7"
	if got, _ := Text(in); strings.Contains(got, "Zx9hunter2Qw7") {
		t.Errorf("padded key: secret survives in %q", got)
	}
}

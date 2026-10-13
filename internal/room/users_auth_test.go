package room

import (
	"bytes"
	"log/slog"
	"os"
	"strings"
	"testing"

	"piperoom/internal/owners"
)

// ownersFromYAML parses an owners file from text (via a temp file, the same
// path the server uses).
func ownersFromYAML(t *testing.T, layers string) *owners.Owners {
	t.Helper()
	path := t.TempDir() + "/owners.yaml"
	src := "pipelines:\n  - id: orders\n    topic_prefix: pg\n    source_tables: [public.orders]\n    connectors: [c1]\n" + layers
	if err := os.WriteFile(path, []byte(src), 0o600); err != nil {
		t.Fatal(err)
	}
	o, err := owners.Load(path)
	if err != nil {
		t.Fatalf("owners.Load: %v", err)
	}
	return o
}

func captureLog(t *testing.T) *bytes.Buffer {
	t.Helper()
	var buf bytes.Buffer
	old := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug})))
	t.Cleanup(func() { slog.SetDefault(old) })
	return &buf
}

func TestValidateUsersSkipsGroupLayers(t *testing.T) {
	users := mustUsers([]User{
		{ID: "arjun", Name: "Arjun", Layers: []string{"kafka"}},
		{ID: "priya", Name: "Priya", Layers: []string{"postgres"}},
	})

	t.Run("layer with a group is skipped, one Info line", func(t *testing.T) {
		buf := captureLog(t)
		// kafka lists a group, so users.yaml (arjun lists kafka) is not compared.
		// consumer names a user who is not in users.yaml, but it also has a group.
		own := ownersFromYAML(t, "layers:\n  postgres: priya\n  kafka: [user:arjun, group:oncall]\n  consumer: [user:ghost, group:oncall]\n")
		if err := ValidateUsers(users, own); err != nil {
			t.Fatalf("ValidateUsers: %v", err)
		}
		out := buf.String()
		if n := strings.Count(out, "not checked against users.yaml"); n != 1 {
			t.Fatalf("want exactly one Info line, got %d:\n%s", n, out)
		}
		if !strings.Contains(out, "level=INFO") || !strings.Contains(out, "consumer") || !strings.Contains(out, "kafka") {
			t.Fatalf("Info line should name the skipped layers:\n%s", out)
		}
	})

	t.Run("no group, no log", func(t *testing.T) {
		buf := captureLog(t)
		own := ownersFromYAML(t, "layers:\n  postgres: priya\n  kafka: arjun\n")
		if err := ValidateUsers(users, own); err != nil {
			t.Fatalf("ValidateUsers: %v", err)
		}
		if strings.Contains(buf.String(), "not checked") {
			t.Fatalf("unexpected log:\n%s", buf.String())
		}
	})

	t.Run("user-only layers are still checked both ways", func(t *testing.T) {
		own := ownersFromYAML(t, "layers:\n  postgres: priya\n  kafka: ghost\n")
		err := ValidateUsers(users, own)
		if err == nil || !strings.Contains(err.Error(), `"ghost", who is not in the users file`) {
			t.Fatalf("owners->users: %v", err)
		}
		own = ownersFromYAML(t, "layers:\n  postgres: priya\n  kafka: priya\n")
		err = ValidateUsers(users, own)
		if err == nil || !strings.Contains(err.Error(), "disagree") {
			t.Fatalf("users->owners: %v", err)
		}
	})

	t.Run("all-user list: each user must list the layer", func(t *testing.T) {
		own := ownersFromYAML(t, "layers:\n  postgres: [user:priya]\n  kafka: [user:arjun, user:priya]\n")
		err := ValidateUsers(users, own)
		if err == nil || !strings.Contains(err.Error(), `"priya" owns layer "kafka"`) {
			t.Fatalf("want priya-does-not-list-kafka error, got %v", err)
		}
	})

	t.Run("users.yaml layer missing from owners still refused", func(t *testing.T) {
		own := ownersFromYAML(t, "layers:\n  postgres: priya\n")
		err := ValidateUsers(users, own)
		if err == nil || !strings.Contains(err.Error(), "does not assign to anyone") {
			t.Fatalf("got %v", err)
		}
	})

	t.Run("user lists a layer whose owners entry has a group: skipped", func(t *testing.T) {
		own := ownersFromYAML(t, "layers:\n  postgres: priya\n  kafka: [group:oncall]\n")
		if err := ValidateUsers(users, own); err != nil {
			t.Fatalf("ValidateUsers: %v", err)
		}
	})
}

func TestCheckSoloUser(t *testing.T) {
	users := mustUsers([]User{{ID: "alice", Name: "Alice"}, {ID: "bob", Name: "Bob"}})
	cases := []struct {
		name    string
		solo    string
		users   *Users
		layers  string
		wantErr string
	}{
		{"solo off", "", users, "layers:\n  kafka: [bob, group:x]\n", ""},
		{"solo user owns everything", "alice", users, "layers:\n  kafka: alice\n  postgres: [user:alice]\nauditors: [alice]\n", ""},
		{"solo user not in users.yaml", "zed", users, "layers:\n  kafka: zed\n", "not in the users file"},
		{"no users file", "alice", nil, "layers:\n  kafka: alice\n", "no users file"},
		{"another user in layers", "alice", users, "layers:\n  kafka: alice\n  postgres: bob\n", "user:bob"},
		{"a group in layers", "alice", users, "layers:\n  kafka: [alice, group:oncall]\n", "group:oncall"},
		{"another user as auditor", "alice", users, "layers:\n  kafka: alice\nauditors: [user:bob]\n", "user:bob"},
		{"another principal in pipeline owners", "alice", users,
			"    owners:\n      kafka: {approvers: [alice], backup: [group:sre]}\nlayers:\n  postgres: alice\n", "group:sre"},
		{"owners file names nobody", "alice", users, "layers: {}\n", ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			own := ownersFromYAML(t, tc.layers)
			err := CheckSoloUser(tc.solo, tc.users, own)
			if tc.wantErr == "" {
				if err != nil {
					t.Fatalf("CheckSoloUser: %v", err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("error = %v, want it to contain %q", err, tc.wantErr)
			}
		})
	}
	if err := CheckSoloUser("alice", users, nil); err != nil {
		t.Fatalf("nil owners: %v", err)
	}
	if err := CheckSoloUser("zed", users, nil); err == nil {
		t.Fatal("solo user missing from users.yaml must fail even without an owners file")
	}
}

func TestParseUsersEmailAndSub(t *testing.T) {
	u, err := ParseUsers([]byte(`
users:
  - id: priya
    name: Priya
    email: Priya@Example.com
    sub: "00u123"
    layers: [postgres]
  - id: arjun
    name: Arjun
`))
	if err != nil {
		t.Fatalf("ParseUsers: %v", err)
	}
	p, _ := u.Get("priya")
	if p.Email != "Priya@Example.com" || p.Sub != "00u123" {
		t.Fatalf("priya = %+v", p)
	}
	a, _ := u.Get("arjun")
	if a.Email != "" || a.Sub != "" {
		t.Fatalf("arjun = %+v", a)
	}

	bad := map[string]string{
		"users:\n  - {id: a, email: nope}\n":                                 "bad email",
		"users:\n  - {id: a, email: 'a b@x.com'}\n":                          "bad email",
		"users:\n  - {id: a, email: X@y.com}\n  - {id: b, email: x@Y.com}\n": "same email",
		"users:\n  - {id: a, sub: s1}\n  - {id: b, sub: s1}\n":               "same sub",
	}
	for src, want := range bad {
		if _, err := ParseUsers([]byte(src)); err == nil || !strings.Contains(err.Error(), want) {
			t.Errorf("ParseUsers(%q) = %v, want %q", src, err, want)
		}
	}
}

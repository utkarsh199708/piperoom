package main

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"

	"piperoom/internal/config"
)

// repoStack points a stack at the real repo (two levels up from cmd/devctl) so the
// tests read dev/owners.shadow.yaml and dev/docker-compose.yml. No Docker is run.
func repoStack(t *testing.T) *stack {
	t.Helper()
	root, err := filepath.Abs(filepath.Join("..", ".."))
	if err != nil {
		t.Fatal(err)
	}
	return &stack{root: root}
}

func TestParseShadowFlags(t *testing.T) {
	tests := []struct {
		name    string
		args    []string
		want    shadowOpts
		wantErr string
	}{
		{"none", nil, shadowOpts{Shell: defaultShell(runtime.GOOS)}, ""},
		{"all set", []string{"--grace", "10m", "--token-budget", "100000", "--max-turns", "8", "--watch-interval", "20s", "--watch-cooldown", "2m", "--shell", "powershell"},
			shadowOpts{Grace: "10m", TokenBudget: "100000", MaxTurns: "8", WatchInterval: "20s", WatchCooldown: "2m", Shell: "powershell"}, ""},
		{"grace zero", []string{"--grace", "0"}, shadowOpts{Grace: "0", Shell: defaultShell(runtime.GOOS)}, ""},
		{"bad grace", []string{"--grace", "soon"}, shadowOpts{}, "PIPEROOM_SHADOW_GRACE"},
		{"negative grace", []string{"--grace", "-5s"}, shadowOpts{}, "PIPEROOM_SHADOW_GRACE"},
		{"bad budget", []string{"--token-budget", "12k"}, shadowOpts{}, "PIPEROOM_SHADOW_TOKEN_BUDGET"},
		{"zero turns", []string{"--max-turns", "0"}, shadowOpts{}, "PIPEROOM_SHADOW_MAX_TURNS"},
		{"interval under the minimum", []string{"--watch-interval", "5s"}, shadowOpts{}, "PIPEROOM_WATCH_INTERVAL"},
		{"bad cooldown", []string{"--watch-cooldown", "x"}, shadowOpts{}, "PIPEROOM_WATCH_COOLDOWN"},
		{"bad shell", []string{"--shell", "fish"}, shadowOpts{}, "--shell"},
		{"unknown flag", []string{"--nope"}, shadowOpts{}, "shadow up"},
		{"positional", []string{"extra"}, shadowOpts{}, `unexpected argument "extra"`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := parseShadowFlags("up", tt.args)
			if tt.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
					t.Fatalf("err = %v, want containing %q", err, tt.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got != tt.want {
				t.Fatalf("got %+v, want %+v", got, tt.want)
			}
		})
	}
}

func TestDefaultShell(t *testing.T) {
	if defaultShell("windows") != "powershell" || defaultShell("linux") != "sh" || defaultShell("darwin") != "sh" {
		t.Fatal("windows should default to powershell, everything else to sh")
	}
}

func TestEnvPairsSkipsUnset(t *testing.T) {
	got := shadowOpts{Grace: " 30s ", MaxTurns: "5"}.envPairs()
	want := map[string]string{"PIPEROOM_SHADOW_GRACE": "30s", "PIPEROOM_SHADOW_MAX_TURNS": "5"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got %v, want %v", got, want)
	}
	if len(shadowOpts{}.envPairs()) != 0 {
		t.Fatal("no overrides must give no env")
	}
}

func TestHostEnvLines(t *testing.T) {
	t.Run("sh is sorted and quoted", func(t *testing.T) {
		got := hostEnvLines("/repo with 'quote", shadowOpts{Shell: "sh", Grace: "1m"})
		want := []string{
			"export OWNERS_FILE=" + shQuote(filepath.Join("/repo with 'quote", "dev", "owners.shadow.yaml")),
			`export PIPEROOM_SHADOW_GRACE='1m'`,
			`export PIPEROOM_WATCH_COOLDOWN='5m'`,
			`export PIPEROOM_WATCH_INTERVAL='15s'`,
			`export PIPEROOM_WATCH_MODE='rooms'`,
		}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("got\n%s\nwant\n%s", strings.Join(got, "\n"), strings.Join(want, "\n"))
		}
	})
	t.Run("powershell", func(t *testing.T) {
		got := hostEnvLines(`C:\Users\me\piperoom`, shadowOpts{Shell: "powershell"})
		if len(got) != 4 || !strings.HasPrefix(got[0], "$env:OWNERS_FILE = 'C:") || got[3] != "$env:PIPEROOM_WATCH_MODE = 'rooms'" {
			t.Fatalf("got %v", got)
		}
	})
	t.Run("an override beats the dev watcher timings", func(t *testing.T) {
		out := strings.Join(hostEnvLines("/r", shadowOpts{Shell: "sh", WatchInterval: "30s"}), "\n")
		if !strings.Contains(out, "PIPEROOM_WATCH_INTERVAL='30s'") || strings.Contains(out, "'15s'") {
			t.Fatalf("got %s", out)
		}
	})
	t.Run("never carries the API key", func(t *testing.T) {
		t.Setenv(apiKeyEnv, "sk-test-secret")
		out := strings.Join(hostEnvLines("/r", shadowOpts{Shell: "sh"}), "\n")
		if strings.Contains(out, "sk-test-secret") || strings.Contains(out, apiKeyEnv) {
			t.Fatalf("env lines must not mention the key: %s", out)
		}
	})
}

func TestPsQuote(t *testing.T) {
	tests := []struct{ in, want string }{
		{"plain", "'plain'"},
		{"it's", "'it''s'"},
		{"Uj\u2019s repo", "'Uj\u2019\u2019s repo'"}, // a typographic quote would end the string
		{"\u2018a\u201b", "'\u2018\u2018a\u201b\u201b'"},
		{"", "''"},
	}
	for _, tt := range tests {
		if got := psQuote(tt.in); got != tt.want {
			t.Errorf("psQuote(%q) = %q, want %q", tt.in, got, tt.want)
		}
	}
}

func TestRequireAPIKey(t *testing.T) {
	get := func(v string) func(string) string { return func(string) string { return v } }
	if err := requireAPIKey(get("sk-x")); err != nil {
		t.Fatalf("key set: %v", err)
	}
	for _, v := range []string{"", "   "} {
		err := requireAPIKey(get(v))
		if err == nil || !strings.Contains(err.Error(), apiKeyEnv) {
			t.Fatalf("value %q: err = %v, want it to name %s", v, err, apiKeyEnv)
		}
	}
	if err := requireAPIKey(get("sk-secret-value")); err != nil && strings.Contains(err.Error(), "sk-secret-value") {
		t.Fatal("error must not echo the key")
	}
}

func TestRequireDevAck(t *testing.T) {
	get := func(v string) func(string) string { return func(string) string { return v } }
	if err := requireDevAck(get(config.DevAckValue)); err != nil {
		t.Fatalf("exact ack: %v", err)
	}
	for _, v := range []string{"", "  "} {
		err := requireDevAck(get(v))
		if err == nil || !strings.Contains(err.Error(), config.DevAckValue) {
			t.Fatalf("value %q: err = %v, want it to tell how to set the ack", v, err)
		}
	}
	for _, v := range []string{"1", "true", "yes"} {
		if err := requireDevAck(get(v)); err == nil || !strings.Contains(err.Error(), "PIPEROOM_DEV_ACK") {
			t.Fatalf("value %q must be refused, got %v", v, err)
		}
	}
}

func TestShadowPipelines(t *testing.T) {
	st := repoStack(t)
	ids, err := shadowPipelines(filepath.Join(st.root, "dev", "owners.shadow.yaml"))
	if err != nil || !reflect.DeepEqual(ids, []string{"orders"}) {
		t.Fatalf("shadow file: ids=%v err=%v", ids, err)
	}
	if _, err := shadowPipelines(filepath.Join(st.root, "dev", "owners.yaml")); err == nil || !strings.Contains(err.Error(), "no pipeline with shadow") {
		t.Fatalf("plain owners file must be refused, got %v", err)
	}
	if _, err := shadowPipelines(filepath.Join(t.TempDir(), "missing.yaml")); err == nil {
		t.Fatal("missing file must fail")
	}
	bad := filepath.Join(t.TempDir(), "o.yaml")
	if err := os.WriteFile(bad, []byte("pipelines:\n  - id: x\n    shadow: {enabled: true, typo: 1}\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := shadowPipelines(bad); err == nil {
		t.Fatal("an unknown key in shadow: must fail")
	}
}

// The two dev owners files must differ only by the shadow block, or `devctl shadow`
// would investigate a different pipeline than the rest of the dev stack. Compared as
// generic YAML: owners.Pipeline keeps yaml line numbers, which differ by the comments.
func TestOwnersShadowMatchesOwners(t *testing.T) {
	st := repoStack(t)
	read := func(name string) map[string]any {
		raw, err := os.ReadFile(filepath.Join(st.root, "dev", name))
		if err != nil {
			t.Fatal(err)
		}
		var doc map[string]any
		if err := yaml.Unmarshal(raw, &doc); err != nil {
			t.Fatal(err)
		}
		return doc
	}
	plain, shadow := read("owners.yaml"), read("owners.shadow.yaml")
	pipes, _ := shadow["pipelines"].([]any)
	if len(pipes) == 0 {
		t.Fatal("shadow file has no pipelines")
	}
	for _, p := range pipes {
		m := p.(map[string]any)
		if _, ok := m["shadow"]; !ok {
			t.Errorf("pipeline %v has no shadow block", m["id"])
		}
		delete(m, "shadow")
	}
	if !reflect.DeepEqual(plain, shadow) {
		t.Fatalf("dev/owners.shadow.yaml drifted from dev/owners.yaml:\nplain  %v\nshadow %v", plain, shadow)
	}
}

// The compose file keeps the shadow server out of a plain `devctl up` and off the network.
func TestComposeShadowService(t *testing.T) {
	st := repoStack(t)
	raw, err := os.ReadFile(filepath.Join(st.root, "dev", "docker-compose.yml"))
	if err != nil {
		t.Fatal(err)
	}
	var doc struct {
		Services map[string]struct {
			Profiles    []string          `yaml:"profiles"`
			Ports       []string          `yaml:"ports"`
			Volumes     []string          `yaml:"volumes"`
			Environment map[string]string `yaml:"environment"`
		} `yaml:"services"`
	}
	if err := yaml.Unmarshal(raw, &doc); err != nil {
		t.Fatal(err)
	}
	svc, ok := doc.Services[shadowService]
	if !ok {
		t.Fatalf("service %q missing", shadowService)
	}
	if !reflect.DeepEqual(svc.Profiles, []string{shadowProfile}) {
		t.Fatalf("profiles = %v, want [%s]", svc.Profiles, shadowProfile)
	}
	// The batch profile's services are opt-in too (dev/batch.md, TestComposeBatchProfile).
	for name, s := range doc.Services {
		allowedProfile := reflect.DeepEqual(s.Profiles, []string{"batch"}) ||
			reflect.DeepEqual(s.Profiles, []string{"clickhouse"}) ||
			reflect.DeepEqual(s.Profiles, []string{stagingProfile}) ||
			reflect.DeepEqual(s.Profiles, []string{"logs"}) ||
			reflect.DeepEqual(s.Profiles, []string{mongoProfile}) ||
			reflect.DeepEqual(s.Profiles, []string{"mysql"}) // the MySQL source (Q8a)
		if name != shadowService && len(s.Profiles) != 0 && !allowedProfile {
			t.Errorf("service %q has profiles %v: only the shadow server, batch, clickhouse, staging, logs, mongo and mysql profiles may be opt-in", name, s.Profiles)
		}
	}
	if len(svc.Ports) != 1 || !strings.HasPrefix(svc.Ports[0], "127.0.0.1:") {
		t.Fatalf("ports = %v: dev login has no password, bind to loopback only", svc.Ports)
	}
	if !strings.Contains(strings.Join(svc.Volumes, " "), "owners.shadow.yaml") {
		t.Fatalf("volumes = %v: must mount dev/owners.shadow.yaml", svc.Volumes)
	}
	if got := svc.Environment["ANTHROPIC_API_KEY"]; got != "${ANTHROPIC_API_KEY:-}" {
		t.Fatalf("ANTHROPIC_API_KEY = %q: must be a pass-through, never a value, and never required (that would break a plain `up`)", got)
	}
	// Dev auth must be explicit here: the image stops baking it in (#127), and the server
	// then needs the ack (R9). The ack is a pass-through, never a value in the file.
	for k, want := range map[string]string{
		"PIPEROOM_AUTH_MODE":             "dev",
		"PIPEROOM_DEV_ALLOW_NONLOOPBACK": "1",
		"PIPEROOM_DEV_ACK":               "${PIPEROOM_DEV_ACK:-}",
		"PIPEROOM_MODEL":                 "${PIPEROOM_MODEL:-}",
	} {
		if svc.Environment[k] != want {
			t.Errorf("%s = %q, want %q", k, svc.Environment[k], want)
		}
	}
	if want := strings.TrimPrefix(shadowURL, "http://"); len(svc.Ports) == 1 && svc.Ports[0] != want+":8090" {
		t.Errorf("ports = %v: must publish %s:8090 so devctl's shadowURL %s reaches it", svc.Ports, want, shadowURL)
	}
	if svc.Environment["PIPEROOM_WATCH_MODE"] != "rooms" {
		t.Fatal("the watcher must be in rooms mode or no shadow room ever opens")
	}
	for k, want := range map[string]string{
		"PIPEROOM_SHADOW_GRACE":        "${PIPEROOM_SHADOW_GRACE:-" + config.DefaultShadowGrace + "}",
		"PIPEROOM_SHADOW_TOKEN_BUDGET": "${PIPEROOM_SHADOW_TOKEN_BUDGET:-" + config.DefaultShadowTokenBudget + "}",
		"PIPEROOM_SHADOW_MAX_TURNS":    "${PIPEROOM_SHADOW_MAX_TURNS:-" + config.DefaultShadowMaxTurns + "}",
		"PIPEROOM_WATCH_INTERVAL":      "${PIPEROOM_WATCH_INTERVAL:-" + devWatchInterval + "}",
		"PIPEROOM_WATCH_COOLDOWN":      "${PIPEROOM_WATCH_COOLDOWN:-" + devWatchCooldown + "}",
	} {
		if svc.Environment[k] != want {
			t.Errorf("%s = %q, want %q (compose and the Go defaults must agree)", k, svc.Environment[k], want)
		}
	}
	for k, v := range svc.Environment {
		if strings.Contains(v, ":?") {
			t.Errorf("%s uses ${…:?…}: compose evaluates it even with the profile off", k)
		}
	}
}

func TestShadowComposeArgs(t *testing.T) {
	got := shadowComposeArgs("up", "-d", shadowService)
	want := []string{"--profile", "shadow", "up", "-d", "piperoom-shadow"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got %v, want %v", got, want)
	}
}

func TestShadowCmd(t *testing.T) {
	st := repoStack(t)
	ctx := context.Background()
	tests := []struct {
		name    string
		args    []string
		wantErr string
	}{
		{"no args", nil, "usage: devctl shadow"},
		{"unknown subcommand", []string{"fly"}, `unknown subcommand "fly"`},
		{"down with extra args", []string{"down", "x"}, "usage: devctl shadow down"},
		{"status with extra args", []string{"status", "x"}, "usage: devctl shadow status"},
		{"up with a bad flag value fails before Docker", []string{"up", "--grace", "never"}, "PIPEROOM_SHADOW_GRACE"},
		{"env with a bad shell", []string{"env", "--shell", "zsh"}, "--shell"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := shadowCmd(ctx, st, tt.args, &bytes.Buffer{})
			if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
				t.Fatalf("err = %v, want containing %q", err, tt.wantErr)
			}
		})
	}

	t.Run("up without the dev ack stops before the lock and Docker", func(t *testing.T) {
		t.Setenv(apiKeyEnv, "sk-test")
		t.Setenv(devAckEnv, "")
		err := shadowCmd(ctx, st, []string{"up", "--owner", "nobody"}, &bytes.Buffer{})
		if err == nil || !strings.Contains(err.Error(), devAckEnv) {
			t.Fatalf("err = %v, want it to name %s", err, devAckEnv)
		}
	})

	t.Run("up without an API key stops before the lock and Docker", func(t *testing.T) {
		t.Setenv(apiKeyEnv, "")
		err := shadowCmd(ctx, st, []string{"up", "--owner", "nobody"}, &bytes.Buffer{})
		if err == nil || !strings.Contains(err.Error(), apiKeyEnv) {
			t.Fatalf("err = %v, want it to name %s", err, apiKeyEnv)
		}
	})

	t.Run("env prints the host settings", func(t *testing.T) {
		var out bytes.Buffer
		if err := shadowCmd(ctx, st, []string{"env", "--shell", "sh", "--grace", "2m"}, &out); err != nil {
			t.Fatal(err)
		}
		s := out.String()
		for _, want := range []string{"export OWNERS_FILE='" + st.root, "owners.shadow.yaml'", "export PIPEROOM_SHADOW_GRACE='2m'", "export PIPEROOM_WATCH_MODE='rooms'", "go run ./cmd/server"} {
			if !strings.Contains(s, want) {
				t.Errorf("output missing %q:\n%s", want, s)
			}
		}
	})
}

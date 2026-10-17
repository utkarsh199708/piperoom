package main

import (
	"bytes"
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"piperoom/internal/config"
)

func TestParseDemoFlags(t *testing.T) {
	tests := []struct {
		name    string
		args    []string
		want    func(demoOpts) bool
		wantErr string
	}{
		{"plain demo", []string{"schema-change"}, func(o demoOpts) bool { return o.scenario == "schema-change" && !o.rehearse && !o.fix }, ""},
		{"fix", []string{"--fix", "schema-change"}, func(o demoOpts) bool { return o.fix }, ""},
		{"rehearse defaults to schema-change", []string{"--rehearse"}, func(o demoOpts) bool {
			return o.rehearse && o.scenario == "schema-change" && o.poll == 2*time.Second && o.timeout == 3*time.Minute && o.envFile == defaultDemoEnv
		}, ""},
		{"flags after the scenario", []string{"--rehearse", "schema-change", "--poll", "500ms", "--timeout", "10s"}, func(o demoOpts) bool {
			return o.poll == 500*time.Millisecond && o.timeout == 10*time.Second
		}, ""},
		{"owner from flag", []string{"--rehearse", "--owner", "ada"}, func(o demoOpts) bool { return o.owner == "ada" }, ""},
		{"no scenario without rehearse", nil, nil, "usage: devctl demo"},
		{"two scenarios", []string{"schema-change", "connector-down"}, nil, "usage: devctl demo"},
		{"unknown scenario", []string{"--rehearse", "nope"}, nil, `unknown scenario "nope"`},
		{"fix and rehearse", []string{"--fix", "--rehearse"}, nil, "cannot be combined"},
		{"zero poll", []string{"--rehearse", "--poll", "0s"}, nil, "--poll"},
		{"negative poll", []string{"--rehearse", "--poll", "-1s"}, nil, "--poll"},
		{"poll longer than timeout", []string{"--rehearse", "--poll", "1m", "--timeout", "10s"}, nil, "--timeout"},
		{"bad duration", []string{"--rehearse", "--poll", "soon"}, nil, "usage: devctl demo"},
		{"empty user", []string{"--rehearse", "--as", " "}, nil, "--as"},
		{"unknown flag", []string{"--bogus", "schema-change"}, nil, "usage: devctl demo"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			o, err := parseDemoFlags(tt.args, "")
			if tt.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
					t.Fatalf("err = %v, want containing %q", err, tt.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if !tt.want(o) {
				t.Fatalf("unexpected options: %+v", o)
			}
		})
	}
}

func TestParseEnvFile(t *testing.T) {
	got, err := parseEnvFile(strings.NewReader("# c\n\nexport A=1\nB = \"two words\"\nC='x'\nD=\n"))
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]string{"A": "1", "B": "two words", "C": "x", "D": ""}
	for k, v := range want {
		if got[k] != v {
			t.Errorf("%s = %q, want %q", k, got[k], v)
		}
	}
	for _, bad := range []string{"no equals sign\n", "=value\n", "ANTHROPIC_API_KEY=sk-x\n", "PIPEROOM_COOKIE=a=b\n"} {
		if _, err := parseEnvFile(strings.NewReader(bad)); err == nil {
			t.Errorf("parseEnvFile(%q) = nil, want error", bad)
		}
	}
}

// The committed profile must satisfy the server's own parser.
func TestCommittedDemoProfile(t *testing.T) {
	root, err := repoRoot()
	if err != nil {
		t.Fatal(err)
	}
	f, err := os.Open(filepath.Join(root, defaultDemoEnv))
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	env, err := parseEnvFile(f)
	if err != nil {
		t.Fatal(err)
	}
	w, err := checkDemoProfile(env)
	if err != nil {
		t.Fatal(err)
	}
	if w.Mode != config.WatchRooms || w.Interval != 15*time.Second || w.Cooldown != 30*time.Second {
		t.Fatalf("profile = %+v, want rooms / 15s / 30s", w)
	}
	// The committed profile must not point the server at the DB that reset wipes.
	if err := checkRoomDB(env["PIPEROOM_DB_URL"], config.Load().SourceDBURL); err != nil {
		t.Fatal(err)
	}
	if _, err := config.ParseLog(config.Config{LogLevel: env["PIPEROOM_LOG_LEVEL"], LogFormat: env["PIPEROOM_LOG_FORMAT"]}); err != nil {
		t.Fatal(err)
	}
	if _, ok := env["PIPEROOM_DEV_ACK"]; ok {
		t.Fatal("PIPEROOM_DEV_ACK must stay commented out in the demo profile")
	}
}

func TestCheckDemoProfile(t *testing.T) {
	tests := []struct {
		name    string
		env     map[string]string
		wantErr string
	}{
		{"ok", map[string]string{"PIPEROOM_WATCH_MODE": "rooms", "PIPEROOM_WATCH_INTERVAL": "15s"}, ""},
		{"log mode opens no room", map[string]string{"PIPEROOM_WATCH_MODE": "log"}, "needs rooms"},
		{"empty mode is log", map[string]string{}, "needs rooms"},
		{"interval below minimum", map[string]string{"PIPEROOM_WATCH_MODE": "rooms", "PIPEROOM_WATCH_INTERVAL": "5s"}, "PIPEROOM_WATCH_INTERVAL"},
		{"bad cooldown", map[string]string{"PIPEROOM_WATCH_MODE": "rooms", "PIPEROOM_WATCH_COOLDOWN": "x"}, "PIPEROOM_WATCH_COOLDOWN"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := checkDemoProfile(tt.env)
			if tt.wantErr == "" {
				if err != nil {
					t.Fatal(err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
				t.Fatalf("err = %v, want containing %q", err, tt.wantErr)
			}
		})
	}
}

type fakeSteps struct {
	calls                []string
	resetErr, breakErr   error
	resetsFailAfterBreak bool
	broke                bool
}

func (f *fakeSteps) Reset(context.Context) error {
	f.calls = append(f.calls, "reset")
	if f.resetErr != nil {
		return f.resetErr
	}
	if f.broke && f.resetsFailAfterBreak {
		return errors.New("docker gone")
	}
	return nil
}
func (f *fakeSteps) Load(context.Context) error { f.calls = append(f.calls, "load"); return nil }
func (f *fakeSteps) Break(context.Context) error {
	f.calls = append(f.calls, "break")
	f.broke = true
	return f.breakErr
}

// fakeRooms returns no room until polls reaches appearAt; the clock advances
// by the sleep, so no test waits for real.
type fakeRooms struct {
	pingErr     error
	preexist    []roomBrief
	appearAt    int // poll number (1-based, after the preflight) that sees the room; 0 = never
	pollErr     error
	polls       int
	preflighted bool
	now         *time.Time
}

func (f *fakeRooms) Ping(context.Context) error { return f.pingErr }
func (f *fakeRooms) OpenRooms(context.Context, string) ([]roomBrief, error) {
	if !f.preflighted {
		f.preflighted = true
		return f.preexist, nil
	}
	f.polls++
	if f.pollErr != nil && f.polls == 1 {
		return nil, f.pollErr
	}
	if f.appearAt > 0 && f.polls >= f.appearAt {
		return []roomBrief{{ID: "r_1", Title: "Connector failed", Severity: "sev2", PipelineID: "orders", Status: "open"}}, nil
	}
	return nil, nil
}

func newTestRehearser(steps *fakeSteps, rooms *fakeRooms, out *bytes.Buffer) *rehearser {
	clock := time.Date(2026, 10, 14, 9, 0, 0, 0, time.UTC)
	rooms.now = &clock
	return &rehearser{
		steps: steps, rooms: rooms, out: out,
		now:      func() time.Time { return clock },
		sleep:    func(_ context.Context, d time.Duration) error { clock = clock.Add(d); return nil },
		scenario: "schema-change", poll: 5 * time.Second, timeout: 30 * time.Second,
		watch: config.Watch{Mode: config.WatchRooms, Interval: 15 * time.Second, Cooldown: 30 * time.Second},
	}
}

func TestRehearsePass(t *testing.T) {
	steps, rooms, out := &fakeSteps{}, &fakeRooms{appearAt: 3}, &bytes.Buffer{}
	if err := newTestRehearser(steps, rooms, out).run(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := strings.Join(steps.calls, ","); got != "reset,load,break,reset" {
		t.Fatalf("steps = %s, want reset,load,break,reset", got)
	}
	for _, want := range []string{"room opened 10s after the break", "r_1", "PASS", "Close room r_1"} {
		if !strings.Contains(out.String(), want) {
			t.Errorf("output lacks %q:\n%s", want, out)
		}
	}
}

func TestRehearseTimeoutStillResets(t *testing.T) {
	steps, rooms, out := &fakeSteps{}, &fakeRooms{pollErr: errors.New("connection refused")}, &bytes.Buffer{}
	err := newTestRehearser(steps, rooms, out).run(context.Background())
	if err == nil || !strings.Contains(err.Error(), "no room opened within 30s") || !strings.Contains(err.Error(), "connection refused") {
		t.Fatalf("err = %v", err)
	}
	if got := strings.Join(steps.calls, ","); got != "reset,load,break,reset" {
		t.Fatalf("steps = %s: the stack must be reset after a timeout", got)
	}
	if !strings.Contains(out.String(), "poll failed: connection refused") {
		t.Errorf("poll error not shown:\n%s", out)
	}
}

func TestRehearsePreflightFailures(t *testing.T) {
	t.Run("server down", func(t *testing.T) {
		steps := &fakeSteps{}
		err := newTestRehearser(steps, &fakeRooms{pingErr: errors.New("refused")}, &bytes.Buffer{}).run(context.Background())
		if err == nil || !strings.Contains(err.Error(), "server not reachable") {
			t.Fatalf("err = %v", err)
		}
		if len(steps.calls) != 0 {
			t.Fatalf("touched the stack: %v", steps.calls)
		}
	})
	t.Run("room already open", func(t *testing.T) {
		steps := &fakeSteps{}
		rooms := &fakeRooms{preexist: []roomBrief{{ID: "r_old"}}}
		err := newTestRehearser(steps, rooms, &bytes.Buffer{}).run(context.Background())
		if err == nil || !strings.Contains(err.Error(), "r_old") {
			t.Fatalf("err = %v", err)
		}
		if len(steps.calls) != 0 {
			t.Fatalf("touched the stack: %v", steps.calls)
		}
	})
}

func TestRehearseBreakFailureAndFinalResetFailure(t *testing.T) {
	steps := &fakeSteps{breakErr: errors.New("psql down"), resetsFailAfterBreak: true}
	err := newTestRehearser(steps, &fakeRooms{appearAt: 1}, &bytes.Buffer{}).run(context.Background())
	if err == nil || !strings.Contains(err.Error(), "psql down") || !strings.Contains(err.Error(), "final reset failed") {
		t.Fatalf("err = %v, want both the break and the reset error", err)
	}
}

func TestRehearseCancelledStillResets(t *testing.T) {
	steps, out := &fakeSteps{}, &bytes.Buffer{}
	r := newTestRehearser(steps, &fakeRooms{}, out)
	ctx, cancel := context.WithCancel(context.Background())
	r.sleep = func(ctx context.Context, _ time.Duration) error { cancel(); return ctx.Err() }
	if err := r.run(ctx); err == nil {
		t.Fatal("want an error after cancel")
	}
	if got := strings.Join(steps.calls, ","); got != "reset,load,break,reset" {
		t.Fatalf("steps = %s", got)
	}
}

func TestHTTPRooms(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("X-User") != "arjun" {
			http.Error(w, "no", http.StatusUnauthorized)
			return
		}
		switch r.URL.Path {
		case "/healthz":
			w.WriteHeader(http.StatusOK)
		case "/api/rooms":
			_, _ = w.Write([]byte(`[
			 {"id":"r_closed","pipeline_id":"orders","status":"closed"},
			 {"id":"r_other","pipeline_id":"billing","status":"open"},
			 {"id":"r_1","title":"T","severity":"sev2","pipeline_id":"orders","status":"open","opened_at":"2026-10-14T09:00:00Z","agent_status":{"state":"working","detail":"step 2"}}]`))
		default:
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()
	h, err := newHTTPRooms(srv.URL+"/", "arjun")
	if err != nil {
		t.Fatal(err)
	}
	if err := h.Ping(context.Background()); err != nil {
		t.Fatal(err)
	}
	got, err := h.OpenRooms(context.Background(), "orders")
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0].ID != "r_1" || got[0].AgentStatus.State != "working" {
		t.Fatalf("rooms = %+v, want only open orders room r_1", got)
	}

	bad, _ := newHTTPRooms(srv.URL, "nobody")
	if _, err := bad.OpenRooms(context.Background(), "orders"); err == nil || !strings.Contains(err.Error(), "HTTP 401") || !strings.Contains(err.Error(), "dev auth mode only") {
		t.Fatalf("err = %v, want HTTP 401", err)
	}
	srv.Close()
	if err := h.Ping(context.Background()); err == nil {
		t.Fatal("Ping on a closed server = nil")
	}
	for _, u := range []string{"", "ftp://x", "localhost:8090"} {
		if _, err := newHTTPRooms(u, "a"); err == nil {
			t.Errorf("newHTTPRooms(%q) = nil, want error", u)
		}
	}
}

func TestCheckRoomDB(t *testing.T) {
	const stack = "postgres://piperoom:piperoom-dev@localhost:5432/orders?sslmode=disable"
	t.Setenv("PIPEROOM_DB_URL", "")
	tests := []struct {
		name    string
		room    string
		wantErr string
	}{
		{"separate port", "postgres://u:p@localhost:5433/piperoom", ""},
		{"separate host", "postgres://u:p@db.internal:5432/piperoom", ""},
		{"unset means the roomdb default (:5433), which reset keeps", "", ""},
		{"same port on 127.0.0.1", "postgres://u:p@127.0.0.1:5432/piperoom", "devctl reset"},
		{"no port means 5432", "postgres://u:p@localhost/piperoom", "devctl reset"},
		{"not a url", "nonsense", "PIPEROOM_DB_URL"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := checkRoomDB(tt.room, stack)
			if tt.wantErr == "" {
				if err != nil {
					t.Fatal(err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
				t.Fatalf("err = %v, want containing %q", err, tt.wantErr)
			}
			if err != nil && strings.Contains(err.Error(), "p@") {
				t.Fatalf("error leaks the URL credentials: %v", err)
			}
		})
	}
	if err := checkRoomDB("postgres://u:p@localhost:5433/piperoom", "garbage"); err == nil {
		t.Fatal("bad SOURCE_DB_URL = nil error")
	}
}

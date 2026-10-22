package actions

import (
	"context"
	"encoding/json"
	"errors"
	"maps"
	"strings"
	"testing"
	"time"
	"unicode/utf8"

	"piperoom/internal/clients"
	"piperoom/internal/events"
)

// x8Connect is a Kafka Connect fake for the X8 shape tests. When prod is
// set, any call fails the test: production must never be touched by Stage.
type x8Connect struct {
	t     *testing.T
	prod  bool
	cfg   map[string]string // the one connector's config; nil = not present
	puts  []map[string]string
	patch []map[string]string
}

func (f *x8Connect) touched(what string) {
	if f.prod {
		f.t.Errorf("Stage called the production client: %s", what)
	}
}

func (f *x8Connect) ListConnectors(context.Context) ([]string, error) {
	f.touched("ListConnectors")
	if f.cfg == nil {
		return nil, nil
	}
	return []string{"orders-src"}, nil
}

func (f *x8Connect) Status(context.Context, string) (clients.ConnectorStatus, error) {
	f.touched("Status")
	return clients.ConnectorStatus{Name: "orders-src", State: "RUNNING", Tasks: []clients.TaskStatus{{ID: 0, State: "RUNNING"}}}, nil
}

func (f *x8Connect) Config(context.Context, string) (map[string]string, error) {
	f.touched("Config")
	return maps.Clone(f.cfg), nil
}

func (f *x8Connect) RestartTask(context.Context, string, int) error {
	f.touched("RestartTask")
	return nil
}

func (f *x8Connect) RestartConnector(context.Context, string) error {
	f.touched("RestartConnector")
	return nil
}

func (f *x8Connect) PauseConnector(context.Context, string) error {
	f.touched("PauseConnector")
	return nil
}

func (f *x8Connect) UpdateConnectorConfigKeys(_ context.Context, _ string, changes map[string]string) error {
	f.touched("UpdateConnectorConfigKeys")
	f.patch = append(f.patch, maps.Clone(changes))
	maps.Copy(f.cfg, changes)
	return nil
}

func (f *x8Connect) PutConnectorConfig(_ context.Context, _ string, cfg map[string]string) error {
	f.touched("PutConnectorConfig")
	f.puts = append(f.puts, maps.Clone(cfg))
	f.cfg = maps.Clone(cfg)
	return nil
}

var (
	_ clients.ConnectClient       = (*x8Connect)(nil)
	_ clients.ConnectFixWriter    = (*x8Connect)(nil)
	_ clients.ConnectConfigPutter = (*x8Connect)(nil)
)

// x8StagedConfig is the shape X8b implements for a connector config step,
// written against the X8-0 names only (E5): Deps.Staging, Stager,
// StageResult, ConnectConfigPutter, events.StageNoStaging.
type x8StagedConfig struct {
	classAction
	d Deps
}

func (a x8StagedConfig) Stage(ctx context.Context, raw json.RawMessage) (StageResult, error) {
	var p struct {
		Connector string            `json:"connector"`
		Changes   map[string]string `json:"changes"`
	}
	if err := json.Unmarshal(raw, &p); err != nil {
		return StageResult{}, err
	}
	if a.d.Staging == nil || a.d.Staging.Connect == nil {
		return StageResult{Detail: events.StageNoStaging}, nil
	}
	c := a.d.Staging.Connect
	names, err := c.ListConnectors(ctx)
	if err != nil {
		return StageResult{}, err
	}
	found := false
	for _, n := range names {
		found = found || n == p.Connector
	}
	if !found {
		return StageResult{Target: "connector " + p.Connector, Detail: "connector not present in staging"}, nil
	}
	before, err := c.Config(ctx, p.Connector)
	if err != nil {
		return StageResult{}, err
	}
	fw, ok1 := c.(clients.ConnectFixWriter)
	put, ok2 := c.(clients.ConnectConfigPutter)
	if !ok1 || !ok2 {
		return StageResult{}, clients.ErrNotSupported
	}
	start := time.Now()
	if err := fw.UpdateConnectorConfigKeys(ctx, p.Connector, p.Changes); err != nil {
		return StageResult{}, err
	}
	st, err := c.Status(ctx, p.Connector)
	res := StageResult{Target: "connector " + p.Connector, Applied: true, Healthy: err == nil && st.State == "RUNNING", DurationMS: time.Since(start).Milliseconds()}
	rctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), StageRestoreTimeout)
	defer cancel()
	if err := put.PutConnectorConfig(rctx, p.Connector, before); err != nil {
		res.Leftover = "restore failed: " + err.Error()
	}
	return res, nil
}

var _ Stager = x8StagedConfig{}

// TC3: an action without Stager is unaffected (registers, keeps its class,
// is simply not stageable); a Stager registers beside it.
func TestX8StagerIsOptional(t *testing.T) {
	reg := NewRegistry()
	plain := classAction{name: "plain_fix"}
	staged := x8StagedConfig{classAction: classAction{name: "staged_fix"}}
	for _, a := range []Action{plain, staged} {
		if err := reg.Register(a); err != nil {
			t.Fatalf("register %s: %v", a.Name(), err)
		}
	}
	if _, ok := Action(plain).(Stager); ok {
		t.Fatal("a plain action reads as a Stager")
	}
	if _, ok := Action(staged).(Stager); !ok {
		t.Fatal("the staged action does not read as a Stager")
	}
	if ClassOf(staged) != ClassFix || ClassOf(plain) != ClassFix {
		t.Fatal("staging changed an action's class")
	}
	// Every existing write action still registers with no staging set.
	if err := Register(NewRegistry(), Deps{}); err != nil {
		t.Fatalf("Register with nil Deps.Staging: %v", err)
	}
}

// E5: an implementation compiles and behaves against the shape: no staging,
// a connector missing on staging, and a staged change that is restored with
// the original whole config, with production never called.
func TestX8StagerShape(t *testing.T) {
	ctx := context.Background()
	raw := json.RawMessage(`{"connector":"orders-src","changes":{"max.batch.size":"4096"}}`)
	prod := &x8Connect{t: t, prod: true, cfg: map[string]string{"max.batch.size": "2048"}}

	a := x8StagedConfig{d: Deps{Connect: prod}}
	got, err := a.Stage(ctx, raw)
	if err != nil || got.Applied || got.Detail != events.StageNoStaging || got.Outcome() != events.StageNotApplied {
		t.Fatalf("no staging = %+v, %v", got, err)
	}

	missing := &x8Connect{t: t}
	a = x8StagedConfig{d: Deps{Connect: prod, Staging: &Staging{Connect: missing}}}
	got, err = a.Stage(ctx, raw)
	if err != nil || got.Applied || got.Detail != "connector not present in staging" || len(missing.puts) != 0 {
		t.Fatalf("missing on staging = %+v, %v (puts %d)", got, err, len(missing.puts))
	}

	orig := map[string]string{"max.batch.size": "2048", "tasks.max": "1"}
	stg := &x8Connect{t: t, cfg: maps.Clone(orig)}
	a = x8StagedConfig{d: Deps{Connect: prod, Staging: &Staging{Connect: stg}}}
	got, err = a.Stage(ctx, raw)
	if err != nil || got.Outcome() != events.StageHealthy || got.Leftover != "" {
		t.Fatalf("staged = %+v, %v", got, err)
	}
	if len(stg.patch) != 1 || stg.patch[0]["max.batch.size"] != "4096" {
		t.Fatalf("staging patches = %v", stg.patch)
	}
	if len(stg.puts) != 1 || !maps.Equal(stg.puts[len(stg.puts)-1], orig) {
		t.Fatalf("final PUT = %v, want the original %v", stg.puts, orig)
	}
}

// E4: CleanStageResult enforces the caps and the healthy-implies-applied
// rule at the boundary, redacts, and leaves the caller's DryRun alone.
func TestX8CleanStageResult(t *testing.T) {
	long := strings.Repeat("é", 3000) // 6000 bytes of two-byte runes
	secret := "jdbc:sqlserver://db:1433;user=sa;password={Br@ce;Pw42x};encrypt=true"
	for _, tc := range []struct {
		name  string
		in    StageResult
		check func(t *testing.T, out StageResult)
	}{
		{"detail capped on a rune boundary", StageResult{Applied: true, Detail: long}, func(t *testing.T, out StageResult) {
			if len(out.Detail) > events.MaxStageDetailBytes || !utf8.ValidString(out.Detail) || !strings.HasSuffix(out.Detail, "…") {
				t.Fatalf("detail: %d bytes, valid %v", len(out.Detail), utf8.ValidString(out.Detail))
			}
		}},
		{"target and leftover capped", StageResult{Target: long, Leftover: long}, func(t *testing.T, out StageResult) {
			if len(out.Target) > events.MaxStageTargetBytes || len(out.Leftover) > events.MaxStageLeftoverBytes {
				t.Fatalf("target %d, leftover %d bytes", len(out.Target), len(out.Leftover))
			}
		}},
		{"short text kept", StageResult{Applied: true, Healthy: true, Detail: "RUNNING for 60 s", DurationMS: 60100}, func(t *testing.T, out StageResult) {
			if out.Detail != "RUNNING for 60 s" || !out.Healthy || out.DurationMS != 60100 {
				t.Fatalf("out = %+v", out)
			}
		}},
		{"secret redacted in detail", StageResult{Applied: true, Detail: "task 0 FAILED: " + secret}, func(t *testing.T, out StageResult) {
			if strings.Contains(out.Detail, "Pw42x") {
				t.Fatalf("secret kept: %q", out.Detail)
			}
		}},
		{"healthy without applied is cleared", StageResult{Healthy: true}, func(t *testing.T, out StageResult) {
			if out.Healthy || out.Outcome() != events.StageNotApplied {
				t.Fatalf("out = %+v", out)
			}
		}},
		{"negative duration is zero", StageResult{DurationMS: -5}, func(t *testing.T, out StageResult) {
			if out.DurationMS != 0 {
				t.Fatalf("duration = %d", out.DurationMS)
			}
		}},
		{"dry-run text capped and redacted", StageResult{DryRun: &DryRunResult{Source: "registry compatibility check", Text: secret + long}}, func(t *testing.T, out StageResult) {
			if len(out.DryRun.Text) > maxDryRunTextBytes || strings.Contains(out.DryRun.Text, "Pw42x") {
				t.Fatalf("dry-run text: %d bytes", len(out.DryRun.Text))
			}
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var before string
			if tc.in.DryRun != nil {
				before = tc.in.DryRun.Text
			}
			tc.check(t, CleanStageResult(tc.in))
			if tc.in.DryRun != nil && tc.in.DryRun.Text != before {
				t.Fatal("CleanStageResult changed the caller's DryRun")
			}
		})
	}
}

// StageExpiry: per step the step and restore timeouts, plus 30 s; never
// shorter than one step.
func TestX8StageExpiry(t *testing.T) {
	start := time.Date(2026, 10, 22, 9, 0, 0, 0, time.UTC)
	per := StageStepTimeout + StageRestoreTimeout
	for _, tc := range []struct {
		steps int
		want  time.Duration
	}{
		{0, per + 30*time.Second},
		{1, per + 30*time.Second},
		{3, 3*per + 30*time.Second},
	} {
		if got := StageExpiry(start, tc.steps).Sub(start); got != tc.want {
			t.Errorf("StageExpiry(%d) = %v, want %v", tc.steps, got, tc.want)
		}
	}
	if StageObserveWindow >= StageStepTimeout {
		t.Fatal("the observe window must fit in one step's timeout")
	}
}

// ConnectConfigPutter is asserted at use time: a staging client without it
// (every fake from before X8) is clients.ErrNotSupported, never a panic or a
// stage that cannot restore; bad params are an error before any I/O.
func TestX8StageNeedsPutter(t *testing.T) {
	a := x8StagedConfig{d: Deps{Staging: &Staging{Connect: &x8Connect{t: t, cfg: map[string]string{}}}}}
	if _, err := a.Stage(context.Background(), json.RawMessage(`{`)); err == nil {
		t.Fatal("bad params staged")
	}
	var stg struct{ clients.ConnectClient }
	stg.ConnectClient = &x8Connect{t: t, cfg: map[string]string{}}
	a = x8StagedConfig{d: Deps{Staging: &Staging{Connect: stg}}}
	if _, err := a.Stage(context.Background(), json.RawMessage(`{"connector":"orders-src"}`)); !errors.Is(err, clients.ErrNotSupported) {
		t.Fatalf("a staging client without the put method: %v, want ErrNotSupported", err)
	}
}

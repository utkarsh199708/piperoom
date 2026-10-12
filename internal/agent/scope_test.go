package agent

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"

	"github.com/anthropics/anthropic-sdk-go"

	"piperoom/internal/owners"
	"piperoom/internal/tools"
)

// scopeProbe is a read tool that records the scope the runner gave it.
type scopeProbe struct {
	owners *owners.Owners

	mu     sync.Mutex
	scopes []tools.Scope
	errs   []error
}

func (p *scopeProbe) Name() string                { return "connect_status" }
func (p *scopeProbe) Description() string         { return "scope probe" }
func (p *scopeProbe) InputSchema() map[string]any { return testSchema("connector") }
func (p *scopeProbe) Kind() tools.Kind            { return tools.KindRead }
func (p *scopeProbe) Run(ctx context.Context, _ json.RawMessage) (tools.Result, error) {
	sc, err := tools.ScopeFor(ctx, p.owners)
	p.mu.Lock()
	p.scopes = append(p.scopes, sc)
	p.errs = append(p.errs, err)
	p.mu.Unlock()
	return tools.Result{Summary: "ok"}, err
}

func probeOwners() *owners.Owners {
	return &owners.Owners{Pipelines: []owners.Pipeline{
		{ID: "orders", TopicPrefix: "pg", SourceTables: []string{"public.orders"}, Connectors: []string{"pg-orders-connector"}},
		{ID: "billing", TopicPrefix: "bill", SourceTables: []string{"public.invoices"}, Connectors: []string{"bill-connector"}},
	}}
}

func probeModel(t *testing.T) *scriptedModel {
	return &scriptedModel{turns: []anthropic.BetaMessage{
		mustMessage(t, `{"stop_reason":"tool_use","content":[
			{"type":"tool_use","id":"tu1","name":"connect_status","input":{"why":"check","connector":"pg-orders-connector"}}
		]}`),
	}}
}

// The runner scopes every read tool to the alert's pipeline, whatever pipeline
// the caller's own context carried.
func TestRunScopesReadToolsToTheAlertPipeline(t *testing.T) {
	probe := &scopeProbe{owners: probeOwners()}
	r := newTestRunner(t, probeModel(t), &fakeInbox{waitErr: context.Canceled}, &fakeSink{}, probe, Options{MaxTurns: 3})

	ctx := tools.WithPipelineID(context.Background(), "billing") // must not win
	if err := r.run(ctx); err != nil {
		t.Fatalf("run: %v", err)
	}
	if len(probe.scopes) != 1 {
		t.Fatalf("tool ran %d times, want 1", len(probe.scopes))
	}
	if probe.errs[0] != nil || probe.scopes[0].Pipeline().ID != "orders" {
		t.Fatalf("scope = %+v, err = %v; want pipeline orders", probe.scopes[0].Pipeline(), probe.errs[0])
	}
}

// An alert with no pipeline gives its tools no scope: refused by default.
func TestRunAlertWithoutPipelineRefusesReadTools(t *testing.T) {
	probe := &scopeProbe{owners: probeOwners()}
	r := newTestRunner(t, probeModel(t), &fakeInbox{waitErr: context.Canceled}, &fakeSink{}, probe, Options{MaxTurns: 3})
	r.alert.PipelineID = ""

	if err := r.run(context.Background()); err != nil {
		t.Fatalf("run: %v", err)
	}
	if len(probe.errs) != 1 || !errors.Is(probe.errs[0], tools.ErrNoScope) {
		t.Fatalf("errs = %v, want ErrNoScope", probe.errs)
	}
}

// The caller of Run can opt out explicitly for CLI/dev use with no room.
func TestRunOptOutIsHonouredOnlyWithoutAPipeline(t *testing.T) {
	probe := &scopeProbe{owners: probeOwners()}
	r := newTestRunner(t, probeModel(t), &fakeInbox{waitErr: context.Canceled}, &fakeSink{}, probe, Options{MaxTurns: 3})
	r.alert.PipelineID = ""

	if err := r.run(tools.WithUnscoped(context.Background())); err != nil {
		t.Fatalf("run: %v", err)
	}
	if probe.errs[0] != nil || !probe.scopes[0].Unscoped() {
		t.Fatalf("scope = %+v, err = %v; want unscoped", probe.scopes[0], probe.errs[0])
	}

	// With a pipeline on the alert the opt-out changes nothing.
	probe2 := &scopeProbe{owners: probeOwners()}
	r2 := newTestRunner(t, probeModel(t), &fakeInbox{waitErr: context.Canceled}, &fakeSink{}, probe2, Options{MaxTurns: 3})
	if err := r2.run(tools.WithUnscoped(context.Background())); err != nil {
		t.Fatalf("run: %v", err)
	}
	if probe2.scopes[0].Unscoped() || probe2.scopes[0].Pipeline().ID != "orders" {
		t.Fatalf("scope = %+v; want pipeline orders, not unscoped", probe2.scopes[0])
	}
}

// Verification and baseline reads go through runStep, which scopes them too.
func TestRunStepScopesToTheAlertPipeline(t *testing.T) {
	probe := &scopeProbe{owners: probeOwners()}
	r := newTestRunner(t, &scriptedModel{}, &fakeInbox{waitErr: context.Canceled}, &fakeSink{}, probe, Options{MaxTurns: 3})
	r.toolList = buildToolList(r.deps.Tools, nil)

	if _, err := r.runStep(context.Background(), "verify", "connect_status", json.RawMessage(`{"connector":"pg-orders-connector"}`)); err != nil {
		t.Fatalf("runStep: %v", err)
	}
	if probe.scopes[0].Pipeline().ID != "orders" {
		t.Fatalf("scope = %+v; want pipeline orders", probe.scopes[0].Pipeline())
	}

	r.alert.PipelineID = ""
	if _, err := r.runStep(context.Background(), "verify", "connect_status", json.RawMessage(`{"connector":"pg-orders-connector"}`)); !errors.Is(err, tools.ErrNoScope) {
		t.Fatalf("err = %v, want ErrNoScope", err)
	}
}

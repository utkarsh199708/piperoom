package setupactions

import (
	"encoding/json"
	"errors"
	"testing"

	"piperoom/internal/setup/plan"
	"piperoom/internal/setup/protocol"
	"piperoom/internal/setup/remoteclients"
	"piperoom/internal/tools"
)

// previewData is what a fake preview op returns.
var previewData = json.RawMessage(`{
	"target": "public.orders",
	"current": "no SELECT for piperoom_reader",
	"expected": "SELECT on public.orders",
	"reversible": true,
	"rollback": "source_revoke_select"
}`)

func TestPreviewSendsOnlyTheT0PreviewOp(t *testing.T) {
	f := &fakeRunner{answer: func(req protocol.OpRequest) (protocol.OpResult, error) {
		return protocol.OpResult{ID: req.ID, OK: true, Summary: "preview", Data: previewData}, nil
	}}
	a := newTestAdapter(t, f)
	res, err := a.Preview(testContext(t, grantParams), grantParams)
	if err != nil {
		t.Fatalf("Preview: %v", err)
	}
	if res.Target != "public.orders" || !res.Reversible || res.Rollback != "source_revoke_select" {
		t.Fatalf("preview = %+v", res)
	}
	calls := f.recorded()
	if len(calls) != 1 {
		t.Fatalf("Preview sent %d ops, want exactly 1", len(calls))
	}
	req := calls[0].req
	if req.Op != "preview_source_grant_select" {
		t.Fatalf("op = %q, want the preview op, never the write op", req.Op)
	}
	if req.Op == "source_grant_select" {
		t.Fatal("Preview sent the write op")
	}
	if req.Tier != protocol.TierT0 {
		t.Fatalf("tier = %q, want T0", req.Tier)
	}
	if req.ProposalID != "" || req.StepIndex != nil || req.ParamsHash != "" {
		t.Fatalf("a preview carried proposal fields: %+v", req)
	}
	if string(req.Params) != string(grantParams) {
		t.Fatalf("params = %s, want the step's", req.Params)
	}
}

func TestPreviewUsesTheClassInTheContext(t *testing.T) {
	f := &fakeRunner{answer: func(req protocol.OpRequest) (protocol.OpResult, error) {
		return protocol.OpResult{ID: req.ID, OK: true, Data: previewData}, nil
	}}
	a := newTestAdapter(t, f)
	ctx := remoteclients.WithClass(testContext(t, grantParams), protocol.ClassProduction)
	if _, err := a.Preview(ctx, grantParams); err != nil {
		t.Fatalf("Preview: %v", err)
	}
	if got := f.recorded()[0].req.Class; got != protocol.ClassProduction {
		t.Fatalf("class = %q, want production", got)
	}
}

func TestPreviewWithoutAPreviewOp(t *testing.T) {
	spec := testSpec()
	spec.PreviewOp = ""
	a, err := New(spec, Deps{Runner: &fakeRunner{}, RoomID: "r_setup"})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if _, err := a.Preview(testContext(t, grantParams), grantParams); err == nil {
		t.Fatal("Preview without a preview op = nil, want an error")
	}
}

func TestPreviewRefusesACutResult(t *testing.T) {
	f := &fakeRunner{answer: func(req protocol.OpRequest) (protocol.OpResult, error) {
		return protocol.OpResult{ID: req.ID, OK: true, Data: previewData, Truncated: true}, nil
	}}
	a := newTestAdapter(t, f)
	if _, err := a.Preview(testContext(t, grantParams), grantParams); !errors.Is(err, ErrBadPreview) {
		t.Fatalf("Preview of a cut result = %v, want ErrBadPreview", err)
	}
}

func TestPreviewRefusesMissingOrBadData(t *testing.T) {
	for name, data := range map[string]json.RawMessage{
		"none":  nil,
		"bad":   json.RawMessage(`[1,2]`),
		"empty": json.RawMessage(`{}`),
	} {
		t.Run(name, func(t *testing.T) {
			f := &fakeRunner{answer: func(req protocol.OpRequest) (protocol.OpResult, error) {
				return protocol.OpResult{ID: req.ID, OK: true, Data: data}, nil
			}}
			a := newTestAdapter(t, f)
			res, err := a.Preview(testContext(t, grantParams), grantParams)
			if name == "empty" {
				// An empty PreviewResult is still a valid (if terse) answer.
				if err != nil {
					t.Fatalf("Preview(empty) = %v", err)
				}
				if res.Target != "" || res.Reversible {
					t.Fatalf("preview = %+v, want the zero result", res)
				}
				return
			}
			if !errors.Is(err, ErrBadPreview) {
				t.Fatalf("Preview(%s) = %v, want ErrBadPreview", name, err)
			}
		})
	}
}

func TestPreviewWithoutARunner(t *testing.T) {
	a, err := New(testSpec(), Deps{})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if _, err := a.Preview(testContext(t, grantParams), grantParams); !errors.Is(err, ErrNoRunner) {
		t.Fatalf("Preview without a runner = %v, want ErrNoRunner", err)
	}
}

func TestPreviewRefusesAnOutOfScopeTargetBeforeTheRunner(t *testing.T) {
	f := &fakeRunner{}
	a := newTestAdapter(t, f)
	out := json.RawMessage(`{"role":"piperoom_reader","tables":["public.customers"]}`)
	ctx := testContext(t, out)
	if _, err := a.Preview(ctx, out); !errors.Is(err, tools.ErrOutOfScope) {
		t.Fatalf("Preview of an out-of-scope target = %v, want ErrOutOfScope", err)
	}
	if got := len(f.recorded()); got != 0 {
		t.Fatalf("an out-of-scope preview sent %d ops", got)
	}
}

func TestPreviewWithoutAPipeline(t *testing.T) {
	f := &fakeRunner{}
	a := newTestAdapter(t, f)
	ctx := tools.WithPipelineID(testContext(t, grantParams), "")
	if _, err := a.Preview(ctx, grantParams); !errors.Is(err, tools.ErrNoScope) {
		t.Fatalf("Preview without a pipeline = %v, want ErrNoScope", err)
	}
}

func TestPreviewMapsParamsForARealPreviewOp(t *testing.T) {
	var spec *Spec
	for _, s := range EnvironmentSpecs() {
		if s.Name == "compose_up" {
			spec = &s
			break
		}
	}
	if spec == nil {
		t.Fatal("compose_up is not in EnvironmentSpecs")
	}
	f := &fakeRunner{answer: func(req protocol.OpRequest) (protocol.OpResult, error) {
		return protocol.OpResult{ID: req.ID, OK: true, Data: json.RawMessage(`{"target":"stack","reversible":true}`)}, nil
	}}
	a, err := New(*spec, Deps{Runner: f, RoomID: "r_setup"})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	params := json.RawMessage(`{"file":"compose.yaml","services":["connect"]}`)
	ctx := tools.WithPipeline(testContext(t, params), testPipeline())
	if _, err := a.Preview(ctx, params); err != nil {
		t.Fatalf("Preview: %v", err)
	}
	req := f.recorded()[0].req
	if req.Op != "compose_config" || req.Tier != plan.TierT0 {
		t.Fatalf("op/tier = %q/%q, want compose_config at T0", req.Op, req.Tier)
	}
	if string(req.Params) != `{"file":"compose.yaml"}` {
		t.Fatalf("preview params = %s, want services dropped for compose_config", req.Params)
	}
}

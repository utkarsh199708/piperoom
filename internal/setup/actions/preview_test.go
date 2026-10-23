package setupactions

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"unicode/utf8"

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
		"none": nil,
		"bad":  json.RawMessage(`[1,2]`),
		// An empty result, or Data of another shape that decodes to the zero
		// PreviewResult, must never count as a preview that passed.
		"empty":           json.RawMessage(`{}`),
		"unknown shape":   json.RawMessage(`{"file":"compose.yaml","body":"services: {}"}`),
		"only reversible": json.RawMessage(`{"reversible":true,"rollback":"x"}`),
	} {
		t.Run(name, func(t *testing.T) {
			f := &fakeRunner{answer: func(req protocol.OpRequest) (protocol.OpResult, error) {
				return protocol.OpResult{ID: req.ID, OK: true, Data: data}, nil
			}}
			a := newTestAdapter(t, f)
			_, err := a.Preview(testContext(t, grantParams), grantParams)
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
		// The real compose_config result shape (internal/setup/targets/compose).
		return protocol.OpResult{ID: req.ID, OK: true, Data: realComposeConfigData}, nil
	}}
	a, err := New(*spec, Deps{Runner: f, RoomID: "r_setup"})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	params := json.RawMessage(`{"file":"compose.yaml","services":["connect"]}`)
	ctx := tools.WithPipeline(testContext(t, params), testPipeline())
	res, err := a.Preview(ctx, params)
	if err != nil {
		t.Fatalf("Preview: %v", err)
	}
	if res.Target != "compose.yaml" {
		t.Fatalf("preview = %+v, want target = the file", res)
	}
	if res.Expected != "start services connect from compose.yaml in the background" {
		t.Fatalf("expected = %q, want one sentence naming the services and the file", res.Expected)
	}
	if res.Current != "compose config --no-interpolate of compose.yaml" {
		t.Fatalf("current = %q, want the read named", res.Current)
	}
	if strings.Contains(res.Expected, "image:") || strings.Contains(res.Current, "image:") {
		t.Fatalf("the render leaked into a sentence: %+v", res)
	}
	if res.DryRun == nil || !res.DryRun.OK || res.DryRun.Source != "compose config" || !strings.Contains(res.DryRun.Text, "image: debezium/connect") {
		t.Fatalf("dry run = %+v, want the render under DryRun", res.DryRun)
	}
	if res.Reversible {
		t.Fatalf("preview = %+v, a compose_config render says nothing about reversibility", res)
	}
	req := f.recorded()[0].req
	if req.Op != "compose_config" || req.Tier != plan.TierT0 {
		t.Fatalf("op/tier = %q/%q, want compose_config at T0", req.Op, req.Tier)
	}
	if string(req.Params) != `{"file":"compose.yaml"}` {
		t.Fatalf("preview params = %s, want services dropped for compose_config", req.Params)
	}
}

// realComposeConfigData is compose_config's Data as the runner sends it
// (composeConfigData in internal/setup/targets/compose/docker.go).
var realComposeConfigData = json.RawMessage(`{
	"file": "compose.yaml",
	"body": "services:\n  connect:\n    image: debezium/connect:2.7\n    environment:\n      DB_PASSWORD: '***'\n",
	"bytes": 96,
	"masked_keys": ["DB_PASSWORD"]
}`)

func TestComposePreviewRefusesAnUnusableRender(t *testing.T) {
	var spec Spec
	for _, s := range EnvironmentSpecs() {
		if s.Name == "compose_down" {
			spec = s
		}
	}
	for name, data := range map[string]json.RawMessage{
		"empty":                json.RawMessage(`{}`),
		"preview result shape": json.RawMessage(`{"target":"stack","reversible":true}`),
		"no body":              json.RawMessage(`{"file":"compose.yaml","body":"","bytes":0}`),
		"no file":              json.RawMessage(`{"body":"services: {}","bytes":12}`),
		"cut render":           json.RawMessage(`{"file":"compose.yaml","body":"services:","bytes":9,"truncated":true}`),
		"not an object":        json.RawMessage(`["compose.yaml"]`),
	} {
		t.Run(name, func(t *testing.T) {
			f := &fakeRunner{answer: func(req protocol.OpRequest) (protocol.OpResult, error) {
				return protocol.OpResult{ID: req.ID, OK: true, Data: data}, nil
			}}
			a, err := New(spec, Deps{Runner: f, RoomID: "r_setup"})
			if err != nil {
				t.Fatalf("New: %v", err)
			}
			params := json.RawMessage(`{"file":"compose.yaml"}`)
			if _, err := a.Preview(tools.WithPipeline(testContext(t, params), testPipeline()), params); !errors.Is(err, ErrBadPreview) {
				t.Fatalf("Preview(%s) = %v, want ErrBadPreview", data, err)
			}
		})
	}
}

func composeSpec(t *testing.T, name string) Spec {
	t.Helper()
	for _, s := range EnvironmentSpecs() {
		if s.Name == name {
			return s
		}
	}
	t.Fatalf("%s is not in EnvironmentSpecs", name)
	return Spec{}
}

func TestComposePreviewSentencesPerOp(t *testing.T) {
	cases := []struct {
		action string
		params string
		want   string
	}{
		{"compose_up", `{}`, "start every service of compose.yaml in the background"},
		{"compose_up", `{"services":["kafka","connect"]}`, "start services kafka, connect from compose.yaml in the background"},
		{"compose_restart", `{}`, "restart every service of compose.yaml in place; volumes kept"},
		{"compose_restart", `{"services":["connect"]}`, "restart services connect of compose.yaml in place; volumes kept"},
		{"compose_down", `{}`, "stop the stack of compose.yaml; volumes kept"},
		{"compose_down", `{"file":"compose.yaml","services":["registry"]}`, "stop services registry of compose.yaml; volumes kept"},
	}
	for _, tc := range cases {
		t.Run(tc.action+tc.params, func(t *testing.T) {
			f := &fakeRunner{answer: func(req protocol.OpRequest) (protocol.OpResult, error) {
				return protocol.OpResult{ID: req.ID, OK: true, Data: realComposeConfigData}, nil
			}}
			a, err := New(composeSpec(t, tc.action), Deps{Runner: f, RoomID: "r_setup"})
			if err != nil {
				t.Fatalf("New: %v", err)
			}
			params := json.RawMessage(tc.params)
			res, err := a.Preview(tools.WithPipeline(testContext(t, params), testPipeline()), params)
			if err != nil {
				t.Fatalf("Preview: %v", err)
			}
			if res.Expected != tc.want {
				t.Fatalf("expected = %q, want %q", res.Expected, tc.want)
			}
			if res.Current != "compose config --no-interpolate of compose.yaml" {
				t.Fatalf("current = %q", res.Current)
			}
		})
	}
}

func TestComposePreviewCapsTheDryRunText(t *testing.T) {
	// A render well over 2 KiB, with a multi-byte rune straddling the cap.
	body := "services:\n" + strings.Repeat("a", maxDryRunText-12) + strings.Repeat("é", 400)
	data, err := json.Marshal(map[string]any{"file": "compose.yaml", "body": body, "bytes": len(body)})
	if err != nil {
		t.Fatal(err)
	}
	f := &fakeRunner{answer: func(req protocol.OpRequest) (protocol.OpResult, error) {
		return protocol.OpResult{ID: req.ID, OK: true, Data: data}, nil
	}}
	a, err := New(composeSpec(t, "compose_down"), Deps{Runner: f, RoomID: "r_setup"})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	params := json.RawMessage(`{}`)
	res, err := a.Preview(tools.WithPipeline(testContext(t, params), testPipeline()), params)
	if err != nil {
		t.Fatalf("Preview: %v", err)
	}
	if res.DryRun == nil || len(res.DryRun.Text) > maxDryRunText || len(res.DryRun.Text) < maxDryRunText-4 {
		t.Fatalf("dry run text is %d bytes, want just under %d", len(res.DryRun.Text), maxDryRunText)
	}
	if !utf8.ValidString(res.DryRun.Text) {
		t.Fatal("dry run text was cut inside a rune")
	}
	if len(res.Expected) > 200 || len(res.Current) > 200 {
		t.Fatalf("sentences are not short: %q / %q", res.Expected, res.Current)
	}
}

func TestComposePreviewSanitisesTheRunnerFileName(t *testing.T) {
	data := json.RawMessage(`{"file":"comp\u001b[31mose.yaml\n","body":"services: {}","bytes":12}`)
	f := &fakeRunner{answer: func(req protocol.OpRequest) (protocol.OpResult, error) {
		return protocol.OpResult{ID: req.ID, OK: true, Data: data}, nil
	}}
	a, err := New(composeSpec(t, "compose_up"), Deps{Runner: f, RoomID: "r_setup"})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	params := json.RawMessage(`{}`)
	res, err := a.Preview(tools.WithPipeline(testContext(t, params), testPipeline()), params)
	if err != nil {
		t.Fatalf("Preview: %v", err)
	}
	for _, s := range []string{res.Target, res.Current, res.Expected} {
		if strings.ContainsAny(s, "\x1b\n") {
			t.Fatalf("a control character reached a preview sentence: %q", s)
		}
	}
}

func TestComposePreviewRefusesABadServiceName(t *testing.T) {
	f := &fakeRunner{answer: func(req protocol.OpRequest) (protocol.OpResult, error) {
		return protocol.OpResult{ID: req.ID, OK: true, Data: realComposeConfigData}, nil
	}}
	a, err := New(composeSpec(t, "compose_up"), Deps{Runner: f, RoomID: "r_setup"})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	for _, svc := range []string{"<b>x</b>", "a b", "-x", "x\\u0007"} {
		params := json.RawMessage(`{"services":["` + svc + `"]}`)
		if _, err := a.Preview(tools.WithPipeline(testContext(t, params), testPipeline()), params); !errors.Is(err, ErrBadPreview) {
			t.Fatalf("Preview(services %q) = %v, want ErrBadPreview", svc, err)
		}
	}
}

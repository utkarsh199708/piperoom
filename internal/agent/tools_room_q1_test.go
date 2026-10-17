package agent

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"
	"testing"

	"github.com/anthropics/anthropic-sdk-go"

	"piperoom/internal/actions"
	"piperoom/internal/events"
	"piperoom/internal/tools"
)

// ---- fakes ----

type pageCall struct{ layer, reason, proposalID string }

// fakePager records every page and answers with owners, or err.
type fakePager struct {
	calls  []pageCall
	owners []string
	err    error
}

func (f *fakePager) Page(_ context.Context, layer, reason, proposalID string) ([]string, error) {
	f.calls = append(f.calls, pageCall{layer, reason, proposalID})
	return f.owners, f.err
}

type withdrawCall struct{ id, reason string }

// withdrawingProposals is a Proposals that can also withdraw, and hands out
// p_1, p_2, ... so a test can tell its proposals apart.
type withdrawingProposals struct {
	n         int
	created   []events.ProposalCreatedPayload
	withdrawn []withdrawCall
	err       error
}

func (w *withdrawingProposals) Create(_ context.Context, p events.ProposalCreatedPayload) (string, error) {
	w.n++
	w.created = append(w.created, p)
	return fmt.Sprintf("p_%d", w.n), nil
}

func (w *withdrawingProposals) Withdraw(_ context.Context, id, reason string) error {
	w.withdrawn = append(w.withdrawn, withdrawCall{id, reason})
	return w.err
}

// roomToolsFor builds the room tools of deps and returns them by name.
func roomToolsFor(t *testing.T, d Deps) map[string]tools.Tool {
	t.Helper()
	if d.Sink == nil {
		d.Sink = &fakeSink{}
	}
	byName := map[string]tools.Tool{}
	for _, tl := range newRoomTools(d, nil) {
		byName[tl.Name()] = tl
	}
	return byName
}

func runTool(t *testing.T, tl tools.Tool, input string) (tools.Result, error) {
	t.Helper()
	if tl == nil {
		t.Fatal("tool is not offered")
	}
	return tl.Run(context.Background(), json.RawMessage(input))
}

// ---- which tools a run offers ----

func TestRoomToolsOfferedByDeps(t *testing.T) {
	base := []string{"update_hypothesis", "propose_fix", "post_message"}
	cases := []struct {
		name string
		deps Deps
		want []string
	}{
		{"nothing optional", Deps{}, base},
		{"plain proposals cannot withdraw", Deps{Proposals: &fakeProposals{id: "p_1"}}, base},
		{"pager only", Deps{Pager: &fakePager{}}, append(slices.Clone(base), "page_owner")},
		{"withdrawing proposals only", Deps{Proposals: &withdrawingProposals{}}, append(slices.Clone(base), "withdraw_proposal")},
		{"both", Deps{Pager: &fakePager{}, Proposals: &withdrawingProposals{}}, append(slices.Clone(base), "page_owner", "withdraw_proposal")},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			var got []string
			for _, tl := range newRoomTools(c.deps, nil) {
				got = append(got, tl.Name())
				if tl.Kind() != tools.KindRoom {
					t.Errorf("%s kind = %s, want room", tl.Name(), tl.Kind())
				}
			}
			if !slices.Equal(got, c.want) {
				t.Fatalf("room tools = %v, want %v", got, c.want)
			}
		})
	}
}

// requestOf runs a one-turn investigation over deps and returns the model
// request, so a test sees the tools and system prompt the model was given.
func requestOf(t *testing.T, d Deps) anthropic.BetaMessageNewParams {
	t.Helper()
	m := &funcModel{fn: func(context.Context, int, anthropic.BetaMessageNewParams) (anthropic.BetaMessage, error) {
		return mustMessage(t, `{"stop_reason":"end_turn","content":[{"type":"text","text":"done"}]}`), nil
	}}
	d.Tools = tools.NewRegistry()
	d.Inbox = &fakeInbox{waitErr: context.Canceled}
	d.Sink = &fakeSink{}
	d.Logger = discardLogger()
	r, err := newRunnerWith(Alert{Title: "orders stale", PipelineID: "orders"}, d, Options{MaxTurns: 3}, m)
	if err != nil {
		t.Fatalf("newRunnerWith: %v", err)
	}
	if err := r.run(context.Background()); err != nil {
		t.Fatalf("run: %v", err)
	}
	if len(m.reqs) == 0 {
		t.Fatal("the model was never called")
	}
	return m.reqs[0]
}

func requestToolNames(req anthropic.BetaMessageNewParams) []string {
	var names []string
	for _, tp := range req.Tools {
		if tp.OfTool != nil {
			names = append(names, tp.OfTool.Name)
		}
	}
	return names
}

// The model gets page_owner and withdraw_proposal in its request, and the
// prompt that explains them, only when the run can really do them.
func TestRunOffersOptionalRoomToolsAndTheirGuidance(t *testing.T) {
	bare := requestOf(t, Deps{Proposals: &fakeProposals{id: "p_1"}})
	if names := requestToolNames(bare); slices.Contains(names, "page_owner") || slices.Contains(names, "withdraw_proposal") {
		t.Fatalf("a run without a pager or a withdrawer offers %v", names)
	}
	if p := bare.System[0].Text; strings.Contains(p, "page_owner") || strings.Contains(p, "withdraw_proposal") {
		t.Fatal("the prompt names a tool this run does not have")
	}
	if bare.System[0].Text != systemPrompt() {
		t.Fatal("a run with no optional tool must get the plain system prompt")
	}

	full := requestOf(t, Deps{Pager: &fakePager{}, Proposals: &withdrawingProposals{}})
	names := requestToolNames(full)
	for _, want := range []string{"page_owner", "withdraw_proposal"} {
		if !slices.Contains(names, want) {
			t.Errorf("request tools %v lack %s", names, want)
		}
		if !strings.Contains(full.System[0].Text, want) {
			t.Errorf("the prompt does not explain %s", want)
		}
	}
}

// ---- page_owner ----

func TestPageOwnerPagesWithTheInput(t *testing.T) {
	pager := &fakePager{owners: []string{"kabir", "meera"}}
	tool := roomToolsFor(t, Deps{Pager: pager})["page_owner"]

	res, err := runTool(t, tool, `{"layer":"consumer","reason":"  lag keeps growing and no action can fix the consumer  ","proposal_id":"p_7"}`)
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	want := []pageCall{{"consumer", "lag keeps growing and no action can fix the consumer", "p_7"}}
	if !slices.Equal(pager.calls, want) {
		t.Fatalf("pager calls = %+v, want %+v", pager.calls, want)
	}
	for _, w := range []string{"consumer", "kabir", "meera"} {
		if !strings.Contains(res.Summary, w) {
			t.Errorf("summary %q lacks %q", res.Summary, w)
		}
	}

	// proposal_id is optional and then passed on empty.
	pager.calls = nil
	if _, err := runTool(t, tool, `{"layer":"warehouse","reason":"loads stopped"}`); err != nil {
		t.Fatalf("run: %v", err)
	}
	if want := []pageCall{{"warehouse", "loads stopped", ""}}; !slices.Equal(pager.calls, want) {
		t.Fatalf("pager calls = %+v, want %+v", pager.calls, want)
	}
}

func TestPageOwnerWithNoOwnerSaysNobodyWasPaged(t *testing.T) {
	tool := roomToolsFor(t, Deps{Pager: &fakePager{owners: []string{}}})["page_owner"]
	res, err := runTool(t, tool, `{"layer":"kafka","reason":"broker is down"}`)
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	if !strings.Contains(res.Summary, "nobody was paged") {
		t.Fatalf("summary = %q, want it to say nobody was paged", res.Summary)
	}
}

func TestPageOwnerListsAtMostTenOwners(t *testing.T) {
	var owners []string
	for i := range 14 {
		owners = append(owners, fmt.Sprintf("user%02d", i))
	}
	tool := roomToolsFor(t, Deps{Pager: &fakePager{owners: owners}})["page_owner"]
	res, err := runTool(t, tool, `{"layer":"kafka","reason":"x"}`)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(res.Summary, "user09") || strings.Contains(res.Summary, "user10") || !strings.Contains(res.Summary, "and 4 more") {
		t.Fatalf("summary = %q", res.Summary)
	}
}

func TestPageOwnerRefusesBadInput(t *testing.T) {
	long := strings.Repeat("é", pageReasonMaxChars+1) // 301 characters, 602 bytes
	cases := []struct {
		name, input, want string
	}{
		{"not json", `{"layer":`, ""},
		{"unknown layer", `{"layer":"datalake","reason":"x"}`, "layer must be one of: postgres, kafka-connect"},
		{"empty layer", `{"layer":"","reason":"x"}`, "layer must be one of"},
		{"empty reason", `{"layer":"kafka","reason":""}`, "reason is required"},
		{"blank reason", `{"layer":"kafka","reason":"  \n\t "}`, "reason is required"},
		{"markup only reason", `{"layer":"kafka","reason":"<br>&nbsp;"}`, "reason is required"},
		{"reason too long", `{"layer":"kafka","reason":"` + long + `"}`, "reason is 301 characters; keep it to 300"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			pager := &fakePager{}
			_, err := runTool(t, roomToolsFor(t, Deps{Pager: pager})["page_owner"], c.input)
			if !errors.Is(err, tools.ErrBadInput) {
				t.Fatalf("err = %v, want ErrBadInput", err)
			}
			if !strings.Contains(err.Error(), c.want) {
				t.Fatalf("err = %q, want it to contain %q", err, c.want)
			}
			if len(pager.calls) != 0 {
				t.Fatalf("a refused call paged: %+v", pager.calls)
			}
		})
	}

	// 300 characters is allowed, counted as characters and not bytes.
	pager := &fakePager{}
	ok := strings.Repeat("é", pageReasonMaxChars)
	if _, err := runTool(t, roomToolsFor(t, Deps{Pager: pager})["page_owner"], `{"layer":"kafka","reason":"`+ok+`"}`); err != nil {
		t.Fatalf("a 300 character reason was refused: %v", err)
	}
}

// The model's input is also checked against the schema by the runner, so a
// layer outside the enum never reaches Run.
func TestPageOwnerSchemaFixesTheLayers(t *testing.T) {
	r, _ := proposeRunner(t, realActions(t))
	r.deps.Pager = &fakePager{}
	r.toolList = buildToolList(r.deps.Tools, newRoomTools(r.deps, r.proposals))

	res := r.runRoomTool(context.Background(), toolUse{id: "tu_1", name: "page_owner", input: json.RawMessage(`{"layer":"datalake","reason":"x"}`)})
	if !res.isError || !strings.Contains(res.content, "invalid input") {
		t.Fatalf("a layer outside the enum was accepted: %q", res.content)
	}
	res = r.runRoomTool(context.Background(), toolUse{id: "tu_2", name: "page_owner", input: json.RawMessage(`{"layer":"kafka"}`)})
	if !res.isError || !strings.Contains(res.content, `missing required field "reason"`) {
		t.Fatalf("a call without a reason was accepted: %q", res.content)
	}
	res = r.runRoomTool(context.Background(), toolUse{id: "tu_3", name: "page_owner", input: json.RawMessage(`{"layer":"kafka","reason":"broker down"}`)})
	if res.isError {
		t.Fatalf("a valid page was refused: %q", res.content)
	}

	schema := newPageOwnerTool(&fakePager{}).InputSchema()
	layer := schema["properties"].(map[string]any)["layer"].(map[string]any)
	if got := layer["enum"].([]string); !slices.Equal(got, events.Layers) {
		t.Fatalf("layer enum = %v, want events.Layers %v", got, events.Layers)
	}
	if req := schema["required"].([]string); !slices.Equal(req, []string{"layer", "reason"}) {
		t.Fatalf("required = %v: proposal_id must stay optional", req)
	}
}

func TestPageOwnerPassesOnTheServersRefusalAndMayRetry(t *testing.T) {
	pager := &fakePager{err: errors.New("room: owners of consumer were paged 3 minutes ago")}
	tool := roomToolsFor(t, Deps{Pager: pager})["page_owner"]
	_, err := runTool(t, tool, `{"layer":"consumer","reason":"still down"}`)
	if err == nil || !strings.Contains(err.Error(), "paged 3 minutes ago") {
		t.Fatalf("err = %v, want the server's words", err)
	}

	// A refused page did not go out, so it is not counted: the same page may
	// be tried again later and succeeds.
	pager.err = nil
	pager.owners = []string{"kabir"}
	if _, err := runTool(t, tool, `{"layer":"consumer","reason":"still down"}`); err != nil {
		t.Fatalf("retry after a refusal failed: %v", err)
	}
	if len(pager.calls) != 2 {
		t.Fatalf("pager calls = %d, want 2", len(pager.calls))
	}
}

func TestPageOwnerNeverPagesTheSameLayerTwice(t *testing.T) {
	pager := &fakePager{owners: []string{"kabir"}}
	tool := roomToolsFor(t, Deps{Pager: pager})["page_owner"]
	if _, err := runTool(t, tool, `{"layer":"consumer","reason":"first"}`); err != nil {
		t.Fatal(err)
	}
	_, err := runTool(t, tool, `{"layer":"consumer","reason":"worded differently"}`)
	if err == nil || !strings.Contains(err.Error(), "do not page them again") {
		t.Fatalf("second page of the same layer: err = %v", err)
	}
	if len(pager.calls) != 1 {
		t.Fatalf("pager calls = %d, want 1: a repeat must not reach the pager", len(pager.calls))
	}
	// Another layer, or the same layer for a proposal that now waits on it, is
	// a different ask.
	for _, in := range []string{
		`{"layer":"warehouse","reason":"loads stopped"}`,
		`{"layer":"consumer","reason":"p_2 waits on you","proposal_id":"p_2"}`,
	} {
		if _, err := runTool(t, tool, in); err != nil {
			t.Fatalf("%s: %v", in, err)
		}
	}
}

func TestPageOwnerIsCappedPerRun(t *testing.T) {
	pager := &fakePager{owners: []string{"kabir"}}
	tool := roomToolsFor(t, Deps{Pager: pager})["page_owner"]
	for i := range pagesPerRun {
		in := fmt.Sprintf(`{"layer":"kafka","reason":"x","proposal_id":"p_%d"}`, i)
		if _, err := runTool(t, tool, in); err != nil {
			t.Fatalf("page %d: %v", i, err)
		}
	}
	_, err := runTool(t, tool, `{"layer":"warehouse","reason":"one more"}`)
	if err == nil || !strings.Contains(err.Error(), "limit") {
		t.Fatalf("page over the cap: err = %v", err)
	}
	if len(pager.calls) != pagesPerRun {
		t.Fatalf("pager calls = %d, want %d", len(pager.calls), pagesPerRun)
	}
}

// ---- withdraw_proposal ----

// proposeOne makes the agent's propose_fix tool create a one-step proposal.
func proposeOne(t *testing.T, tl map[string]tools.Tool, title string) string {
	t.Helper()
	in := fmt.Sprintf(`{"title":%q,"steps":[{"action":"connect_restart_connector","params":"{\"connector\":\"pg-orders-connector\"}","description":"restart"}],"risk":"low","rollback":"restart is repeatable, nothing to undo"}`, title)
	res, err := runTool(t, tl["propose_fix"], in)
	if err != nil {
		t.Fatalf("propose_fix: %v", err)
	}
	return res.Data.(proposeFixResult).ProposalID
}

func roomToolsWithActions(t *testing.T, d Deps) map[string]tools.Tool {
	t.Helper()
	d.Actions = realActions(t)
	return roomToolsFor(t, d)
}

func TestWithdrawProposalWithdrawsAnOwnProposal(t *testing.T) {
	store := &withdrawingProposals{}
	tl := roomToolsWithActions(t, Deps{Proposals: store})
	first := proposeOne(t, tl, "restart the connector")
	second := proposeOne(t, tl, "restart it again")
	if first != "p_1" || second != "p_2" {
		t.Fatalf("ids = %s, %s", first, second)
	}

	res, err := runTool(t, tl["withdraw_proposal"], `{"proposal_id":"p_2","reason":"  the slot is gone, a restart cannot help  "}`)
	if err != nil {
		t.Fatalf("withdraw: %v", err)
	}
	if want := []withdrawCall{{"p_2", "the slot is gone, a restart cannot help"}}; !slices.Equal(store.withdrawn, want) {
		t.Fatalf("withdrawn = %+v, want %+v", store.withdrawn, want)
	}
	if !strings.Contains(res.Summary, "p_2") || !strings.Contains(res.Summary, "restart it again") {
		t.Fatalf("summary = %q", res.Summary)
	}
}

func TestWithdrawProposalRefusesProposalsOfOthers(t *testing.T) {
	store := &withdrawingProposals{}
	mine := roomToolsWithActions(t, Deps{Proposals: store})
	proposeOne(t, mine, "restart the connector")

	// p_9 was made by the driver or by an earlier run; this run has no right to it.
	_, err := runTool(t, mine["withdraw_proposal"], `{"proposal_id":"p_9","reason":"I want it gone"}`)
	if !errors.Is(err, tools.ErrBadInput) || !strings.Contains(err.Error(), "yours: p_1") {
		t.Fatalf("withdraw of a foreign proposal: err = %v", err)
	}
	// Another run's tools do not share this run's proposals either.
	other := roomToolsWithActions(t, Deps{Proposals: store})
	_, err = runTool(t, other["withdraw_proposal"], `{"proposal_id":"p_1","reason":"not mine"}`)
	if !errors.Is(err, tools.ErrBadInput) || !strings.Contains(err.Error(), "yours: none yet") {
		t.Fatalf("withdraw by a run that made nothing: err = %v", err)
	}
	if len(store.withdrawn) != 0 {
		t.Fatalf("a refused withdrawal reached the store: %+v", store.withdrawn)
	}
}

func TestWithdrawProposalRefusesBadInput(t *testing.T) {
	long := strings.Repeat("x", withdrawReasonMaxChars+1)
	cases := []struct{ name, input, want string }{
		{"not json", `{"proposal_id":`, ""},
		{"empty id", `{"proposal_id":" ","reason":"r"}`, "proposal_id is required"},
		{"empty reason", `{"proposal_id":"p_1","reason":""}`, "reason is required"},
		{"markup reason", `{"proposal_id":"p_1","reason":"<p></p>"}`, "reason is required"},
		{"reason too long", `{"proposal_id":"p_1","reason":"` + long + `"}`, "reason is 501 characters; keep it to 500"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			store := &withdrawingProposals{}
			tl := roomToolsWithActions(t, Deps{Proposals: store})
			proposeOne(t, tl, "restart")
			_, err := runTool(t, tl["withdraw_proposal"], c.input)
			if !errors.Is(err, tools.ErrBadInput) || !strings.Contains(err.Error(), c.want) {
				t.Fatalf("err = %v, want ErrBadInput containing %q", err, c.want)
			}
			if len(store.withdrawn) != 0 {
				t.Fatalf("a refused call withdrew: %+v", store.withdrawn)
			}
		})
	}
	// 500 characters is allowed.
	store := &withdrawingProposals{}
	tl := roomToolsWithActions(t, Deps{Proposals: store})
	proposeOne(t, tl, "restart")
	if _, err := runTool(t, tl["withdraw_proposal"], `{"proposal_id":"p_1","reason":"`+strings.Repeat("x", withdrawReasonMaxChars)+`"}`); err != nil {
		t.Fatalf("a 500 character reason was refused: %v", err)
	}
}

func TestWithdrawProposalPassesOnTheServersRefusal(t *testing.T) {
	store := &withdrawingProposals{err: errors.New("room: proposal is already executing")}
	tl := roomToolsWithActions(t, Deps{Proposals: store})
	proposeOne(t, tl, "restart")
	_, err := runTool(t, tl["withdraw_proposal"], `{"proposal_id":"p_1","reason":"too late?"}`)
	if err == nil || !strings.Contains(err.Error(), "already executing") {
		t.Fatalf("err = %v, want the server's words", err)
	}
	if errors.Is(err, tools.ErrBadInput) {
		t.Fatal("a server refusal is not bad input")
	}
}

// A proposal made through NewProposeFixTool (the evals' tool) has no record of
// ownership and nothing to withdraw with.
func TestNewProposeFixToolNeedsNoOwnership(t *testing.T) {
	store := &withdrawingProposals{}
	tool := NewProposeFixTool(realActions(t), store)
	if _, err := tool.Run(context.Background(), json.RawMessage(`{"title":"t","steps":[{"action":"connect_restart_connector","params":{"connector":"pg-orders-connector"},"description":"d"}],"risk":"low","rollback":"restart is repeatable, nothing to undo"}`)); err != nil {
		t.Fatalf("run: %v", err)
	}
}

// ---- propose_fix: impacted layers and risk level (G5) ----

const connectorFix = `"steps":[{"action":"connect_update_connector_config","params":"{\"connector\":\"pg-orders-connector\",\"key\":\"decimal.handling.mode\",\"value\":\"string\"}","description":"carry decimals as strings"}]`

// The fixture step is one the real action registry accepts, and the action
// itself reports consumer+warehouse at medium risk. The agent's own risk_level
// is a separate field: the cases below pass "high" through unchanged.
func TestConnectorFixImpactIsConsumerAndWarehouse(t *testing.T) {
	act, ok := realActions(t).Get("connect_update_connector_config")
	if !ok {
		t.Fatal("connect_update_connector_config is not registered")
	}
	imp, ok := act.(actions.Impacting)
	if !ok {
		t.Fatalf("%T does not implement actions.Impacting", act)
	}
	got := imp.Impact(json.RawMessage(`{"connector":"pg-orders-connector","key":"decimal.handling.mode","value":"string"}`))
	if !slices.Equal(got.Layers, []string{"consumer", "warehouse"}) || got.RiskLevel != events.RiskMedium {
		t.Fatalf("impact = %v %q, want consumer+warehouse at medium", got.Layers, got.RiskLevel)
	}
}

func TestProposeFixCarriesImpactedLayersAndRisk(t *testing.T) {
	cases := []struct {
		name, extra string
		wantLayers  []string
		wantRisk    string
	}{
		{"neither field", ``, nil, ""},
		{"layers and risk", `,"impacted_layers":["consumer","warehouse"],"risk_level":"high"`, []string{"consumer", "warehouse"}, "high"},
		{"only risk", `,"risk_level":"medium"`, nil, "medium"},
		{"risk is normalised", `,"risk_level":" HIGH "`, nil, "high"},
		{"duplicates are folded", `,"impacted_layers":["warehouse","consumer","warehouse"]`, []string{"warehouse", "consumer"}, ""},
		{"empty list", `,"impacted_layers":[]`, nil, ""},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			store := &withdrawingProposals{}
			tool := roomToolsWithActions(t, Deps{Proposals: store})["propose_fix"]
			in := `{"title":"decimals as strings",` + connectorFix + `,"risk":"precision","rollback":"set precise back"` + c.extra + `}`
			if _, err := runTool(t, tool, in); err != nil {
				t.Fatalf("run: %v", err)
			}
			got := store.created[0]
			if !slices.Equal(got.ImpactedLayers, c.wantLayers) || got.RiskLevel != c.wantRisk {
				t.Fatalf("payload impacted=%v risk=%q, want %v %q", got.ImpactedLayers, got.RiskLevel, c.wantLayers, c.wantRisk)
			}
			if !slices.Equal(got.RequiredLayers, []string{"kafka-connect"}) {
				t.Fatalf("required layers = %v: the step layers only, the server adds the impacted ones", got.RequiredLayers)
			}
		})
	}
}

func TestProposeFixSummaryNamesEveryApprover(t *testing.T) {
	store := &withdrawingProposals{}
	tool := roomToolsWithActions(t, Deps{Proposals: store})["propose_fix"]
	in := `{"title":"t",` + connectorFix + `,"risk":"r","rollback":"b","impacted_layers":["consumer","kafka-connect","warehouse"]}`
	res, err := runTool(t, tool, in)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(res.Summary, "waiting for approval from: kafka-connect, consumer, warehouse") {
		t.Fatalf("summary = %q", res.Summary)
	}
}

func TestProposeFixRefusesBadImpact(t *testing.T) {
	cases := []struct{ name, extra, want string }{
		{"unknown layer", `,"impacted_layers":["consumer","datalake"]`, `"datalake" is not a layer; use: postgres, kafka-connect`},
		{"empty layer", `,"impacted_layers":[""]`, "is not a layer"},
		{"unknown risk", `,"risk_level":"catastrophic"`, "risk_level must be low, medium or high"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			store := &withdrawingProposals{}
			tool := roomToolsWithActions(t, Deps{Proposals: store})["propose_fix"]
			in := `{"title":"t",` + connectorFix + `,"risk":"r","rollback":"b"` + c.extra + `}`
			_, err := runTool(t, tool, in)
			if !errors.Is(err, tools.ErrBadInput) || !strings.Contains(err.Error(), c.want) {
				t.Fatalf("err = %v, want ErrBadInput containing %q", err, c.want)
			}
			if len(store.created) != 0 {
				t.Fatalf("a refused proposal was stored: %+v", store.created)
			}
		})
	}
}

// Through the runner, the schema check refuses a layer or a level outside the
// enum before Run, and a proposal with both fields is stored.
func TestProposeFixImpactThroughTheRunner(t *testing.T) {
	r, proposals := proposeRunner(t, realActions(t))
	bad := `{"title":"t",` + connectorFix + `,"risk":"r","rollback":"b","impacted_layers":["datalake"]}`
	if res := r.runRoomTool(context.Background(), toolUse{id: "tu_1", name: "propose_fix", input: json.RawMessage(bad)}); !res.isError || !strings.Contains(res.content, "impacted_layers") {
		t.Fatalf("a layer outside the enum was accepted: %q", res.content)
	}
	bad = `{"title":"t",` + connectorFix + `,"risk":"r","rollback":"b","risk_level":"extreme"}`
	if res := r.runRoomTool(context.Background(), toolUse{id: "tu_2", name: "propose_fix", input: json.RawMessage(bad)}); !res.isError || !strings.Contains(res.content, "risk_level") {
		t.Fatalf("a risk level outside the enum was accepted: %q", res.content)
	}
	if proposals.created.Title != "" {
		t.Fatal("a refused proposal was stored")
	}
	good := `{"title":"t",` + connectorFix + `,"risk":"r","rollback":"b","impacted_layers":["consumer","warehouse"],"risk_level":"high"}`
	if res := r.runRoomTool(context.Background(), toolUse{id: "tu_3", name: "propose_fix", input: json.RawMessage(good)}); res.isError {
		t.Fatalf("a proposal with impact was refused: %q", res.content)
	}
	if !slices.Equal(proposals.created.ImpactedLayers, []string{"consumer", "warehouse"}) || proposals.created.RiskLevel != "high" {
		t.Fatalf("stored impact = %v %q", proposals.created.ImpactedLayers, proposals.created.RiskLevel)
	}
}

// The strict schema offers both fields, as optional properties with the closed
// sets, and the description tells the model what they are for.
func TestProposeFixSchemaHasImpactFields(t *testing.T) {
	tool := &proposeFixTool{actions: realActions(t)}
	schema := tool.InputSchema()
	props := schema["properties"].(map[string]any)

	layers := props["impacted_layers"].(map[string]any)
	if layers["type"] != "array" {
		t.Fatalf("impacted_layers type = %v", layers["type"])
	}
	items := layers["items"].(map[string]any)
	if got := items["enum"].([]string); !slices.Equal(got, events.Layers) {
		t.Fatalf("impacted_layers items enum = %v, want %v", got, events.Layers)
	}
	risk := props["risk_level"].(map[string]any)
	if got := risk["enum"].([]string); !slices.Equal(got, []string{"low", "medium", "high"}) {
		t.Fatalf("risk_level enum = %v", got)
	}
	if req := schema["required"].([]string); !slices.Equal(req, []string{"title", "steps", "risk", "rollback"}) {
		t.Fatalf("required = %v: the impact fields must stay optional so old callers still validate", req)
	}
	// The model-facing schema keeps them too, and mutating it must not touch
	// the tool's own enum.
	param := toolParam(tool, false, true)
	b, _ := json.Marshal(param)
	for _, w := range []string{`"impacted_layers"`, `"risk_level"`, `"warehouse"`, `"high"`} {
		if !strings.Contains(string(b), w) {
			t.Errorf("tool definition lacks %s: %s", w, b)
		}
	}
	d := tool.Description()
	for _, w := range []string{"impacted_layers", "risk_level", "money"} {
		if !strings.Contains(d, w) {
			t.Errorf("description lacks %q", w)
		}
	}
}

// With all five room tools the strict set (5 room + 3 read tools) must still
// fit the byte budget the live bisect established.
func TestStrictSchemaBytesWithAllRoomTools(t *testing.T) {
	reg, _ := productionToolSet(t)
	areg := actions.NewRegistry()
	if err := actions.Register(areg, actions.Deps{}); err != nil {
		t.Fatal(err)
	}
	room := newRoomTools(Deps{Actions: areg, Pager: &fakePager{}, Proposals: &withdrawingProposals{}}, newProposalTracker())
	if len(room) != 5 {
		t.Fatalf("%d room tools, want 5", len(room))
	}
	got, n := strictSchemaBytes(t, buildToolList(reg, room))
	t.Logf("%d strict tools, %d schema bytes (budget %d)", n, got, maxStrictSchemaBytes)
	if n != maxStrictTools {
		t.Fatalf("%d strict tools, want %d", n, maxStrictTools)
	}
	if got > maxStrictSchemaBytes {
		t.Errorf("strict schemas total %d bytes with every room tool, budget is %d", got, maxStrictSchemaBytes)
	}
	order, strict := strictNames(t, buildToolList(reg, room))
	for _, rt := range room {
		if !strict[rt.Name()] {
			t.Errorf("room tool %s must stay strict (tools %v)", rt.Name(), order)
		}
	}
}

// QA S6: decimal.handling.mode=string with no impacted_layers given must still
// name every approver, because the action's own impact adds consumer and
// warehouse.
func TestProposeFixSummaryNamesActionImpactWithoutImpactedLayers(t *testing.T) {
	store := &withdrawingProposals{}
	tool := roomToolsWithActions(t, Deps{Proposals: store})["propose_fix"]
	in := `{"title":"decimals as strings",` + connectorFix + `,"risk":"precision","rollback":"set precise back"}`
	res, err := runTool(t, tool, in)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(res.Summary, "waiting for approval from: kafka-connect, consumer, warehouse") {
		t.Fatalf("summary = %q", res.Summary)
	}
}

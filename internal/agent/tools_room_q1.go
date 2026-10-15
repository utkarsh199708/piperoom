package agent

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"strings"
	"sync"
	"unicode"
	"unicode/utf8"

	"piperoom/internal/events"
	"piperoom/internal/tools"
)

// The two room tools of the QA run's G2 and G9 gaps. Neither touches a
// customer system: page_owner asks the room service to notify people, and
// withdraw_proposal takes back a proposal the agent made. Each is offered only
// when its collaborator exists (Deps.Pager, a Proposals that implements
// ProposalWithdrawer), so the model is never given a tool that cannot work.

const (
	// pageReasonMaxChars and withdrawReasonMaxChars are the longest reason the
	// room service stores (events.OwnerPagedPayload, ProposalWithdrawnPayload).
	// The tools refuse a longer one instead of cutting it, so the model writes
	// the sentence it means.
	pageReasonMaxChars     = 300
	withdrawReasonMaxChars = 500
	// pagesPerRun bounds how many pages one run may send. A page reaches people
	// outside the app, so a looping model, or tool text that tries to make the
	// agent page, must not be able to wear them out. The room service adds its
	// own limit of one page per layer per ten minutes.
	pagesPerRun = 6
	// pagedNamesShown caps the owner ids listed in the tool result.
	pagedNamesShown = 10
)

// ---- shared record of this run's proposals ----

// ownProposals remembers the proposals this run created, by id with their
// title, so withdraw_proposal can refuse any other id. The agent has no right
// to withdraw a proposal the driver or an earlier run made. A nil
// *ownProposals records nothing (NewProposeFixTool has none).
type ownProposals struct {
	mu     sync.Mutex
	titles map[string]string
	order  []string
}

func newOwnProposals() *ownProposals { return &ownProposals{titles: map[string]string{}} }

func (o *ownProposals) add(id, title string) {
	if o == nil || id == "" {
		return
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	if _, ok := o.titles[id]; !ok {
		o.order = append(o.order, id)
	}
	o.titles[id] = title
}

func (o *ownProposals) title(id string) (string, bool) {
	if o == nil {
		return "", false
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	t, ok := o.titles[id]
	return t, ok
}

func (o *ownProposals) ids() []string {
	if o == nil {
		return nil
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	return slices.Clone(o.order)
}

// ---- page_owner ----

type pageOwnerTool struct {
	pager Pager

	mu    sync.Mutex
	paged map[string]bool // layer and proposal id of every page that went out
}

func newPageOwnerTool(p Pager) *pageOwnerTool {
	return &pageOwnerTool{pager: p, paged: map[string]bool{}}
}

func (t *pageOwnerTool) Name() string     { return "page_owner" }
func (t *pageOwnerTool) Kind() tools.Kind { return tools.KindRoom }
func (t *pageOwnerTool) Description() string {
	return "Ask the owners of one layer to look at this room. They are notified in the app and, where it is set up, outside it. " +
		"Use it once for a layer when no available action can fix the cause, or when a proposal waits on a layer nobody in the room is watching, instead of only writing that a human is needed. Never page the same layer again: the room refuses a second page within ten minutes. " +
		fmt.Sprintf("reason is one plain sentence of at most %d characters saying what you need from them. ", pageReasonMaxChars) +
		"It is sent outside the app, so it holds no record values, emails, tokens or connection strings, and it names a proposal by its title, not its id. " +
		"proposal_id is the proposal that waits on that layer, when there is one. Returns the people paged."
}
func (t *pageOwnerTool) InputSchema() map[string]any {
	return roomObjectSchema(map[string]any{
		"layer":       map[string]any{"type": "string", "enum": slices.Clone(events.Layers), "description": "the layer whose owners to page"},
		"reason":      map[string]any{"type": "string", "description": "one sentence: what you need from the owners"},
		"proposal_id": map[string]any{"type": "string", "description": "optional: the proposal that waits on this layer"},
	}, "layer", "reason")
}

type pageOwnerInput struct {
	Layer      string `json:"layer"`
	Reason     string `json:"reason"`
	ProposalID string `json:"proposal_id"`
}

func (t *pageOwnerTool) Run(ctx context.Context, input json.RawMessage) (tools.Result, error) {
	var in pageOwnerInput
	if err := json.Unmarshal(input, &in); err != nil {
		return tools.Result{}, fmt.Errorf("%w: %v", tools.ErrBadInput, err)
	}
	layer := strings.TrimSpace(in.Layer)
	if !slices.Contains(events.Layers, layer) {
		return tools.Result{}, fmt.Errorf("%w: layer must be one of: %s", tools.ErrBadInput, strings.Join(events.Layers, ", "))
	}
	reason, err := checkReason("reason", in.Reason, pageReasonMaxChars)
	if err != nil {
		return tools.Result{}, err
	}
	proposalID := strings.TrimSpace(in.ProposalID)
	if t.pager == nil {
		return tools.Result{}, fmt.Errorf("agent: paging is not available")
	}

	// One page per layer and proposal, and a bounded number per run. The lock
	// is held across the call: room tools run one at a time, and a second page
	// of the same layer must see the first.
	t.mu.Lock()
	defer t.mu.Unlock()
	key := layer + "\x00" + proposalID
	if t.paged[key] {
		return tools.Result{}, fmt.Errorf("agent: you already paged the owners of %s for this; do not page them again", layer)
	}
	if len(t.paged) >= pagesPerRun {
		return tools.Result{}, fmt.Errorf("agent: you have paged %d times in this run, which is the limit; ask the driver in the room instead", pagesPerRun)
	}

	owners, err := t.pager.Page(ctx, layer, reason, proposalID)
	if err != nil {
		// The room service words its refusals for the model (unknown layer,
		// paged in the last ten minutes, room closed), so the text is passed on.
		return tools.Result{}, fmt.Errorf("agent: page owner of %s: %w", layer, err)
	}
	t.paged[key] = true
	return tools.Result{Summary: pagedSummary(layer, owners)}, nil
}

// pagedSummary tells the model who was paged. The ids come from the server's
// owner file, not from the model, but they are shown as plain single-line text.
func pagedSummary(layer string, owners []string) string {
	if len(owners) == 0 {
		return fmt.Sprintf("no owner is on record for %s, so nobody was paged; say so in the room and ask the driver who should look", layer)
	}
	shown := make([]string, 0, min(len(owners), pagedNamesShown))
	for _, o := range owners[:min(len(owners), pagedNamesShown)] {
		shown = append(shown, oneLine(o))
	}
	s := fmt.Sprintf("paged the owners of %s: %s", layer, strings.Join(shown, ", "))
	if extra := len(owners) - len(shown); extra > 0 {
		s += fmt.Sprintf(" and %d more", extra)
	}
	summary, _ := tools.Truncate(s)
	return summary
}

// oneLine drops control characters, newlines included.
func oneLine(s string) string {
	return strings.Map(func(r rune) rune {
		if unicode.IsControl(r) {
			return -1
		}
		return r
	}, s)
}

// checkReason trims a tool's reason and refuses an empty or too long one. The
// error wraps tools.ErrBadInput, so the model sees a failed call and rewrites it.
func checkReason(field, value string, maxChars int) (string, error) {
	v := strings.TrimSpace(value)
	if v == "" || !hasContent(v) {
		return "", fmt.Errorf("%w: %s is required: say in one sentence why", tools.ErrBadInput, field)
	}
	if n := utf8.RuneCountInString(v); n > maxChars {
		return "", fmt.Errorf("%w: %s is %d characters; keep it to %d or fewer", tools.ErrBadInput, field, n, maxChars)
	}
	return v, nil
}

// ---- withdraw_proposal ----

type withdrawProposalTool struct {
	withdrawer ProposalWithdrawer
	own        *ownProposals
}

func (t *withdrawProposalTool) Name() string     { return "withdraw_proposal" }
func (t *withdrawProposalTool) Kind() tools.Kind { return tools.KindRoom }
func (t *withdrawProposalTool) Description() string {
	return "Withdraw one of your own open proposals, so nobody can approve or run it any more. " +
		"Use it when new evidence makes the proposal wrong or risky, or when you want the approvers to hold; propose again later if a fix is still needed. " +
		"A proposal whose steps have already started cannot be withdrawn. Only proposals you created in this run can be withdrawn. " +
		fmt.Sprintf("reason is one short sentence of at most %d characters for the people who were asked to approve.", withdrawReasonMaxChars)
}
func (t *withdrawProposalTool) InputSchema() map[string]any {
	return roomObjectSchema(map[string]any{
		"proposal_id": map[string]any{"type": "string", "description": "id of one of your own open proposals"},
		"reason":      map[string]any{"type": "string", "description": "one sentence for the approvers: why you withdraw it"},
	}, "proposal_id", "reason")
}

type withdrawProposalInput struct {
	ProposalID string `json:"proposal_id"`
	Reason     string `json:"reason"`
}

func (t *withdrawProposalTool) Run(ctx context.Context, input json.RawMessage) (tools.Result, error) {
	var in withdrawProposalInput
	if err := json.Unmarshal(input, &in); err != nil {
		return tools.Result{}, fmt.Errorf("%w: %v", tools.ErrBadInput, err)
	}
	id := strings.TrimSpace(in.ProposalID)
	if id == "" {
		return tools.Result{}, fmt.Errorf("%w: proposal_id is required", tools.ErrBadInput)
	}
	reason, err := checkReason("reason", in.Reason, withdrawReasonMaxChars)
	if err != nil {
		return tools.Result{}, err
	}
	title, ok := t.own.title(id)
	if !ok {
		mine := "none yet"
		if ids := t.own.ids(); len(ids) > 0 {
			mine = strings.Join(ids, ", ")
		}
		return tools.Result{}, fmt.Errorf("%w: proposal %q was not created by you in this run, and you can withdraw only your own (yours: %s)", tools.ErrBadInput, id, mine)
	}
	if t.withdrawer == nil {
		return tools.Result{}, fmt.Errorf("agent: withdrawing proposals is not available")
	}
	if err := t.withdrawer.Withdraw(ctx, id, reason); err != nil {
		// The room service words its refusals for the model (not found, already
		// approved or executing, room closed), so the text is passed on.
		return tools.Result{}, fmt.Errorf("agent: withdraw proposal: %w", err)
	}
	return tools.Result{Summary: fmt.Sprintf("proposal %s (%q) withdrawn: nobody can approve or run it now", id, title)}, nil
}

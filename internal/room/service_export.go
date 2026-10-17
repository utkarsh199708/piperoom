package room

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"hash"
	"io"
	"strconv"
	"time"

	"github.com/gowebpki/jcs"

	"piperoom/internal/events"
	"piperoom/internal/store"
)

// ExportSchema names the audit export format. It changes only when the shape
// of the document changes incompatibly.
const ExportSchema = "piperoom.room-export/v1"

// DefaultExportMaxEvents is the event cap of one export when Deps does not set
// one. A room larger than this answers 413 rather than tying up the database
// for minutes; the events endpoint can still page through it.
const DefaultExportMaxEvents int64 = 500_000

// exportPage is how many events one store read returns while walking the log.
const exportPage = 1000

// exportCanonicalization is stored in the document so a verifier needs no
// other source to recompute the hash. JCS is the product's one canonicalizer
// (the auth design uses it for params_hash too).
const exportCanonicalization = "RFC 8785 (JCS)"

// exportCovers and exportNumbersNote are the wording of the integrity block.
// Keep them in step with docs/api.md.
const (
	exportCovers = `SHA-256 of the RFC 8785 (JCS) form of this document with the top-level "integrity" member removed. ` +
		`It is not the hash of the file bytes; sha256sum of the file will differ.`
	exportNumbersNote = "JCS writes numbers as IEEE 754 doubles; this document shows numbers exactly as stored. " +
		"Numbers a double cannot hold exactly are covered only at double precision, and numbers outside double range " +
		"are hashed as their exact text. See inexact_numbers."
)

// ErrExportChanged is returned by Export.Write when the log no longer matches
// what PrepareExport hashed (for example retention deleted the room in
// between). The caller must not present the bytes it already wrote as a valid
// export.
var ErrExportChanged = errors.New("room: export source changed while exporting")

// exportProposal is a proposal as frozen at creation: Steps carries the exact
// params the approvers saw and the executor ran.
type exportProposal struct {
	ProposalID     string                `json:"proposal_id"`
	Title          string                `json:"title"`
	Risk           string                `json:"risk"`
	Rollback       string                `json:"rollback"`
	RequiredLayers []string              `json:"required_layers"`
	Steps          []events.ProposalStep `json:"steps"`
	// ParamsHash is hex SHA-256 of the RFC 8785 form of {"v":2,"room_id",
	// "proposal_id","title","risk","rollback","required_layers","steps"} built
	// from this entry and the room id (design section 3.1), so an auditor can
	// re-hash it. A proposal created before auth PR 4 has no hash in its event;
	// the export computes it from the same fields. Empty only when the
	// proposal cannot be hashed.
	ParamsHash string       `json:"params_hash"`
	CreatedBy  events.Actor `json:"created_by"`
	CreatedAt  time.Time    `json:"created_at"`
	CreatedSeq int64        `json:"created_seq"`
	Status     string       `json:"status"` // open | approved | rejected | executed | failed | withdrawn
	// ImpactedLayers, RiskLevel and ImpactReasons are what proposal.created
	// recorded (Q0, QA G5); the impacted layers are in RequiredLayers too.
	// Omitted when empty, so the export of a room from before Q0 hashes as
	// it did.
	ImpactedLayers []string `json:"impacted_layers,omitempty"`
	RiskLevel      string   `json:"risk_level,omitempty"`
	ImpactReasons  []string `json:"impact_reasons,omitempty"`
	// Withdrawn is the proposal.withdrawn that ended the proposal (Q0, QA
	// G8/G9); absent when it was not withdrawn.
	Withdrawn *exportWithdrawal `json:"withdrawn,omitempty"`

	approved map[string]bool
	rejected bool
	okSteps  int
	failed   bool
}

// exportWithdrawal is who withdrew a proposal, when and why.
type exportWithdrawal struct {
	Seq    int64        `json:"seq"`
	By     events.Actor `json:"by"`
	At     time.Time    `json:"at"`
	Reason string       `json:"reason"`
}

// exportApproval is one layer decision with who made it and when.
type exportApproval struct {
	Seq        int64        `json:"seq"`
	ProposalID string       `json:"proposal_id"`
	Layer      string       `json:"layer"`
	Decision   string       `json:"decision"` // approved | rejected
	Reason     string       `json:"reason"`
	Approver   events.Actor `json:"approver"`
	At         time.Time    `json:"at"`
	// ParamsHash and Identity are what the approval event recorded (design
	// section 3.2). Both are empty/null for a decision made before auth PR 4.
	ParamsHash string           `json:"params_hash"`
	Identity   *events.Identity `json:"identity"`
}

// exportActionResult is the outcome of one executed proposal step.
type exportActionResult struct {
	Seq        int64        `json:"seq"`
	ProposalID string       `json:"proposal_id"`
	StepIndex  int          `json:"step_index"`
	OK         bool         `json:"ok"`
	Output     string       `json:"output"`
	Executor   events.Actor `json:"executor"`
	At         time.Time    `json:"at"`
	// ParamsHash is the hash the executor checked before this step ("" for a
	// result recorded before auth PR 4).
	ParamsHash string `json:"params_hash"`
}

// exportHandoff is a hand-off with the agent-written summary.
type exportHandoff struct {
	HandoffID     string                `json:"handoff_id"`
	From          events.Actor          `json:"from"`
	To            events.Actor          `json:"to"`
	StayAsWatcher bool                  `json:"stay_as_watcher"`
	Summary       events.HandoffSummary `json:"summary"`
	Status        string                `json:"status"` // pending | accepted
	CreatedAt     time.Time             `json:"created_at"`
	CreatedSeq    int64                 `json:"created_seq"`
	AcceptedBy    *events.Actor         `json:"accepted_by"`
	AcceptedAt    *time.Time            `json:"accepted_at"`
	AcceptedSeq   *int64                `json:"accepted_seq"`
}

// exportRoom is the room row plus the resolution recorded by room.closed. It
// leaves out the driver lease: that changes without any event and would make
// the hash of an unchanged log unstable. The driver history is in the events.
type exportRoom struct {
	ID         string     `json:"id"`
	Title      string     `json:"title"`
	Severity   string     `json:"severity"`
	PipelineID string     `json:"pipeline_id"`
	Status     string     `json:"status"`
	OpenedAt   time.Time  `json:"opened_at"`
	ClosedAt   *time.Time `json:"closed_at"`
	Resolution string     `json:"resolution"`
	LastSeq    int64      `json:"last_seq"`
}

// Export is a prepared audit export of one room, fixed at the room's last_seq
// when PrepareExport ran. Write streams the document; SHA256 is its hash.
type Export struct {
	svc  *Service
	row  store.Room
	upTo int64
	sum  string

	room        exportRoom
	proposals   []*exportProposal
	approvals   []exportApproval
	results     []exportActionResult
	handoffs    []*exportHandoff
	eventCount  int64
	inexact     int // numbers in the document the hash covers only approximately; see integrity.inexact_numbers
	proposalIdx map[string]*exportProposal
	handoffIdx  map[string]*exportHandoff
	shadow      *exportShadow // set only for ?include=shadow
}

// exportShadow is the top-level "shadow" member of an export with
// ?include=shadow (docs/api.md): the run row and the whole shadow log with
// tool_result.data. It is read once in PrepareExportShadow, so the hash and
// the streamed bytes always cover the same shadow content.
type exportShadow struct {
	Run    store.ShadowRun `json:"run"`
	Events []ShadowEvent   `json:"events"`
}

// SHA256 is the hex SHA-256 of the canonical document without its "integrity"
// member. It is the value of the X-Piperoom-Export-SHA256 header and of
// integrity.sha256 in the document.
func (e *Export) SHA256() string { return e.sum }

// RoomID is the exported room's id.
func (e *Export) RoomID() string { return e.row.ID }

// EventCount is the number of events in the document.
func (e *Export) EventCount() int64 { return e.eventCount }

// PrepareExport reads the room's whole log once to build the derived sections
// (proposals, approvals, action results, hand-offs) and once more to compute the
// document hash, so the hash is known before the first byte is sent. Nothing is
// held in memory except those sections: the events are streamed. It returns a
// 404 room error for an unknown room and 413 when the log exceeds the cap.
func (s *Service) PrepareExport(ctx context.Context, roomID string) (*Export, error) {
	return s.prepareExport(ctx, roomID, nil)
}

// PrepareExportShadow is PrepareExport with the shadow run added as the
// top-level "shadow" member (GET /api/rooms/{id}/export?include=shadow). Only a
// closed shadow room that has its blind label may be exported this way: 404
// for a live room, 409 room_open while open, 409 not_labelled before the label.
// The member is inside the hashed document. The caller checks export access.
func (s *Service) PrepareExportShadow(ctx context.Context, roomID string) (*Export, error) {
	st, err := s.lockClosedShadow(ctx, roomID)
	if err != nil {
		return nil, err
	}
	labelled := st.snap.shadowLabelled
	s.unlockRoom(st)
	if !labelled {
		return nil, errNotLabelled()
	}
	run, evs, err := s.readShadow(ctx, roomID, true)
	if err != nil {
		return nil, err
	}
	return s.prepareExport(ctx, roomID, &exportShadow{Run: run, Events: evs})
}

func (s *Service) prepareExport(ctx context.Context, roomID string, shadow *exportShadow) (*Export, error) {
	row, err := s.store.GetRoom(ctx, roomID)
	if err != nil {
		return nil, mapStoreErr(err)
	}
	limit := s.exportMax
	if limit <= 0 {
		limit = DefaultExportMaxEvents
	}
	if row.LastSeq > limit {
		return nil, &Error{
			Status: 413, Code: "export_too_large",
			Msg: fmt.Sprintf("room has %d events; an export holds at most %d (page through GET /api/rooms/{id}/events instead)", row.LastSeq, limit),
		}
	}
	e := &Export{
		svc: s, row: row, upTo: row.LastSeq,
		proposalIdx: map[string]*exportProposal{},
		handoffIdx:  map[string]*exportHandoff{},
		shadow:      shadow,
	}
	e.eventCount, err = e.walk(ctx, e.fold)
	if err != nil {
		return nil, fmt.Errorf("room: export %s: %w", roomID, err)
	}
	e.finish()

	h := sha256.New()
	e.inexact, err = e.writeDocument(ctx, io.Discard, h, nil)
	if err != nil {
		return nil, fmt.Errorf("room: export %s: hash: %w", roomID, err)
	}
	e.sum = hex.EncodeToString(h.Sum(nil))
	return e, nil
}

// Write streams the document to w and returns ErrExportChanged if the bytes it
// hashed on the way differ from the hash PrepareExport computed. The caller
// should abort the connection in that case.
func (e *Export) Write(ctx context.Context, w io.Writer) error {
	integrity, _, _, err := canonicalJSON(map[string]any{
		"algorithm":        "sha256",
		"canonicalization": exportCanonicalization,
		"covers":           exportCovers,
		"inexact_numbers":  e.inexact,
		"numbers":          exportNumbersNote,
		"sha256":           e.sum,
	})
	if err != nil {
		return err
	}
	bw := bufio.NewWriterSize(w, 64<<10)
	h := sha256.New()
	inexact, err := e.writeDocument(ctx, bw, h, integrity)
	if err != nil {
		return err
	}
	if err := bw.Flush(); err != nil {
		return fmt.Errorf("room: export: flush: %w", err)
	}
	if hex.EncodeToString(h.Sum(nil)) != e.sum || inexact != e.inexact {
		return ErrExportChanged
	}
	return nil
}

// walk calls fn for every event up to e.upTo in seq order, a page at a time,
// and returns how many it saw. The log has no gaps, so a count that differs
// from upTo means the room changed under the export.
func (e *Export) walk(ctx context.Context, fn func(events.Event) error) (int64, error) {
	var n int64
	after := int64(0)
	for after < e.upTo {
		evs, err := e.svc.store.Since(ctx, e.row.ID, after, exportPage)
		if err != nil {
			return n, fmt.Errorf("read events: %w", err)
		}
		if len(evs) == 0 {
			break
		}
		for _, ev := range evs {
			if ev.Seq > e.upTo {
				return e.checkCount(n)
			}
			if err := fn(ev); err != nil {
				return n, err
			}
			n++
			after = ev.Seq
		}
	}
	return e.checkCount(n)
}

func (e *Export) checkCount(n int64) (int64, error) {
	if n != e.upTo {
		return n, fmt.Errorf("%w (read %d of %d events)", ErrExportChanged, n, e.upTo)
	}
	return n, nil
}

// fold builds the derived sections from one event. An event whose payload does
// not decode stays in the events array and is left out of the sections; it must
// not break the export of an incident.
func (e *Export) fold(ev events.Event) error {
	switch ev.Type {
	case events.RoomClosed:
		if p, ok := decode[events.RoomClosedPayload](ev); ok {
			e.room.Resolution = p.Resolution
		}
		at := ev.At
		e.room.ClosedAt = &at
	case events.ProposalCreated:
		p, ok := decode[events.ProposalCreatedPayload](ev)
		if !ok {
			return nil
		}
		ep := &exportProposal{
			ProposalID: p.ProposalID, Title: p.Title, Risk: p.Risk, Rollback: p.Rollback,
			RequiredLayers: nonNilStrings(p.RequiredLayers), Steps: nonNilSteps(p.Steps),
			ParamsHash: eventParamsHash(ev.RoomID, p),
			CreatedBy:  ev.Actor, CreatedAt: ev.At, CreatedSeq: ev.Seq, approved: map[string]bool{},
			ImpactedLayers: p.ImpactedLayers, RiskLevel: p.RiskLevel, ImpactReasons: p.ImpactReasons,
		}
		e.proposals = append(e.proposals, ep)
		e.proposalIdx[p.ProposalID] = ep
	case events.ApprovalGiven:
		p, ok := decode[events.ApprovalGivenPayload](ev)
		if !ok {
			return nil
		}
		e.approvals = append(e.approvals, exportApproval{
			Seq: ev.Seq, ProposalID: p.ProposalID, Layer: p.Layer, Decision: "approved", Approver: ev.Actor, At: ev.At,
			ParamsHash: p.ParamsHash, Identity: p.Identity,
		})
		if ep := e.proposalIdx[p.ProposalID]; ep != nil {
			ep.approved[p.Layer] = true
		}
	case events.ApprovalRejected:
		p, ok := decode[events.ApprovalRejectedPayload](ev)
		if !ok {
			return nil
		}
		e.approvals = append(e.approvals, exportApproval{
			Seq: ev.Seq, ProposalID: p.ProposalID, Layer: p.Layer, Decision: "rejected", Reason: p.Reason, Approver: ev.Actor, At: ev.At,
			ParamsHash: p.ParamsHash, Identity: p.Identity,
		})
		if ep := e.proposalIdx[p.ProposalID]; ep != nil {
			ep.rejected = true
		}
	case events.ActionExecuted:
		p, ok := decode[events.ActionExecutedPayload](ev)
		if !ok {
			return nil
		}
		e.results = append(e.results, exportActionResult{
			Seq: ev.Seq, ProposalID: p.ProposalID, StepIndex: p.StepIndex, OK: p.OK, Output: p.Output, Executor: ev.Actor, At: ev.At,
			ParamsHash: p.ParamsHash,
		})
		if ep := e.proposalIdx[p.ProposalID]; ep != nil {
			if p.OK {
				ep.okSteps++
			} else {
				ep.failed = true
			}
		}
	case events.ProposalWithdrawn:
		p, ok := decode[events.ProposalWithdrawnPayload](ev)
		if !ok {
			return nil
		}
		if ep := e.proposalIdx[p.ProposalID]; ep != nil && ep.Withdrawn == nil {
			ep.Withdrawn = &exportWithdrawal{Seq: ev.Seq, By: ev.Actor, At: ev.At, Reason: p.Reason}
		}
	case events.HandoffCreated:
		p, ok := decode[events.HandoffCreatedPayload](ev)
		if !ok {
			return nil
		}
		sum := p.Summary
		sum.Done, sum.RuledOut = nonNilStrings(sum.Done), nonNilStrings(sum.RuledOut)
		sum.Open, sum.Risks = nonNilStrings(sum.Open), nonNilStrings(sum.Risks)
		eh := &exportHandoff{
			HandoffID: p.HandoffID, From: ev.Actor, To: p.To, StayAsWatcher: p.StayAsWatcher, Summary: sum,
			Status: "pending", CreatedAt: ev.At, CreatedSeq: ev.Seq,
		}
		e.handoffs = append(e.handoffs, eh)
		e.handoffIdx[p.HandoffID] = eh
	case events.HandoffAccepted:
		p, ok := decode[events.HandoffAcceptedPayload](ev)
		if !ok {
			return nil
		}
		if eh := e.handoffIdx[p.HandoffID]; eh != nil {
			by, at, seq := ev.Actor, ev.At, ev.Seq
			eh.Status, eh.AcceptedBy, eh.AcceptedAt, eh.AcceptedSeq = "accepted", &by, &at, &seq
		}
	}
	return nil
}

// finish settles the sections that depend on the whole log: proposal status
// and the room block.
func (e *Export) finish() {
	for _, p := range e.proposals {
		switch {
		case p.rejected:
			p.Status = "rejected"
		case p.Withdrawn != nil:
			p.Status = "withdrawn"
		case p.failed:
			p.Status = "failed"
		case len(p.Steps) > 0 && p.okSteps >= len(p.Steps):
			p.Status = "executed"
		case len(p.RequiredLayers) > 0 && allTrue(p.RequiredLayers, p.approved):
			p.Status = "approved"
		default:
			p.Status = "open"
		}
	}
	r := e.row
	closedAt := e.room.ClosedAt // from room.closed, set by fold
	resolution := e.room.Resolution
	e.room = exportRoom{
		ID: r.ID, Title: r.Title, Severity: r.Severity, PipelineID: r.PipelineID, Status: r.Status,
		OpenedAt: r.OpenedAt.UTC(), Resolution: resolution, LastSeq: e.upTo,
	}
	if r.ClosedAt != nil {
		c := r.ClosedAt.UTC()
		e.room.ClosedAt = &c
	}
	// The room.closed event is appended before the rooms row is updated; an
	// export in that gap must still say the room is closed.
	if closedAt != nil {
		e.room.Status = "closed"
		if e.room.ClosedAt == nil {
			c := closedAt.UTC()
			e.room.ClosedAt = &c
		}
	}
}

func allTrue(keys []string, set map[string]bool) bool {
	for _, k := range keys {
		if !set[k] {
			return false
		}
	}
	return true
}

func nonNilStrings(s []string) []string {
	if s == nil {
		return []string{}
	}
	return s
}

func nonNilSteps(s []events.ProposalStep) []events.ProposalStep {
	if s == nil {
		return []events.ProposalStep{}
	}
	return s
}

// writeDocument writes the document with top-level keys in sorted order. out
// gets every value exactly as stored (numbers keep their digits); h gets the
// same document in RFC 8785 form, which is what the hash covers. The structural
// bytes are identical in both. The "integrity" member (when given) goes to out
// only, which is why the hash covers "the document without integrity". A
// hash-only pass uses io.Discard for out and a nil integrity. It returns how
// many numbers the hash covers only approximately (integrity.inexact_numbers).
func (e *Export) writeDocument(ctx context.Context, out io.Writer, h hash.Hash, integrity []byte) (int, error) {
	var (
		werr    error
		inexact int
	)
	put := func(w io.Writer, p []byte) {
		if werr != nil {
			return
		}
		if _, err := w.Write(p); err != nil {
			werr = err
		}
	}
	both := func(s string) {
		put(out, []byte(s))
		put(h, []byte(s))
	}
	value := func(v any) {
		if werr != nil {
			return
		}
		exact, canon, n, err := canonicalJSON(v)
		if err != nil {
			werr = err
			return
		}
		inexact += n
		put(out, exact)
		put(h, canon)
	}
	section := func(key string, v any) {
		both(strconv.Quote(key) + ":")
		value(v)
	}

	both("{")
	section("action_results", nonNilResults(e.results))
	both(",")
	section("approvals", nonNilApprovals(e.approvals))
	both(`,"event_count":` + strconv.FormatInt(e.eventCount, 10) + `,"events":[`)
	if werr != nil {
		return 0, fmt.Errorf("room: export: %w", werr)
	}

	first := true
	n, err := e.walk(ctx, func(ev events.Event) error {
		if !first {
			both(",")
		}
		first = false
		value(ev)
		return werr
	})
	if err != nil {
		return 0, fmt.Errorf("room: export: %w", err)
	}
	if n != e.eventCount {
		return 0, ErrExportChanged
	}

	both("],")
	section("handoffs", nonNilHandoffs(e.handoffs))
	both(",")
	if integrity != nil {
		put(out, []byte(`"integrity":`))
		put(out, integrity)
		put(out, []byte(","))
	}
	section("proposals", nonNilProposals(e.proposals))
	both(",")
	section("room", e.room)
	both(`,"schema":` + strconv.Quote(ExportSchema))
	// "shadow" sorts after "schema"; without the flag the document is byte for
	// byte what it was before shadow mode.
	if e.shadow != nil {
		both(",")
		section("shadow", e.shadow)
	}
	both("}")
	if werr != nil {
		return 0, fmt.Errorf("room: export: %w", werr)
	}
	return inexact, nil
}

func nonNilResults(s []exportActionResult) []exportActionResult {
	if s == nil {
		return []exportActionResult{}
	}
	return s
}

func nonNilApprovals(s []exportApproval) []exportApproval {
	if s == nil {
		return []exportApproval{}
	}
	return s
}

func nonNilHandoffs(s []*exportHandoff) []*exportHandoff {
	if s == nil {
		return []*exportHandoff{}
	}
	return s
}

func nonNilProposals(s []*exportProposal) []*exportProposal {
	if s == nil {
		return []*exportProposal{}
	}
	return s
}

// canonicalJSON returns v in two forms: exact is compact JSON with sorted keys
// and numbers kept verbatim (what the export shows), canon is the RFC 8785
// (JCS) form of it (what the hash covers). inexact counts the numbers that canon
// covers only approximately.
//
// A generic round trip comes first: it makes duplicate object keys in a
// verbatim payload (an alert body may have them; Postgres jsonb keeps the last)
// resolve the same way on every store, which jcs.Transform alone rejects.
//
// Numbers: JCS writes every number as an IEEE 754 double. An integer above 2^53
// (12345678901234567890 becomes 12345678901234567000) or a decimal finer than a
// double is hashed in its double form, and counted in inexact. A number outside
// double range (1e400; jsonb stores it) would make jcs.Transform fail, so in
// the hash input only it is replaced by a string holding its exact text, and
// counted. The document itself always shows the digits as stored.
func canonicalJSON(v any) (exact, canon []byte, inexact int, err error) {
	raw, err := encodeJSON(v)
	if err != nil {
		return nil, nil, 0, fmt.Errorf("room: canonical json: %w", err)
	}
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber()
	var generic any
	if err := dec.Decode(&generic); err != nil {
		return nil, nil, 0, fmt.Errorf("room: canonical json: %w", err)
	}
	exact, err = encodeJSON(generic)
	if err != nil {
		return nil, nil, 0, fmt.Errorf("room: canonical json: %w", err)
	}
	forHash, inexact := hashForm(exact)
	canon, err = jcs.Transform(forHash)
	if err != nil {
		return nil, nil, 0, fmt.Errorf("room: canonical json: %w", err)
	}
	return exact, canon, inexact, nil
}

func encodeJSON(v any) ([]byte, error) {
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(v); err != nil {
		return nil, err
	}
	return bytes.TrimRight(buf.Bytes(), "\n"), nil
}

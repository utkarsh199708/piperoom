package actions

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"

	"piperoom/internal/clients"
	"piperoom/internal/clients/connect"
	"piperoom/internal/owners"
	"piperoom/internal/redact"
	"piperoom/internal/tools"
)

// Errors of the Q5b actions. They are typed so the room and the agent can act
// on them: ErrStopStepRequired means "add connect_stop_connector before this
// step in the same proposal"; the position errors mean "read the stored
// offsets first and keep their shape".
var (
	// ErrStopStepRequired refuses a reset of a connector that is not STOPPED
	// when the step did not promise the stop step. The action never stops a
	// connector itself: a reset is approved as a whole, and an automatic stop
	// would add an unapproved write to it.
	ErrStopStepRequired = errors.New("actions: a reset needs a connect_stop_connector step first in the same proposal")
	// ErrSourcePosition refuses a position the connector's stored offsets say
	// the source cannot have: another partition, another key, another type or
	// a negative value.
	ErrSourcePosition = errors.New("actions: the position does not match the connector's stored source offsets")
	// ErrResumeAfterReset marks a reset whose offsets were written but whose
	// resume_after resume failed: the connector stays STOPPED and a person
	// resumes it. The reset must not be retried as if it had not run.
	ErrResumeAfterReset = errors.New("actions: the source offsets were reset but the connector was not resumed")
	// ErrMultipleSourceOffsets refuses a full reset of a connector that stores
	// more than one offset: this action can write only one position back, so it
	// could not offer the full reset's rollback.
	ErrMultipleSourceOffsets = errors.New("actions: the connector stores more than one source offset; a full reset cannot be rolled back by this action")
)

const (
	// maxPositionEntries caps one partition or offset map.
	maxPositionEntries = 32
	// maxPositionKeyBytes caps a position map key.
	maxPositionKeyBytes = 64
	// maxPositionStringBytes caps a string position value (a binlog file name,
	// a GTID set).
	maxPositionStringBytes = 512
	// maxPositionTextBytes caps a position rendered in a preview or result.
	maxPositionTextBytes = 1024
	// maxEstimateTables caps the tables Preview estimates for one snapshot.
	maxEstimateTables = 20
)

// ---- connect_stop_connector ----

// stopConnector stops a connector and its tasks (PUT /connectors/{name}/stop).
// It exists so connect_reset_source_offsets can run: KIP-875 resets require a
// STOPPED connector.
type stopConnector struct{ d LifecycleDeps }

var (
	_ Action  = (*stopConnector)(nil)
	_ Scoped  = (*stopConnector)(nil)
	_ Preview = (*stopConnector)(nil)
)

func (a *stopConnector) Name() string  { return StopConnectorName }
func (a *stopConnector) Layer() string { return "kafka-connect" }
func (a *stopConnector) Describe() string {
	return "Stops a Kafka Connect connector and its tasks (PUT /connectors/{name}/stop, Kafka Connect 3.5+). Nothing is dropped: a source resumes from its committed offsets, a sink from its consumer group's offsets. Consumers see lag grow while it is stopped, and a Postgres source's replication slot keeps WAL (disk use grows). Undo with connect_resume_connector. A stopped connector's stored offsets can be reset with connect_reset_source_offsets."
}
func (a *stopConnector) ParamsSchema() map[string]any {
	return objectSchema(map[string]any{"connector": strProp("connector name")}, "connector")
}

func (a *stopConnector) CheckScope(p owners.Pipeline, raw json.RawMessage) error {
	name, err := parseConnectorParam(raw)
	if err != nil {
		return err
	}
	return inScopeConnector(p, name)
}

func (a *stopConnector) Validate(ctx context.Context, raw json.RawMessage) error {
	name, err := parseConnectorParam(raw)
	if err != nil {
		return err
	}
	if err := ctxConnectorScope(ctx, name); err != nil {
		return err
	}
	ctx, cancel := a.d.ctx(ctx)
	defer cancel()
	if err := a.d.connectorExists(ctx, name); err != nil {
		return err
	}
	if _, err := a.d.Connect.Status(ctx, name); err != nil {
		return fmt.Errorf("actions: stop %s: read status: %w", name, err)
	}
	return nil
}

// Preview reads the status only (GET /connectors/{name}/status).
func (a *stopConnector) Preview(ctx context.Context, raw json.RawMessage) (PreviewResult, error) {
	name, err := parseConnectorParam(raw)
	if err != nil {
		return PreviewResult{}, previewErr(a.Name(), err)
	}
	if err := ctxConnectorScope(ctx, name); err != nil {
		return PreviewResult{}, previewErr(a.Name(), err)
	}
	ctx, cancel := a.d.ctx(ctx)
	defer cancel()
	st, err := a.d.Connect.Status(ctx, name)
	if err != nil {
		return PreviewResult{}, previewErr(a.Name(), err)
	}
	res := PreviewResult{Target: "connector " + name}
	desc, n, _ := connectorAndTasks(st)
	if st.State == "STOPPED" {
		// Execute writes nothing then; the preview must not promise a change.
		res.Current = fmt.Sprintf("STOPPED (GET /connectors/%s/status)", name)
		res.Expected = "no change: the connector is already STOPPED"
		return res, nil
	}
	res.Current = fmt.Sprintf("%s (GET /connectors/%s/status)", desc, name)
	res.Expected = fmt.Sprintf("STOPPED: the connector and its %d tasks stop; no data is lost (a source resumes from its committed offsets, a sink from its consumer group's offsets)", n)
	if st.Type == "source" {
		res.Expected += "; the source keeps the change log meanwhile (a Postgres replication slot retains WAL, so disk use grows)"
	}
	res.Reversible = true
	res.Rollback = rollbackText("connect_resume_connector", map[string]string{"connector": name})
	if st.State == "PAUSED" {
		// A resume moves a connector to RUNNING, never back to PAUSED: the
		// approver must not read the undo as restoring the paused state.
		res.Expected += "; note: the undo (connect_resume_connector) sets it RUNNING, not back to PAUSED"
	}
	return res, nil
}

func (a *stopConnector) Execute(ctx context.Context, raw json.RawMessage) (string, error) {
	name, err := parseConnectorParam(raw)
	if err != nil {
		return "", err
	}
	if err := ctxConnectorScope(ctx, name); err != nil {
		return "", err
	}
	ctx, cancel := a.d.ctx(ctx)
	defer cancel()
	// Asserted here only: with write mode off the client is read-only, and the
	// agent must still be able to propose (Validate) and preview.
	lw, ok := a.d.Connect.(connect.ConnectLifecycleWriter)
	if !ok {
		return "", fmt.Errorf("actions: stop %s: %w", name, ErrNoLifecycleWriter)
	}
	// A stop of a STOPPED connector would change nothing; say so instead. A
	// failed read is not a reason to skip the write.
	if st, err := a.d.Connect.Status(ctx, name); err == nil && st.State == "STOPPED" {
		return fmt.Sprintf("%s is already STOPPED; no change made", name), nil
	}
	if err := lw.StopConnector(ctx, name); err != nil {
		return "", fmt.Errorf("actions: stop %s: %w", name, err)
	}
	// Connect applies a stop asynchronously. Once the call has succeeded the
	// write is done, so every later problem is a non-error result: an error
	// would tell the agent the stop failed and invite a retry.
	state, stopped, err := awaitState(ctx, a.d.poll(), "STOPPED", func(ctx context.Context) (string, error) {
		st, err := a.d.Connect.Status(ctx, name)
		if err != nil {
			return "", err
		}
		return st.State, nil
	})
	switch {
	case err != nil:
		return fmt.Sprintf("stop accepted for %s; status read failed: %v", name, err), nil
	case stopped:
		return fmt.Sprintf("stopped %s (STOPPED); undo with connect_resume_connector", name), nil
	}
	return fmt.Sprintf("stop accepted for %s; state now %s (Connect applies a stop asynchronously)", name, orUnknown(state)), nil
}

// ---- connect_reset_source_offsets ----

// resetSourceOffsets clears a stopped source connector's stored offsets
// (DELETE /connectors/{name}/offsets) or replaces one partition's position
// (PATCH /connectors/{name}/offsets), both KIP-875 and Kafka Connect 3.6+.
type resetSourceOffsets struct{ d LifecycleDeps }

var (
	_ Action  = (*resetSourceOffsets)(nil)
	_ Scoped  = (*resetSourceOffsets)(nil)
	_ Preview = (*resetSourceOffsets)(nil)
)

func (a *resetSourceOffsets) Name() string  { return ResetSourceOffsetsName }
func (a *resetSourceOffsets) Layer() string { return "kafka-connect" }
func (a *resetSourceOffsets) Describe() string {
	return "Clears or moves the stored source offsets of one of the pipeline's Kafka Connect source connectors (KIP-875, Kafka Connect 3.6+). With no position the connector forgets where it was: on the next start it decides from its config (snapshot.mode) whether to re-read the tables in a snapshot, so use it to re-read data the connector skipped. With a position (partition and offset as connect_source_offsets shows them) it resumes from that position instead; the position must match a partition and the offset keys the connector stores. The connector must be STOPPED: put a connect_stop_connector step before this one in the same proposal and set stop_first: true. To bring the connector back in the same proposal, set resume_after: true on this step (after the reset it resumes the connector, PUT /connectors/{name}/resume, and waits for RUNNING); do not add a connect_resume_connector step after it, which is refused because the connector is not yet stopped when the proposal is checked. The reset itself loses nothing in Kafka; a snapshot re-emits existing rows, so sinks that do not upsert by key see duplicates."
}

func (a *resetSourceOffsets) ParamsSchema() map[string]any {
	return objectSchema(map[string]any{
		"connector": strProp("connector name"),
		"stop_first": map[string]any{
			"type":        "boolean",
			"description": "set only when a connect_stop_connector step for the same connector precedes this step in the same proposal (the reset refuses a running connector without it)",
		},
		"resume_after": map[string]any{
			"type":        "boolean",
			"description": "resume the connector (to RUNNING) right after the reset, in this step; use it instead of a separate connect_resume_connector step",
		},
		"position": map[string]any{
			"type":        "object",
			"description": "optional concrete position; omit it to clear every stored offset (see connect_source_offsets for the shape)",
			"properties": map[string]any{
				"partition": map[string]any{"type": "object", "description": "the partition map exactly as connect_source_offsets shows it"},
				"offset":    map[string]any{"type": "object", "description": "the offset map with every position field the connector stores, scalar values only"},
			},
			"required":             []string{"partition", "offset"},
			"additionalProperties": false,
		},
	}, "connector")
}

// sourcePosition is one concrete position: a partition and its position map.
// Both maps are the connector's own shape, so they stay free-form.
type sourcePosition struct {
	Partition map[string]any `json:"partition"`
	Offset    map[string]any `json:"offset"`
}

type resetSourceOffsetsParams struct {
	Connector string `json:"connector"`
	StopFirst bool   `json:"stop_first"`
	// ResumeAfter resumes the connector once the reset is done (KAN-514). A
	// separate connect_resume_connector step cannot follow the reset in the
	// same proposal: its Validate reads the live state when the proposal is
	// created, sees the connector still RUNNING and refuses it, and Validate
	// has no view of the earlier steps (actions.Action).
	ResumeAfter bool            `json:"resume_after"`
	Position    *sourcePosition `json:"position"`
}

// parseResetParams decodes params strictly (unknown fields are refused so a
// misspelled stop_first cannot silently drop the stop requirement) and checks
// the position's map sizes and scalar values without I/O.
func parseResetParams(raw json.RawMessage) (resetSourceOffsetsParams, error) {
	var p resetSourceOffsetsParams
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber() // a Postgres LSN is a 64-bit integer; float64 loses digits
	dec.DisallowUnknownFields()
	if err := dec.Decode(&p); err != nil {
		return p, fmt.Errorf("actions: %s: bad params: %w", ResetSourceOffsetsName, err)
	}
	if p.Connector == "" {
		return p, fmt.Errorf("actions: %s: connector is required", ResetSourceOffsetsName)
	}
	if len(p.Connector) > maxConnectorNameLen {
		return p, fmt.Errorf("actions: %s: connector name is longer than %d bytes", ResetSourceOffsetsName, maxConnectorNameLen)
	}
	if p.Position == nil {
		return p, nil
	}
	if len(p.Position.Partition) == 0 || len(p.Position.Offset) == 0 {
		return p, fmt.Errorf("actions: %s: position needs a non-empty partition and a non-empty offset", ResetSourceOffsetsName)
	}
	if err := checkPositionMap("partition", p.Position.Partition); err != nil {
		return p, err
	}
	if err := checkPositionMap("offset", p.Position.Offset); err != nil {
		return p, err
	}
	return p, nil
}

// checkPositionMap bounds one position map and refuses a value that is not a
// scalar: the offsets API returns positions only, never nested structures.
func checkPositionMap(what string, m map[string]any) error {
	if len(m) > maxPositionEntries {
		return fmt.Errorf("actions: %s: %s has %d keys; at most %d", ResetSourceOffsetsName, what, len(m), maxPositionEntries)
	}
	for k, v := range m {
		if len(k) > maxPositionKeyBytes {
			return fmt.Errorf("actions: %s: a %s key is longer than %d bytes", ResetSourceOffsetsName, what, maxPositionKeyBytes)
		}
		switch x := v.(type) {
		case json.Number:
			if len(x.String()) > 64 {
				return fmt.Errorf("actions: %s: %s.%s is a number longer than 64 bytes", ResetSourceOffsetsName, what, shortText(k))
			}
		case string:
			// Enforced at parse so the no-stored-shape path (after a full
			// reset) cannot write an unbounded or control-character string:
			// checkPositionValue only runs against a stored shape.
			if len(x) > maxPositionStringBytes {
				return fmt.Errorf("actions: %s: %s.%s is a string longer than %d bytes", ResetSourceOffsetsName, what, shortText(k), maxPositionStringBytes)
			}
			if strings.ContainsAny(x, "\x00\r\n") {
				return fmt.Errorf("actions: %s: %s.%s contains a control character", ResetSourceOffsetsName, what, shortText(k))
			}
		case bool, nil:
		default:
			return fmt.Errorf("actions: %s: %s.%s is a %T; a position holds only strings, numbers, booleans and null", ResetSourceOffsetsName, what, shortText(k), x)
		}
	}
	return nil
}

func (a *resetSourceOffsets) CheckScope(p owners.Pipeline, raw json.RawMessage) error {
	name, err := parseConnectorParam(raw)
	if err != nil {
		return err
	}
	return inScopeConnector(p, name)
}

// readStoredOffsets reads the connector's stored offsets; the action needs the
// read for its Preview, its position checks and its no-op check.
func (a *resetSourceOffsets) readStoredOffsets(ctx context.Context, name string) ([]connect.SourceOffset, error) {
	r, ok := a.d.Connect.(connect.SourceOffsetReader)
	if !ok {
		return nil, fmt.Errorf("actions: read stored offsets of %s: this Connect client cannot read connector offsets (Kafka Connect 3.5+, KIP-875): %w", name, clients.ErrNotSupported)
	}
	offs, err := r.SourceOffsets(ctx, name)
	if err != nil {
		return nil, fmt.Errorf("actions: read stored offsets of %s: %w", name, err)
	}
	return offs, nil
}

// requireSourceStatus refuses a sink connector before any offsets are read.
func requireSourceStatus(name string, st clients.ConnectorStatus) error {
	if st.Type != "source" {
		typ := st.Type
		if typ == "" {
			typ = "unknown"
		}
		return fmt.Errorf("actions: %s: %s is a %s connector; only a source connector stores source offsets", ResetSourceOffsetsName, name, typ)
	}
	return nil
}

// checkStopStep applies the same-proposal rule at proposal creation: a
// connector that is not STOPPED needs stop_first, which the proposer sets only
// when the stop step precedes this one.
func checkStopStep(name, state string, stopFirst bool) error {
	if state == "STOPPED" || stopFirst {
		return nil
	}
	return fmt.Errorf("actions: %s: connector %s is %s; add a %s step for it before this step in the same proposal and set stop_first: true: %w",
		ResetSourceOffsetsName, name, orUnknown(state), StopConnectorName, ErrStopStepRequired)
}

// requireStoppedNow is Execute's re-check: the promised stop step must really
// have run, because Connect refuses offset changes on a running connector.
// A stop is applied asynchronously and the stop step reports success once the
// stop is accepted, so a connector still mid-stop gets a short settling poll
// before the refusal; the refusal then tells a slow stop apart from a missing
// stop step.
//
// The settling poll ends after both settleMinReads status reads and
// 6 poll intervals, never later than max(6 intervals, settleHardCap). Counting
// reads, not only wall-clock time, keeps a short poll interval from giving a
// slow (loaded or -race) machine only one or two reads; with the default
// interval the window is the same 6 intervals as before.
func (a *resetSourceOffsets) requireStoppedNow(ctx context.Context, name, state string) error {
	if state == "STOPPED" {
		return nil
	}
	poll := a.d.poll()
	window := 6 * poll
	pctx, cancel := context.WithTimeout(ctx, max(window, settleHardCap))
	defer cancel()
	deadline := time.Now().Add(window)
	last := ""
	for reads := 1; ; reads++ {
		st, err := a.d.Connect.Status(pctx, name)
		if err != nil {
			// A failed read ends the wait; the refusal below names the last
			// state seen, as a never-settling stop would.
			break
		}
		if st.State != "" {
			last = st.State
		}
		if st.State == "STOPPED" {
			return nil
		}
		if reads >= settleMinReads && !time.Now().Before(deadline) {
			break
		}
		select {
		case <-pctx.Done():
		case <-time.After(poll):
		}
		if pctx.Err() != nil {
			break
		}
	}
	if last == "" {
		last = state
	}
	return fmt.Errorf("actions: %s: connector %s is %s at execution; if the %s step just ran, its stop has not settled yet (retry shortly); otherwise that step must run before this one in the same proposal: %w",
		ResetSourceOffsetsName, name, orUnknown(last), StopConnectorName, ErrStopStepRequired)
}

// settleMinReads is how many status reads requireStoppedNow makes at least
// before it refuses a connector that is not STOPPED yet.
const settleMinReads = 6

// settleHardCap bounds requireStoppedNow's settling poll when the poll
// interval is short, so the read count cannot stretch it without limit.
const settleHardCap = 2 * time.Second

func (a *resetSourceOffsets) Validate(ctx context.Context, raw json.RawMessage) error {
	p, err := parseResetParams(raw)
	if err != nil {
		return err
	}
	if err := ctxConnectorScope(ctx, p.Connector); err != nil {
		return err
	}
	ctx, cancel := a.d.ctx(ctx)
	defer cancel()
	if err := a.d.connectorExists(ctx, p.Connector); err != nil {
		return err
	}
	st, err := a.d.Connect.Status(ctx, p.Connector)
	if err != nil {
		return fmt.Errorf("actions: %s: read status of %s: %w", ResetSourceOffsetsName, p.Connector, err)
	}
	if err := requireSourceStatus(p.Connector, st); err != nil {
		return err
	}
	offs, err := a.readStoredOffsets(ctx, p.Connector)
	if err != nil {
		return err
	}
	if err := checkStopStep(p.Connector, st.State, p.StopFirst); err != nil {
		return err
	}
	if err := checkResetTarget(offs, p); err != nil {
		return err
	}
	if p.Position != nil && len(offs) == 0 {
		cfg, err := a.d.Connect.Config(ctx, p.Connector)
		if err != nil {
			return fmt.Errorf("actions: %s: read config of %s: %w", ResetSourceOffsetsName, p.Connector, err)
		}
		return checkPositionWithoutShape(p.Connector, cfg, *p.Position)
	}
	return nil
}

// checkResetTarget checks the offsets a reset writes against what Connect
// stores now, without I/O.
func checkResetTarget(offs []connect.SourceOffset, p resetSourceOffsetsParams) error {
	if p.Position == nil {
		if n := len(storedPositions(offs)); n > 1 {
			return fmt.Errorf("actions: %s: connector %s stores %d source offsets; this action can write only one position back, so a full reset's rollback is not offered: %w",
				ResetSourceOffsetsName, p.Connector, n, ErrMultipleSourceOffsets)
		}
		return nil
	}
	if len(offs) == 0 {
		// A full reset may leave no entry at all; the caller then checks the
		// position against the connector's own Debezium partition (see
		// checkPositionWithoutShape).
		return nil
	}
	_, err := matchPosition(p.Connector, offs, *p.Position)
	return err
}

// checkPositionWithoutShape checks a position when the connector stores no
// entry to compare with, as after a full reset. For a Debezium connector the
// partition is known from its config ({"server": topic.prefix}); its offset
// keys cannot be checked against a stored shape then, so only their scalar
// values are checked (parseResetParams). Any other connector is refused: fail
// closed rather than write an unverifiable position.
func checkPositionWithoutShape(name string, cfg map[string]string, pos sourcePosition) error {
	class := cfg["connector.class"]
	if !strings.HasPrefix(strings.TrimSpace(class), "io.debezium.connector.") {
		return fmt.Errorf("actions: %s: connector %s stores no offsets and is not a Debezium connector, so no partition shape is known; propose the position while a stored offset still exists: %w",
			ResetSourceOffsetsName, name, ErrSourcePosition)
	}
	prefix := strings.TrimSpace(cfg["topic.prefix"])
	if prefix == "" {
		return fmt.Errorf("actions: %s: connector %s has no topic.prefix, so its Debezium partition cannot be checked: %w",
			ResetSourceOffsetsName, name, ErrSourcePosition)
	}
	if len(pos.Partition) != 1 || !sameJSON(pos.Partition["server"], prefix) {
		return fmt.Errorf("actions: %s: connector %s: a Debezium partition is {\"server\": %q}, not %s: %w",
			ResetSourceOffsetsName, name, prefix, jsonText(pos.Partition), ErrSourcePosition)
	}
	return nil
}

// storedPositions keeps the offsets that still hold a position. A KIP-875
// full reset answers DELETE with a partition whose offset is null rather than
// removing the entry, so an offset-less entry is not a stored extent.
func storedPositions(offs []connect.SourceOffset) []connect.SourceOffset {
	out := make([]connect.SourceOffset, 0, len(offs))
	for _, o := range offs {
		if o.Offset != nil {
			out = append(out, o)
		}
	}
	return out
}

// Preview says what the reset does and, with resume_after, that the same step
// then resumes the connector.
func (a *resetSourceOffsets) Preview(ctx context.Context, raw json.RawMessage) (PreviewResult, error) {
	p, err := parseResetParams(raw)
	if err != nil {
		return PreviewResult{}, previewErr(a.Name(), err)
	}
	res, err := a.previewReset(ctx, p)
	if err != nil || !p.ResumeAfter {
		return res, err
	}
	res.Expected += fmt.Sprintf("; then resume: PUT /connectors/%s/resume moves the connector from STOPPED to RUNNING in this same step, and the step waits for RUNNING", p.Connector)
	if res.Reversible {
		// Rollback stays "<action> {params}" for machines; the order it needs
		// is said here, since the connector runs again after this step.
		res.Expected += fmt.Sprintf("; to undo, %s %s first (it runs again after this step), then the rollback with stop_first: true", StopConnectorName, p.Connector)
	}
	res.Downstream = append(res.Downstream, "the connector starts again right after the reset in this step, so whatever its restart does (a snapshot under snapshot.mode, or streaming from the new position) begins without a further approval")
	return res, nil
}

// previewReset is the reset's own preview, without resume_after.
func (a *resetSourceOffsets) previewReset(ctx context.Context, p resetSourceOffsetsParams) (PreviewResult, error) {
	if err := ctxConnectorScope(ctx, p.Connector); err != nil {
		return PreviewResult{}, previewErr(a.Name(), err)
	}
	ctx, cancel := a.d.ctx(ctx)
	defer cancel()
	st, err := a.d.Connect.Status(ctx, p.Connector)
	if err != nil {
		return PreviewResult{}, previewErr(a.Name(), err)
	}
	if err := requireSourceStatus(p.Connector, st); err != nil {
		return PreviewResult{}, previewErr(a.Name(), err)
	}
	if err := checkStopStep(p.Connector, st.State, p.StopFirst); err != nil {
		return PreviewResult{}, previewErr(a.Name(), err)
	}
	offs, err := a.readStoredOffsets(ctx, p.Connector)
	if err != nil {
		return PreviewResult{}, previewErr(a.Name(), err)
	}
	cfg, err := a.d.Connect.Config(ctx, p.Connector)
	if err != nil {
		return PreviewResult{}, previewErr(a.Name(), fmt.Errorf("read config of %s: %w", p.Connector, err))
	}
	if err := checkResetTarget(offs, p); err != nil {
		return PreviewResult{}, previewErr(a.Name(), err)
	}
	if p.Position != nil && len(offs) == 0 {
		if err := checkPositionWithoutShape(p.Connector, cfg, *p.Position); err != nil {
			return PreviewResult{}, previewErr(a.Name(), err)
		}
	}
	state := "STOPPED"
	if st.State != "STOPPED" {
		state = fmt.Sprintf("%s; it must be stopped first (the stop step)", orUnknown(st.State))
	}
	res := PreviewResult{
		Target:  fmt.Sprintf("connector %s stored source offsets", p.Connector),
		Current: fmt.Sprintf("stored offsets: %s; status %s (GET /connectors/%s/status, /offsets)", formatSourceOffsets(offs), state, p.Connector),
	}
	if p.Position == nil {
		positions := storedPositions(offs)
		if len(positions) == 0 {
			res.Expected = "no change: the connector stores no offsets"
			return res, nil
		}
		res.Expected = "no stored offsets after the reset; " + snapshotRestart(cfg, a.tableEstimate(ctx), false)
		// The advertised rollback writes the cleared position back AFTER the
		// clear, when the connector stores no shape to compare with: that
		// path (checkPositionWithoutShape) only accepts a Debezium
		// {"server": topic.prefix} partition, so for any other stored
		// partition the rollback would be refused and the reset must not
		// claim to be reversible.
		rbPos := sourcePosition{Partition: positions[0].Partition, Offset: positions[0].Offset}
		if err := checkPositionWithoutShape(p.Connector, cfg, rbPos); err != nil {
			res.Reversible = false
			res.Expected += "; this action could not write the cleared position back afterwards (the partition's shape cannot be re-checked once cleared), so the clear is not reversible here"
		} else {
			res.Reversible = true
			res.Rollback = resetRollback(p.Connector, positions[0])
		}
	} else if len(offs) == 0 {
		// No entry to compare with (a full reset left none): the partition was
		// checked against the config above.
		res.Expected = fmt.Sprintf("stored offset set to %s (the connector stores no position now, so only the partition and the scalar value types are checked); %s",
			formatPosition(*p.Position), snapshotRestart(cfg, "", true))
		res.Reversible = false // there is no stored position to write back
	} else {
		stored, err := matchPosition(p.Connector, offs, *p.Position)
		if err != nil {
			return PreviewResult{}, previewErr(a.Name(), err)
		}
		if sameSourceOffset(stored, *p.Position) {
			// Execute skips this write; the preview must not promise a change.
			res.Expected = "no change: the stored offset already is " + formatStoredOffset(stored)
			return res, nil
		}
		extra := ""
		if len(offs) > 1 {
			extra = fmt.Sprintf(" (the other %d stored offsets are unchanged)", len(offs)-1)
		}
		if stored.Offset == nil {
			// There is no stored position to write back; a full reset would
			// clear every partition, not just this one.
			res.Expected = fmt.Sprintf("stored offset set to %s%s (the connector stores no position for this partition, so only the partition and the scalar value types are checked); %s",
				formatPosition(*p.Position), extra, snapshotRestart(cfg, "", true))
			res.Reversible = false
			return res, nil
		}
		res.Expected = fmt.Sprintf("stored offset %s replaced by %s%s; %s", formatStoredOffset(stored), formatPosition(*p.Position), extra, snapshotRestart(cfg, "", true))
		res.Reversible = true
		res.Rollback = resetRollback(p.Connector, stored)
	}
	res.Downstream = []string{
		"the reset changes where the connector resumes, not what is already in its topics; a following snapshot re-emits existing rows (op r), so sinks that do not upsert by key see duplicates",
	}
	return res, nil
}

func (a *resetSourceOffsets) Execute(ctx context.Context, raw json.RawMessage) (string, error) {
	p, err := parseResetParams(raw)
	if err != nil {
		return "", err
	}
	if err := ctxConnectorScope(ctx, p.Connector); err != nil {
		return "", err
	}
	lw, ok := a.d.Connect.(connect.ConnectLifecycleWriter)
	if !ok {
		return "", fmt.Errorf("actions: %s: %w", ResetSourceOffsetsName, ErrNoLifecycleWriter)
	}
	// Checked before any write: a client that cannot resume must not get a
	// reset that then leaves the connector STOPPED against the approval.
	var rc connectorResumer
	if p.ResumeAfter {
		if rc, ok = a.d.Connect.(connectorResumer); !ok {
			return "", fmt.Errorf("actions: %s: resume_after: this Connect client cannot resume a connector; nothing was reset: %w", ResetSourceOffsetsName, ErrNoLifecycleWriter)
		}
	}
	done, err := a.executeReset(ctx, p, lw)
	if err != nil || !p.ResumeAfter {
		return done, err
	}
	return a.resumeAfterReset(ctx, rc, p.Connector, done)
}

// resumeAfterReset resumes the connector once its offsets are reset
// (resume_after). It gets a deadline of its own, so a slow reset cannot
// starve it. The reset already happened: a failed resume is an error that
// says so (ErrResumeAfterReset), and every problem after an accepted resume is
// a non-error result, as in connect_resume_connector.
func (a *resetSourceOffsets) resumeAfterReset(ctx context.Context, rc connectorResumer, name, done string) (string, error) {
	ctx, cancel := a.d.ctx(ctx)
	defer cancel()
	if err := rc.Resume(ctx, name); err != nil {
		return "", fmt.Errorf("actions: %s: %s; the reset succeeded but the resume failed, so %s stays STOPPED: resume it by hand (%s, or PUT /connectors/%s/resume): %w: %w",
			ResetSourceOffsetsName, done, name, "connect_resume_connector", url.PathEscape(name), ErrResumeAfterReset, err)
	}
	state, running, err := awaitState(ctx, a.d.poll(), "RUNNING", func(ctx context.Context) (string, error) {
		st, err := a.d.Connect.Status(ctx, name)
		if err != nil {
			return "", err
		}
		return st.State, nil
	})
	switch {
	case err != nil:
		return fmt.Sprintf("%s; resume accepted for %s; status read failed: %v", done, name, err), nil
	case running:
		return fmt.Sprintf("%s; resumed %s (RUNNING)", done, name), nil
	}
	return fmt.Sprintf("%s; resume accepted for %s; state now %s (Connect applies a resume asynchronously)", done, name, orUnknown(state)), nil
}

// executeReset is the reset itself: the live-state re-check, then the write.
func (a *resetSourceOffsets) executeReset(ctx context.Context, p resetSourceOffsetsParams, lw connect.ConnectLifecycleWriter) (string, error) {
	ctx, cancel := a.d.ctx(ctx)
	defer cancel()
	st, err := a.d.Connect.Status(ctx, p.Connector)
	if err != nil {
		return "", fmt.Errorf("actions: %s: read status of %s: %w", ResetSourceOffsetsName, p.Connector, err)
	}
	if err := requireSourceStatus(p.Connector, st); err != nil {
		return "", err
	}
	if err := a.requireStoppedNow(ctx, p.Connector, st.State); err != nil {
		return "", err
	}
	offs, err := a.readStoredOffsets(ctx, p.Connector)
	if err != nil {
		return "", err
	}
	if err := checkResetTarget(offs, p); err != nil {
		return "", err
	}
	if p.Position != nil && len(offs) == 0 {
		cfg, err := a.d.Connect.Config(ctx, p.Connector)
		if err != nil {
			return "", fmt.Errorf("actions: %s: read config of %s: %w", ResetSourceOffsetsName, p.Connector, err)
		}
		if err := checkPositionWithoutShape(p.Connector, cfg, *p.Position); err != nil {
			return "", err
		}
	}
	if p.Position == nil {
		if len(storedPositions(offs)) == 0 {
			return fmt.Sprintf("%s stores no offsets; no change made", p.Connector), nil
		}
		was := formatSourceOffsets(offs)
		if err := lw.ResetSourceOffsets(ctx, p.Connector); err != nil {
			return "", resetWriteErr(p.Connector, err)
		}
		return a.afterWrite(ctx, p.Connector, fmt.Sprintf("cleared the stored offsets of %s (was %s)", p.Connector, was), false)
	}
	if len(offs) == 0 {
		if err := lw.AlterSourceOffsets(ctx, p.Connector, []connect.SourceOffset{{Partition: p.Position.Partition, Offset: p.Position.Offset}}); err != nil {
			return "", resetWriteErr(p.Connector, err)
		}
		return a.afterWrite(ctx, p.Connector, fmt.Sprintf("set the stored offset of %s to %s (the connector stored none)", p.Connector, formatPosition(*p.Position)), true)
	}
	stored, err := matchPosition(p.Connector, offs, *p.Position)
	if err != nil {
		return "", err
	}
	if sameSourceOffset(stored, *p.Position) {
		return fmt.Sprintf("%s is already at %s; no change made", p.Connector, formatStoredOffset(stored)), nil
	}
	if err := lw.AlterSourceOffsets(ctx, p.Connector, []connect.SourceOffset{{
		Partition: p.Position.Partition,
		Offset:    p.Position.Offset,
	}}); err != nil {
		return "", resetWriteErr(p.Connector, err)
	}
	return a.afterWrite(ctx, p.Connector, fmt.Sprintf("set the stored offset of %s to %s (was %s)", p.Connector, formatPosition(*p.Position), formatStoredOffset(stored)), true)
}

// resetWriteErr keeps the typed "not stopped" refusal when Connect answers it,
// so the executor can tell the proposer to add the stop step.
func resetWriteErr(name string, err error) error {
	if errors.Is(err, connect.ErrConnectorNotStopped) {
		return fmt.Errorf("actions: %s: connector %s is not STOPPED (Connect refused the offset change); run the %s step before this one: %w",
			ResetSourceOffsetsName, name, StopConnectorName, errors.Join(ErrStopStepRequired, err))
	}
	return fmt.Errorf("actions: %s: %s: %w", ResetSourceOffsetsName, name, err)
}

// afterWrite re-reads the stored offsets once. The write already happened, so
// every later problem is a non-error result.
func (a *resetSourceOffsets) afterWrite(ctx context.Context, name, done string, expectPosition bool) (string, error) {
	offs, err := a.readStoredOffsets(ctx, name)
	if err != nil {
		return done + "; the re-read of the stored offsets failed", nil
	}
	if !expectPosition {
		if len(storedPositions(offs)) == 0 {
			return done + "; the connector now stores no offsets and decides from snapshot.mode on its next start", nil
		}
		return fmt.Sprintf("%s; the re-read still shows %s", done, formatSourceOffsets(offs)), nil
	}
	if len(offs) == 1 {
		return done + "; the re-read shows " + formatStoredOffset(offs[0]), nil
	}
	return fmt.Sprintf("%s; the re-read shows %s", done, formatSourceOffsets(offs)), nil
}

// matchPosition returns the stored offset whose partition equals pos.Partition
// after checking every offset key and value type against it.
func matchPosition(name string, stored []connect.SourceOffset, pos sourcePosition) (connect.SourceOffset, error) {
	for _, so := range stored {
		if !sameJSON(so.Partition, pos.Partition) {
			continue
		}
		// A partition whose offset is null has no stored shape to compare with:
		// a position written back after a full reset still needs to pass on the
		// partition and its scalar values (checked by parseResetParams).
		if so.Offset != nil {
			if err := checkOffsetShape(name, so.Offset, pos.Offset); err != nil {
				return connect.SourceOffset{}, err
			}
		}
		return so, nil
	}
	return connect.SourceOffset{}, fmt.Errorf("actions: %s: connector %s stores no partition %s; stored partitions: %s: %w",
		ResetSourceOffsetsName, name, jsonText(pos.Partition), storedPartitions(stored), ErrSourcePosition)
}

// checkOffsetShape requires the requested offset to carry exactly the keys the
// connector stores, each with the stored value's type. A missing key would
// write a partial position; an extra key is one the source cannot have.
func checkOffsetShape(name string, stored, want map[string]any) error {
	var missing, extra []string
	for k := range stored {
		if _, ok := want[k]; !ok {
			missing = append(missing, k)
		}
	}
	for k := range want {
		if _, ok := stored[k]; !ok {
			extra = append(extra, k)
		}
	}
	if len(missing) > 0 || len(extra) > 0 {
		var parts []string
		if len(missing) > 0 {
			sort.Strings(missing)
			parts = append(parts, "missing "+strings.Join(missing, ", "))
		}
		if len(extra) > 0 {
			sort.Strings(extra)
			parts = append(parts, "not stored by the source: "+strings.Join(extra, ", "))
		}
		return fmt.Errorf("actions: %s: connector %s: the offset must carry exactly the stored position keys (%s): %w",
			ResetSourceOffsetsName, name, strings.Join(parts, "; "), ErrSourcePosition)
	}
	keys := make([]string, 0, len(want))
	for k := range want {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		if err := checkPositionValue(name, k, stored[k], want[k]); err != nil {
			return err
		}
	}
	return nil
}

// checkPositionValue compares one requested value with the stored one's type
// and refuses a value the source cannot hold.
func checkPositionValue(name, key string, stored, want any) error {
	switch sv := stored.(type) {
	case json.Number:
		n, ok := want.(json.Number)
		if !ok {
			return fmt.Errorf("actions: %s: connector %s: %s is a number in the stored offset, not a %T: %w",
				ResetSourceOffsetsName, name, key, want, ErrSourcePosition)
		}
		if n.String() == sv.String() {
			// Writing back exactly what is stored is always sound, whatever
			// its form: a source may store a negative sentinel (Confluent
			// JDBC's "incrementing": -1) or an exponent-form number the
			// uint64 rule below would refuse — which would make the
			// advertised rollback of such a connector un-runnable.
			return nil
		}
		if len(n.String()) > 20 {
			return fmt.Errorf("actions: %s: connector %s: %s is longer than 20 digits: %w", ResetSourceOffsetsName, name, key, ErrSourcePosition)
		}
		if _, err := strconv.ParseUint(n.String(), 10, 64); err != nil {
			// ParseUint also refuses a negative value and a decimal fraction.
			return fmt.Errorf("actions: %s: connector %s: %s must be a whole non-negative number (or exactly the stored value): %w", ResetSourceOffsetsName, name, key, ErrSourcePosition)
		}
	case string:
		s, ok := want.(string)
		if !ok {
			return fmt.Errorf("actions: %s: connector %s: %s is a string in the stored offset, not a %T: %w",
				ResetSourceOffsetsName, name, key, want, ErrSourcePosition)
		}
		if s == "" || len(s) > maxPositionStringBytes {
			return fmt.Errorf("actions: %s: connector %s: %s must be a non-empty string of at most %d bytes: %w",
				ResetSourceOffsetsName, name, key, maxPositionStringBytes, ErrSourcePosition)
		}
		if strings.ContainsAny(s, "\x00\r\n") {
			return fmt.Errorf("actions: %s: connector %s: %s contains a control character: %w", ResetSourceOffsetsName, name, key, ErrSourcePosition)
		}
	case bool:
		if _, ok := want.(bool); !ok {
			return fmt.Errorf("actions: %s: connector %s: %s is a boolean in the stored offset, not a %T: %w",
				ResetSourceOffsetsName, name, key, want, ErrSourcePosition)
		}
	case nil:
		if want != nil {
			return fmt.Errorf("actions: %s: connector %s: %s is null in the stored offset: %w", ResetSourceOffsetsName, name, key, ErrSourcePosition)
		}
	default:
		return fmt.Errorf("actions: %s: connector %s: %s holds a %T in the stored offset; this action only writes scalar positions: %w",
			ResetSourceOffsetsName, name, key, sv, ErrSourcePosition)
	}
	return nil
}

// sameSourceOffset reports whether pos already is so (partition and every
// value), so Execute can skip a write that would change nothing.
func sameSourceOffset(so connect.SourceOffset, pos sourcePosition) bool {
	return sameJSON(so.Partition, pos.Partition) && sameJSON(so.Offset, pos.Offset)
}

// sameJSON compares two JSON values by their canonical encoding (maps sort
// their keys; json.Number keeps a 64-bit position exact).
func sameJSON(a, b any) bool {
	ab, aerr := json.Marshal(a)
	bb, berr := json.Marshal(b)
	return aerr == nil && berr == nil && bytes.Equal(ab, bb)
}

// storedPartitions renders the stored partitions for a refusal.
func storedPartitions(offs []connect.SourceOffset) string {
	if len(offs) == 0 {
		return "none"
	}
	parts := make([]string, 0, len(offs))
	for _, o := range offs {
		parts = append(parts, jsonText(o.Partition))
	}
	return strings.Join(parts, ", ")
}

// formatSourceOffsets renders every stored position, capped.
func formatSourceOffsets(offs []connect.SourceOffset) string {
	if len(offs) == 0 {
		return "none"
	}
	parts := make([]string, 0, len(offs))
	for _, o := range offs {
		if o.Offset == nil {
			parts = append(parts, "partition "+jsonText(o.Partition)+" offset cleared")
			continue
		}
		parts = append(parts, formatStoredOffset(o))
	}
	return capPositionText(strings.Join(parts, "; "))
}

// formatPosition renders one position.
func formatPosition(p sourcePosition) string {
	return formatPositionParts(p.Partition, p.Offset)
}

// formatStoredOffset renders one stored position.
func formatStoredOffset(o connect.SourceOffset) string {
	return formatPositionParts(o.Partition, o.Offset)
}

func formatPositionParts(partition, offset map[string]any) string {
	return "partition " + jsonText(partition) + " offset " + jsonText(offset)
}

// jsonText renders a position map, redacted and capped. Positions hold no
// record data, but a hostile connector could put any text in them.
func jsonText(v any) string {
	b, err := json.Marshal(v)
	if err != nil {
		return "?"
	}
	s, _ := redact.Text(string(b))
	return capPositionText(s)
}

func capPositionText(s string) string {
	if len(s) <= maxPositionTextBytes {
		return s
	}
	return cutText(s, maxPositionTextBytes)
}

// resetRollback is the undo of a reset: put the stored position back. The
// stored maps are untrusted Connect data, so the text is redacted and capped
// like every other rendering of them (jsonText's rules). For an ordinary
// position both passes are the identity and the text stays valid params for
// the action; a position so hostile that redaction or the cap alters it was
// never one this action could write back anyway.
func resetRollback(name string, so connect.SourceOffset) string {
	b, err := json.Marshal(map[string]any{
		"connector": name,
		"position":  map[string]any{"partition": so.Partition, "offset": so.Offset},
	})
	if err != nil {
		return ResetSourceOffsetsName
	}
	s, _ := redact.Text(string(b))
	if len(s) > 4*maxPositionTextBytes {
		s = cutText(s, 4*maxPositionTextBytes)
	}
	return ResetSourceOffsetsName + " " + s
}

// ---- snapshot.mode ----

// snapshotRestart says what a restart does under the connector's snapshot.mode
// when the reset leaves stored[0] or none, and appends the table-size estimate
// when a snapshot follows.
func snapshotRestart(cfg map[string]string, estimate string, hasOffset bool) string {
	class := cfg["connector.class"]
	if !strings.HasPrefix(strings.TrimSpace(class), "io.debezium.connector.") {
		if hasOffset {
			return "not a Debezium connector; on restart it resumes from the stored position as its own implementation decides"
		}
		return "not a Debezium connector; snapshot.mode does not apply and its own implementation decides what a start without offsets does"
	}
	raw := strings.TrimSpace(cfg["snapshot.mode"])
	mode := strings.ToLower(raw)
	label := raw
	if mode == "" {
		mode, label = "initial", "initial (unset; Debezium's default)"
	}
	if hasOffset {
		switch mode {
		case "initial", "when_needed", "schema_only", "no_data", "never":
			return fmt.Sprintf("snapshot.mode=%s does not snapshot while an offset exists; the connector resumes from the stored position", label)
		case "always":
			return fmt.Sprintf("snapshot.mode=%s snapshots on every start even though an offset exists; the connector resumes from the stored position after it", label)
		case "initial_only":
			return fmt.Sprintf("snapshot.mode=%s does not stream changes after its snapshot; check the connector before relying on it", label)
		case "recovery", "schema_only_recovery":
			return fmt.Sprintf("snapshot.mode=%s rebuilds the schema history on start; the connector resumes from the stored position after it", label)
		case "configuration_based":
			return fmt.Sprintf("snapshot.mode=%s: the snapshot.mode.configuration.based.* keys decide what runs on start; the connector resumes from the stored position unless they request a data snapshot", label)
		case "exported", "custom":
			return fmt.Sprintf("snapshot.mode=%s: a custom or exported snapshot decides what runs; the connector resumes from the stored position after it", label)
		}
		return fmt.Sprintf("snapshot.mode=%s is not one Debezium documents; the connector decides what a restart does", label)
	}
	switch mode {
	case "initial":
		return "snapshot.mode=" + label + ": with no stored offsets a snapshot of the configured tables follows: " + estimate + "; existing rows are re-emitted (op r), then streaming starts"
	case "when_needed":
		return "snapshot.mode=" + label + ": with no stored offsets a snapshot follows: " + estimate + "; existing rows are re-emitted (op r), then streaming starts"
	case "always":
		return "snapshot.mode=" + label + ": a full snapshot follows on every start: " + estimate + "; existing rows are re-emitted (op r), then streaming starts"
	case "initial_only":
		return "snapshot.mode=" + label + ": a snapshot follows (" + estimate + ") and the connector then stops; it does not stream changes"
	case "schema_only", "no_data":
		return "snapshot.mode=" + label + ": no data snapshot; the connector streams from the change log's current position, so rows written before the reset are not re-emitted"
	case "never":
		return "snapshot.mode=" + label + ": no snapshot; the connector streams from the change log's current position, so rows written before the reset are not re-emitted"
	case "recovery", "schema_only_recovery":
		return "snapshot.mode=" + label + ": no data snapshot; the schema history is rebuilt and the connector streams from the change log's current position, so rows written before the reset are not re-emitted"
	case "configuration_based":
		return "snapshot.mode=" + label + ": the snapshot.mode.configuration.based.* keys decide whether data or schema is snapshotted; check the connector's config"
	case "exported":
		return "snapshot.mode=" + label + ": the snapshot comes from an exported snapshot; whether one runs depends on the exporter"
	case "custom":
		return "snapshot.mode=" + label + ": a custom snapshotter decides whether and what to snapshot"
	}
	return fmt.Sprintf("snapshot.mode=%s is not one Debezium documents; the connector decides whether to snapshot", label)
}

// tableEstimate renders the source planner's row estimate per pipeline table,
// or why there is none. It reads only counts (tableRowEstimator, snapshot_table.go).
func (a *resetSourceOffsets) tableEstimate(ctx context.Context) string {
	re, ok := a.d.Source.(tableRowEstimator)
	if !ok {
		return "row estimate unavailable (no source client with a planner estimate)"
	}
	p, ok := tools.PipelineFrom(ctx)
	if !ok || len(p.SourceTables) == 0 {
		return "row estimate unavailable (the room's pipeline lists no source tables)"
	}
	tables := p.SourceTables
	omitted := 0
	if len(tables) > maxEstimateTables {
		omitted = len(tables) - maxEstimateTables
		tables = tables[:maxEstimateTables]
	}
	parts := make([]string, 0, len(tables)+1)
	for _, t := range tables {
		n, err := re.EstimatedRows(ctx, t)
		switch {
		case err != nil:
			parts = append(parts, t+": estimate unavailable")
		case n < 0:
			parts = append(parts, t+": never analysed, no estimate")
		default:
			parts = append(parts, fmt.Sprintf("%s: ~%d rows", t, n))
		}
	}
	if omitted > 0 {
		parts = append(parts, fmt.Sprintf("and %d more tables", omitted))
	}
	return strings.Join(parts, ", ")
}

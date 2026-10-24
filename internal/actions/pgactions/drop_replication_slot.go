package pgactions

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"slices"
	"strings"
	"time"

	"piperoom/internal/actions"
	"piperoom/internal/clients"
	"piperoom/internal/clients/postgres"
	"piperoom/internal/owners"
	"piperoom/internal/tools"
	"piperoom/internal/topology"
)

// DropSlotName is the registered name of the action.
const DropSlotName = "pg_drop_replication_slot"

// defaultDropSlotTimeout bounds the reads of Validate and Preview and the
// whole of Execute (AGENTS.md §4 rule 4).
const defaultDropSlotTimeout = 10 * time.Second

// slotIdentRe is AGENTS.md §4 rule 6. A slot name is passed to Postgres as a $1
// value, never as an identifier, but it is checked anyway; owners.ValidSlotName
// (lower-case, at most 63 bytes) narrows it to what Postgres accepts as a slot.
var slotIdentRe = regexp.MustCompile(`^[a-zA-Z_][a-zA-Z0-9_]*$`)

// ErrLostNeedsForce refuses a slot whose wal_status is "lost" when the step
// does not set force: true.
var ErrLostNeedsForce = errors.New("pgactions: slot has wal_status lost; set force: true to drop it")

// errNoWriteCredentials is Execute's refusal when wiring gave no SourceWriter
// (write mode off, or SOURCE_DB_WRITE_URL empty).
var errNoWriteCredentials = errors.New("pgactions: no write credentials for the source database (PIPEROOM_WRITE_MODE=on and SOURCE_DB_WRITE_URL are needed)")

// DropSlotDeps is what pg_drop_replication_slot reads and writes through.
type DropSlotDeps struct {
	// Source is the read-only source client: Validate, Preview and Execute
	// read pg_replication_slots through it.
	Source clients.SourceReader
	// Connect reads the connector config, whose slot.name names the slot.
	Connect tools.ConfigReader
	// Writer is nil unless write mode is on and SOURCE_DB_WRITE_URL is set.
	// Only Execute touches it.
	Writer clients.SourceWriter

	// Timeout overrides defaultDropSlotTimeout; tests only. Zero = default.
	Timeout time.Duration
}

// checkedSlotDropper is the postgres Writer's drop that re-reads active and
// wal_status in the drop's own transaction and reports what it saw.
type checkedSlotDropper interface {
	DropReplicationSlotChecked(ctx context.Context, slot string, allowLost bool) (postgres.SlotSeen, error)
}

var _ checkedSlotDropper = (*postgres.Writer)(nil)

// namedSlotReader is the scoped read of the postgres client (T2): only the
// named slot reaches the process.
type namedSlotReader interface {
	ReplicationSlotsNamed(ctx context.Context, names []string) ([]clients.ReplicationSlot, int, error)
}

// NewDropReplicationSlot returns the pg_drop_replication_slot action.
func NewDropReplicationSlot(d DropSlotDeps) actions.Action { return &dropSlot{d: d} }

type dropSlot struct{ d DropSlotDeps }

var (
	_ actions.Action  = (*dropSlot)(nil)
	_ actions.Scoped  = (*dropSlot)(nil)
	_ actions.Preview = (*dropSlot)(nil)
)

type dropSlotParams struct {
	Connector string `json:"connector"`
	Slot      string `json:"slot"`
	Force     bool   `json:"force,omitempty"`
}

func (a *dropSlot) Name() string  { return DropSlotName }
func (a *dropSlot) Layer() string { return "postgres" }
func (a *dropSlot) Describe() string {
	return "Drops the inactive logical replication slot of one of the pipeline's Debezium connectors " +
		"(its slot.name), so Postgres can free the WAL the slot retains. Not reversible: the connector " +
		"loses its position and needs a new snapshot. Refused while the slot is active; a slot with " +
		"wal_status lost needs force: true."
}

func (a *dropSlot) ParamsSchema() map[string]any {
	return map[string]any{
		"type": "object",
		"properties": map[string]any{
			"connector": map[string]any{"type": "string", "description": "the pipeline's Debezium Postgres connector whose slot this is"},
			"slot":      map[string]any{"type": "string", "description": "replication slot name; must equal the connector's slot.name"},
			"force":     map[string]any{"type": "boolean", "description": "required when the slot's wal_status is lost"},
		},
		"additionalProperties": false,
		"required":             []string{"connector", "slot"},
	}
}

// parseDropSlot decodes params strictly and checks them without I/O.
func parseDropSlot(raw json.RawMessage) (dropSlotParams, error) {
	var p dropSlotParams
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&p); err != nil {
		return p, fmt.Errorf("pgactions: %s: bad params: %w", DropSlotName, err)
	}
	if p.Connector == "" {
		return p, fmt.Errorf("pgactions: %s: connector is required", DropSlotName)
	}
	if !slotIdentRe.MatchString(p.Slot) || !owners.ValidSlotName(p.Slot) {
		return p, fmt.Errorf("pgactions: %s: invalid slot name %q (lower-case letters, digits and _, at most 63)", DropSlotName, capSlotParam(p.Slot))
	}
	return p, nil
}

// CheckScope refuses a connector that is not the pipeline's and, when
// owners.yaml declares watch.slot, any other slot. Whether the slot is the
// connector's slot.name needs Connect, so Validate checks that.
func (a *dropSlot) CheckScope(pl owners.Pipeline, raw json.RawMessage) error {
	p, err := parseDropSlot(raw)
	if err != nil {
		return err
	}
	if err := refuseNonPostgres(DropSlotName, pl); err != nil {
		return err
	}
	if !slices.Contains(pl.Connectors, p.Connector) {
		return fmt.Errorf("pgactions: %s: %w: connector %q is not part of pipeline %q", DropSlotName, tools.ErrOutOfScope, capSlotParam(p.Connector), pl.ID)
	}
	if declared := pl.Watch().Slot; declared != "" && declared != p.Slot {
		return fmt.Errorf("pgactions: %s: %w: slot %q is not pipeline %q's slot (owners.yaml watch.slot is %q)", DropSlotName, tools.ErrOutOfScope, p.Slot, pl.ID, declared)
	}
	return nil
}

// Validate reads the connector config and the slot and refuses: a slot that
// is not the connector's slot.name (tools.ErrOutOfScope), a missing or
// physical slot (clients.ErrNotFound), an active one (clients.ErrSlotActive)
// and a lost one without force (ErrLostNeedsForce). It does not depend on a
// scope in ctx (CheckScope is the room's check) and never touches the writer.
func (a *dropSlot) Validate(ctx context.Context, raw json.RawMessage) error {
	p, err := parseDropSlot(raw)
	if err != nil {
		return err
	}
	ctx, cancel := a.ctx(ctx)
	defer cancel()
	_, err = a.check(ctx, p)
	return err
}

// check is Validate's read: the connector's slot, then the slot's state.
func (a *dropSlot) check(ctx context.Context, p dropSlotParams) (clients.ReplicationSlot, error) {
	if err := a.connectorSlot(ctx, p); err != nil {
		return clients.ReplicationSlot{}, err
	}
	s, err := a.readSlot(ctx, p.Slot)
	if err != nil {
		return s, err
	}
	return s, refusal(s, p.Force)
}

// refusal is the state rule shared by Validate and Execute.
func refusal(s clients.ReplicationSlot, force bool) error {
	if s.Active {
		return fmt.Errorf("pgactions: %s: slot %q is in use by a consumer; stop the connector first (dropping the slot of a running connector loses its position): %w", DropSlotName, s.Name, clients.ErrSlotActive)
	}
	if s.WALStatus == "lost" && !force {
		return fmt.Errorf("%w: Postgres already removed WAL slot %q needed (wal_status lost), so changes made while it fell behind are missing downstream and the connector can never resume from it; dropping it removes the last record of where the connector stopped. Either way the connector needs a new snapshot; set force once the room accepts that loss and plans it", ErrLostNeedsForce, s.Name)
	}
	return nil
}

// connectorSlot refuses a slot that is not p.Connector's slot.name.
func (a *dropSlot) connectorSlot(ctx context.Context, p dropSlotParams) error {
	if a.d.Connect == nil {
		return fmt.Errorf("pgactions: %s: no Connect client to read connector %q's slot.name", DropSlotName, capSlotParam(p.Connector))
	}
	cfg, err := a.d.Connect.Config(ctx, p.Connector)
	if err != nil {
		return fmt.Errorf("pgactions: %s: read connector %q config: %w", DropSlotName, capSlotParam(p.Connector), err)
	}
	slot, err := owners.ConnectorSlot(cfg)
	if err != nil {
		return fmt.Errorf("pgactions: %s: connector %q: %w", DropSlotName, capSlotParam(p.Connector), err)
	}
	if slot == "" {
		return fmt.Errorf("pgactions: %s: %w: connector %q is not a Debezium Postgres source, it has no slot", DropSlotName, tools.ErrOutOfScope, capSlotParam(p.Connector))
	}
	if slot != p.Slot {
		return fmt.Errorf("pgactions: %s: %w: slot %q is not connector %q's slot (slot.name is %q)", DropSlotName, tools.ErrOutOfScope, p.Slot, capSlotParam(p.Connector), slot)
	}
	return nil
}

// readSlot reads one slot through the read-only client. A physical slot (no
// plugin) is refused as not found: a CDC fix never drops a standby's slot.
func (a *dropSlot) readSlot(ctx context.Context, name string) (clients.ReplicationSlot, error) {
	if a.d.Source == nil {
		return clients.ReplicationSlot{}, fmt.Errorf("pgactions: %s: no source database client", DropSlotName)
	}
	var slots []clients.ReplicationSlot
	var err error
	if nr, ok := a.d.Source.(namedSlotReader); ok {
		slots, _, err = nr.ReplicationSlotsNamed(ctx, []string{name})
	} else {
		slots, err = a.d.Source.ReplicationSlots(ctx)
	}
	if err != nil {
		return clients.ReplicationSlot{}, fmt.Errorf("pgactions: %s: read replication slots: %w", DropSlotName, err)
	}
	for _, s := range slots {
		if s.Name != name {
			continue
		}
		if s.Plugin == "" {
			return s, fmt.Errorf("pgactions: %s: slot %q is a physical slot, never dropped by a CDC fix: %w", DropSlotName, name, clients.ErrNotFound)
		}
		return s, nil
	}
	return clients.ReplicationSlot{}, fmt.Errorf("pgactions: %s: slot %q: %w", DropSlotName, name, clients.ErrNotFound)
}

// Preview shows the slot's state and what the drop costs. It reads only
// through the Connect config and the read-only source client.
func (a *dropSlot) Preview(ctx context.Context, raw json.RawMessage) (actions.PreviewResult, error) {
	p, err := parseDropSlot(raw)
	if err != nil {
		return actions.PreviewResult{}, err
	}
	if err := a.ctxScope(ctx, raw); err != nil {
		return actions.PreviewResult{}, err
	}
	ctx, cancel := a.ctx(ctx)
	defer cancel()
	if err := a.connectorSlot(ctx, p); err != nil {
		return actions.PreviewResult{}, err
	}
	s, err := a.readSlot(ctx, p.Slot)
	if err != nil {
		return actions.PreviewResult{}, err
	}
	state := "inactive"
	if s.Active {
		state = "ACTIVE"
	}
	cur := fmt.Sprintf("logical slot, plugin %s, %s, wal_status %s, retains %s of WAL (pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn)), restart_lsn %s",
		s.Plugin, state, slotOrUnknown(s.WALStatus), topology.HumanBytes(s.RetainedWALBytes), slotOrUnknown(s.RestartLSN))
	exp := fmt.Sprintf("slot %s is gone and Postgres can recycle the %s of WAL it retains at the next checkpoints.", s.Name, topology.HumanBytes(s.RetainedWALBytes))
	if err := refusal(s, p.Force); err != nil {
		exp = "refused as things stand: " + err.Error()
	}
	return actions.PreviewResult{
		Target:   fmt.Sprintf("replication slot %s of connector %s", s.Name, p.Connector),
		Current:  cur,
		Expected: exp,
		Downstream: []string{
			fmt.Sprintf("NOT reversible: re-creating slot %s starts it at the current WAL position, not where %s stopped, so connector %s cannot resume from its stored offset; it needs a new snapshot (connect_snapshot_table or a snapshot.mode change) and changes made between its last offset and the new slot reach the topics only through that snapshot", s.Name, s.Name, p.Connector),
		},
		Reversible: false,
	}, nil
}

// Execute re-checks the connector's slot, then drops the slot through the
// writer, which re-reads active and wal_status in the drop's own transaction.
func (a *dropSlot) Execute(ctx context.Context, raw json.RawMessage) (string, error) {
	p, err := parseDropSlot(raw)
	if err != nil {
		return "", err
	}
	if err := a.ctxScope(ctx, raw); err != nil {
		return "", err
	}
	if a.d.Writer == nil {
		return "", errNoWriteCredentials
	}
	ctx, cancel := a.ctx(ctx)
	defer cancel()
	if err := a.connectorSlot(ctx, p); err != nil {
		return "", err
	}

	var seen postgres.SlotSeen
	if cd, ok := a.d.Writer.(checkedSlotDropper); ok {
		seen, err = cd.DropReplicationSlotChecked(ctx, p.Slot, p.Force)
		if errors.Is(err, postgres.ErrSlotLost) {
			err = fmt.Errorf("%w (%w)", refusal(clients.ReplicationSlot{Name: p.Slot, WALStatus: "lost"}, false), err)
		}
	} else {
		// A writer without the checked drop (a fake or replay client):
		// re-read through the source client right before the drop. Postgres
		// still refuses a slot that turned active in between.
		var s clients.ReplicationSlot
		if s, err = a.readSlot(ctx, p.Slot); err == nil {
			if err = refusal(s, p.Force); err == nil {
				err = a.d.Writer.DropReplicationSlot(ctx, p.Slot)
			}
		}
		seen = postgres.SlotSeen{Name: p.Slot, Plugin: s.Plugin, Active: s.Active, WALStatus: s.WALStatus, RetainedWALBytes: s.RetainedWALBytes}
	}
	if err != nil {
		if errors.Is(err, clients.ErrSlotActive) {
			return "", fmt.Errorf("pgactions: %s: refused, nothing dropped: slot %q became active since the proposal was approved: %w", DropSlotName, p.Slot, err)
		}
		return "", fmt.Errorf("pgactions: %s %q: %w", DropSlotName, p.Slot, err)
	}
	return fmt.Sprintf("dropped replication slot %s of connector %s; right before the drop it was inactive, plugin %s, wal_status %s, retaining %s of WAL, which Postgres can now recycle. Connector %s cannot resume from its stored offset: it needs a new snapshot.",
		p.Slot, p.Connector, slotOrUnknown(seen.Plugin), slotOrUnknown(seen.WALStatus), topology.HumanBytes(seen.RetainedWALBytes), p.Connector), nil
}

// ctxScope is the fail-closed scope gate of Preview and Execute, before any
// I/O: the room's pipeline must be in ctx (tools.WithPipeline) and the params
// in it; tools.WithUnscoped (tests, CLI) passes; anything else is
// tools.ErrNoScope.
func (a *dropSlot) ctxScope(ctx context.Context, raw json.RawMessage) error {
	sc, err := tools.ScopeFor(ctx, nil)
	if err != nil {
		if !errors.Is(err, tools.ErrNoScope) {
			err = fmt.Errorf("%w: %w", tools.ErrNoScope, err)
		}
		return fmt.Errorf("pgactions: %s: needs the room's pipeline in ctx: %w", DropSlotName, err)
	}
	if sc.Unscoped() {
		return nil
	}
	return a.CheckScope(sc.Pipeline(), raw)
}

func (a *dropSlot) ctx(ctx context.Context) (context.Context, context.CancelFunc) {
	t := defaultDropSlotTimeout
	if a.d.Timeout > 0 {
		t = a.d.Timeout
	}
	return context.WithTimeout(ctx, t)
}

func slotOrUnknown(s string) string {
	if s == "" {
		return "unknown"
	}
	return s
}

// short caps a param echoed in an error.
func capSlotParam(s string) string {
	const n = 80
	if len(s) <= n {
		return s
	}
	return strings.ToValidUTF8(s[:n], "") + "…"
}

package postgres

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"piperoom/internal/clients"
	pgclient "piperoom/internal/clients/postgres"
	"piperoom/internal/setup/plan"
)

// Constants of the driver: the source kind, the connector class and the
// defaults Debezium and this repository use (dev/connectors/pg-orders-connector.json).
const (
	// SourceKind is the SetupRequest.SourceKind this driver serves.
	SourceKind = "postgres"
	// ConnectorClass is the Debezium Postgres connector plugin.
	ConnectorClass = "io.debezium.connector.postgresql.PostgresConnector"
	// OutputPlugin is the logical decoding plugin the setup uses.
	OutputPlugin = "pgoutput"
	// DefaultPublication is Debezium's publication name when the config sets
	// none.
	DefaultPublication = "dbz_publication"
	// DefaultSlot is Debezium's slot name when the config sets none.
	DefaultSlot = "debezium"
	// StatsRole is the only predefined role the setup's read role is a member
	// of: the blocker and slot reads need other sessions' statistics, nothing
	// more.
	StatsRole = "pg_read_all_stats"
)

// The real names of the operations that fix a failing check. A check names
// one of these; a test pins every one to the runner's op table or a registered
// action, so a drift is a test failure, not an agent proposing an op that
// does not exist.
const (
	opAlterSystem         = plan.OpSourceAlterSystem // "source_alter_system", T3
	opCreatePublication   = "source_create_publication"
	opAddPublicationTable = "pg_add_publication_table"
	opDropSlot            = "pg_drop_replication_slot"
	opSetReplicaIdentity  = "pg_set_replica_identity"
	opCreateReadRole      = "source_create_read_role"
	opGrantSelect         = "source_grant_select"
)

// The check ids, stable in logs and tests.
const (
	CheckWALLevel            = "wal_level"
	CheckMaxWALSenders       = "max_wal_senders"
	CheckMaxReplicationSlots = "max_replication_slots"
	CheckReplicationSlot     = "replication_slot"
	CheckReplicationSlotRoom = "replication_slot_room"
	CheckPublication         = "publication"
	CheckPublicationTables   = "publication_tables"
	CheckReplicaIdentity     = "replica_identity"
	CheckReadRole            = "read_role"
	CheckReadRoleGrants      = "read_role_grants"
	CheckRDSParameterGroup   = "rds_parameter_group"
)

// Reader is the read-only source surface the checks need. It is exactly what
// *postgres.DB provides on the read-only pool; a setup room's remote client
// (remoteclients.Source) gains PublicationTables when the runner's
// source_publication_tables op lands (see the PR's gaps).
type Reader interface {
	Settings(ctx context.Context) ([]pgclient.DBSetting, error)
	ReplicationSlots(ctx context.Context) ([]clients.ReplicationSlot, error)
	TableReplication(ctx context.Context, tables []string) ([]pgclient.TableReplication, error)
	RoleInfo(ctx context.Context, role string, tables []string) (pgclient.RoleInfo, error)
	PublicationTables(ctx context.Context, publication, role string, tables []string) ([]pgclient.PublicationTable, error)
}

var _ Reader = (*pgclient.DB)(nil)

// rdsReplicationReader is the optional local adapter behind Evidence's
// RDSReplication: *postgres.DB answers it; the runner's remote client does
// not, so an RDS/Aurora role without the rolreplication attribute fails
// read_role over the runner until the runner's source_role_info carries the
// flag (contract change request).
type rdsReplicationReader interface {
	RDSRoleReplication(ctx context.Context, role string) (bool, error)
}

// Evidence is what one check's read returned.
type Evidence struct {
	Settings    []pgclient.DBSetting
	Slots       []clients.ReplicationSlot
	Tables      []pgclient.TableReplication
	Role        pgclient.RoleInfo
	Publication []pgclient.PublicationTable

	// PublicationExists is false when the read reported that the publication
	// is absent (clients.ErrNotFound), which is a check failure, not an error.
	PublicationExists bool
	// RDSReplication is the role's membership in rds_replication (RDS/Aurora),
	// read through the optional rdsReplicationReader; false when the reader
	// does not carry the read.
	RDSReplication bool
}

// ReadFunc is one check's read.
type ReadFunc func(ctx context.Context, r Reader, d Driver) (Evidence, error)

// Check is one prerequisite: the read that answers it, the condition that
// passes, and the fix a failure needs. ReadOp is the source read's name
// (source_settings, source_table_replication, ...), FixOp the operation the
// room proposes (see the op* constants), FixAsk the documented ask for a
// failure no runner op can fix (the RDS parameter group).
//
// FixFor overrides FixOp/FixAsk for a check whose fix depends on what the read
// returned (the read role exists but has no LOGIN: creating it cannot help).
// FixParam is the factory default ALTER SYSTEM parameter a fix sets, so the
// guard's allow-list exception is asked about the parameter that will run.
type Check struct {
	ID          string
	Description string
	ReadOp      string
	Read        ReadFunc
	Pass        func(d Driver, ev Evidence) bool
	FixOp       string
	FixFor      func(d Driver, ev Evidence) (fixOp, ask string)
	TierFloor   string
	FixParam    string
	FixAsk      string
}

// Fix is the operation and the ask a failure of this check needs for one
// reading: FixFor's answer when it has one, the check's static fields
// otherwise. Only one of the two is set.
func (c Check) Fix(d Driver, ev Evidence) (fixOp, ask string) {
	if c.FixFor != nil {
		return c.FixFor(d, ev)
	}
	return c.FixOp, c.FixAsk
}

// guardOp is the fix's op as the guard table spells it. The drop-slot action
// is registered under pgactions.DropSlotName and the guard row under
// plan.OpSourceDropSlot; plan §3.6 forbids the drop on production whichever
// name the step carries, so the check asks about the guard's name (the
// contract change that should give the guard every registered spelling is
// recorded in the PR, not worked around here).
func (c Check) guardOp() string {
	if c.FixOp == opDropSlot {
		return plan.OpSourceDropSlot
	}
	return c.FixOp
}

// Tier is the tier the check's fix runs at on class: the higher of the op's
// floor and the class (plan.TierOf), so a production target routes a T2 fix to
// T3. It returns plan.ErrGuard when the guard table forbids the fix on class
// (plan §3.6): a production slot drop is forbidden, never merely T3, so the
// room can never offer it as an approvable step.
func (c Check) Tier(class string) (string, error) {
	tier, err := plan.TierOf(c.TierFloor, class)
	if err != nil {
		return "", err
	}
	if c.guardOp() == "" {
		return tier, nil
	}
	if row, forbidden := plan.Forbidden(plan.GuardRequest{
		Op:    c.guardOp(),
		Class: class,
		Facts: plan.StepFacts{Param: c.FixParam},
	}); forbidden {
		return "", fmt.Errorf("%w: %s's fix %q on a %s target: %s", plan.ErrGuard, c.ID, c.FixOp, class, row.Reason)
	}
	return tier, nil
}

// Result is one check's outcome. Err is a read failure (the database was
// unreachable, the role was refused); OK is false then too. Note is set when
// the check could not be read because an earlier check has to pass first (the
// publication reads name the connector's role, which cannot be read before the
// role exists): OK is false, Err is nil, and Note says what to do.
type Result struct {
	Check    Check
	OK       bool
	Err      error
	Evidence Evidence
	Note     string
}

// Driver is the Postgres prerequisite driver for one pipeline: the
// publication and slot the connector will use, the role it logs in as, and the
// source tables.
type Driver struct {
	Publication string
	Slot        string
	Role        string
	Tables      []string
}

// publicationName is the configured publication or Debezium's default.
func (d Driver) publicationName() string {
	if d.Publication == "" {
		return DefaultPublication
	}
	return d.Publication
}

// slotName is the configured slot or Debezium's default.
func (d Driver) slotName() string {
	if d.Slot == "" {
		return DefaultSlot
	}
	return d.Slot
}

// Validate checks the driver's configuration before any read: a role, a
// non-empty table list, identifier-shaped table, publication, slot and role
// names. It performs no I/O.
func (d Driver) Validate() error {
	if err := checkTables(d.Tables); err != nil {
		return err
	}
	if _, err := quoteIdent(d.publicationName()); err != nil {
		return fmt.Errorf("publication: %w", err)
	}
	if _, err := quoteSlot(d.slotName()); err != nil {
		return fmt.Errorf("slot: %w", err)
	}
	if d.Role == "" {
		return fmt.Errorf("%w: the connector's database.user is required", ErrParams)
	}
	if _, err := quoteRole(d.Role); err != nil {
		return fmt.Errorf("role: %w", err)
	}
	return nil
}

// Run validates the driver, then runs every check in order. A configuration
// error is returned before any read; a read's own error is stored on its
// Result so the other checks still run.
//
// The publication reads name the connector's role (has_table_privilege), so
// when the role does not exist they are not issued: they would carry a read
// error ("role ... does not exist") instead of a verdict. Their Results say so
// and the room sees the read_role fix (create the role) first.
func (d Driver) Run(ctx context.Context, r Reader) ([]Result, error) {
	if err := d.Validate(); err != nil {
		return nil, err
	}
	if r == nil {
		return nil, fmt.Errorf("%w: no source reader", ErrParams)
	}
	checks := d.Checks()
	out := make([]Result, 0, len(checks))
	roleMissing := false
	for _, c := range checks {
		if roleMissing && c.ReadOp == "source_publication_tables" {
			out = append(out, Result{Check: c, Note: "the connector's role does not exist yet: create it (the read_role check names the fix) and re-read"})
			continue
		}
		ev, err := c.Read(ctx, r, d)
		if c.ID == CheckReadRole && err == nil && !ev.Role.Exists {
			roleMissing = true
		}
		out = append(out, Result{Check: c, OK: err == nil && c.Pass(d, ev), Err: err, Evidence: ev})
	}
	return out, nil
}

// Checks returns the driver's ordered checklist: the settings that enable
// logical decoding, room for the connector's slot, the replica identity of
// every table, the connector role and its grants, the publication and its
// tables, and the RDS/Aurora parameter. The role's checks run before the
// publication read, which names the role in has_table_privilege and cannot
// answer for one that does not exist yet.
func (d Driver) Checks() []Check {
	return []Check{
		{
			ID:          CheckWALLevel,
			Description: "wal_level must be logical: pgoutput decodes nothing below it.",
			ReadOp:      "source_settings",
			Pass: func(d Driver, ev Evidence) bool {
				v, ok := settingValue(ev.Settings, "wal_level")
				return ok && v == "logical"
			},
			FixOp:     opAlterSystem,
			FixParam:  "wal_level",
			TierFloor: plan.TierT3,
			FixAsk:    "On RDS/Aurora, ALTER SYSTEM is unavailable: change wal_level and rds.logical_replication together in the DB parameter group (pending-reboot) and apply it in a maintenance window.",
			Read:      readSettings,
		},
		{
			ID:          CheckMaxWALSenders,
			Description: "max_wal_senders must be at least 1 (one per connector, plus a spare for a standby or a replica slot).",
			ReadOp:      "source_settings",
			Pass: func(d Driver, ev Evidence) bool {
				n, ok := settingInt(ev.Settings, "max_wal_senders")
				return ok && n >= 1
			},
			FixOp:     opAlterSystem,
			FixParam:  "max_wal_senders",
			TierFloor: plan.TierT3,
			FixAsk:    "On RDS/Aurora, change it in the DB parameter group and apply it in a maintenance window.",
			Read:      readSettings,
		},
		{
			ID:          CheckMaxReplicationSlots,
			Description: "max_replication_slots must be at least 1: the connector creates one logical slot.",
			ReadOp:      "source_settings",
			Pass: func(d Driver, ev Evidence) bool {
				n, ok := settingInt(ev.Settings, "max_replication_slots")
				return ok && n >= 1
			},
			FixOp:     opAlterSystem,
			FixParam:  "max_replication_slots",
			TierFloor: plan.TierT3,
			FixAsk:    "On RDS/Aurora, change it in the DB parameter group and apply it in a maintenance window.",
			Read:      readSettings,
		},
		{
			ID:          CheckReplicationSlot,
			Description: "A slot already using the connector's name must be the connector's own pgoutput slot; another plugin's slot cannot be taken over.",
			ReadOp:      "source_replication_slots",
			Pass:        slotOwned,
			FixOp:       opDropSlot,
			TierFloor:   plan.TierT2,
			FixAsk:      "A slot of the name that is active and uses another plugin belongs to a running process: stop that process first; the drop is refused while the slot is active.",
			Read:        readSlots,
		},
		{
			ID:          CheckReplicationSlotRoom,
			Description: "max_replication_slots must leave room for one more slot while the connector's own slot does not exist yet.",
			ReadOp:      "source_replication_slots",
			Pass:        slotRoom,
			FixOp:       opAlterSystem,
			FixParam:    "max_replication_slots",
			TierFloor:   plan.TierT3,
			FixAsk:      "On RDS/Aurora, change max_replication_slots in the DB parameter group and apply it in a maintenance window.",
			Read:        readSlots,
		},
		{
			ID:          CheckReplicaIdentity,
			Description: "Every table needs REPLICA IDENTITY FULL, a USING INDEX identity, or DEFAULT with a primary key: REPLICA IDENTITY NOTHING (or DEFAULT without a key) makes UPDATE and DELETE fail once the table is published. The fix is pg_set_replica_identity.",
			ReadOp:      "source_table_replication",
			Pass:        replicaIdentityReady,
			FixOp:       opSetReplicaIdentity,
			TierFloor:   plan.TierT2,
			Read:        readTables,
		},
		{
			ID:          CheckReadRole,
			Description: "The connector's database.user must exist, log in and hold REPLICATION (or be a member of rds_replication on RDS/Aurora, which Debezium accepts in its place).",
			ReadOp:      "source_role_info",
			Pass:        readRoleReady,
			FixOp:       opCreateReadRole,
			FixFor:      readRoleFix,
			TierFloor:   plan.TierT2,
			Read:        readRole,
		},
		{
			ID:          CheckReadRoleGrants,
			Description: "The connector's role must have SELECT on every source table.",
			ReadOp:      "source_role_info",
			Pass:        roleSelectsAll,
			FixOp:       opGrantSelect,
			TierFloor:   plan.TierT2,
			Read:        readRole,
		},
		{
			ID:          CheckPublication,
			Description: "The connector's publication must exist (publication.name).",
			ReadOp:      "source_publication_tables",
			Pass:        func(d Driver, ev Evidence) bool { return ev.PublicationExists },
			FixOp:       opCreatePublication,
			TierFloor:   plan.TierT2,
			Read:        readPublication,
		},
		{
			ID:          CheckPublicationTables,
			Description: "Every source table must be a member of the publication, or its changes never reach Kafka.",
			ReadOp:      "source_publication_tables",
			Pass:        publicationHasTables,
			FixOp:       opAddPublicationTable,
			TierFloor:   plan.TierT2,
			Read:        readPublication,
		},
		{
			ID:          CheckRDSParameterGroup,
			Description: "On RDS/Aurora, rds.logical_replication must be on (the parameter group value, not a setting a session can change).",
			ReadOp:      "source_settings",
			Pass:        rdsLogicalReplicationOn,
			FixOp:       "",
			TierFloor:   plan.TierT3,
			FixAsk:      "No runner op changes an RDS parameter group: the person sets rds.logical_replication=1 in the group, applies it in a maintenance window (pending-reboot), and the check is re-read.",
			Read:        readSettings,
		},
	}
}

// ---- reads ----

func readSettings(ctx context.Context, r Reader, d Driver) (Evidence, error) {
	settings, err := r.Settings(ctx)
	if err != nil {
		return Evidence{}, fmt.Errorf("postgres driver: read settings: %w", err)
	}
	return Evidence{Settings: settings}, nil
}

// readSlots reads the slots and max_replication_slots: room for the
// connector's slot is a slot-list fact, not a settings-only one.
func readSlots(ctx context.Context, r Reader, d Driver) (Evidence, error) {
	slots, err := r.ReplicationSlots(ctx)
	if err != nil {
		return Evidence{}, fmt.Errorf("postgres driver: read replication slots: %w", err)
	}
	settings, err := r.Settings(ctx)
	if err != nil {
		return Evidence{}, fmt.Errorf("postgres driver: read settings: %w", err)
	}
	return Evidence{Slots: slots, Settings: settings}, nil
}

func readTables(ctx context.Context, r Reader, d Driver) (Evidence, error) {
	tables, err := r.TableReplication(ctx, d.Tables)
	if err != nil {
		return Evidence{}, fmt.Errorf("postgres driver: read table replication: %w", err)
	}
	return Evidence{Tables: tables}, nil
}

func readRole(ctx context.Context, r Reader, d Driver) (Evidence, error) {
	role, err := r.RoleInfo(ctx, d.Role, d.Tables)
	if err != nil {
		return Evidence{}, fmt.Errorf("postgres driver: read role %s: %w", clipName(d.Role), err)
	}
	ev := Evidence{Role: role}
	if role.Exists {
		if rr, ok := r.(rdsReplicationReader); ok {
			rds, err := rr.RDSRoleReplication(ctx, d.Role)
			if err != nil {
				return Evidence{}, fmt.Errorf("postgres driver: read rds_replication membership for %s: %w", clipName(d.Role), err)
			}
			ev.RDSReplication = rds
		}
	}
	return ev, nil
}

func readPublication(ctx context.Context, r Reader, d Driver) (Evidence, error) {
	tables, err := r.PublicationTables(ctx, d.publicationName(), d.Role, d.Tables)
	if errors.Is(err, clients.ErrNotFound) {
		// A missing publication is the check's failure, not a read error.
		return Evidence{PublicationExists: false}, nil
	}
	if err != nil {
		return Evidence{}, fmt.Errorf("postgres driver: read publication %s: %w", clipName(d.publicationName()), err)
	}
	return Evidence{Publication: tables, PublicationExists: true}, nil
}

// ---- pass predicates ----

// settingValue returns a setting's value, and false when the server did not
// report it.
func settingValue(settings []pgclient.DBSetting, name string) (string, bool) {
	for _, s := range settings {
		if s.Name == name {
			return s.Value, true
		}
	}
	return "", false
}

// settingInt parses a numeric setting; a value with a unit is not one of the
// settings this driver counts.
func settingInt(settings []pgclient.DBSetting, name string) (int, bool) {
	v, ok := settingValue(settings, name)
	if !ok {
		return 0, false
	}
	n, err := strconv.Atoi(strings.TrimSpace(v))
	return n, err == nil
}

// slotOwned reports whether no slot uses the connector's name yet, or the one
// that does is its own pgoutput slot. A physical slot (no plugin) or another
// decoding plugin's slot cannot be taken over by Debezium.
func slotOwned(d Driver, ev Evidence) bool {
	for _, s := range ev.Slots {
		if s.Name == d.slotName() {
			return s.Plugin == OutputPlugin
		}
	}
	return true
}

// slotRoom reports whether the connector can create its slot: with its own
// slot already present it needs no room, otherwise max_replication_slots must
// exceed the slots that exist.
func slotRoom(d Driver, ev Evidence) bool {
	for _, s := range ev.Slots {
		if s.Name == d.slotName() {
			return true
		}
	}
	max, ok := settingInt(ev.Settings, "max_replication_slots")
	return ok && len(ev.Slots) < max
}

// publicationHasTables reports whether every source table is a publication
// member. A missing publication fails (the previous check names its fix).
func publicationHasTables(d Driver, ev Evidence) bool {
	if !ev.PublicationExists {
		return false
	}
	members := make(map[string]bool, len(ev.Publication))
	for _, t := range ev.Publication {
		members[t.Schema+"."+t.Table] = t.InPublication
	}
	for _, t := range d.Tables {
		if !members[t] {
			return false
		}
	}
	return true
}

// replicaIdentityReady reports whether every table can emit updates and
// deletes. REPLICA IDENTITY FULL and USING INDEX always can. DEFAULT can
// because it publishes the primary key -- but only when the table has one:
// under DEFAULT without a key Postgres publishes nothing, exactly as under
// NOTHING. NOTHING ignores the primary key, so a keyed table with NOTHING
// still fails UPDATE/DELETE once it is published and must fail here.
func replicaIdentityReady(d Driver, ev Evidence) bool {
	seen := make(map[string]pgclient.TableReplication, len(ev.Tables))
	for _, t := range ev.Tables {
		seen[t.Table] = t
	}
	for _, name := range d.Tables {
		t, ok := seen[name]
		if !ok {
			return false
		}
		ready := t.ReplicaIdentity == "full" || t.ReplicaIdentity == "index" ||
			(t.ReplicaIdentity == "default" && t.HasPrimaryKey)
		if !ready {
			return false
		}
	}
	return true
}

// readRoleReady reports whether the connector's role can log in and replicate.
// RDS/Aurora grants replication through membership in rds_replication instead
// of the rolreplication attribute, and Debezium's own login check accepts
// either.
func readRoleReady(d Driver, ev Evidence) bool {
	return ev.Role.Exists && ev.Role.CanLogin && (ev.Role.Replication || ev.RDSReplication)
}

// readRoleFix names what a failing read_role check needs. A role that does not
// exist is created (source_create_read_role). An existing role cannot be
// created again and must never be re-passworded (clients.SetupSourceWriter),
// so it gets no op and an honest ask: ALTER ROLE adds the missing attribute,
// or GRANT rds_replication on RDS/Aurora.
func readRoleFix(d Driver, ev Evidence) (fixOp, ask string) {
	if ev.Role.Exists {
		return "", "The role exists but lacks LOGIN or REPLICATION: no setup op changes an existing role (it is never re-passworded). Run ALTER ROLE " +
			quoteRoleOrName(d.Role) + " LOGIN REPLICATION, or on RDS/Aurora GRANT rds_replication TO " + quoteRoleOrName(d.Role) + ", then re-read."
	}
	return opCreateReadRole, ""
}

// quoteRoleOrName quotes role for the message when it is a valid identifier
// (the driver validated it already) and falls back to the clipped name.
func quoteRoleOrName(role string) string {
	if q, err := quoteRole(role); err == nil {
		return q
	}
	return clipName(role)
}

// roleSelectsAll reports whether the role has SELECT on every source table.
func roleSelectsAll(d Driver, ev Evidence) bool {
	if !ev.Role.Exists {
		return false
	}
	for _, t := range d.Tables {
		if !ev.Role.Selectable[t] {
			return false
		}
	}
	return true
}

// rdsLogicalReplicationOn passes on a server without rds.logical_replication
// (not RDS/Aurora) and on one that has it on.
func rdsLogicalReplicationOn(d Driver, ev Evidence) bool {
	v, ok := settingValue(ev.Settings, "rds.logical_replication")
	if !ok {
		return true
	}
	switch strings.ToLower(strings.TrimSpace(v)) {
	case "1", "on", "true", "yes":
		return true
	}
	return false
}

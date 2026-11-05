package mongodb

import (
	"context"
	"fmt"
	"strings"

	mongoclient "piperoom/internal/clients/mongodb"
	"piperoom/internal/setup/plan"
)

// Constants of the driver.
const (
	// SourceKind is the SetupRequest.SourceKind this driver serves.
	SourceKind = "mongodb"
	// ConnectorClass is the Debezium MongoDB connector plugin.
	ConnectorClass = "io.debezium.connector.mongodb.MongoDbConnector"
	// CanaryCollection is the ladder's canary collection, created in the
	// first captured database (Driver.CanaryNamespace).
	CanaryCollection = "piperoom_canary"
	// DefaultAuthDB is where the capture user lives: the writer creates it in
	// admin (internal/clients/mongodb SetupWriter.CreateReadRole).
	DefaultAuthDB = "admin"
)

// Platforms of Driver.Platform: where the MongoDB runs. Only the person or the
// provisional pipeline sets it, never model text.
const (
	PlatformSelfManaged = ""      // a mongod or mongos the person runs (host, VM, container)
	PlatformAtlas       = "atlas" // MongoDB Atlas: users and the deployment are managed in the Atlas UI or API
)

// The operations that fix a failing check. TestCheckFixOpsAreRegistered pins
// each to the runner's catalogue or remoteclients' op table.
const (
	opComposeUp      = "compose_up"              // the room's own stack, rendered with --replSet (T1)
	opCreateReadRole = "source_create_read_role" // the capture user with its role (T2)
)

// The read ops each check names (remoteclients and the MySQL driver use the
// same names; the wire is source-neutral).
const (
	readSettings = "source_settings"
	readRoleInfo = "source_role_info"
)

// The check ids. The first two are the names of cmd/devctl's MongoDB setup
// breaks (U7c: standalone-mongod, no-oplog-role), so a scenario label and the
// setup run's failed check name one thing.
const (
	CheckChangeStreams       = "mongo-change-streams"
	CheckCaptureUser         = "mongo-capture-user"
	CheckOplogRead           = "mongo-oplog-read"
	CheckCollectionsReadable = "mongo-collections-readable"
)

// Reader is the read-only source surface the checks need: the U7a client's
// settings and role reads.
type Reader interface {
	Settings(ctx context.Context) ([]mongoclient.DBSetting, error)
	RoleInfo(ctx context.Context, role string, tables []string) (mongoclient.RoleInfo, error)
}

var _ Reader = (*mongoclient.Client)(nil)

// Evidence is what one check's read returned.
type Evidence struct {
	Settings []mongoclient.DBSetting
	User     mongoclient.RoleInfo
}

// ReadFunc is one check's read.
type ReadFunc func(ctx context.Context, r Reader, d Driver) (Evidence, error)

// Check is one prerequisite, shaped as the other drivers': ReadOp is the
// source read's name, FixOp the operation the room proposes, FixAsk the
// documented ask when no op can fix it, FixFor the override when the fix
// depends on the evidence or on who created the source, TierFloor the fix's
// floor.
type Check struct {
	ID          string
	Description string
	ReadOp      string
	Read        ReadFunc
	Pass        func(d Driver, ev Evidence) bool
	FixOp       string
	FixFor      func(d Driver, ev Evidence) (fixOp, ask string)
	TierFloor   string
	FixAsk      string
}

// Fix is the operation or the ask a failure of this check needs for one
// reading. Only one of the two is set.
func (c Check) Fix(d Driver, ev Evidence) (fixOp, ask string) {
	if c.FixFor != nil {
		return c.FixFor(d, ev)
	}
	return c.FixOp, c.FixAsk
}

// Tier is the tier the check's fix runs at on class (plan.TierOf), or
// plan.ErrGuard when the guard table forbids the fix there.
func (c Check) Tier(class string) (string, error) {
	tier, err := plan.TierOf(c.TierFloor, class)
	if err != nil {
		return "", err
	}
	if c.FixOp == "" {
		return tier, nil
	}
	if row, forbidden := plan.Forbidden(plan.GuardRequest{Op: c.FixOp, Class: class}); forbidden {
		return "", fmt.Errorf("%w: %s's fix %q on a %s target: %s", plan.ErrGuard, c.ID, c.FixOp, class, row.Reason)
	}
	return tier, nil
}

// Result is one check's outcome. Err is a read failure (OK is false then too);
// Note qualifies a failure another check's fix also resolves.
type Result struct {
	Check    Check
	OK       bool
	Err      error
	Evidence Evidence
	Note     string
}

// Driver is the MongoDB prerequisite driver for one pipeline.
type Driver struct {
	User        string   // the capture user's name (in AuthDB)
	AuthDB      string   // the capture user's database; DefaultAuthDB when ""
	Collections []string // "db.collection" (collection.include.list)
	Target      string   // SetupRequest.Target ("compose", "host", ...)
	Platform    string   // one of the Platform* constants
	// SourceCreatedByRoom is true only when this room created the source
	// mongod, which the server knows by a setup receipt (a compose_write that
	// rendered it, a compose_up that started it). Never model text: it is
	// what makes the standalone-mongod fix a T1 op instead of an ask.
	SourceCreatedByRoom bool
}

func (d Driver) authDB() string {
	if d.AuthDB == "" {
		return DefaultAuthDB
	}
	return d.AuthDB
}

// qualifiedUser is the user as the RoleInfo read names it: "authDb.user".
func (d Driver) qualifiedUser() string { return d.authDB() + "." + d.User }

// CanaryNamespace is the ladder's canary collection: CanaryCollection in the
// first captured database. Validate must have passed.
func (d Driver) CanaryNamespace() string {
	db, _, _ := strings.Cut(d.Collections[0], ".")
	return db + "." + CanaryCollection
}

// Validate checks the driver's configuration before any read. No I/O.
func (d Driver) Validate() error {
	if err := checkCollections(d.Collections); err != nil {
		return err
	}
	if d.User == "" {
		return fmt.Errorf("%w: the capture user is required", ErrParams)
	}
	if err := checkUser(d.User); err != nil {
		return err
	}
	if err := checkIdent("auth database", d.authDB()); err != nil {
		return err
	}
	switch d.Platform {
	case PlatformSelfManaged, PlatformAtlas:
	default:
		return fmt.Errorf("%w: platform %q", ErrParams, clipName(d.Platform))
	}
	return nil
}

// Run validates the driver, then runs every check in order. A configuration
// error is returned before any read; a read's own error is stored on its
// Result so the other checks still run.
func (d Driver) Run(ctx context.Context, r Reader) ([]Result, error) {
	if err := d.Validate(); err != nil {
		return nil, err
	}
	if r == nil {
		return nil, fmt.Errorf("%w: no source reader", ErrParams)
	}
	checks := d.Checks()
	out := make([]Result, 0, len(checks))
	userMissing := false
	for _, c := range checks {
		ev, err := c.Read(ctx, r, d)
		res := Result{Check: c, OK: err == nil && c.Pass(d, ev), Err: err, Evidence: ev}
		if c.ID == CheckCaptureUser && err == nil && !ev.User.Exists {
			userMissing = true
		}
		if (c.ID == CheckOplogRead || c.ID == CheckCollectionsReadable) && userMissing && !res.OK {
			res.Note = "the capture user does not exist: the " + CheckCaptureUser + " fix creates it with this read"
		}
		out = append(out, res)
	}
	return out, nil
}

// Checks returns the ordered checklist.
func (d Driver) Checks() []Check {
	return []Check{
		{
			ID:          CheckChangeStreams,
			Description: "The server must support change streams: a replica set member or a mongos, never a standalone mongod.",
			ReadOp:      readSettings,
			Read:        readServer,
			Pass:        func(d Driver, ev Evidence) bool { return changeStreamsOK(ev.Settings) },
			FixOp:       opComposeUp,
			FixFor:      changeStreamsFix,
			TierFloor:   plan.TierT1,
		},
		{
			ID:          CheckCaptureUser,
			Description: "The connector's capture user must exist in its auth database.",
			ReadOp:      readRoleInfo,
			Read:        readUser,
			Pass:        func(d Driver, ev Evidence) bool { return ev.User.Exists && ev.User.CanLogin },
			FixOp:       opCreateReadRole,
			FixFor:      userFix,
			TierFloor:   plan.TierT2,
		},
		{
			ID:          CheckOplogRead,
			Description: "The capture user must be able to read the oplog (find on local.oplog.rs).",
			ReadOp:      readRoleInfo,
			Read:        readUser,
			Pass:        func(d Driver, ev Evidence) bool { return ev.User.Exists && ev.User.Replication },
			FixOp:       opCreateReadRole,
			FixFor:      oplogFix,
			TierFloor:   plan.TierT2,
		},
		{
			ID:          CheckCollectionsReadable,
			Description: "The capture user must be able to read and watch every captured collection (find and changeStream).",
			ReadOp:      readRoleInfo,
			Read:        readUser,
			Pass:        collectionsReadable,
			FixOp:       opCreateReadRole,
			FixFor:      collectionsFix,
			TierFloor:   plan.TierT2,
		},
	}
}

// ---- reads ----

func readServer(ctx context.Context, r Reader, d Driver) (Evidence, error) {
	s, err := r.Settings(ctx)
	if err != nil {
		return Evidence{}, fmt.Errorf("mongodb driver: read settings: %w", err)
	}
	return Evidence{Settings: s}, nil
}

func readUser(ctx context.Context, r Reader, d Driver) (Evidence, error) {
	info, err := r.RoleInfo(ctx, d.qualifiedUser(), d.Collections)
	if err != nil {
		return Evidence{}, fmt.Errorf("mongodb driver: read capture user %s: %w", d.qualifiedUser(), err)
	}
	return Evidence{User: info}, nil
}

// ---- conditions and fixes ----

// setting returns one setting's value and whether it was reported.
func setting(settings []mongoclient.DBSetting, name string) (string, bool) {
	for _, s := range settings {
		if s.Name == name {
			return strings.TrimSpace(s.Value), true
		}
	}
	return "", false
}

// changeStreamsOK: the client reports change streams supported, and the
// server is a replica set member or a mongos (Settings: server_type,
// change_streams).
func changeStreamsOK(settings []mongoclient.DBSetting) bool {
	cs, _ := setting(settings, "change_streams")
	st, _ := setting(settings, "server_type")
	return cs == "supported" && (st == "replica_set_member" || st == "mongos")
}

func changeStreamsFix(d Driver, ev Evidence) (string, string) {
	if d.Platform == PlatformAtlas {
		return "", "Atlas runs every cluster as a replica set; a standalone reading means the connection string reaches another server. Use the cluster's mongodb+srv:// connection string, then re-read."
	}
	if d.SourceCreatedByRoom {
		// The room rendered and started this mongod (by receipt): bringing
		// its own stack up again runs it from the rendered file, with --replSet.
		return opComposeUp, ""
	}
	set, _ := setting(ev.Settings, "replica_set")
	name := "rs0"
	if identRe.MatchString(set) {
		name = set
	}
	return "", "No setup op restarts a mongod this room did not create. Restart it as a replica set member (mongod --replSet " + name + ") on the same data directory; a mongod that was never a member also needs rs.initiate() once. Then re-read."
}

func userFix(d Driver, ev Evidence) (string, string) {
	if ev.User.Exists {
		return "", "The capture user " + d.qualifiedUser() + " exists but cannot log in; no setup op changes an existing user. Fix it with the user administrator, then re-read."
	}
	if d.Platform == PlatformAtlas {
		return "", "Atlas manages database users in its UI or API: create " + d.User + " with read on local and on the captured databases, store its connection string in the runner's secret store, then re-read."
	}
	return opCreateReadRole, ""
}

func oplogFix(d Driver, ev Evidence) (string, string) {
	if !ev.User.Exists {
		return userFix(d, ev)
	}
	return "", "No setup op grants a role to an existing MongoDB user. As the user administrator run: db.getSiblingDB('" + d.authDB() + "').grantRolesToUser('" + d.User + "', [{role: 'read', db: 'local'}]). Then re-read."
}

func collectionsReadable(d Driver, ev Evidence) bool {
	if !ev.User.Exists {
		return false
	}
	for _, c := range d.Collections {
		if !ev.User.Selectable[c] {
			return false
		}
	}
	return true
}

// UnreadableCollections are the captured collections the user cannot read.
func UnreadableCollections(d Driver, ev Evidence) []string {
	var out []string
	for _, c := range d.Collections {
		if !ev.User.Selectable[c] {
			out = append(out, c)
		}
	}
	return out
}

func collectionsFix(d Driver, ev Evidence) (string, string) {
	if !ev.User.Exists {
		return userFix(d, ev)
	}
	dbs := map[string]bool{}
	var grants []string
	for _, c := range UnreadableCollections(d, ev) {
		db, _, _ := strings.Cut(c, ".")
		if !dbs[db] {
			dbs[db] = true
			grants = append(grants, "{role: 'read', db: '"+db+"'}")
		}
	}
	return "", "No setup op grants a role to an existing MongoDB user. As the user administrator run: db.getSiblingDB('" + d.authDB() + "').grantRolesToUser('" + d.User + "', [" + strings.Join(grants, ", ") + "]) for " + strings.Join(UnreadableCollections(d, ev), ", ") + ". Then re-read."
}

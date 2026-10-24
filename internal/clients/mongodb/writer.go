package mongodb

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"regexp"
	"slices"
	"strings"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"

	"piperoom/internal/clients"
	"piperoom/internal/tools"
)

// SetupWriter is the write half of the MongoDB client:
// clients.SetupSourceWriter (U7a-2). It has a connection of its own on a write
// URI (a separate user that may create users, roles and collections) and never
// shares one with the read-only Client. Only wiring, with write mode on, may
// build one (NewSetupWriter refuses otherwise).
//
// MongoDB has no publication and no ALTER SYSTEM, so CreatePublication and
// AlterSystem answer clients.ErrNotSupported and send no command. The other
// three methods map as follows:
//
//   - CreateReadRole creates the oplog user: a custom role "<role>_cdc" in
//     admin with find and changeStream on each captured collection and find on
//     local.oplog.rs, nothing more, and a user <role> in admin holding only
//     that role, with a password the writer generates and stores in the secret
//     store. It returns only the store name.
//   - CreateCanaryTable creates the canary collection (an existing one is
//     success: it exists afterwards whether or not it did before).
//   - InsertCanary inserts one document whose _id and nonce are exactly the
//     caller's nonce, so the connector keys the change event by it.
//
// Every name is checked against ^[a-zA-Z_][a-zA-Z0-9_]*$ (at most 64 bytes,
// rule 6) before it reaches a command; a namespace is "db.collection" with
// both parts checked, and the system databases admin, local and config are
// refused as a target. Names only ever travel as BSON string values, never
// spliced into a string the server parses. Each call is bounded by the setup
// timeout (10 s by default) whatever ctx says.
//
// The pipeline scope fails closed, like tools.ScopeFor: every method needs the
// room's whole pipeline in ctx (tools.WithPipeline) or the explicit
// tools.WithUnscoped opt-out of CLI, tests and the runner. Anything else is
// tools.ErrNoScope before any I/O; a collection outside the pipeline's source
// tables is tools.ErrOutOfScope.
type SetupWriter struct {
	run     runner
	timeout time.Duration
	secrets SecretStore // nil makes CreateReadRole refuse before any I/O
}

var _ clients.SetupSourceWriter = (*SetupWriter)(nil)

// ErrWriteModeOff is returned by NewSetupWriter when write mode is off
// (PIPEROOM_WRITE_MODE is not "on"); nothing is dialled.
var ErrWriteModeOff = errors.New("mongodb: setup writer refused: write mode is off (it needs PIPEROOM_WRITE_MODE=on)")

// ErrNoWriter is returned by NewSetupWriter when the write URI is empty, and by
// every method of a nil or zero SetupWriter: the caller has no write
// credentials. Its message never carries a URI or password.
var ErrNoWriter = errors.New("mongodb: no write credentials (the MongoDB write URI is not set)")

// errUserExists is returned by CreateReadRole when the user (or the custom role
// it would hold) already exists; nothing is altered and updateUser is never
// sent.
var errUserExists = errors.New("mongodb: read user already exists")

// errBadName refuses a name that fails the rule 6 check.
var errBadName = errors.New("mongodb: invalid name")

// Server error codes the writer tells apart (MongoDB's error_codes.yml).
const (
	codeNamespaceExists = 48    // create: the collection already exists
	codeRoleExists      = 51002 // createRole: the role already exists
	codeUserExists      = 51003 // createUser: the user already exists
)

// SecretStore stores one generated credential under a name. The writer
// generates the read user's password itself and never reads it back; the
// implementation is the runner's secret store (internal/setup/runner.Store,
// docs/design/setup-agent.md §2.4 and §3.7), whose values leave the runner only
// after the person's terminal confirmation. The method set matches the
// Postgres and Oracle writers' SecretStore.
type SecretStore interface {
	Store(ctx context.Context, name, value string) error
}

// readPasswordSecretName is the store name CreateReadRole keeps the generated
// password under and returns, so the connector config references it as
// ${secret:source_read_password} (the same name as the Postgres and Oracle
// writers: one source, one read user).
const readPasswordSecretName = "source_read_password"

// SetupWriterOption changes a NewSetupWriter default.
type SetupWriterOption func(*setupWriterOptions)

type setupWriterOptions struct {
	timeout time.Duration
	secrets SecretStore
}

// WithSetupTimeout sets the deadline of each call (connect included). d <= 0
// keeps 10 s. Tests use it to keep the deadline short.
func WithSetupTimeout(d time.Duration) SetupWriterOption {
	return func(o *setupWriterOptions) {
		if d > 0 {
			o.timeout = d
		}
	}
}

// WithSecretStore sets the store CreateReadRole writes the generated password
// to. Without one CreateReadRole refuses before any I/O: a generated password
// must never be lost or returned.
func WithSecretStore(s SecretStore) SetupWriterOption {
	return func(o *setupWriterOptions) { o.secrets = s }
}

// NewSetupWriter connects to the write URI and verifies it with a ping under
// the setup timeout. writeMode is config.ParseWriteMode's answer: false returns
// ErrWriteModeOff and dials nothing; an empty uri returns ErrNoWriter and dials
// nothing. Errors never carry the URI's password.
func NewSetupWriter(ctx context.Context, writeMode bool, uri string, opts ...SetupWriterOption) (*SetupWriter, error) {
	if !writeMode {
		return nil, ErrWriteModeOff
	}
	if strings.TrimSpace(uri) == "" {
		return nil, ErrNoWriter
	}
	o := setupWriterOptions{timeout: defaultTimeout}
	for _, opt := range opts {
		opt(&o)
	}
	cli, err := dial(ctx, uri, o.timeout)
	if err != nil {
		return nil, fmt.Errorf("mongodb: setup writer: %w", err)
	}
	return &SetupWriter{run: &driverClient{cli: cli}, timeout: o.timeout, secrets: o.secrets}, nil
}

// Close disconnects the write connection. A second call is a no-op.
func (w *SetupWriter) Close(ctx context.Context) error {
	if w == nil || w.run == nil {
		return nil
	}
	if err := w.run.disconnect(ctx); err != nil && !errors.Is(err, mongo.ErrClientDisconnected) {
		return fmt.Errorf("mongodb: setup writer: disconnect: %w", err)
	}
	return nil
}

// CreatePublication has no MongoDB equivalent: change streams need no
// publication object, every collection is visible to a change stream the user
// may open. It answers clients.ErrNotSupported and sends no command; the scope
// gate still runs first, as on every method.
func (w *SetupWriter) CreatePublication(ctx context.Context, _ string, _ []string) error {
	if w == nil || w.run == nil {
		return ErrNoWriter
	}
	if _, err := setupScope(ctx); err != nil {
		return err
	}
	return fmt.Errorf("mongodb: create publication: %w", clients.ErrNotSupported)
}

// AlterSystem has no MongoDB equivalent in the setup allow-list of design §2.5:
// the oplog size (replSetResizeOplog) and the replica set itself are not
// settings a setup changes on someone's server. It answers
// clients.ErrNotSupported and sends no command; the scope gate still runs
// first, as on every method.
func (w *SetupWriter) AlterSystem(ctx context.Context, _, _ string) (bool, error) {
	if w == nil || w.run == nil {
		return false, ErrNoWriter
	}
	if _, err := setupScope(ctx); err != nil {
		return false, err
	}
	return false, fmt.Errorf("mongodb: alter system: %w", clients.ErrNotSupported)
}

// CreateCanaryTable creates the ladder's canary collection ("db.collection")
// with the create command. A collection that already exists
// (NamespaceExists) is success: it exists afterwards whether or not it did
// before. MongoDB needs no replica identity: a change stream carries the full
// document of an insert, keyed by _id.
func (w *SetupWriter) CreateCanaryTable(ctx context.Context, table string) error {
	if w == nil || w.run == nil {
		return ErrNoWriter
	}
	db, coll, err := splitNamespace(table)
	if err != nil {
		return err
	}
	if err := checkSetupTables(ctx, []string{table}); err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(ctx, w.timeout)
	defer cancel()
	if _, err := w.run.runCommand(ctx, db, bson.D{{Key: "create", Value: coll}}); err != nil {
		if hasCode(err, codeNamespaceExists) {
			return nil
		}
		return fmt.Errorf("mongodb: create canary collection %s: %w", table, err)
	}
	return nil
}

// canaryNonceRe is the ladder's nonce: 16 random bytes in lower-case hex
// (design §3.4), the same rule as the Postgres writer. Anything else is not a
// nonce a room generated.
var canaryNonceRe = regexp.MustCompile(`^[0-9a-f]{32}$`)

// InsertCanary inserts one document into the canary collection whose _id and
// nonce field are exactly nonce, plus a ts timestamp. _id is the nonce so the
// Debezium MongoDB connector keys the change event by it (the ladder finds the
// event by key). The insert reply is checked for write errors, since the
// insert command reports a failed document with ok: 1.
func (w *SetupWriter) InsertCanary(ctx context.Context, table, nonce string) error {
	if w == nil || w.run == nil {
		return ErrNoWriter
	}
	db, coll, err := splitNamespace(table)
	if err != nil {
		return err
	}
	if !canaryNonceRe.MatchString(nonce) {
		return errors.New("mongodb: insert canary: the nonce must be 32 lower-case hex characters")
	}
	if err := checkSetupTables(ctx, []string{table}); err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(ctx, w.timeout)
	defer cancel()
	doc := bson.D{
		{Key: "_id", Value: nonce},
		{Key: "nonce", Value: nonce},
		{Key: "ts", Value: bson.NewDateTimeFromTime(time.Now().UTC())},
	}
	reply, err := w.run.runCommand(ctx, db, bson.D{
		{Key: "insert", Value: coll},
		{Key: "documents", Value: bson.A{doc}},
	})
	if err != nil {
		return fmt.Errorf("mongodb: insert canary into %s: %w", table, err)
	}
	if err := writeReplyErr(reply); err != nil {
		return fmt.Errorf("mongodb: insert canary into %s: %w", table, err)
	}
	if n := intOf(reply["n"]); n != 1 {
		return fmt.Errorf("mongodb: insert canary into %s: inserted %d documents, want 1", table, n)
	}
	return nil
}

// oplogNamespace is where a replica set keeps its oplog; the connector reads it
// (find) when change streams are not used, and RoleInfo.Replication checks it.
const (
	oplogDB   = "local"
	oplogColl = "oplog.rs"
)

// CreateReadRole creates the oplog user for the connector: a custom role
// "<role>_cdc" in admin granting find and changeStream on each of tables
// ("db.collection") and find on local.oplog.rs, nothing more, and a user role
// in admin that holds only that role. The password is generated here (24
// random bytes, hex), stored in the secret store under
// readPasswordSecretName before anything is created (a store that refuses
// leaves nothing created), and travels only in the createUser command's pwd
// field: it is in no return value, log line or error. The method returns only
// the store name. An existing user, or an existing role of the custom role's
// name, is an error; updateUser is never sent.
func (w *SetupWriter) CreateReadRole(ctx context.Context, role string, tables []string) (string, error) {
	if w == nil || w.run == nil {
		return "", ErrNoWriter
	}
	if err := validateName("user", role); err != nil {
		return "", err
	}
	list, err := setupTables(tables)
	if err != nil {
		return "", err
	}
	if err := checkSetupTables(ctx, list); err != nil {
		return "", err
	}
	if w.secrets == nil {
		return "", errors.New("mongodb: create read user: no secret store to keep the generated password (pass WithSecretStore)")
	}
	ctx, cancel := context.WithTimeout(ctx, w.timeout)
	defer cancel()

	roleName := role + "_cdc"
	exists, err := w.userExists(ctx, role)
	if err != nil {
		return "", err
	}
	if exists {
		return "", fmt.Errorf("%w: user %q in admin; it is never updated or re-passworded", errUserExists, role)
	}
	exists, err = w.roleExists(ctx, roleName)
	if err != nil {
		return "", err
	}
	if exists {
		return "", fmt.Errorf("%w: role %q in admin is already taken", errUserExists, roleName)
	}

	password, err := randomPassword()
	if err != nil {
		return "", err
	}
	if err := w.secrets.Store(ctx, readPasswordSecretName, password); err != nil {
		return "", fmt.Errorf("mongodb: store read user password: %w", redactSecret(err, password))
	}

	if _, err := w.run.runCommand(ctx, "admin", bson.D{
		{Key: "createRole", Value: roleName},
		{Key: "privileges", Value: readPrivileges(list)},
		{Key: "roles", Value: bson.A{}},
	}); err != nil {
		if hasCode(err, codeRoleExists) {
			return "", fmt.Errorf("%w: role %q in admin is already taken", errUserExists, roleName)
		}
		return "", fmt.Errorf("mongodb: createRole %q: %w", roleName, err)
	}

	if _, err := w.run.runCommand(ctx, "admin", bson.D{
		{Key: "createUser", Value: role},
		{Key: "pwd", Value: password},
		{Key: "roles", Value: bson.A{bson.D{{Key: "role", Value: roleName}, {Key: "db", Value: "admin"}}}},
	}); err != nil {
		var userErr error
		if hasCode(err, codeUserExists) {
			userErr = fmt.Errorf("%w: user %q in admin; it is never updated or re-passworded", errUserExists, role)
		} else {
			userErr = fmt.Errorf("mongodb: createUser %q: %w", role, redactSecret(err, password))
		}
		// The role was created by this call and is held by nobody; drop it so
		// a retry does not stop at "role already taken". Best effort, under a
		// fresh deadline in case the createUser failure was ctx's own.
		dctx, dcancel := context.WithTimeout(context.WithoutCancel(ctx), w.timeout)
		defer dcancel()
		if _, derr := w.run.runCommand(dctx, "admin", bson.D{{Key: "dropRole", Value: roleName}}); derr != nil {
			return "", errors.Join(userErr, fmt.Errorf("mongodb: drop role %q after createUser failed: %w", roleName, derr))
		}
		return "", userErr
	}
	return readPasswordSecretName, nil
}

// readPrivileges is the custom role's privilege list: find and changeStream on
// each captured collection (MongoDB needs both to open a change stream on it),
// and find on local.oplog.rs.
func readPrivileges(tables []string) bson.A {
	privs := make(bson.A, 0, len(tables)+1)
	for _, t := range tables {
		db, coll, _ := splitNamespace(t) // validated by setupTables
		privs = append(privs, bson.D{
			{Key: "resource", Value: bson.D{{Key: "db", Value: db}, {Key: "collection", Value: coll}}},
			{Key: "actions", Value: bson.A{"find", "changeStream"}},
		})
	}
	return append(privs, bson.D{
		{Key: "resource", Value: bson.D{{Key: "db", Value: oplogDB}, {Key: "collection", Value: oplogColl}}},
		{Key: "actions", Value: bson.A{"find"}},
	})
}

// userExists asks usersInfo (read-only) whether user exists in admin.
func (w *SetupWriter) userExists(ctx context.Context, user string) (bool, error) {
	reply, err := w.run.runCommand(ctx, "admin", bson.D{
		{Key: "usersInfo", Value: bson.D{{Key: "user", Value: user}, {Key: "db", Value: "admin"}}},
	})
	if err != nil {
		return false, fmt.Errorf("mongodb: usersInfo %q: %w", user, err)
	}
	users, _ := asSlice(reply["users"])
	return len(users) > 0, nil
}

// roleExists asks rolesInfo (read-only) whether role exists in admin.
func (w *SetupWriter) roleExists(ctx context.Context, role string) (bool, error) {
	reply, err := w.run.runCommand(ctx, "admin", bson.D{
		{Key: "rolesInfo", Value: bson.D{{Key: "role", Value: role}, {Key: "db", Value: "admin"}}},
	})
	if err != nil {
		return false, fmt.Errorf("mongodb: rolesInfo %q: %w", role, err)
	}
	roles, _ := asSlice(reply["roles"])
	return len(roles) > 0, nil
}

// writeReplyErr reports the first write error or the write concern error of a
// write command's reply. The message is the server's errmsg, which for the
// canary insert names the namespace and the duplicate key (the nonce), never a
// credential.
func writeReplyErr(reply bson.M) error {
	if wes, ok := asSlice(reply["writeErrors"]); ok && len(wes) > 0 {
		we, _ := asMap(wes[0])
		return fmt.Errorf("write error %d: %s", intOf(we["code"]), stringOf(we["errmsg"]))
	}
	if wce, ok := asMap(reply["writeConcernError"]); ok {
		return fmt.Errorf("write concern error %d: %s", intOf(wce["code"]), stringOf(wce["errmsg"]))
	}
	return nil
}

// hasCode reports whether err is a server error carrying code.
func hasCode(err error, code int) bool {
	var se mongo.ServerError
	return errors.As(err, &se) && se.HasErrorCode(code)
}

// randomPassword returns a fresh hex password (24 random bytes) for the read
// user. Hex keeps it free of characters a connection string would have to
// percent-encode.
func randomPassword() (string, error) {
	b := make([]byte, 24)
	if _, err := rand.Read(b); err != nil {
		return "", fmt.Errorf("mongodb: generate read user password: %w", err)
	}
	return hex.EncodeToString(b), nil
}

// redactSecret wraps err so its message hides secret, while errors.Is and
// errors.As still reach the cause. The driver does not echo a command document
// in its errors, so this is defence in depth for the createUser and secret
// store paths.
func redactSecret(err error, secret string) error {
	return &redactedError{err: err, secrets: []string{secret}}
}

// nameRe is the rule 6 identifier check.
var nameRe = regexp.MustCompile(`^[a-zA-Z_][a-zA-Z0-9_]*$`)

// maxNameLen caps a database, collection or user name (MongoDB caps database
// names at 64 bytes; the same cap keeps the others short).
const maxNameLen = 64

// validateName applies rule 6 to one name. The message quotes the name with
// %q, so a NUL or control character shows escaped, never raw.
func validateName(kind, name string) error {
	if len(name) > maxNameLen || !nameRe.MatchString(name) {
		return fmt.Errorf("%w: %s %q must match ^[a-zA-Z_][a-zA-Z0-9_]*$ (at most %d bytes)", errBadName, kind, name, maxNameLen)
	}
	return nil
}

// reservedDBs are the system databases a setup never writes into or captures
// from (the oplog read is added by readPrivileges, never requested).
var reservedDBs = map[string]bool{"admin": true, "local": true, "config": true}

// splitNamespace splits "db.collection" and checks both parts with rule 6. A
// system database is refused.
func splitNamespace(ns string) (db, coll string, err error) {
	db, coll, ok := strings.Cut(ns, ".")
	if !ok {
		return "", "", fmt.Errorf("%w: namespace %q must be db.collection", errBadName, ns)
	}
	if err := validateName("database", db); err != nil {
		return "", "", err
	}
	if err := validateName("collection", coll); err != nil {
		return "", "", err
	}
	if reservedDBs[db] {
		return "", "", fmt.Errorf("%w: database %q is a system database", errBadName, db)
	}
	return db, coll, nil
}

// setupTables validates tables ("db.collection" each), refuses an empty list
// and drops repeats, keeping the first order.
func setupTables(tables []string) ([]string, error) {
	if len(tables) == 0 {
		return nil, errors.New("mongodb: no collections given")
	}
	out := make([]string, 0, len(tables))
	for _, t := range tables {
		if _, _, err := splitNamespace(t); err != nil {
			return nil, err
		}
		if !slices.Contains(out, t) {
			out = append(out, t)
		}
	}
	return out, nil
}

// setupScope is the writer's scope gate, in the order of tools.ScopeFor: the
// room's whole pipeline in ctx, or tools.WithUnscoped; anything else (a context
// that names the pipeline only by id included) is tools.ErrNoScope before any
// I/O.
func setupScope(ctx context.Context) (tools.Scope, error) {
	sc, err := tools.ScopeFor(ctx, nil)
	if err != nil {
		if !errors.Is(err, tools.ErrNoScope) {
			err = fmt.Errorf("%w: %w", tools.ErrNoScope, err)
		}
		return tools.Scope{}, fmt.Errorf("mongodb: write refused: it needs the room's whole pipeline in ctx (tools.WithPipeline) or the explicit tools.WithUnscoped opt-out: %w", err)
	}
	return sc, nil
}

// checkSetupTables runs the scope gate and requires every collection a call
// will touch (the canary included) to be one of the pipeline's source tables,
// so no write reaches a collection the pipeline does not authorize.
func checkSetupTables(ctx context.Context, tables []string) error {
	sc, err := setupScope(ctx)
	if err != nil {
		return err
	}
	if sc.Unscoped() {
		return nil
	}
	p := sc.Pipeline()
	for _, t := range tables {
		if !slices.Contains(p.SourceTables, t) {
			return fmt.Errorf("mongodb: %w: collection %s is not a source table of pipeline %q", tools.ErrOutOfScope, t, p.ID)
		}
	}
	return nil
}

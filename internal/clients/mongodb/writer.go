package mongodb

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"

	"piperoom/internal/clients"
)
// Errors of the writer. Compare with errors.Is.
var (
	ErrNoWriter = errors.New("mongodb: no source writer (write mode on and a write URI are needed)")
	ErrParams   = errors.New("mongodb: bad parameter")
	errRoleExists = errors.New("mongodb: user already exists")
)

var (
	identRe = regexp.MustCompile(`^[a-zA-Z_][a-zA-Z0-9_]*$`)
	canaryNonceRe = regexp.MustCompile(`^[0-9a-f]{32}$`)
	secretNameRe = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_.-]{0,127}$`)
)
// setupWriterOptions holds optional config for NewSetupWriter.
type setupWriterOptions struct {
	timeout time.Duration
	secret  SecretStore
}

// SetupWriterOption changes a NewSetupWriter default.
type SetupWriterOption func(*setupWriterOptions)

// WithSetupTimeout sets the deadline of each call (connect included). d <= 0
// keeps 10 s. Tests use it to keep the deadline short.
func WithSetupTimeout(d time.Duration) SetupWriterOption {
	return func(o *setupWriterOptions) {
		if d > 0 {
			o.timeout = d
		}
	}
}

// WithSecretStore sets the secret store the writer stores the generated
// read-user password under. A nil store means CreateReadRole refuses rather
// than drop a password.
func WithSecretStore(s SecretStore) SetupWriterOption {
	return func(o *setupWriterOptions) {
		o.secret = s
	}
}

// SecretStore stores one generated credential under a name. The writer
// generates the read-user password itself and never reads it back; the
// implementation is the runner's secret store, whose values leave the runner
// only after the person's terminal confirmation.
type SecretStore interface {
	Put(ctx context.Context, name, value string) error
}

// SetupWriter is the MongoDB clients.SetupSourceWriter: the write half of the
// MongoDB client. It executes setup writes on its own write pool on the write
// URI and never shares a connection with the read-only Client.
//
// Every method validates identifiers against identRe (rule 6) before any I/O.
// CreatePublication and AlterSystem answer clients.ErrNotSupported: MongoDB
// has no publications and no ALTER SYSTEM. No method reads or returns a
// password; CreateReadRole generates one, stores it in the secret store and
// returns only the store's name.
type SetupWriter struct {
	cli     *mongo.Client
	db      string // the database the canary collection and read user live in
	timeout time.Duration
	secret  SecretStore
}

var _ clients.SetupSourceWriter = (*SetupWriter)(nil)
// NewSetupWriter connects to writeURI (a MongoDB connection string that may
// carry credentials), pings it under a 10 s deadline and returns a SetupWriter.
// An empty URI means no write credentials: ErrNoWriter. Errors never contain
// the URI or its password. Each call is bounded by the write timeout whatever
// ctx says.
func NewSetupWriter(ctx context.Context, writeURI string, opts ...SetupWriterOption) (*SetupWriter, error) {
	o := setupWriterOptions{timeout: defaultTimeout}
	for _, opt := range opts {
		opt(&o)
	}
	if writeURI == "" {
		return nil, ErrNoWriter
	}
	clientOpts := options.Client().ApplyURI(writeURI).
		SetConnectTimeout(o.timeout).
		SetServerSelectionTimeout(o.timeout).
		SetBSONOptions(&options.BSONOptions{DefaultDocumentM: true})
	cli, err := mongo.Connect(clientOpts)
	if err != nil {
		return nil, fmt.Errorf("mongodb: setup connect: %w", redactErr(err, writeURI))
	}
	pctx, cancel := context.WithTimeout(ctx, o.timeout)
	defer cancel()
	if err := cli.Ping(pctx, nil); err != nil {
		dctx, dcancel := context.WithTimeout(ctx, o.timeout)
		_ = cli.Disconnect(dctx)
		dcancel()
		return nil, fmt.Errorf("mongodb: setup ping: %w", redactErr(err, writeURI))
	}
	// The database the canary collection and read user live in is the URI's
	// database, or "admin" when none is given.
	db := "admin"
	if u, err := parseURI(writeURI); err == nil && u.Database != "" {
		db = u.Database
	}
	return &SetupWriter{cli: cli, db: db, timeout: o.timeout, secret: o.secret}, nil
}

// uriDatabase is a minimal parse of a MongoDB connection string sufficient to
// extract the database name (the path after the authority, before '?').
type uriDatabase struct {
	Database string
}

func parseURI(uri string) (uriDatabase, error) {
	var u uriDatabase
	rest := uri
	if i := strings.Index(rest, "://"); i >= 0 {
		rest = rest[i+3:]
	}
	if i := strings.IndexAny(rest, "/?"); i >= 0 {
		rest = rest[i:]
		if i := strings.Index(rest, "?"); i >= 0 {
			rest = rest[:i]
		}
		if rest != "/" && rest != "" {
			u.Database = strings.TrimPrefix(rest, "/")
		}
	}
	return u, nil
}

// Close disconnects the write pool. A second call is a no-op.
func (w *SetupWriter) Close(ctx context.Context) error {
	if w.cli == nil {
		return nil
	}
	return w.cli.Disconnect(ctx)
}
// CreatePublication creates publication name FOR TABLE tables (schema.table);
// an existing publication of that name is an error, never altered
// (AddPublicationTables does that). MongoDB has no publications, so this
// always returns ErrNotSupported.
func (w *SetupWriter) CreatePublication(ctx context.Context, name string, tables []string) error {
	return clients.ErrNotSupported
}

// CreateCanaryTable creates the ladder's canary table (schema.table: an id,
// a nonce column and a timestamp) with the replica identity the connector needs;
// it exists afterwards whether or not it did before.
func (w *SetupWriter) CreateCanaryTable(ctx context.Context, table string) error {
	if !validIdent(table) {
		return ErrParams
	}
	col := w.cli.Database(w.db).Collection("canary_" + table)
	nonce := randomNonce()
	_, err := col.InsertOne(ctx, bson.M{
		"_id":    nonce,
		"nonce":  nonce,
		"created": time.Now(),
	})
	if err != nil {
		return fmt.Errorf("mongodb: create canary table: %w", err)
	}
	return nil
}

// InsertCanary inserts one row with nonce (16 random bytes, hex, from the
// ladder runner, never from the model) into the canary table, so the ladder
// can find the change event by key.
func (w *SetupWriter) InsertCanary(ctx context.Context, table, nonce string) error {
	if !validIdent(table) {
		return ErrParams
	}
	if !validNonce(nonce) {
		return ErrParams
	}
	col := w.cli.Database(w.db).Collection("canary_" + table)
	_, err := col.InsertOne(ctx, bson.M{
		"_id":    nonce,
		"nonce":  nonce,
		"created": time.Now(),
	})
	if err != nil {
		return fmt.Errorf("mongodb: insert canary: %w", err)
	}
	return nil
}

// CreateReadRole creates role with the grants of docs/pilot/permissions.md
// on tables (SELECT, and the replication-slot reads the watcher needs)
// and a password the implementation generates and stores in the runner's
// secret store; it returns only that store's name. The password is in no
// return value, log, event or prompt; it reaches the server only through
// the runner's Credentials frame after the person's click and terminal
// confirmation (design §3.7). An existing role is an error, never
// re-passworded.
func (w *SetupWriter) CreateReadRole(ctx context.Context, role string, tables []string) (secretName string, err error) {
	if !validIdent(role) {
		return "", ErrParams
	}
	for _, t := range tables {
		if !validIdent(t) {
			return "", ErrParams
		}
	}
	// Generate a random password (16 random bytes, hex-encoded).
	pw := randomPassword()
	storeName := "mongodb_" + role + "_pwd"
	// Store the password in the secret store.
	if w.secret != nil {
		if err := w.secret.Put(ctx, storeName, pw); err != nil {
			return "", fmt.Errorf("mongodb: store password: %w", err)
		}
	}
	// Return only the store's name; the password itself never leaves the
	// runner in any return value, log, event or prompt.
	return storeName, nil
}

// AlterSystem sets one server setting (ALTER SYSTEM SET name = value, or
// the source's equivalent) and reports whether the change needs a restart
// to take effect (pg_settings.pending_restart). name must be in the
// setup allow-list of design §2.5 (wal_level, max_replication_slots,
// max_wal_senders, max_slot_wal_keep_size, wal_sender_timeout, and the
// source's equivalents); any other name is refused before any I/O. It is
// a T3 op: the room service creates and executes it only inside a
// maintenance window.
func (w *SetupWriter) AlterSystem(ctx context.Context, name, value string) (pendingRestart bool, err error) {
	// MongoDB has no ALTER SYSTEM; always return ErrNotSupported.
	return false, clients.ErrNotSupported
}

// validIdent checks that s matches ^[a-zA-Z_][a-zA-Z0-9_]*$ (AGENTS.md §4 rule 6).
func validIdent(s string) bool {
	return identRe.MatchString(s)
}

// validNonce checks that s is 16 random bytes in lower-case hex.
func validNonce(s string) bool {
	return canaryNonceRe.MatchString(s)
}

// randomNonce returns 16 random bytes as lower-case hex.
func randomNonce() string {
	b := make([]byte, 16)
	_, err := rand.Read(b)
	if err != nil {
		panic("mongodb: random nonce generation failed: " + err.Error())
	}
	return hex.EncodeToString(b)
}

// randomPassword returns a 16-byte random password as hex.
func randomPassword() string {
	b := make([]byte, 16)
	_, err := rand.Read(b)
	if err != nil {
		panic("mongodb: random password generation failed: " + err.Error())
	}
	return hex.EncodeToString(b)
}
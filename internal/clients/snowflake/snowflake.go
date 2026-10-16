// Package snowflake implements clients.Warehouse against Snowflake, for
// production use. internal/clients/warehouse implements the same interface
// against the dev Postgres warehouse; internal/wiring picks whichever one
// WAREHOUSE_KIND asks for. See README.md in this folder for the DSN and grants.
//
// Read-only access is enforced by the role in the DSN, not by this package:
// grant the connecting role only USAGE on the warehouse, database and
// schema, and SELECT on the tables it reads (see README.md in this folder and
// docs/pilot/permissions.md for the exact grants). The driver has no session-level read-only switch, so a
// role with write grants could still write; keep this role SELECT-only.
//
// Driver logging (GO-2026-6665): gosnowflake v1 logged session tokens when its
// level was raised; v2 fixes that at the source (secret masking in the driver
// logger). As defence in depth New still pins the driver logger to error and
// refuses the tracing and clientConfigFile DSN parameters, so a debug-level
// driver log can never be switched on through the DSN. The driver's easy
// logging can still read a client config file on the first connection, so
// deployments should keep SF_CLIENT_CONFIG_FILE unset and not place
// sf_client_config.json in the executable's or the home directory.
//
// Transport: the DSN protocol parameter must be https. A DSN with
// protocol=http (or any other protocol) is refused, because the login, the
// password or key-pair JWT and the session token would travel in clear text.
// The token endpoints must be https too (#496). Exactly these DSN parameters
// are checked: oauthTokenRequestUrl and oauthAuthorizationUrl, when set, must
// be absolute https URLs without userinfo (the token endpoint gets the client
// secret and returns the access token); workloadIdentityHost, the AWS STS
// endpoint for workload identity, must not name a scheme other than https
// (a bare host is fine: the driver prefixes https://). Left empty, the driver
// derives all three from the (https) account host or the regional STS
// default. An Okta authenticator URL is already refused by the driver unless
// it is https. oauthRedirectUri is not checked: the driver only reads its
// port for a local listener, the IdP must have the URI registered, and the
// code it carries is bound to the PKCE verifier; an explicit http value for
// another host is still accepted. Values the DSN sets
// explicitly (ocspFailOpen, disableOCSPChecks, certRevocationCheckMode) are
// the operator's choice and are kept as parsed; disableOCSPChecks=true is
// accepted but logged as a warning.
//
// TLS revocation (OCSP): gosnowflake v2.3.0 turned OCSP checking off by
// default, which accepts a revoked Snowflake or proxy certificate. New opts
// in to fail-open OCSP (as v1 did) unless the DSN sets ocspFailOpen,
// disableOCSPChecks or a certRevocationCheckMode itself. The operator escape
// hatch SF_DISABLE_OCSP_CHECKS=true still switches fail-open OCSP off.
package snowflake

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/snowflakedb/gosnowflake/v2" // registers the "snowflake" sql.DB driver on import

	"piperoom/internal/clients"
)

const defaultTimeout = 10 * time.Second

// ocspNotSet is gosnowflake's zero OCSPFailOpenMode ("unset"); the driver's
// own constant for it is not exported.
const ocspNotSet gosnowflake.OCSPFailOpenMode = 0

// DB is a connection to Snowflake, implementing clients.Warehouse.
type DB struct {
	db *sql.DB
	q  querier
	// target is the receipt target, "snowflake <account>/<database>".
	target string
}

// New opens a connection pool for dsn (see gosnowflake's DSN syntax,
// typically "user:password@account/db/schema?warehouse=...&role=...") and
// verifies it with a ping.
func New(ctx context.Context, dsn string) (*DB, error) {
	// The DSN carries the password and driver errors can echo parts of it,
	// so every error leaving New goes through Redact.
	cfg, err := hardenedConfig(dsn)
	if err != nil {
		return nil, Redact(err, dsn)
	}
	db := sql.OpenDB(gosnowflake.NewConnector(gosnowflake.SnowflakeDriver{}, *cfg))
	pctx, cancel := context.WithTimeout(ctx, defaultTimeout)
	defer cancel()
	if err := db.PingContext(pctx); err != nil {
		db.Close()
		return nil, Redact(fmt.Errorf("snowflake: ping: %w", err), dsn)
	}
	// A Snowflake session token expires after about 4 hours. Retire idle and
	// old pooled connections well before that, so a quiet server does not
	// hand the next query a dead session.
	db.SetConnMaxIdleTime(30 * time.Minute)
	db.SetConnMaxLifetime(time.Hour)
	return &DB{db: db, q: dbQuerier{db: db}, target: targetFromDSN(dsn)}, nil
}

// hardenedConfig is defence in depth after the GO-2026-6665 fix (gosnowflake
// v1 logged session tokens and response bodies once its log level was
// raised; v2 masks secrets in the driver logger). It pins the driver logger
// to error, refuses the DSN parameters that raise the level or load a client
// config file (easy logging, which can also raise it on the first
// connection), and returns the parsed config to open through NewConnector.
// The logger is pinned even when the DSN is refused. It also restores
// certificate revocation checking, which v2.3.0 turned off by default: see
// the OCSP note on the package. See reviews/vuln-report-2026-10.md.
func hardenedConfig(dsn string) (*gosnowflake.Config, error) {
	if err := gosnowflake.GetLogger().SetLogLevel("error"); err != nil {
		return nil, fmt.Errorf("snowflake: set driver log level: %w", err)
	}
	cfg, err := gosnowflake.ParseDSN(dsn)
	if err != nil {
		return nil, fmt.Errorf("snowflake: parse dsn: %w", err)
	}
	// ParseDSN splits database/schema at the last "/", so a raw "/" in a
	// parameter value folds the query string before it into the path: those
	// parameters (protocol=http, authenticator=) are silently dropped and the
	// rest, secrets included, becomes the database name in receipts (#514).
	// The message names neither the value nor the DSN.
	if strings.ContainsAny(cfg.Database, "?&=") || strings.ContainsAny(cfg.Schema, "?&=") {
		return nil, errors.New("snowflake: dsn database or schema holds query parameters: a parameter value has an unencoded \"/\"; URL-encode every parameter value (\"/\" as %2F); refused")
	}
	if cfg.Tracing != "" {
		return nil, errors.New("snowflake: dsn parameter tracing is not allowed: it raises the driver log level; refused as defence in depth (GO-2026-6665)")
	}
	if cfg.ClientConfigFile != "" {
		return nil, errors.New("snowflake: dsn parameter clientConfigFile is not allowed: it can raise the driver log level; refused as defence in depth (GO-2026-6665)")
	}
	// ParseDSN fills in "https" when the DSN has no protocol, so anything
	// else was set on purpose. The message names the parameter, not its value
	// or the DSN, which holds the password.
	if !strings.EqualFold(cfg.Protocol, "https") {
		return nil, errors.New("snowflake: dsn parameter protocol must be https: any other value sends the login, password or key-pair JWT and session token in clear text; refused")
	}
	for _, p := range []struct{ name, value string }{
		{"oauthTokenRequestUrl", cfg.OauthTokenRequestURL},
		{"oauthAuthorizationUrl", cfg.OauthAuthorizationURL},
	} {
		if err := checkHTTPSURL(p.name, p.value); err != nil {
			return nil, err
		}
	}
	if err := checkWorkloadIdentityHost(cfg.WorkloadIdentityHost); err != nil {
		return nil, err
	}
	if cfg.DisableOCSPChecks {
		slog.Warn("snowflake: disableOCSPChecks=true in the DSN: certificate revocation is not checked, a revoked Snowflake or proxy certificate is accepted")
	}
	// gosnowflake v2.3.0 stopped checking certificate revocation unless asked
	// (v1.19.1 and v2.2.0 defaulted to OCSP fail-open). Opt back in, but only
	// when the DSN left every revocation setting at its default: an explicit
	// ocspFailOpen=false (fail-closed), disableOCSPChecks or a CRL mode is the
	// operator's choice and stays as parsed.
	if cfg.OCSPFailOpen == ocspNotSet && !cfg.DisableOCSPChecks &&
		cfg.CertRevocationCheckMode == gosnowflake.CertRevocationCheckDisabled {
		cfg.OCSPFailOpen = gosnowflake.OCSPFailOpenTrue
	}
	return cfg, nil
}

// checkHTTPSURL refuses a set token endpoint that is not an absolute https
// URL, or that carries userinfo. Empty means the driver default (the https
// account host) and is allowed. The error names the parameter only: the URL
// can hold credentials, and url.Parse errors quote the whole URL.
func checkHTTPSURL(param, raw string) error {
	if raw == "" {
		return nil
	}
	u, err := url.Parse(raw)
	if err != nil || !strings.EqualFold(u.Scheme, "https") || u.Host == "" {
		return fmt.Errorf("snowflake: dsn parameter %s must be an absolute https URL: any other value sends the OAuth client secret or token in clear text; refused", param)
	}
	if u.User != nil {
		return fmt.Errorf("snowflake: dsn parameter %s must not carry credentials in the URL (user:password@); refused", param)
	}
	return nil
}

// checkWorkloadIdentityHost mirrors gosnowflake's own parsing of
// workloadIdentityHost (trim spaces, a value without "://" is a bare host
// that gets https://), but refuses http, which the driver allows: the STS
// calls carry the signed AWS session token and return temporary credentials
// or the web-identity JWT used as the Snowflake login.
func checkWorkloadIdentityHost(raw string) error {
	if raw == "" {
		return nil
	}
	host := strings.TrimSpace(raw)
	if host == "" {
		// The driver refuses this too, but only when it connects.
		return errors.New("snowflake: dsn parameter workloadIdentityHost is blank; refused")
	}
	if !strings.Contains(host, "://") {
		host = "https://" + host
	}
	return checkHTTPSURL("workloadIdentityHost", host)
}

// Close releases the underlying connection pool.
func (d *DB) Close() error {
	if d == nil || d.db == nil {
		return nil
	}
	return d.db.Close()
}

// Freshness returns max(tsColumn) of table ("schema.table" or
// "db.schema.table"). A zero time means the table has no rows.
func (d *DB) Freshness(ctx context.Context, table, tsColumn string) (time.Time, error) {
	ctx, cancel := context.WithTimeout(ctx, defaultTimeout)
	defer cancel()

	q, err := freshnessSQL(table, tsColumn)
	if err != nil {
		return time.Time{}, err
	}
	rows, err := d.query(gosnowflake.WithHigherPrecision(ctx), q)
	if err != nil {
		return time.Time{}, wrapQueryErr("freshness of", table, err)
	}
	defer rows.Close()

	if !rows.Next() {
		if err := rows.Err(); err != nil {
			return time.Time{}, wrapQueryErr("freshness of", table, err)
		}
		return time.Time{}, nil
	}
	var ts *time.Time
	if err := rows.Scan(&ts); err != nil {
		return time.Time{}, fmt.Errorf("snowflake: freshness of %s: decode: %w", table, err)
	}
	if ts == nil {
		return time.Time{}, nil
	}
	// Compare in UTC: the driver returns TIMESTAMP_NTZ as UTC wall time and
	// TIMESTAMP_LTZ in the session zone, so normalise before the caller does
	// any age arithmetic or formatting.
	return ts.UTC(), nil
}

// Row reads one row by primary key. table is "schema.table" or
// "db.schema.table". Values are converted to JSON-friendly types (numbers
// and high-precision values -> strings, times -> RFC 3339).
//
// Column names are lower-cased, matching internal/clients/warehouse (the
// Postgres implementation of this same interface): Snowflake returns an
// unquoted column's name upper-cased, e.g. "AMOUNT", and both the topology
// builder's trace diff and the DB tools' timestamp-column detection assume
// lower-case keys like "amount" or "updated_at". A column created WITH
// quotes, preserving mixed/lower case, would have its name folded here too
// — there is no way to tell that case apart from the normal unquoted one
// once the driver has already returned the name as a plain string.
func (d *DB) Row(ctx context.Context, table, pkColumn, pk string) (map[string]any, bool, error) {
	ctx, cancel := context.WithTimeout(ctx, defaultTimeout)
	defer cancel()

	col, err := d.resolveKeyColumn(ctx, table, pkColumn)
	if err != nil {
		return nil, false, err
	}
	q, err := rowSQL(table, col)
	if err != nil {
		return nil, false, err
	}
	rows, err := d.query(gosnowflake.WithHigherPrecision(ctx), q, pk)
	if err != nil {
		return nil, false, wrapQueryErr("query row from", table, err)
	}
	defer rows.Close()

	cols, err := rows.Columns()
	if err != nil {
		return nil, false, fmt.Errorf("snowflake: row from %s: columns: %w", table, err)
	}
	if !rows.Next() {
		if err := rows.Err(); err != nil {
			return nil, false, wrapQueryErr("query row from", table, err)
		}
		return nil, false, nil
	}

	vals := make([]any, len(cols))
	ptrs := make([]any, len(cols))
	for i := range vals {
		ptrs[i] = &vals[i]
	}
	if err := rows.Scan(ptrs...); err != nil {
		return nil, false, fmt.Errorf("snowflake: row from %s: decode: %w", table, err)
	}

	// Two columns that fold to one name (a table with both "id" and ID) keep
	// their exact names, so neither value is lost.
	folded := make(map[string]int, len(cols))
	for _, name := range cols {
		folded[strings.ToLower(name)]++
	}
	m := make(map[string]any, len(cols))
	for i, name := range cols {
		if lower := strings.ToLower(name); folded[lower] == 1 {
			name = lower
		}
		m[name] = toJSON(vals[i])
	}
	return m, true, nil
}

// resolveKeyColumn maps the key column a caller names to the column's exact
// stored name, which Row then quotes as is. Callers pass the name in whatever
// case they have: warehouse_row passes PrimaryKeyColumn's answer, the topology
// trace passes the source Postgres key ("id") for the sink table. A name that
// equals a declared key column exactly is used as is; otherwise one that
// matches a single key column ignoring case is mapped to it, so "id" reaches
// an unquoted ID key. A composite key is clients.ErrNotSupported, whatever
// the name. A table without a declared key, or a name that is not its key,
// keeps the old behaviour (fold to upper case, as for an unquoted
// identifier) so lookups that worked before still do.
func (d *DB) resolveKeyColumn(ctx context.Context, table, pkColumn string) (string, error) {
	if _, err := validateIdent(pkColumn); err != nil {
		return "", err
	}
	keys, err := d.primaryKeys(ctx, table)
	if err != nil {
		if errors.Is(err, clients.ErrNotFound) {
			// Either no declared key or a missing table; the row query below
			// reports a missing table itself.
			return strings.ToUpper(pkColumn), nil
		}
		return "", err
	}
	// No single column of a composite key names one row: a lookup by ID of
	// PRIMARY KEY (TENANT_ID, ID) reads another tenant's row (#531).
	if len(keys) > 1 {
		return "", fmt.Errorf("snowflake: primary key of %s is composite (%d columns): a lookup by %q alone is not one row: %w", table, len(keys), pkColumn, clients.ErrNotSupported)
	}
	var folded []string
	for _, k := range keys {
		if k == pkColumn {
			return k, nil
		}
		if strings.EqualFold(k, pkColumn) {
			folded = append(folded, k)
		}
	}
	if len(folded) == 1 {
		return folded[0], nil
	}
	return strings.ToUpper(pkColumn), nil
}

var _ clients.Warehouse = (*DB)(nil)

// PrimaryKeyColumn returns the primary key column of table ("schema.table"
// or "db.schema.table"), spelled exactly as Snowflake stores it: ID for a
// column created unquoted, id for one created as "id". Row quotes that
// spelling as is, so the two never get mixed up. A table without a primary
// key is clients.ErrNotFound; a composite key is clients.ErrNotSupported,
// since no single column names one row. warehouse_row uses it to refuse
// lookups by any other column, which would make the tool an existence oracle
// for personal data. Snowflake does not enforce primary keys, but the
// declared key is what the pipeline owners maintain.
func (d *DB) PrimaryKeyColumn(ctx context.Context, table string) (string, error) {
	keys, err := d.primaryKeys(ctx, table)
	if err != nil {
		return "", err
	}
	// A lookup by the first column of a composite key reads whichever row
	// shares it (another tenant's, with its timestamps): refuse it (#531).
	if len(keys) > 1 {
		return "", fmt.Errorf("snowflake: primary key of %s is composite (%d columns): a lookup by %q alone is not one row: %w", table, len(keys), keys[0], clients.ErrNotSupported)
	}
	return keys[0], nil
}

// primaryKeys returns the primary key columns of table in key order, with
// their stored spelling. It never returns an empty slice without an error.
func (d *DB) primaryKeys(ctx context.Context, table string) ([]string, error) {
	ctx, cancel := context.WithTimeout(ctx, defaultTimeout)
	defer cancel()

	qualified, err := quoteTable(table)
	if err != nil {
		return nil, err
	}
	rows, err := d.query(ctx, "SHOW PRIMARY KEYS IN TABLE "+qualified)
	if err != nil {
		return nil, wrapQueryErr("primary key of", table, err)
	}
	defer rows.Close()

	cols, err := rows.Columns()
	if err != nil {
		return nil, fmt.Errorf("snowflake: primary key of %s: columns: %w", table, err)
	}
	nameIdx, seqIdx := -1, -1
	for i, c := range cols {
		switch strings.ToLower(c) {
		case "column_name":
			nameIdx = i
		case "key_sequence":
			seqIdx = i
		}
	}
	if nameIdx < 0 || seqIdx < 0 {
		return nil, fmt.Errorf("snowflake: primary key of %s: SHOW PRIMARY KEYS result has no column_name/key_sequence (got %v)", table, cols)
	}

	type key struct {
		name string
		seq  int
	}
	var found []key
	for rows.Next() {
		vals := make([]any, len(cols))
		ptrs := make([]any, len(cols))
		for i := range vals {
			ptrs[i] = &vals[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			return nil, fmt.Errorf("snowflake: primary key of %s: decode: %w", table, err)
		}
		seq, err := strconv.Atoi(fmt.Sprint(vals[seqIdx]))
		if err != nil {
			return nil, fmt.Errorf("snowflake: primary key of %s: key_sequence %v: %w", table, vals[seqIdx], err)
		}
		found = append(found, key{fmt.Sprint(vals[nameIdx]), seq})
	}
	if err := rows.Err(); err != nil {
		return nil, wrapQueryErr("primary key of", table, err)
	}
	if len(found) == 0 {
		return nil, fmt.Errorf("snowflake: primary key of %s: table has no declared primary key (it may exist): %w", table, clients.ErrNotFound)
	}
	sort.SliceStable(found, func(i, j int) bool { return found[i].seq < found[j].seq })
	names := make([]string, len(found))
	for i, k := range found {
		names[i] = k.name
	}
	return names, nil
}

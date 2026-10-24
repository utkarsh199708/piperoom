// Package oracle implements a read-only Oracle client over go-ora (pure Go, no
// Oracle Instant Client). It answers the prerequisite questions the Oracle
// setup driver needs from the dictionary: archive log mode, database-level and
// table-level supplemental logging, the LogMiner user's grants, the CDB/PDB
// names, and the version and edition. Every method is a SELECT; there are no
// write methods, so the compiler cannot hand one to a caller.
package oracle

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net/url"
	"strings"

	go_ora "github.com/sijms/go-ora/v2"
)

// Client is a read-only connection to an Oracle database.
type Client struct {
	db     *sql.DB
	q      querier
	target string // receipt target: oracle host:port/service, never credentials
}

// New opens one connection and verifies it with a ping under the per-call
// deadline. The DSN is a go-ora URL: oracle://user:password@host:port/service.
func New(ctx context.Context, dsn string) (*Client, error) {
	if _, err := go_ora.ParseConfig(dsn); err != nil {
		// go-ora returns url.Parse's error unchanged, and *url.Error prints the
		// full URL (including the password). Report only the inner cause.
		var ue *url.Error
		if errors.As(err, &ue) {
			return nil, fmt.Errorf("oracle: parse config: %w", ue.Err)
		}
		return nil, fmt.Errorf("oracle: parse config: %w", err)
	}

	db := sql.OpenDB(go_ora.NewConnector(dsn))
	// One session against the source is enough for the read-only checks; a
	// larger pool would open extra sessions for no benefit.
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	db.SetConnMaxIdleTime(queryTimeout)

	pingCtx, cancel := context.WithTimeout(ctx, queryTimeout)
	defer cancel()
	if err := db.PingContext(pingCtx); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("oracle: ping: %w", err)
	}

	return &Client{db: db, q: dbQuerier{db: db}, target: targetOf(dsn)}, nil
}

// Close releases the connection.
func (c *Client) Close() error {
	if c == nil || c.db == nil {
		return nil
	}
	return c.db.Close()
}

var _ Reader = (*Client)(nil)

// targetOf renders a credential-free receipt target from the DSN. url.Parse
// splits the userinfo off on its own, so the password never appears in the
// result.
func targetOf(dsn string) string {
	u, err := url.Parse(dsn)
	if err != nil || u.Host == "" {
		return "oracle"
	}
	host := u.Host
	service := strings.TrimPrefix(u.Path, "/")
	if service == "" {
		return host
	}
	return host + "/" + service
}

// Database reads the database identity and prerequisites from V$DATABASE and
// V$VERSION in at most three queries: the V$DATABASE row, the PDB list (only
// when the database is a CDB), and the version banners. Archive log mode,
// force logging and database-level supplemental logging ride on the V$DATABASE
// row, so they share one round trip.
func (c *Client) Database(ctx context.Context) (DatabaseInfo, error) {
	var (
		name, logMode, forceLog string
		cdb, container          sql.NullString
		supMin, supPK, supAll   sql.NullString
		supFK, supUI            sql.NullString
	)
	if err := c.scanOne(ctx, `
		SELECT NAME, CDB, SYS_CONTEXT('USERENV','CON_NAME'), LOG_MODE, FORCE_LOGGING,
		       SUPPLEMENTAL_LOG_DATA_MIN, SUPPLEMENTAL_LOG_DATA_PK, SUPPLEMENTAL_LOG_DATA_ALL,
		       SUPPLEMENTAL_LOG_DATA_FK, SUPPLEMENTAL_LOG_DATA_UI
		  FROM V$DATABASE`,
		&name, &cdb, &container, &logMode, &forceLog, &supMin, &supPK, &supAll, &supFK, &supUI); err != nil {
		return DatabaseInfo{}, fmt.Errorf("oracle: database: %w", err)
	}

	info := DatabaseInfo{
		Name:      name,
		IsCDB:     yesNo(cdb),
		Container: nullStr(container),
		LogMode:   logMode,
		ForceLog:  forceLog,
		Supplemental: SupplementalLogging{
			Minimal:     yesNo(supMin),
			PrimaryKey:  yesNo(supPK),
			All:         yesNo(supAll),
			ForeignKey:  yesNo(supFK),
			UniqueIndex: yesNo(supUI),
		},
	}

	if info.IsCDB {
		pdbs, err := c.pdbs(ctx)
		if err != nil {
			return DatabaseInfo{}, err
		}
		info.PDBs = pdbs
	}

	banner, err := c.versionBanner(ctx)
	if err != nil {
		return DatabaseInfo{}, err
	}
	info.Version = banner
	info.Edition = editionFromBanner(banner)
	return info, nil
}

// pdbs lists the pluggable database names in CON_ID order, excluding the CDB
// root (1) and the seed (2).
func (c *Client) pdbs(ctx context.Context) ([]string, error) {
	var out []string
	err := c.each(ctx, `SELECT NAME FROM V$CONTAINERS WHERE CON_ID > 2 ORDER BY CON_ID`, nil, func(rows sqlRows) error {
		var name sql.NullString
		if err := rows.Scan(&name); err != nil {
			return fmt.Errorf("oracle: scan pdb: %w", err)
		}
		out = append(out, nullStr(name))
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("oracle: list pdbs: %w", err)
	}
	return out, nil
}

// versionBanner returns the first database product banner from V$VERSION:
// "Oracle Database …", or "Oracle AI Database …" from 23.26 (isDatabaseBanner).
func (c *Client) versionBanner(ctx context.Context) (string, error) {
	var banner string
	err := c.each(ctx, `SELECT BANNER FROM V$VERSION ORDER BY ROWNUM`, nil, func(rows sqlRows) error {
		var b sql.NullString
		if err := rows.Scan(&b); err != nil {
			return fmt.Errorf("oracle: scan version: %w", err)
		}
		if isDatabaseBanner(nullStr(b)) {
			banner = nullStr(b)
			return errStop
		}
		return nil
	})
	if err != nil {
		return "", fmt.Errorf("oracle: read version: %w", err)
	}
	if banner == "" {
		return "", fmt.Errorf("oracle: no Oracle database product banner in V$VERSION")
	}
	return banner, nil
}

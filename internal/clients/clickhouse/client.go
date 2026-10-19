package clickhouse

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	driver "github.com/ClickHouse/clickhouse-go/v2"

	"piperoom/internal/clients"
)

const defaultTimeout = 10 * time.Second

// Client reads warehouse data from ClickHouse through one pooled connection.
type Client struct {
	db              *sql.DB
	q               querier
	dsn             string
	target          string
	defaultDatabase string
}

// New parses the ClickHouse DSN, enforces a read-only session, and verifies
// connectivity before returning the pooled client.
func New(ctx context.Context, dsn string) (*Client, error) {
	opts, err := optionsForDSN(dsn)
	if err != nil {
		return nil, Redact(err, dsn)
	}

	db := driver.OpenDB(opts)
	pingCtx, cancel := context.WithTimeout(ctx, defaultTimeout)
	defer cancel()
	if err := db.PingContext(pingCtx); err != nil {
		_ = db.Close()
		return nil, Redact(fmt.Errorf("clickhouse: ping: %w", err), dsn)
	}

	return &Client{
		db:              db,
		q:               dbQuerier{db: db},
		dsn:             dsn,
		target:          targetFromOptions(opts),
		defaultDatabase: opts.Auth.Database,
	}, nil
}

func optionsForDSN(dsn string) (*driver.Options, error) {
	opts, err := driver.ParseDSN(dsn)
	if err != nil {
		return nil, fmt.Errorf("clickhouse: parse dsn: %w", err)
	}
	if opts.Settings == nil {
		opts.Settings = driver.Settings{}
	}
	// The role should also have SELECT-only grants. Pinning this session
	// setting prevents accidental writes if those grants are broader.
	opts.Settings["readonly"] = 1
	opts.DialTimeout = defaultTimeout
	opts.ReadTimeout = defaultTimeout
	opts.MaxOpenConns = 5
	opts.MaxIdleConns = 5
	return opts, nil
}

func targetFromOptions(opts *driver.Options) string {
	target := "clickhouse"
	if len(opts.Addr) > 0 {
		target += " " + strings.Join(opts.Addr, ",")
	}
	if opts.Auth.Database != "" {
		target += "/" + opts.Auth.Database
	}
	return target
}

// Close releases the underlying connection pool.
func (c *Client) Close() error {
	if c == nil || c.db == nil {
		return nil
	}
	return c.db.Close()
}

var _ clients.Warehouse = (*Client)(nil)
var _ clients.CountReader = (*Client)(nil)
var _ clients.LoadErrorReader = (*Client)(nil)

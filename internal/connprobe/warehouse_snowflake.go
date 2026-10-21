package connprobe

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net"
	"net/http"
	"regexp"
	"strings"

	"github.com/snowflakedb/gosnowflake/v2"
)

// Snowflake warehouse probe. Differences from what the real client does
// (internal/clients/snowflake), all deliberate:
//
//   - The probe opens its own connection instead of snowflake.New, because
//     New cannot be told to dial through the guard. The guard is attached with
//     Config.Transporter, an http.RoundTripper the driver uses for every HTTP
//     request of the login; its DialContext is Guard.DialContext, so each
//     connection goes to an address the guard checked.
//   - The driver uses a custom Transporter instead of its own transports
//     (gosnowflake transport.go, createTransport). Two consequences: no
//     certificate-revocation (OCSP/CRL) check in the probe, and no HTTP proxy.
//     A network that needs a proxy fails the reach or login step here although
//     the real client may work; the fix text says the probe connects directly.
//   - The hardening of snowflake.New is repeated only where it matters for a
//     probe the admin can point anywhere: driver log level error, protocol
//     https, no tracing, no client config file. Authenticators that open a
//     browser or call other hosts (external browser, Okta, OAuth, workload
//     identity) are not tested: login and select_sink are skipped for them.
//   - select_sink runs "SELECT <col> FROM <table> LIMIT 0" per sink table; the
//     privilege error comes at compile time, no row is read. Snowflake does
//     not tell a missing object from a missing grant, so the problem says both.
//
// The SELECT probe is covered by unit tests with a fake session only; it has
// not been run against a real Snowflake account.

var warehouseSFChecks = []string{"config", "reach", "login", "select_sink"}

// sfSession is the part of a Snowflake connection the checks use.
type sfSession interface {
	// probe runs a statement that returns no rows of interest.
	probe(ctx context.Context, query string) error
	close()
}

// sfConnector logs in with cfg, dialing only through g.
type sfConnector func(ctx context.Context, cfg *gosnowflake.Config, g *Guard) (sfSession, error)

type sfDB struct {
	db *sql.DB
	tr *http.Transport
}

func connectSnowflake(ctx context.Context, cfg *gosnowflake.Config, g *Guard) (sfSession, error) {
	c := *cfg
	tr := &http.Transport{
		DialContext:         g.DialContext,
		TLSHandshakeTimeout: DefaultStepTimeout,
		MaxIdleConns:        2,
		IdleConnTimeout:     DefaultStepTimeout,
	}
	c.Transporter = tr
	db := sql.OpenDB(gosnowflake.NewConnector(gosnowflake.SnowflakeDriver{}, c))
	db.SetMaxOpenConns(1)
	if err := db.PingContext(ctx); err != nil {
		_ = db.Close()
		tr.CloseIdleConnections()
		return nil, fmt.Errorf("connprobe: snowflake ping: %w", err)
	}
	return &sfDB{db: db, tr: tr}, nil
}

func (s *sfDB) probe(ctx context.Context, query string) error {
	rows, err := s.db.QueryContext(ctx, query)
	if err != nil {
		return fmt.Errorf("connprobe: snowflake query: %w", err)
	}
	return rows.Close()
}

func (s *sfDB) close() {
	_ = s.db.Close()
	s.tr.CloseIdleConnections()
}

func probeWarehouseSnowflake(ctx context.Context, t Target, g *Guard, connect sfConnector) Result {
	res := Result{Kind: KindWarehouse}

	var cfg *gosnowflake.Config
	cfgCheck := step(func() Check {
		c, chk := parseSnowflakeConfig(t.Config.WarehouseDBURL)
		cfg = c
		return chk
	})
	res.Checks = append(res.Checks, cfgCheck)
	if cfgCheck.Status == StatusFail {
		res.Checks = skipRest(res.Checks, "not run: config failed", warehouseSFChecks[1:]...)
		return res
	}

	host := cfg.Host
	port := cfg.Port
	if port == 0 {
		port = 443
	}
	addr := net.JoinHostPort(host, fmt.Sprint(port))
	reach := step(func() Check {
		rctx, cancel := context.WithTimeout(ctx, DefaultStepTimeout)
		defer cancel()
		// Resolve first so a refused name is reported as such, not as a dial error.
		if _, err := g.Resolve(rctx, host); err != nil {
			return snowflakeReachFailure(err, addr)
		}
		conn, err := g.DialContext(rctx, "tcp", addr)
		if err != nil {
			return snowflakeReachFailure(err, addr)
		}
		_ = conn.Close()
		return Check{ID: "reach", Status: StatusOK, Detail: "TCP connection to " + addr}
	})
	res.Checks = append(res.Checks, reach)
	if reach.Status == StatusFail {
		res.Checks = skipRest(res.Checks, "not run: reach failed", warehouseSFChecks[2:]...)
		return res
	}

	if cfg.Authenticator != gosnowflake.AuthTypeSnowflake && cfg.Authenticator != gosnowflake.AuthTypeJwt {
		why := "not tested: the DSN uses an authenticator that opens a browser or calls other hosts; only password and key-pair logins are tested"
		res.Checks = skipRest(res.Checks, why, warehouseSFChecks[2:]...)
		return res
	}

	var sess sfSession
	login := step(func() Check {
		lctx, cancel := context.WithTimeout(ctx, DefaultStepTimeout)
		defer cancel()
		s, err := connect(lctx, cfg, g)
		if err != nil {
			if errors.Is(err, ErrBlockedAddress) {
				return Check{ID: "login", Status: StatusFail,
					Problem: "the Snowflake address is not allowed",
					Fix:     "Use the account's normal Snowflake host name in WAREHOUSE_DB_URL."}
			}
			return Check{ID: "login", Status: StatusFail,
				Problem: "Snowflake did not accept the login",
				Fix:     "Check the user, password or key, account, role and warehouse in WAREHOUSE_DB_URL. The test connects directly: it uses no HTTP proxy and does not check certificate revocation."}
		}
		sess = s
		return Check{ID: "login", Status: StatusOK, Detail: "logged in to " + safeName(host)}
	})
	res.Checks = append(res.Checks, login)
	if login.Status == StatusFail {
		res.Checks = skipRest(res.Checks, "not run: login failed", warehouseSFChecks[3:]...)
		return res
	}
	defer sess.close()

	res.Checks = append(res.Checks, snowflakeSinkCheck(ctx, sess, cfg, sinkItems(t.Pipelines)))
	return res
}

// parseSnowflakeConfig parses the DSN and applies the hardening a probe needs.
func parseSnowflakeConfig(dsn string) (*gosnowflake.Config, Check) {
	if strings.TrimSpace(dsn) == "" {
		return nil, Check{ID: "config", Status: StatusFail,
			Problem: "no connection string is set for the warehouse",
			Fix:     "Set WAREHOUSE_DB_URL (or the pipeline's connections.warehouse_db_url) to a Snowflake DSN: user:password@account/database/schema?warehouse=...&role=...  (URL-encode the password)."}
	}
	// Pinned as snowflake.New does: the driver logs more when raised.
	if err := gosnowflake.GetLogger().SetLogLevel("error"); err != nil {
		return nil, Check{ID: "config", Status: StatusFail,
			Problem: "the Snowflake driver's log level could not be pinned, so the test did not run",
			Fix:     "Report this to the Piperoom maintainers."}
	}
	cfg, err := gosnowflake.ParseDSN(dsn)
	if err != nil {
		// The driver's error can quote the DSN.
		return nil, Check{ID: "config", Status: StatusFail,
			Problem: "the Snowflake DSN in WAREHOUSE_DB_URL cannot be parsed",
			Fix:     "Use user:password@account/database/schema?warehouse=...&role=... and URL-encode the password and every parameter value."}
	}
	switch {
	case strings.ContainsAny(cfg.Database, "?&=") || strings.ContainsAny(cfg.Schema, "?&="):
		return nil, Check{ID: "config", Status: StatusFail,
			Problem: "the Snowflake DSN holds query parameters inside its database or schema part",
			Fix:     "URL-encode every parameter value in the DSN (a \"/\" becomes %2F)."}
	case cfg.Tracing != "" || cfg.ClientConfigFile != "":
		return nil, Check{ID: "config", Status: StatusFail,
			Problem: "the Snowflake DSN sets tracing or clientConfigFile, which Piperoom refuses",
			Fix:     "Remove the tracing and clientConfigFile parameters from the DSN."}
	case !strings.EqualFold(cfg.Protocol, "https"):
		return nil, Check{ID: "config", Status: StatusFail,
			Problem: "the Snowflake DSN sets a protocol other than https, which would send the login in clear text",
			Fix:     "Remove the protocol parameter or set protocol=https."}
	case cfg.Host == "":
		return nil, Check{ID: "config", Status: StatusFail,
			Problem: "no Snowflake account host can be derived from the DSN",
			Fix:     "Put the account in the DSN as user:password@account/database/schema."}
	}
	return cfg, Check{ID: "config", Status: StatusOK, Detail: "Snowflake DSN parsed (account host " + safeName(cfg.Host) + ")"}
}

func snowflakeReachFailure(err error, addr string) Check {
	switch {
	case errors.Is(err, ErrBlockedAddress):
		return Check{ID: "reach", Status: StatusFail,
			Problem: fmt.Sprintf("the address of %s is not allowed (link-local, cloud-metadata and reserved addresses are always refused; loopback and private ones only when the server allows them)", addr),
			Fix:     "Use the account's normal Snowflake host name in WAREHOUSE_DB_URL."}
	case classify(err) == classTimeout:
		return Check{ID: "reach", Status: StatusFail,
			Problem: fmt.Sprintf("connecting to %s timed out", addr),
			Fix:     "Allow outbound HTTPS from this server to the Snowflake account host. The test connects directly and uses no HTTP proxy."}
	}
	return Check{ID: "reach", Status: StatusFail,
		Problem: fmt.Sprintf("cannot open a TCP connection to %s", addr),
		Fix:     "Check the account name, and allow outbound HTTPS from this server to the Snowflake account host. The test connects directly and uses no HTTP proxy."}
}

// sfIdentRE is the part rule of internal/clients/snowflake/identifier.go.
var sfIdentRE = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_$]*$`)

// sfQuote renders "schema.table" or "db.schema.table" the way the real client
// does: each part upper-cased and double-quoted. ok is false for any part
// outside the identifier rule, so nothing unchecked reaches a statement.
func sfQuote(name string) (string, bool) {
	parts := strings.Split(name, ".")
	if len(parts) < 2 || len(parts) > 3 {
		return "", false
	}
	for i, p := range parts {
		if !sfIdentRE.MatchString(p) {
			return "", false
		}
		parts[i] = `"` + strings.ToUpper(p) + `"`
	}
	return strings.Join(parts, "."), true
}

func snowflakeSinkCheck(ctx context.Context, s sfSession, cfg *gosnowflake.Config, items []selectItem) Check {
	if len(items) == 0 {
		return Check{ID: "select_sink", Status: StatusSkip, Detail: "no sink tables are configured for this connection"}
	}
	return step(func() Check {
		sctx, cancel := context.WithTimeout(ctx, DefaultStepTimeout)
		defer cancel()
		role := "PIPEROOM_RO"
		if r := strings.ToUpper(cfg.Role); sfIdentRE.MatchString(r) {
			role = r
		}
		var invalid, failed, fixes []string
		cols := 0
		for _, it := range items {
			q, ok := sfQuote(it.name)
			if !ok {
				invalid = append(invalid, "a sink table name")
				continue
			}
			sel := "1"
			if it.tsCol != "" {
				if !sfIdentRE.MatchString(it.tsCol) {
					invalid = append(invalid, "the timestamp column of "+it.name)
					continue
				}
				sel = `"` + strings.ToUpper(it.tsCol) + `"`
			}
			err := s.probe(sctx, fmt.Sprintf("SELECT %s FROM %s LIMIT 0", sel, q))
			if err == nil {
				if it.tsCol != "" {
					cols++
				}
				continue
			}
			if classify(err) == classTimeout {
				return Check{ID: "select_sink", Status: StatusFail,
					Problem: "checking SELECT on the sink tables timed out",
					Fix:     "Check that the warehouse is running and answers queries from this role."}
			}
			what := it.name
			if it.tsCol != "" {
				what = it.name + " (column " + it.tsCol + ")"
			}
			failed = append(failed, what)
			fixes = append(fixes, fmt.Sprintf("GRANT SELECT ON TABLE %s TO ROLE %s;", q, role))
		}
		if len(invalid) == 0 && len(failed) == 0 {
			d := fmt.Sprintf("SELECT on %s", plural(len(items), "sink table", "sink tables"))
			if cols > 0 {
				d += fmt.Sprintf(" and %s", plural(cols, "timestamp column", "timestamp columns"))
			}
			return Check{ID: "select_sink", Status: StatusOK, Detail: d}
		}
		var problems []string
		if len(invalid) > 0 {
			problems = append(problems, fmt.Sprintf("%s is not a valid Snowflake name", listNames(invalid, 3)))
		}
		if len(failed) > 0 {
			problems = append(problems, fmt.Sprintf("SELECT failed on %s: it does not exist or role %s cannot read it", listNames(failed, 5), role))
		}
		fix := dedupeJoin(fixes, 6)
		if fix != "" {
			fix += " Also USAGE on its database and schema (docs/pilot/permissions.md)."
		}
		if len(invalid) > 0 {
			if fix != "" {
				fix += " "
			}
			fix += "Fix sink_tables / sink_ts_column in owners.yaml."
		}
		return Check{ID: "select_sink", Status: StatusFail, Problem: strings.Join(problems, "; ") + ".", Fix: fix}
	})
}

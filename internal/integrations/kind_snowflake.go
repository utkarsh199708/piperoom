package integrations

import (
	"context"
	"crypto/rsa"
	"crypto/x509"
	"database/sql"
	"encoding/pem"
	"errors"
	"fmt"
	"net/http"
	"regexp"
	"strings"

	"github.com/snowflakedb/gosnowflake"

	"piperoom/internal/clients/snowflake"
	"piperoom/internal/integrations/spec"
)

// KindSnowflake is the Snowflake warehouse card.
const KindSnowflake = "snowflake"

const (
	sfAuthKeyPair  = "key_pair"
	sfAuthPassword = "password"
)

var sfIdentRe = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_$]*$`)

// sfGrant is one row of SHOW GRANTS TO ROLE.
type sfGrant struct {
	Privilege string // e.g. SELECT, INSERT, USAGE, OWNERSHIP
	GrantedOn string // e.g. TABLE, SCHEMA, ROLE
	Name      string // the object's fully qualified name
}

// sfSession is what the Snowflake checks ask.
type sfSession interface {
	Ping(ctx context.Context) error
	Grants(ctx context.Context, role string) ([]sfGrant, error)
	Close() error
}

type sfOpener func(cfg gosnowflake.Config) (sfSession, error)

type sqlSnowflake struct{ db *sql.DB }

func realSnowflakeOpen(cfg gosnowflake.Config) (sfSession, error) {
	return sqlSnowflake{db: sql.OpenDB(gosnowflake.NewConnector(gosnowflake.SnowflakeDriver{}, cfg))}, nil
}

func (s sqlSnowflake) Ping(ctx context.Context) error { return s.db.PingContext(ctx) }
func (s sqlSnowflake) Close() error                   { return s.db.Close() }

// Grants reads SHOW GRANTS TO ROLE by column name, so a reordered result
// still parses. role is checked against sfIdentRe before it is quoted.
func (s sqlSnowflake) Grants(ctx context.Context, role string) ([]sfGrant, error) {
	if !sfIdentRe.MatchString(role) {
		return nil, fmt.Errorf("%w: role name", errBadInput)
	}
	rows, err := s.db.QueryContext(ctx, `SHOW GRANTS TO ROLE "`+strings.ToUpper(role)+`"`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	cols, err := rows.Columns()
	if err != nil {
		return nil, err
	}
	idx := map[string]int{}
	for i, c := range cols {
		idx[strings.ToLower(c)] = i
	}
	for _, need := range []string{"privilege", "granted_on", "name"} {
		if _, ok := idx[need]; !ok {
			return nil, fmt.Errorf("integrations: SHOW GRANTS has no %s column", need)
		}
	}
	var out []sfGrant
	for rows.Next() {
		vals := make([]any, len(cols))
		ptrs := make([]any, len(cols))
		for i := range vals {
			ptrs[i] = &vals[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			return nil, err
		}
		str := func(k string) string {
			v := vals[idx[k]]
			if b, ok := v.([]byte); ok {
				return string(b)
			}
			if v == nil {
				return ""
			}
			return fmt.Sprint(v)
		}
		out = append(out, sfGrant{Privilege: strings.ToUpper(str("privilege")), GrantedOn: strings.ToUpper(str("granted_on")), Name: str("name")})
	}
	return out, rows.Err()
}

// sfConfig builds the driver config from the form.
func sfConfig(v spec.Values) (gosnowflake.Config, error) {
	cfg := gosnowflake.Config{
		Account: strings.TrimSpace(v.Get("account")), User: strings.TrimSpace(v.Get("user")),
		Role: strings.TrimSpace(v.Get("role")), Warehouse: strings.TrimSpace(v.Get("warehouse")),
		Database: strings.TrimSpace(v.Get("database")), Schema: strings.TrimSpace(v.Get("schema")),
	}
	if cfg.Account == "" {
		return cfg, &spec.ValidationError{Field: "account", Message: "required"}
	}
	if cfg.Role != "" && !sfIdentRe.MatchString(cfg.Role) {
		return cfg, &spec.ValidationError{Field: "role", Message: "must be a plain identifier"}
	}
	switch auth := v.Get("auth"); auth {
	case "", sfAuthKeyPair:
		key, err := parseRSAKey(v.Secret("private_key").Reveal())
		if err != nil {
			return cfg, &spec.ValidationError{Field: "private_key", Message: err.Error()}
		}
		cfg.Authenticator = gosnowflake.AuthTypeJwt
		cfg.PrivateKey = key
	case sfAuthPassword:
		if v.Secret("password") == "" {
			return cfg, &spec.ValidationError{Field: "password", Message: "required for password login"}
		}
		cfg.Password = v.Secret("password").Reveal()
	default:
		return cfg, &spec.ValidationError{Field: "auth", Message: "must be key_pair or password"}
	}
	return cfg, nil
}

// parseRSAKey reads an unencrypted PKCS#8 (or PKCS#1) RSA private key.
// Errors never contain key material.
func parseRSAKey(p string) (*rsa.PrivateKey, error) {
	if strings.TrimSpace(p) == "" {
		return nil, errors.New("required for key-pair login")
	}
	block, _ := pem.Decode([]byte(p))
	if block == nil {
		return nil, errors.New("not a PEM private key")
	}
	if k, err := x509.ParsePKCS8PrivateKey(block.Bytes); err == nil {
		if rk, ok := k.(*rsa.PrivateKey); ok {
			return rk, nil
		}
		return nil, errors.New("the key is not an RSA key")
	}
	if rk, err := x509.ParsePKCS1PrivateKey(block.Bytes); err == nil {
		return rk, nil
	}
	return nil, errors.New("cannot parse the private key (encrypted keys are not supported; use an unencrypted PKCS#8 key)")
}

// sfWrite reports whether a privilege lets the role change data or objects.
func sfWrite(g sfGrant) bool {
	switch g.Privilege {
	case "INSERT", "UPDATE", "DELETE", "TRUNCATE", "OWNERSHIP", "MODIFY", "ALL", "ALL PRIVILEGES", "REBUILD", "EVOLVE SCHEMA":
		return true
	}
	return strings.HasPrefix(g.Privilege, "CREATE ") || strings.HasPrefix(g.Privilege, "APPLY")
}

func (d Deps) snowflakeTest(ctx context.Context, v spec.Values) []spec.Check {
	ctx, cancel := testCtx(ctx, d.TestTimeout)
	defer cancel()
	cl := newChecklist(ctx, d.CheckTimeout, v)
	cfg, err := sfConfig(v)
	if err != nil {
		cl.add("input", "Form values are valid", fail(err.Error(), ""))
		return cl.result()
	}
	host := cfg.Account + ".snowflakecomputing.com"
	port := d.sfPort
	if port == "" {
		port = "443"
	}
	if cl.run("reach", "Can reach "+host, reachCheck(d.dial(), host, port)) != spec.CheckPass {
		cl.skip("login", "Login works", "skipped: the account host is not reachable")
		return cl.result()
	}
	tr, ok := http.DefaultTransport.(*http.Transport)
	if ok {
		tr = tr.Clone()
		tr.Proxy = nil
		tr.DialContext = d.dial()
		cfg.Transporter = tr
	}
	cfg.LoginTimeout = d.checkTimeout()
	sess, err := d.sfOpen(cfg)
	if err != nil {
		cl.add("login", "Login works", fail(errText(err), ""))
		return cl.result()
	}
	defer sess.Close()
	st := cl.run("login", "Login works as "+cfg.User, func(ctx context.Context) outcome {
		if err := sess.Ping(ctx); err != nil {
			var se *gosnowflake.SnowflakeError
			if errors.As(err, &se) && (se.Number == 390100 || se.Number == 390144) {
				return fail("Snowflake rejected the login", "Check the user, and the key pair (ALTER USER <user> SET RSA_PUBLIC_KEY='<public key>';) or password.")
			}
			return fail("cannot log in: "+errText(err), "Check the account identifier (orgname-account), user and credentials.")
		}
		return pass("")
	})
	if st != spec.CheckPass {
		cl.skip("read_only", "Role has no write privileges", "skipped: the login failed")
		return cl.result()
	}
	if cfg.Role == "" {
		cl.skip("read_only", "Role has no write privileges", "skipped: no role set; set the read-only role the setup SQL creates")
		return cl.result()
	}
	cl.run("read_only", "Role "+cfg.Role+" has no write privileges", func(ctx context.Context) outcome {
		grants, err := sess.Grants(ctx, cfg.Role)
		if err != nil {
			return skippedOutcome("skipped: cannot run SHOW GRANTS TO ROLE (" + errText(err) + ")")
		}
		var bad, fixes, inherited []string
		for _, g := range grants {
			if g.GrantedOn == "ROLE" && g.Privilege == "USAGE" {
				inherited = append(inherited, g.Name)
				continue
			}
			if sfWrite(g) {
				bad = append(bad, g.Privilege+" on "+g.GrantedOn+" "+g.Name)
				fixes = append(fixes, fmt.Sprintf("REVOKE %s ON %s %s FROM ROLE %s;", g.Privilege, g.GrantedOn, g.Name, cfg.Role))
			}
		}
		if len(bad) > 0 {
			return fail("the role can write: "+strings.Join(bad, "; "), strings.Join(fixes, " "))
		}
		if len(inherited) > 0 {
			return warn("the role inherits other roles, whose grants were not checked: "+strings.Join(inherited, ", "),
				"Use a role that inherits no other role, created by the setup SQL.")
		}
		return pass(fmt.Sprintf("%d grant(s), none can write", len(grants)))
	})
	return cl.result()
}

func (d Deps) snowflakeBuild(ctx context.Context, v spec.Values) (any, func(), error) {
	cfg, err := sfConfig(v)
	if err != nil {
		return nil, nil, err
	}
	if err := d.guard().CheckHost(ctx, cfg.Account+".snowflakecomputing.com"); err != nil {
		return nil, nil, err
	}
	dsn, err := gosnowflake.DSN(&cfg)
	if err != nil {
		return nil, nil, fmt.Errorf("%w: snowflake settings: %s", errBadInput, snowflake.RedactString(err.Error(), ""))
	}
	open := d.sfNew
	if open == nil {
		open = func(ctx context.Context, dsn string) (any, func(), error) {
			db, err := snowflake.New(ctx, dsn)
			if err != nil {
				return nil, nil, err
			}
			return db, func() { _ = db.Close() }, nil
		}
	}
	db, closeFn, err := open(ctx, dsn)
	if err != nil {
		// snowflake.New redacts the DSN; scrubbing again guards the key-pair path.
		return nil, nil, scrubErr(snowflakeRedact(err, dsn), v.Secret("password"))
	}
	return db, closeFn, nil
}

// snowflakeRedact removes DSN parts (password, private key) from err.
func snowflakeRedact(err error, dsn string) error {
	if msg := snowflake.RedactString(err.Error(), dsn); msg != err.Error() {
		return &scrubbedError{msg: msg, err: err}
	}
	return err
}

func sfSetupScript(v spec.Values) string {
	id := func(name, def string) string {
		s := strings.ToUpper(strings.TrimSpace(v.Get(name)))
		if !sfIdentRe.MatchString(s) {
			return def
		}
		return s
	}
	role, user := id("role", "PIPEROOM_RO"), id("user", "PIPEROOM")
	wh, db, sc := id("warehouse", "<WAREHOUSE>"), id("database", "<DATABASE>"), id("schema", "<SCHEMA>")
	var b strings.Builder
	b.WriteString("-- Read-only role and user for Piperoom. Run as SECURITYADMIN (or a role that can grant).\n")
	fmt.Fprintf(&b, "CREATE ROLE IF NOT EXISTS %s;\n", role)
	fmt.Fprintf(&b, "GRANT USAGE ON WAREHOUSE %s TO ROLE %s;\n", wh, role)
	fmt.Fprintf(&b, "GRANT USAGE ON DATABASE %s TO ROLE %s;\n", db, role)
	fmt.Fprintf(&b, "GRANT USAGE ON SCHEMA %s.%s TO ROLE %s;\n", db, sc, role)
	fmt.Fprintf(&b, "GRANT SELECT ON ALL TABLES IN SCHEMA %s.%s TO ROLE %s;\n", db, sc, role)
	fmt.Fprintf(&b, "GRANT SELECT ON FUTURE TABLES IN SCHEMA %s.%s TO ROLE %s;\n", db, sc, role)
	fmt.Fprintf(&b, "CREATE USER IF NOT EXISTS %s DEFAULT_ROLE = %s DEFAULT_WAREHOUSE = %s;\n", user, role, wh)
	fmt.Fprintf(&b, "GRANT ROLE %s TO USER %s;\n", role, user)
	fmt.Fprintf(&b, "-- Key-pair login: paste the PEM public key body (no header lines).\n")
	fmt.Fprintf(&b, "ALTER USER %s SET RSA_PUBLIC_KEY='<public key>';\n", user)
	b.WriteString("-- Do not grant INSERT, UPDATE, DELETE, TRUNCATE, OWNERSHIP or any CREATE privilege.\n")
	return b.String()
}

func (d Deps) snowflakeKind() spec.Kind {
	idf := func(name, label string, req bool) spec.Field {
		return spec.Field{Name: name, Label: label, Type: spec.FieldText, Required: req, Pattern: `^[A-Za-z_][A-Za-z0-9_$]*$`}
	}
	return spec.Kind{
		ID: KindSnowflake, Category: spec.CategoryWarehouse, Title: "Snowflake",
		Summary: "Snowflake as the pipeline's sink", Logo: "snowflake",
		Reads: []string{"the newest timestamp of each sink table (freshness)", "single rows by primary key during an investigation", "the grants of its own role"},
		Never: []string{"writes to any table", "creates or alters objects"},
		Fields: []spec.Field{
			{Name: "account", Label: "Account identifier", Help: "orgname-accountname", Type: spec.FieldText, Required: true, Pattern: `^[A-Za-z0-9._-]+$`},
			{Name: "user", Label: "User", Type: spec.FieldText, Required: true, Pattern: `^[A-Za-z0-9_.@$-]+$`},
			idf("role", "Role", true), idf("warehouse", "Warehouse", true), idf("database", "Database", true), idf("schema", "Schema", true),
			{Name: "auth", Label: "Login with", Type: spec.FieldSelect, Default: sfAuthKeyPair, Options: []string{sfAuthKeyPair, sfAuthPassword}},
			{Name: "private_key", Label: "Private key", Help: "Unencrypted PKCS#8 PEM; Piperoom never shows it again", Type: spec.FieldPEM},
			{Name: "password", Label: "Password", Help: "Only for password login", Type: spec.FieldSecret},
		},
		Required: true, SetupScript: sfSetupScript,
		Test: d.snowflakeTest, Build: d.snowflakeBuild,
	}
}

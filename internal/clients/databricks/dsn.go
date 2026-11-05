package databricks

import (
	"errors"
	"fmt"
	"net/url"
	"regexp"
	"sort"
	"strconv"
	"strings"
)

var (
	// hostRe is a DNS host name or IPv4 address: the workspace host, such as
	// dbc-1234abcd-5678.cloud.databricks.com or adb-123.4.azuredatabricks.net.
	hostRe = regexp.MustCompile(`^[a-zA-Z0-9]([a-zA-Z0-9.-]*[a-zA-Z0-9])?$`)
	// httpPathRe is the SQL warehouse (or cluster) HTTP path, such as
	// /sql/1.0/warehouses/1234567890abcdef.
	httpPathRe = regexp.MustCompile(`^/[a-zA-Z0-9_.\-/]+$`)
)

// Config is a parsed WAREHOUSE_DB_URL for Databricks.
type Config struct {
	Host     string // workspace host, without scheme or port
	Port     int    // 443 unless the DSN names another
	HTTPPath string // the SQL warehouse's HTTP path
	// Catalog is the Unity Catalog catalog that holds the sink tables;
	// "schema.table" names resolve in it.
	Catalog string
	// Schema is the session's initial schema. Optional; tables are always
	// qualified with their schema, so it changes nothing this client reads.
	Schema string

	// Token is the personal access token. It is never printed: Config has a
	// String method that leaves it out.
	Token string

	// ClientID and ClientSecret are the OAuth M2M (service principal)
	// credentials; when either is set the DSN is M2M, not PAT. They are never
	// printed: Config.String leaves them out, and errors scrub them.
	ClientID     string
	ClientSecret string
}

// isM2M reports whether the DSN is OAuth M2M (service principal) rather than
// a personal access token.
func (c Config) isM2M() bool {
	return c.ClientID != "" || c.ClientSecret != ""
}

// String leaves the token out, so a Config printed by mistake (%v in a log
// line) does not leak it.
func (c Config) String() string {
	return fmt.Sprintf("databricks{host=%s port=%d path=%s catalog=%s}", c.Host, c.Port, c.HTTPPath, c.Catalog)
}

// GoString is String, for %#v.
func (c Config) GoString() string { return c.String() }

// target names the workspace and catalog for a provenance receipt.
func (c Config) target() string {
	return "databricks " + c.Host + "/" + c.Catalog
}

// ParseDSN parses the databricks-sql-go DSN form, restricted to what a
// read-only warehouse client needs:
//
//	token:<personal-access-token>@<host>[:<port>]/<http-path>?catalog=<catalog>[&schema=<schema>]
//
// or, for OAuth M2M (service principal):
//
//	<host>[:<port>]/<http-path>?catalog=<catalog>[&schema=<schema>]&clientId=<id>&clientSecret=<secret>[&authType=OAuthM2M]
//
// An optional https:// prefix is accepted; http:// is refused (the token
// would travel in clear text). The port defaults to 443. accessToken=<pat>
// (with optional authType=Pat) may stand in for the token: user info, and
// clientID (capital ID) for clientId. catalog is required. Any other
// parameter is refused: the driver would pass it on as a session parameter.
// So are a user name other than "token", OAuth U2M (interactive, it opens a
// browser), mixing a token with client credentials, a repeated parameter and
// a #fragment.
//
// Error messages never quote the DSN or any part of it: a token pasted in
// the wrong place (the path, the host) must not reach a log.
func ParseDSN(dsn string) (Config, error) {
	full := dsn
	switch {
	case hasPrefixFold(dsn, "https://"):
	case hasPrefixFold(dsn, "http://"):
		return Config{}, errors.New("databricks: dsn must not use http://: the token would travel in clear text; use https:// or no scheme")
	case strings.Contains(dsn, "://"):
		return Config{}, errors.New("databricks: dsn scheme must be https:// or none")
	default:
		full = "https://" + dsn
	}
	u, err := url.Parse(full)
	if err != nil {
		// url.Parse errors quote the input, which holds the token.
		return Config{}, errors.New("databricks: dsn is not in the form token:<pat>@<host>[:<port>]/<http-path>?catalog=<catalog>")
	}
	if u.Fragment != "" || u.RawFragment != "" {
		return Config{}, errors.New("databricks: dsn must not have a #fragment")
	}
	cfg := Config{Host: u.Hostname(), Port: 443, HTTPPath: u.Path}
	if !hostRe.MatchString(cfg.Host) {
		return Config{}, errors.New("databricks: dsn host is missing or not a host name")
	}
	if p := u.Port(); p != "" {
		n, err := strconv.Atoi(p)
		if err != nil || n < 1 || n > 65535 {
			return Config{}, errors.New("databricks: dsn port is not a port number")
		}
		cfg.Port = n
	}
	if !httpPathRe.MatchString(cfg.HTTPPath) || strings.Contains(cfg.HTTPPath, "..") {
		return Config{}, errors.New("databricks: dsn has no valid http path (want /sql/1.0/warehouses/<id>)")
	}

	q, err := url.ParseQuery(u.RawQuery)
	if err != nil {
		return Config{}, errors.New("databricks: dsn query string does not parse")
	}
	var m2mAuth bool
	for k, v := range q {
		if len(v) != 1 {
			return Config{}, fmt.Errorf("databricks: dsn parameter %s is repeated", safeKey(k))
		}
		val := v[0]
		switch k {
		case "catalog":
			if !identRe.MatchString(val) {
				return Config{}, errors.New("databricks: dsn catalog must match ^[a-zA-Z_][a-zA-Z0-9_]*$")
			}
			cfg.Catalog = val
		case "schema":
			if !identRe.MatchString(val) {
				return Config{}, errors.New("databricks: dsn schema must match ^[a-zA-Z_][a-zA-Z0-9_]*$")
			}
			cfg.Schema = val
		case "authType":
			switch strings.ToLower(val) {
			case "pat":
			case "oauthm2m":
				m2mAuth = true
			case "oauthu2m":
				return Config{}, errors.New("databricks: dsn authType=OAuthU2M is not supported: it opens a browser; use OAuth M2M (clientId+clientSecret) or a personal access token")
			default:
				return Config{}, errors.New("databricks: dsn authType must be Pat or OAuthM2M")
			}
		case "accessToken":
			cfg.Token = val
		case "clientId", "clientID":
			if cfg.ClientID != "" {
				return Config{}, errors.New("databricks: dsn parameter clientId is repeated")
			}
			cfg.ClientID = val
		case "clientSecret":
			if cfg.ClientSecret != "" {
				return Config{}, errors.New("databricks: dsn parameter clientSecret is repeated")
			}
			cfg.ClientSecret = val
		default:
			return Config{}, fmt.Errorf("databricks: dsn parameter %s is not allowed (allowed: catalog, schema, authType=Pat|OAuthM2M, accessToken, clientId, clientSecret)", safeKey(k))
		}
	}
	if cfg.Catalog == "" {
		return Config{}, errors.New("databricks: dsn has no catalog parameter; add ?catalog=<catalog>")
	}

	if u.User != nil {
		if u.User.Username() != "token" {
			return Config{}, errors.New("databricks: dsn user must be \"token\" (token:<pat>@host); user name/password login is not supported")
		}
		if cfg.Token != "" {
			return Config{}, errors.New("databricks: dsn has a token twice (token: and accessToken)")
		}
		cfg.Token, _ = u.User.Password()
		if cfg.Token == "" {
			return Config{}, errors.New("databricks: dsn token is empty")
		}
	}

	// An OAuth M2M DSN is one with client credentials (explicit authType or
	// inferred from clientId/clientSecret, as the driver infers it).
	if isM2M := m2mAuth || cfg.ClientID != "" || cfg.ClientSecret != ""; isM2M {
		if cfg.Token != "" {
			return Config{}, errors.New("databricks: dsn mixes a personal access token with OAuth M2M; use one credential")
		}
		if cfg.ClientID == "" || cfg.ClientSecret == "" {
			return Config{}, errors.New("databricks: dsn OAuth M2M needs both clientId and clientSecret")
		}
		return cfg, nil
	}

	if cfg.Token == "" {
		return Config{}, errors.New("databricks: dsn has no credential; want token:<pat>@host, or clientId+clientSecret for OAuth M2M")
	}
	return cfg, nil
}

func hasPrefixFold(s, prefix string) bool {
	return len(s) >= len(prefix) && strings.EqualFold(s[:len(prefix)], prefix)
}

// safeKey prints a parameter name only when it looks like one; anything
// else (a token pasted as a key) is not echoed.
func safeKey(k string) string {
	if identRe.MatchString(k) && len(k) <= 32 {
		return k
	}
	return "(unprintable)"
}

const redacted = "***"

// minSecretLen is the shortest secret replaced wherever it appears; a
// shorter one would blank unrelated words.
const minSecretLen = 4

// redactedError carries a scrubbed message but keeps the original error
// reachable for errors.Is and errors.As.
type redactedError struct {
	msg string
	err error
}

func (e *redactedError) Error() string { return e.msg }
func (e *redactedError) Unwrap() error { return e.err }

// Redact returns err with every secret of dsn removed from its message: the
// whole DSN, the token and any OAuth client secret (ParseDSN refuses one, but
// the error about it must not echo it), raw and URL-escaped. The
// result still unwraps to err. A nil err gives nil. internal/wiring uses it
// on every error from New, like snowflake.Redact.
func Redact(err error, dsn string) error {
	if err == nil {
		return nil
	}
	return &redactedError{msg: RedactString(err.Error(), dsn), err: err}
}

// RedactString removes the secrets found in dsn from s. It works on a DSN
// ParseDSN refuses too, so an error about a bad DSN is still scrubbed.
func RedactString(s, dsn string) string {
	if dsn == "" {
		return s
	}
	secrets := dsnSecrets(dsn)
	sort.Slice(secrets, func(i, j int) bool { return len(secrets[i]) > len(secrets[j]) })
	for _, sec := range secrets {
		s = strings.ReplaceAll(s, sec, redacted)
	}
	return s
}

// dsnSecrets lists the substrings of dsn that must never be shown.
func dsnSecrets(dsn string) []string {
	out := []string{dsn}
	add := func(v string) {
		for _, x := range []string{v, url.QueryEscape(v), url.PathEscape(v)} {
			if len(x) >= minSecretLen {
				out = append(out, x)
			}
		}
		if un, err := url.QueryUnescape(v); err == nil && len(un) >= minSecretLen {
			out = append(out, un)
		}
	}
	// The user info is everything before the last '@' ahead of the path.
	rest := dsn
	if i := strings.Index(rest, "://"); i >= 0 {
		rest = rest[i+3:]
	}
	hostPart := rest
	if i := strings.IndexAny(hostPart, "/?"); i >= 0 {
		hostPart = hostPart[:i]
	}
	if at := strings.LastIndex(hostPart, "@"); at >= 0 {
		ui := hostPart[:at]
		if _, pw, ok := strings.Cut(ui, ":"); ok {
			add(pw)
		} else {
			add(ui)
		}
	}
	if i := strings.Index(dsn, "?"); i >= 0 {
		for _, kv := range strings.Split(dsn[i+1:], "&") {
			k, v, ok := strings.Cut(kv, "=")
			if !ok {
				continue
			}
			switch strings.ToLower(k) {
			case "accesstoken", "clientsecret", "token", "password":
				add(v)
			}
		}
	}
	if cfg, err := ParseDSN(dsn); err == nil {
		add(cfg.Token)
		add(cfg.ClientSecret)
	}
	return out
}

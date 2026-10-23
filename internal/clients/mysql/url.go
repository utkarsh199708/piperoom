package mysql

import (
	"errors"
	"fmt"
	"net"
	"net/url"
	"strconv"
	"strings"
	"time"

	gomysql "github.com/go-sql-driver/mysql"

	"piperoom/internal/clients/netsec"
)

// ErrBadURL is returned (wrapped) for a source URL the client refuses. The
// error names the part that is wrong, never the URL itself: it holds the
// password.
var ErrBadURL = errors.New("mysql: invalid source URL")

// Accepted URL parameters (design §6.1). Everything else is refused: the
// driver would turn an unknown parameter into a SET statement, and its own
// parameters include allowAllFiles (LOAD DATA LOCAL INFILE reads this host's
// files), multiStatements and allowCleartextPasswords.
const (
	paramTLS           = "tls"
	paramTLSCA         = "tls_ca"
	paramTLSServerName = "tls_server_name"
)

// Timeouts the client sets itself, never from the URL.
const (
	dialTimeout = 5 * time.Second
	ioTimeout   = 10 * time.Second
)

// readParams are the session variables of every read-pool connection: the
// session cannot write, and the server stops any SELECT after 10 s on its
// own, as well as the client's context deadline. The driver sends them as one
// SET on each new connection; the values are constants, never input.
var readParams = map[string]string{
	"transaction_read_only": "1",
	"max_execution_time":    "10000",
}

// sourceURL is a parsed mysql:// URL. String hides the password.
type sourceURL struct {
	user, password string
	host           string // without brackets
	port           int
	db             string
	tls            bool
	tlsCA          string
	tlsServerName  string
}

// parseURL reads mysql://user:pass@host[:port]/[db]?tls=...&tls_ca=...&tls_server_name=...
// It does not use the driver's DSN parser: no text of the URL reaches it.
func parseURL(raw string) (sourceURL, error) {
	if strings.Contains(raw, "unix(") {
		return sourceURL{}, fmt.Errorf("%w: unix sockets are not supported", ErrBadURL)
	}
	u, err := url.Parse(raw)
	if err != nil {
		// url.Error repeats the URL, password included: never wrap it.
		return sourceURL{}, fmt.Errorf("%w: not a URL", ErrBadURL)
	}
	switch u.Scheme {
	case "mysql":
	case "":
		return sourceURL{}, fmt.Errorf("%w: want a mysql:// URL, got no scheme (driver DSN syntax is not accepted)", ErrBadURL)
	default:
		// mariadb://, mysqlx://, postgres:// ... The scheme is not secret.
		return sourceURL{}, fmt.Errorf("%w: want a mysql:// URL, got %s://", ErrBadURL, cutName(u.Scheme))
	}
	if u.Opaque != "" {
		return sourceURL{}, fmt.Errorf("%w: want mysql://host, not mysql:host", ErrBadURL)
	}
	if u.Fragment != "" || strings.Contains(raw, "#") {
		return sourceURL{}, fmt.Errorf("%w: a fragment (#...) is not allowed", ErrBadURL)
	}
	out := sourceURL{port: 3306}
	if u.User == nil || u.User.Username() == "" {
		return sourceURL{}, fmt.Errorf("%w: no user name", ErrBadURL)
	}
	out.user = u.User.Username()
	out.password, _ = u.User.Password()
	if len(out.user) > 32 {
		return sourceURL{}, fmt.Errorf("%w: user name longer than 32 characters", ErrBadURL)
	}
	out.host = u.Hostname()
	if out.host == "" {
		return sourceURL{}, fmt.Errorf("%w: no host", ErrBadURL)
	}
	if p := u.Port(); p != "" {
		n, err := strconv.Atoi(p)
		if err != nil || n < 1 || n > 65535 {
			return sourceURL{}, fmt.Errorf("%w: port must be 1 to 65535", ErrBadURL)
		}
		out.port = n
	} else if strings.HasSuffix(u.Host, ":") {
		return sourceURL{}, fmt.Errorf("%w: empty port", ErrBadURL)
	}
	switch path := strings.TrimPrefix(u.EscapedPath(), "/"); {
	case path == "":
	case !identRe.MatchString(path):
		return sourceURL{}, fmt.Errorf("%w: database name must match %s (at most 64 characters)", ErrBadURL, identRe)
	default:
		out.db = path
	}

	q, err := url.ParseQuery(u.RawQuery)
	if err != nil {
		return sourceURL{}, fmt.Errorf("%w: query string is not valid", ErrBadURL)
	}
	loop := loopback(out.host)
	out.tls = !loop
	for name, vals := range q {
		if len(vals) != 1 {
			return sourceURL{}, fmt.Errorf("%w: parameter %q given %d times", ErrBadURL, cutName(name), len(vals))
		}
		v := vals[0]
		switch name {
		case paramTLS:
			switch v {
			case "true":
				out.tls = true
			case "false":
				if !loop {
					return sourceURL{}, fmt.Errorf("%w: tls=false is allowed only for a loopback host (the password would cross the network in clear)", ErrBadURL)
				}
				out.tls = false
			default:
				return sourceURL{}, fmt.Errorf("%w: tls must be true or false", ErrBadURL)
			}
		case paramTLSCA:
			if v == "" {
				return sourceURL{}, fmt.Errorf("%w: tls_ca is empty", ErrBadURL)
			}
			out.tlsCA = v
		case paramTLSServerName:
			if v == "" || strings.ContainsAny(v, " /\\") {
				return sourceURL{}, fmt.Errorf("%w: tls_server_name must be a host name", ErrBadURL)
			}
			out.tlsServerName = v
		default:
			return sourceURL{}, fmt.Errorf("%w: parameter %q is not allowed (only %s, %s and %s)", ErrBadURL, cutName(name), paramTLS, paramTLSCA, paramTLSServerName)
		}
	}
	if !out.tls && (out.tlsCA != "" || out.tlsServerName != "") {
		return sourceURL{}, fmt.Errorf("%w: tls_ca and tls_server_name need TLS on", ErrBadURL)
	}
	return out, nil
}

// addr is host:port for the dialer.
func (s sourceURL) addr() string { return net.JoinHostPort(s.host, strconv.Itoa(s.port)) }

// target names the server for a provenance receipt: never the user or
// password.
func (s sourceURL) target() string { return "mysql " + s.db + "@" + s.addr() }

// String hides the password so a sourceURL can never leak through a log.
func (s sourceURL) String() string { return "mysql://" + s.user + ":***@" + s.addr() + "/" + s.db }

// config builds the driver configuration. Every safety switch is set here,
// whatever the driver's defaults: no LOAD DATA LOCAL INFILE, one statement
// per call, server-side prepared statements, no clear-text or old password
// plugins, no fall back to a plain connection when TLS is asked for.
// program is the connection attribute program_name (piperoom-reader or
// piperoom-writer), the MySQL twin of Postgres' application_name.
func (s sourceURL) config(program string, params map[string]string, logger gomysql.Logger) (*gomysql.Config, error) {
	cfg := gomysql.NewConfig()
	cfg.User = s.user
	cfg.Passwd = s.password
	cfg.Net = "tcp"
	cfg.Addr = s.addr()
	cfg.DBName = s.db
	cfg.AllowAllFiles = false
	cfg.AllowCleartextPasswords = false
	cfg.AllowFallbackToPlaintext = false
	cfg.AllowOldPasswords = false
	cfg.MultiStatements = false
	cfg.InterpolateParams = false
	cfg.ParseTime = true
	cfg.Loc = time.UTC
	cfg.Timeout = dialTimeout
	cfg.ReadTimeout = ioTimeout
	cfg.WriteTimeout = ioTimeout
	cfg.ConnectionAttributes = "program_name:" + program
	cfg.Logger = logger
	cfg.Params = make(map[string]string, len(params))
	for k, v := range params {
		cfg.Params[k] = v
	}
	if s.tls {
		// netsec always verifies the server certificate (TLS 1.2+); there is
		// no skip-verify. A *tls.Config on the Config, not a name registered
		// globally with the driver.
		tc, err := netsec.TLS{CAFile: s.tlsCA, ServerName: s.tlsServerName}.Config()
		if err != nil {
			return nil, fmt.Errorf("mysql: tls: %w", err)
		}
		if tc.ServerName == "" {
			tc.ServerName = s.host
		}
		cfg.TLS = tc
	}
	return cfg, nil
}

// loopback reports whether host is localhost or a loopback IP (the netsec
// rule: only there may the connection be unencrypted).
func loopback(host string) bool {
	if strings.EqualFold(host, "localhost") {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}

// cutName keeps a refused parameter or scheme name short in an error.
func cutName(s string) string {
	if len(s) > 40 {
		return s[:40] + "..."
	}
	return s
}

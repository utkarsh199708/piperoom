package pgconv

import (
	"net/url"
	"regexp"
	"sort"
	"strings"

	"github.com/jackc/pgx/v5/pgconn"
)

// redactedMark stands in for a removed secret.
const redactedMark = "[redacted]"

// minSecretLen is the shortest password replaced wherever it appears. A
// shorter one (say "pw") would blank unrelated words, so it is replaced only
// where DSN syntax puts it: ":pw@" and "password=pw".
const minSecretLen = 4

// backticked matches what pgx quotes in backticks: the whole connection
// string in a ParseConfigError (pgx masks the password there only on a best
// effort basis, and the rest of the DSN stays) and "user=... database=..." in
// a ConnectError.
var backticked = regexp.MustCompile("`[^`]*`")

// kvPassword finds password and sslpassword in keyword/value DSNs:
// password=secret or password='se cret'.
var kvPassword = regexp.MustCompile(`(?i)(?:^|\s)(?:ssl)?password\s*=\s*('(?:[^'\\]|\\.)*'|[^\s']\S*)`)

// redactedError carries a scrubbed message and keeps the original error
// reachable for errors.Is and errors.As.
type redactedError struct {
	msg string
	err error
}

func (e *redactedError) Error() string { return e.msg }
func (e *redactedError) Unwrap() error { return e.err }

// RedactDSN returns err with dsn removed from its message: every backtick
// quoted part (where pgx prints the connection string), the DSN itself, and
// the password and sslpassword in it, in raw and URL-escaped forms. The result
// still unwraps to err, so errors.Is and errors.As keep working. A nil err
// gives nil. Use it on every error of a constructor that took a DSN.
func RedactDSN(err error, dsn string) error {
	if err == nil {
		return nil
	}
	return &redactedError{msg: RedactDSNString(err.Error(), dsn), err: err}
}

// RedactDSNString removes dsn and its credentials from s (see RedactDSN).
func RedactDSNString(s, dsn string) string {
	s = backticked.ReplaceAllLiteralString(s, "`"+redactedMark+"`")
	if strings.TrimSpace(dsn) == "" {
		return s
	}
	always, contextual := dsnSecrets(dsn)
	// Longest first, so the whole DSN goes before the pieces inside it.
	sort.Slice(always, func(i, j int) bool { return len(always[i]) > len(always[j]) })
	for _, sec := range always {
		s = strings.ReplaceAll(s, sec, redactedMark)
	}
	for _, sec := range contextual {
		s = strings.ReplaceAll(s, ":"+sec+"@", ":"+redactedMark+"@")
		s = strings.ReplaceAll(s, "password="+sec, "password="+redactedMark)
	}
	return s
}

// dsnSecrets lists the substrings of dsn that must never be shown. "always"
// are replaced anywhere; "contextual" are short passwords, replaced only in
// DSN syntax. The DSN may be malformed (that is often why it is in an error),
// so the password is looked for by hand as well as through the parsers.
func dsnSecrets(dsn string) (always, contextual []string) {
	always = []string{dsn}
	if t := strings.TrimSpace(dsn); t != dsn {
		always = append(always, t)
	}
	add := func(v string) {
		if v == "" {
			return
		}
		variants := []string{v}
		if u, err := url.QueryUnescape(v); err == nil && u != v {
			variants = append(variants, u)
		}
		if u, err := url.PathUnescape(v); err == nil && u != v {
			variants = append(variants, u)
		}
		if e := url.QueryEscape(v); e != v {
			variants = append(variants, e)
		}
		if e := url.PathEscape(v); e != v {
			variants = append(variants, e)
		}
		for _, x := range variants {
			if len(x) >= minSecretLen {
				always = append(always, x)
			} else {
				contextual = append(contextual, x)
			}
		}
	}

	t := strings.TrimSpace(dsn)
	if rest, ok := cutScheme(t); ok {
		// user:password@host...: the userinfo ends at the last '@' before
		// the query string.
		auth := rest
		if q := strings.Index(auth, "?"); q >= 0 {
			auth = auth[:q]
		}
		if at := strings.LastIndex(auth, "@"); at >= 0 {
			if _, pw, ok := strings.Cut(auth[:at], ":"); ok {
				add(pw)
			}
		}
		if _, query, ok := strings.Cut(rest, "?"); ok {
			for _, kv := range strings.Split(query, "&") {
				k, v, ok := strings.Cut(kv, "=")
				if ok && (strings.EqualFold(k, "password") || strings.EqualFold(k, "sslpassword")) {
					add(v)
				}
			}
		}
		if u, err := url.Parse(t); err == nil {
			if pw, ok := u.User.Password(); ok {
				add(pw)
			}
			add(u.Query().Get("password"))
			add(u.Query().Get("sslpassword"))
		}
	} else {
		for _, m := range kvPassword.FindAllStringSubmatch(t, -1) {
			v := m[1]
			if strings.HasPrefix(v, "'") {
				add(v)
				v = strings.TrimSuffix(strings.TrimPrefix(v, "'"), "'")
				add(v)
				v = strings.NewReplacer(`\'`, `'`, `\\`, `\`).Replace(v)
			}
			add(v)
		}
	}
	// Whatever pgx itself reads as the password (it also reads PGPASSWORD
	// and the passfile).
	if cfg, err := pgconn.ParseConfig(t); err == nil && cfg != nil {
		add(cfg.Password)
	}
	return always, contextual
}

// cutScheme returns what follows "postgres://" or "postgresql://".
func cutScheme(dsn string) (string, bool) {
	for _, p := range []string{"postgres://", "postgresql://"} {
		if len(dsn) >= len(p) && strings.EqualFold(dsn[:len(p)], p) {
			return dsn[len(p):], true
		}
	}
	return "", false
}

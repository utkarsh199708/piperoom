package snowflake

import (
	"net/url"
	"sort"
	"strings"

	"github.com/snowflakedb/gosnowflake"
)

const redacted = "***"

// minSecretLen is the shortest password or token replaced wherever it
// appears. A shorter one (say "WH") would blank unrelated words, so it is only
// replaced in the exact ":secret@" context of the DSN.
const minSecretLen = 4

// secretParams are DSN query parameters (lower-cased) whose values are
// credentials, as gosnowflake reads them in dsn.go.
var secretParams = map[string]bool{
	"password": true, "token": true, "privatekey": true, "private_key": true,
	"passcode": true, "oauthclientsecret": true, "proxypassword": true,
}

// redactedError carries a scrubbed message but keeps the original error
// reachable for errors.Is and errors.As.
type redactedError struct {
	msg string
	err error
}

func (e *redactedError) Error() string { return e.msg }
func (e *redactedError) Unwrap() error { return e.err }

// Redact returns err with every secret from dsn removed from its message:
// the whole DSN, the password in the user:password part, and the values of
// credential query parameters (password, token, proxyPassword, ...). The
// result still unwraps to err, so errors.Is and errors.As keep working. A nil
// err gives nil.
func Redact(err error, dsn string) error {
	if err == nil {
		return nil
	}
	return &redactedError{msg: RedactString(err.Error(), dsn), err: err}
}

// RedactString removes the secrets found in dsn from s.
func RedactString(s, dsn string) string {
	if dsn == "" {
		return s
	}
	always, contextual := dsnSecrets(dsn)
	// Longest first, so the whole DSN goes before the pieces inside it.
	sort.Slice(always, func(i, j int) bool { return len(always[i]) > len(always[j]) })
	for _, sec := range always {
		s = strings.ReplaceAll(s, sec, redacted)
	}
	for _, sec := range contextual {
		s = strings.ReplaceAll(s, ":"+sec+"@", ":"+redacted+"@")
	}
	return s
}

// dsnSecrets lists the substrings of dsn that must never be shown. "always"
// are replaced anywhere; "contextual" are short secrets, replaced only as
// ":secret@".
func dsnSecrets(dsn string) (always, contextual []string) {
	always = []string{dsn}
	add := func(v string) {
		if v == "" {
			return
		}
		variants := []string{v}
		if u, err := url.QueryUnescape(v); err == nil && u != v {
			variants = append(variants, u)
		}
		if e := url.QueryEscape(v); e != v {
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

	// Split the DSN the way the driver does (dsn.go ParseDSN): the user and
	// password are before the last '@' that precedes the last '/', and the
	// parameters start at the first '?' after that last '/'. An unencoded
	// '?' or '/' inside a password therefore stays in the password part.
	userPart, queryPart := dsn, ""
	if slash := strings.LastIndex(dsn, "/"); slash >= 0 {
		userPart = dsn[:slash]
		if q := strings.Index(dsn[slash:], "?"); q >= 0 {
			queryPart = dsn[slash+q+1:]
		}
	} else if q := strings.LastIndex(dsn, "?"); q >= 0 {
		// No database part: the driver scans back from the end.
		userPart, queryPart = dsn[:q], dsn[q+1:]
	}
	if at := strings.LastIndex(userPart, "@"); at >= 0 {
		if colon := strings.Index(userPart[:at], ":"); colon >= 0 {
			add(userPart[colon+1 : at])
		}
	}
	for _, kv := range strings.Split(queryPart, "&") {
		k, v, ok := strings.Cut(kv, "=")
		if ok && secretParams[strings.ToLower(k)] {
			add(v)
		}
	}

	// Also take whatever the driver itself parses as a credential.
	if cfg, err := gosnowflake.ParseDSN(dsn); err == nil && cfg != nil {
		for _, v := range []string{cfg.Password, cfg.Token, cfg.Passcode, cfg.ProxyPassword, cfg.OauthClientSecret} {
			add(v)
		}
	}
	return always, contextual
}

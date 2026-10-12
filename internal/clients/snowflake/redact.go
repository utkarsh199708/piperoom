package snowflake

import (
	"net/url"
	"sort"
	"strings"
)

const redacted = "***"

// secretParams are DSN query parameters (lower-cased) whose values are
// credentials.
var secretParams = map[string]bool{
	"password": true, "token": true, "privatekey": true, "private_key": true,
	"passcode": true, "oauthclientsecret": true,
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
// credential query parameters (password, token, ...). The result still
// unwraps to err, so errors.Is and errors.As keep working. A nil err gives nil.
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
	secrets := dsnSecrets(dsn)
	// Longest first, so the whole DSN goes before the pieces inside it.
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
		if v == "" {
			return
		}
		out = append(out, v)
		if u, err := url.QueryUnescape(v); err == nil && u != v {
			out = append(out, u)
		}
		if e := url.QueryEscape(v); e != v {
			out = append(out, e)
		}
	}

	rest, query, _ := strings.Cut(dsn, "?")
	// "user:password@account/db/schema": the password sits between the first
	// ':' and the last '@' of the part before the query. A password may itself
	// hold '@' or '/', so cut at the last '@'.
	if at := strings.LastIndex(rest, "@"); at >= 0 {
		if colon := strings.Index(rest[:at], ":"); colon >= 0 {
			add(rest[colon+1 : at])
		}
	}
	for _, kv := range strings.Split(query, "&") {
		k, v, ok := strings.Cut(kv, "=")
		if ok && secretParams[strings.ToLower(k)] {
			add(v)
		}
	}
	return out
}

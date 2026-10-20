package mongodb

import (
	"net/url"
	"strings"
)

// splitURI splits a MongoDB connection string into scheme (including ://),
// authority and the rest (database and options). It is a plain string split:
// the authority is between "://" and the next '/' or '?'.
func splitURI(uri string) (prefix, auth, suffix string) {
	rest := uri
	if i := strings.Index(rest, "://"); i >= 0 {
		prefix = rest[:i+3]
		rest = rest[i+3:]
	}
	if i := strings.IndexAny(rest, "/?"); i >= 0 {
		auth = rest[:i]
		suffix = rest[i:]
	} else {
		auth = rest
	}
	return prefix, auth, suffix
}

// passwordOf returns the password part of a connection string, or "" when the
// URI carries no credentials. It is the only part of the URI that is a secret.
func passwordOf(uri string) string {
	_, auth, _ := splitURI(uri)
	at := strings.LastIndex(auth, "@")
	if at < 0 {
		return ""
	}
	userinfo := auth[:at]
	colon := strings.Index(userinfo, ":")
	if colon < 0 {
		return ""
	}
	return userinfo[colon+1:]
}

// redactURI returns uri with the password (only) replaced, so it can be logged
// without leaking credentials.
func redactURI(uri string) string {
	prefix, auth, suffix := splitURI(uri)
	if at := strings.LastIndex(auth, "@"); at >= 0 {
		userinfo := auth[:at]
		if colon := strings.Index(userinfo, ":"); colon >= 0 {
			userinfo = userinfo[:colon+1] + "***"
		}
		auth = userinfo + auth[at:]
	}
	return prefix + auth + suffix
}

// redactedError wraps err so that Error() hides any occurrence of the secrets
// (the connection string's password), while Unwrap preserves the original
// error for errors.Is / errors.As.
type redactedError struct {
	err     error
	secrets []string
}

func (e *redactedError) Error() string {
	msg := e.err.Error()
	for _, s := range e.secrets {
		if s != "" {
			msg = strings.ReplaceAll(msg, s, "***")
		}
	}
	return msg
}

func (e *redactedError) Unwrap() error { return e.err }

// redactErr wraps err so a driver error that echoes the connection string
// cannot leak the password. It redacts both the raw password and its
// percent-decoded form, so a message carrying either is covered. It returns
// err unchanged when err is nil.
func redactErr(err error, uri string) error {
	if err == nil {
		return nil
	}
	var secrets []string
	if pw := passwordOf(uri); pw != "" {
		secrets = append(secrets, pw)
		if dec, derr := url.PathUnescape(pw); derr == nil && dec != pw {
			secrets = append(secrets, dec)
		}
	}
	return &redactedError{err: err, secrets: secrets}
}

var _ error = (*redactedError)(nil)

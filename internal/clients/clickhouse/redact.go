package clickhouse

import (
	"net/url"
	"sort"
	"strings"
)

const redacted = "***"

type redactedError struct {
	message string
	cause   error
}

func (e *redactedError) Error() string { return e.message }
func (e *redactedError) Unwrap() error { return e.cause }

// Redact removes the ClickHouse DSN and its password or token from an error,
// while preserving errors.Is and errors.As behavior.
func Redact(err error, dsn string) error {
	if err == nil {
		return nil
	}
	return &redactedError{message: RedactString(err.Error(), dsn), cause: err}
}

// RedactString scrubs the DSN, its user-info password, and credential query
// parameters from text returned by the driver.
func RedactString(message, dsn string) string {
	if dsn == "" {
		return message
	}
	secrets := []string{dsn}
	addRawCredentials(&secrets, dsn)
	if parsed, err := url.Parse(dsn); err == nil {
		if parsed.User != nil {
			if password, ok := parsed.User.Password(); ok {
				addSecret(&secrets, password)
			}
		}
		for key, values := range parsed.Query() {
			switch strings.ToLower(key) {
			case "password", "token", "access_token":
				for _, value := range values {
					addSecret(&secrets, value)
				}
			}
		}
	}
	sort.Slice(secrets, func(i, j int) bool { return len(secrets[i]) > len(secrets[j]) })
	for _, secret := range secrets {
		if secret != "" {
			message = strings.ReplaceAll(message, secret, redacted)
		}
	}
	return message
}

func addRawCredentials(secrets *[]string, dsn string) {
	if scheme := strings.Index(dsn, "://"); scheme >= 0 {
		authority := dsn[scheme+3:]
		if end := strings.IndexAny(authority, "/?#"); end >= 0 {
			authority = authority[:end]
		}
		if at := strings.LastIndex(authority, "@"); at >= 0 {
			if colon := strings.Index(authority[:at], ":"); colon >= 0 {
				addSecret(secrets, authority[colon+1:at])
			}
		}
	}

	if queryStart := strings.Index(dsn, "?"); queryStart >= 0 {
		query := dsn[queryStart+1:]
		if fragment := strings.IndexByte(query, '#'); fragment >= 0 {
			query = query[:fragment]
		}
		for _, pair := range strings.Split(query, "&") {
			key, value, ok := strings.Cut(pair, "=")
			if !ok {
				continue
			}
			switch strings.ToLower(key) {
			case "password", "token", "access_token":
				addSecret(secrets, value)
			}
		}
	}
}

func addSecret(secrets *[]string, value string) {
	if value == "" {
		return
	}
	variants := []string{value, url.QueryEscape(value), url.PathEscape(value)}
	if decoded, err := url.QueryUnescape(value); err == nil && decoded != value {
		variants = append(variants, decoded)
	}
	for _, variant := range variants {
		if variant != "" {
			*secrets = append(*secrets, variant)
		}
	}
}

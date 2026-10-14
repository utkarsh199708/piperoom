package secrets

import (
	"fmt"
	"net/url"
	"regexp"
	"strings"
	"unicode"

	"piperoom/internal/integrations/spec"
)

// Ref names one secret value in one provider. It is stored; the value never is.
type Ref struct {
	// Provider is the provider id: a secret_manager row's name
	// (spec.ProviderNameField), spec.ProviderFiles, or an id declared in
	// PIPEROOM_SECRET_PROVIDERS.
	Provider string
	// Path is the provider-native name: an ARN, a Vault path, a file path
	// without its leading slash, ... It is percent-decoded.
	Path string
	// Key optionally picks one field of a JSON or key/value secret.
	Key string
	// Version optionally pins a version; "" means latest.
	Version string
}

// maxRefLen bounds a reference so a pasted blob cannot become a path.
const maxRefLen = 2048

var providerIDRe = regexp.MustCompile(spec.ProviderNamePattern)

// ValidProviderID reports whether id is a well-formed provider id.
func ValidProviderID(id string) bool { return providerIDRe.MatchString(id) }

// IsRef reports whether s looks like a secret:// reference (it may still be
// invalid; ParseRef decides). Environment variables use it to tell a
// reference from a literal value.
func IsRef(s string) bool { return strings.HasPrefix(s, spec.RefPrefix) }

// ParseRef parses secret://<provider>/<path>[#<key>][?version=<v>].
//
// Rules (docs/design/secret-providers.md §2): the provider id matches
// spec.ProviderNamePattern; path, key and version are percent-decoded exactly
// once; the path must not be empty, contain empty, "." or ".." segments, or
// control characters. A raw '#' or '?' always starts the key or the query, so
// a path or key that contains one must percent-encode it. The query may come
// before or after the key; "version" is its only parameter.
//
// Errors wrap ErrInvalidRef and never quote the input (a mistyped reference
// can be a pasted password).
func ParseRef(s string) (Ref, error) {
	if len(s) > maxRefLen {
		return Ref{}, fmt.Errorf("%w: longer than %d bytes", ErrInvalidRef, maxRefLen)
	}
	if !strings.HasPrefix(s, spec.RefPrefix) {
		return Ref{}, fmt.Errorf("%w: must start with %s", ErrInvalidRef, spec.RefPrefix)
	}
	rest := s[len(spec.RefPrefix):]
	slash := strings.IndexByte(rest, '/')
	if slash < 0 {
		return Ref{}, fmt.Errorf("%w: missing path after the provider id", ErrInvalidRef)
	}
	var r Ref
	r.Provider = rest[:slash]
	if !ValidProviderID(r.Provider) {
		return Ref{}, fmt.Errorf("%w: provider id must match %s", ErrInvalidRef, spec.ProviderNamePattern)
	}
	rest = rest[slash+1:]

	rawPath, rawKey, rawQuery, err := splitRest(rest)
	if err != nil {
		return Ref{}, err
	}
	if r.Path, err = decodeOnce(rawPath, "path"); err != nil {
		return Ref{}, err
	}
	if err := checkPath(r.Path); err != nil {
		return Ref{}, err
	}
	if rawKey != nil {
		if r.Key, err = decodeOnce(*rawKey, "key"); err != nil {
			return Ref{}, err
		}
		if r.Key == "" {
			return Ref{}, fmt.Errorf("%w: empty key after '#'", ErrInvalidRef)
		}
		if hasControl(r.Key) {
			return Ref{}, fmt.Errorf("%w: key contains control characters", ErrInvalidRef)
		}
	}
	if rawQuery != nil {
		name, val, ok := strings.Cut(*rawQuery, "=")
		if !ok || name != "version" || strings.ContainsAny(val, "&;") {
			return Ref{}, fmt.Errorf("%w: the only query parameter is version=<v>", ErrInvalidRef)
		}
		if r.Version, err = decodeOnce(val, "version"); err != nil {
			return Ref{}, err
		}
		if r.Version == "" || hasControl(r.Version) || strings.ContainsAny(r.Version, "/") {
			return Ref{}, fmt.Errorf("%w: bad version", ErrInvalidRef)
		}
	}
	return r, nil
}

// splitRest splits "<path>[#key][?query]" (either order of key and query).
func splitRest(rest string) (path string, key, query *string, err error) {
	end := strings.IndexAny(rest, "#?")
	if end < 0 {
		return rest, nil, nil, nil
	}
	path = rest[:end]
	tail := rest[end:]
	for tail != "" {
		mark := tail[0]
		body := tail[1:]
		next := strings.IndexAny(body, "#?")
		part := body
		if next >= 0 {
			part, tail = body[:next], body[next:]
		} else {
			tail = ""
		}
		switch mark {
		case '#':
			if key != nil {
				return "", nil, nil, fmt.Errorf("%w: more than one '#'", ErrInvalidRef)
			}
			p := part
			key = &p
		case '?':
			if query != nil {
				return "", nil, nil, fmt.Errorf("%w: more than one '?'", ErrInvalidRef)
			}
			p := part
			query = &p
		}
	}
	return path, key, query, nil
}

func decodeOnce(s, what string) (string, error) {
	out, err := url.PathUnescape(s)
	if err != nil {
		return "", fmt.Errorf("%w: bad percent-encoding in %s", ErrInvalidRef, what)
	}
	return out, nil
}

// checkPath runs on the decoded path, so %2e%2e is caught like "..".
func checkPath(p string) error {
	if p == "" {
		return fmt.Errorf("%w: empty path", ErrInvalidRef)
	}
	if hasControl(p) {
		return fmt.Errorf("%w: path contains control characters", ErrInvalidRef)
	}
	if strings.Contains(p, "\\") {
		return fmt.Errorf("%w: path contains a backslash", ErrInvalidRef)
	}
	for _, seg := range strings.Split(p, "/") {
		switch seg {
		case "":
			return fmt.Errorf("%w: path has an empty segment", ErrInvalidRef)
		case ".", "..":
			return fmt.Errorf("%w: path has a '.' or '..' segment", ErrInvalidRef)
		}
	}
	return nil
}

func hasControl(s string) bool {
	for _, c := range s {
		if unicode.IsControl(c) {
			return true
		}
	}
	return false
}

// String formats r as a secret:// reference that ParseRef reads back to r.
// A reference is not a secret, so it may be logged and shown.
func (r Ref) String() string {
	var b strings.Builder
	b.WriteString(spec.RefPrefix)
	b.WriteString(r.Provider)
	b.WriteByte('/')
	for i, seg := range strings.Split(r.Path, "/") {
		if i > 0 {
			b.WriteByte('/')
		}
		b.WriteString(url.PathEscape(seg))
	}
	if r.Key != "" {
		b.WriteByte('#')
		b.WriteString(url.PathEscape(r.Key))
	}
	if r.Version != "" {
		b.WriteString("?version=")
		b.WriteString(url.PathEscape(r.Version))
	}
	return b.String()
}

// secretID is r without the key: the unit a provider fetches and the cache
// stores, so username and password of one Vault lease come from one read.
func (r Ref) secretID() Ref {
	r.Key = ""
	return r
}

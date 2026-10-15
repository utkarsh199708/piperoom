package netsec

import (
	"errors"
	"fmt"
	"strings"
)

// ErrBadPath is returned for a request path with an empty, "." or ".."
// segment, before any request is sent.
var ErrBadPath = errors.New("netsec: empty or dot path segment")

// CheckPath refuses a request path (as built by the Connect and Schema
// Registry clients, query string allowed) holding an empty, "." or ".."
// segment. url.PathEscape leaves dots alone, so a connector or subject named
// ".." would otherwise address another resource on a server that removes dot
// segments: PUT /config/. is the registry's global compatibility.
func CheckPath(path string) error {
	p, _, _ := strings.Cut(path, "?")
	for _, seg := range strings.Split(strings.TrimPrefix(p, "/"), "/") {
		if seg == "" || seg == "." || seg == ".." {
			return fmt.Errorf("%w: %q", ErrBadPath, p)
		}
	}
	return nil
}

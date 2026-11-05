package mongodb

import (
	"errors"
	"fmt"
	"regexp"
	"strings"
)

// Errors of the driver and the writer. Compare with errors.Is.
var (
	// ErrIdentifier: a name is not a bare identifier (rule 6).
	ErrIdentifier = errors.New("setup/sources/mongodb: invalid identifier")
	// ErrNoCollections: an operation needs the pipeline's collections and was
	// given none.
	ErrNoCollections = errors.New("setup/sources/mongodb: no source collections")
	// ErrNoWriter: the setup writer is absent (write mode off, or no runner).
	ErrNoWriter = errors.New("setup/sources/mongodb: no source writer (write mode on and a paired runner are needed)")
	// ErrParams: a parameter is outside the range this package accepts.
	ErrParams = errors.New("setup/sources/mongodb: bad parameter")
)

// maxNameBytes is the MongoDB client's cap on a database, collection or user
// name (internal/clients/mongodb maxNameLen): MongoDB caps database names at
// 64 bytes, and the same cap keeps the others short.
const maxNameBytes = 64

var (
	identRe       = regexp.MustCompile(`^[a-zA-Z_][a-zA-Z0-9_]*$`)
	secretNameRe  = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_.-]{0,127}$`)
	topicPrefixRe = regexp.MustCompile(`^[A-Za-z0-9_][A-Za-z0-9_.-]{0,63}$`)
	canaryNonceRe = regexp.MustCompile(`^[0-9a-f]{32}$`)
)

// systemDatabases are never captured or written by a setup (the MongoDB
// client refuses them too; the oplog read on local is granted by the writer's
// role, never requested as a collection).
var systemDatabases = map[string]bool{"admin": true, "local": true, "config": true}

// reservedUsers are users a setup must never create or name as the capture
// user: the usual administrator names. Matched case-insensitively.
var reservedUsers = map[string]bool{"root": true, "admin": true, "__system": true}

// clipName cuts a name for an error message (every accepted name is ASCII).
func clipName(s string) string {
	if len(s) <= 80 {
		return s
	}
	return s[:80] + "…"
}

// checkIdent checks one name against rule 6 and the 64-byte cap.
func checkIdent(kind, name string) error {
	if len(name) == 0 || len(name) > maxNameBytes || !identRe.MatchString(name) {
		return fmt.Errorf("%w: %s %q must match %s (at most %d bytes)", ErrIdentifier, kind, clipName(name), identRe, maxNameBytes)
	}
	return nil
}

// checkDatabase checks a captured database: an identifier, not a system one.
func checkDatabase(db string) error {
	if err := checkIdent("database", db); err != nil {
		return err
	}
	if systemDatabases[strings.ToLower(db)] {
		return fmt.Errorf("%w: database %q is a system database", ErrIdentifier, db)
	}
	return nil
}

// splitCollection splits and checks one "db.collection" namespace.
func splitCollection(ns string) (db, coll string, err error) {
	db, coll, ok := strings.Cut(ns, ".")
	if !ok || strings.Contains(coll, ".") {
		return "", "", fmt.Errorf("%w: %q is not \"db.collection\"", ErrIdentifier, clipName(ns))
	}
	if err := checkDatabase(db); err != nil {
		return "", "", err
	}
	if err := checkIdent("collection", coll); err != nil {
		return "", "", err
	}
	return db, coll, nil
}

// checkCollections checks a non-empty list of namespaces.
func checkCollections(collections []string) error {
	if len(collections) == 0 {
		return ErrNoCollections
	}
	for _, c := range collections {
		if _, _, err := splitCollection(c); err != nil {
			return err
		}
	}
	return nil
}

// checkUser checks the capture user: an identifier that is not an
// administrator's name.
func checkUser(user string) error {
	if err := checkIdent("user", user); err != nil {
		return err
	}
	if reservedUsers[strings.ToLower(user)] {
		return fmt.Errorf("%w: user %q is reserved; the capture user is its own user", ErrIdentifier, user)
	}
	return nil
}

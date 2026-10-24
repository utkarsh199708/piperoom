package oracle

import (
	"errors"
	"fmt"
	"regexp"
	"strings"
)

var (
	ErrIdentifier = errors.New("setup/sources/oracle: invalid identifier")
	ErrNoTables   = errors.New("setup/sources/oracle: no source tables")
	ErrNoWriter   = errors.New("setup/sources/oracle: no source writer")
	ErrParams     = errors.New("setup/sources/oracle: bad parameter")
)

var (
	oracleIdentRe = regexp.MustCompile("^[A-Za-z_][A-Za-z0-9_$#]{0,127}$")
	secretNameRe  = regexp.MustCompile("^[A-Za-z_][A-Za-z0-9_.-]{0,127}$")
	topicPrefixRe = regexp.MustCompile("^[A-Za-z0-9_][A-Za-z0-9_.-]{0,63}$")
	canaryNonceRe = regexp.MustCompile("^[0-9a-f]{32}$")
	hostnameRe    = regexp.MustCompile("^[A-Za-z0-9]([A-Za-z0-9.-]{0,251}[A-Za-z0-9])?$")
)

func clipName(s string) string {
	if len(s) <= 80 {
		return s
	}
	return s[:80] + "…"
}

func checkIdent(name string) error {
	if !oracleIdentRe.MatchString(name) {
		return fmt.Errorf("%w: %q is not a bare Oracle identifier", ErrIdentifier, clipName(name))
	}
	return nil
}

func tableParts(table string) (owner, name string, err error) {
	parts := strings.Split(table, ".")
	if len(parts) != 2 {
		return "", "", fmt.Errorf("%w: table %q must be owner.table", ErrIdentifier, clipName(table))
	}
	if err := checkIdent(parts[0]); err != nil {
		return "", "", fmt.Errorf("table owner: %w", err)
	}
	if err := checkIdent(parts[1]); err != nil {
		return "", "", fmt.Errorf("table name: %w", err)
	}
	return parts[0], parts[1], nil
}

func checkTables(tables []string) error {
	if len(tables) == 0 {
		return ErrNoTables
	}
	for _, table := range tables {
		if _, _, err := tableParts(table); err != nil {
			return err
		}
	}
	return nil
}

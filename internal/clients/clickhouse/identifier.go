package clickhouse

import (
	"fmt"
	"regexp"
	"strings"
)

var identRe = regexp.MustCompile("^[A-Za-z_][A-Za-z0-9_]*$")

func validateIdent(name string) (string, error) {
	if !identRe.MatchString(name) {
		return "", fmt.Errorf("clickhouse: invalid identifier %q", name)
	}
	return name, nil
}

func quoteIdent(name string) (string, error) {
	valid, err := validateIdent(name)
	if err != nil {
		return "", err
	}
	tick := string(rune(96))
	return tick + strings.ReplaceAll(valid, tick, tick+tick) + tick, nil
}

func quoteTable(table string) (string, error) {
	parts, err := tableParts(table)
	if err != nil {
		return "", err
	}
	tick := string(rune(96))
	quoted := make([]string, len(parts))
	for i, part := range parts {
		quoted[i] = tick + strings.ReplaceAll(part, tick, tick+tick) + tick
	}
	return strings.Join(quoted, "."), nil
}

func tableParts(table string) ([]string, error) {
	parts := strings.Split(table, ".")
	if len(parts) > 2 {
		return nil, fmt.Errorf("clickhouse: table must be table or db.table, got %q", table)
	}
	for _, part := range parts {
		if _, err := validateIdent(part); err != nil {
			return nil, fmt.Errorf("clickhouse: invalid table %q: %w", table, err)
		}
	}
	return parts, nil
}

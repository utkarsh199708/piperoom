package clickhouse

import (
	"fmt"
	"regexp"
	"strings"
	"testing"
	"unicode/utf8"
)

// insertTargetPatternSeeds are exercised both as a plain unit test and as the
// seed corpus for FuzzInsertTargetPattern. They include well-formed names, an
// empty database, regex metacharacters, backticks and a very long name so the
// pattern builder is exercised over the shapes LoadErrors can actually receive.
var insertTargetPatternSeeds = []struct{ database, table string }{
	{"analytics", "orders"},
	{"", "orders"},
	{"a.b", "orders"},
	{"a|b", "orders"},
	{"(x", "orders"},
	{". *", "orders"},
	{"\\", "orders"},
	{"`analytics`", "orders"},
	{"analytics", "`orders`"},
	{"analytics", strings.Repeat("a", 10*1024)},
}

// checkInsertTargetPattern holds the invariants every input must satisfy. It is
// shared by the seed unit test and the fuzz target so the properties stay in one
// place.
func checkInsertTargetPattern(t *testing.T, database, table string) {
	t.Helper()
	pattern := insertTargetPattern(database, table)

	// insertTargetPattern is only reachable through LoadErrors, which runs the
	// names through validateIdent (^[A-Za-z_][A-Za-z0-9_]*$) first. That rejects
	// any name with invalid UTF-8, which regexp.QuoteMeta would otherwise copy
	// verbatim into an uncompilable pattern. Mirror that precondition: for
	// binary input the only invariant is that building the pattern must not
	// panic (the fuzzer reports panics itself).
	if !utf8.ValidString(database) || !utf8.ValidString(table) {
		return
	}

	re, err := regexp.Compile(pattern)
	if err != nil {
		t.Fatalf("insertTargetPattern(%q, %q) = %q: regexp.Compile: %v", database, table, pattern, err)
	}

	_, dbErr := validateIdent(database)
	_, tbErr := validateIdent(table)

	if dbErr == nil && tbErr == nil {
		// Qualified target: both names are valid identifiers, so the pattern
		// must pin the exact db.table target and reject near-misses.
		other := database + "X" // valid identifier, guaranteed != database
		target := database + "." + table
		mustMatch := []string{
			"INSERT INTO " + target,
			"INSERT INTO " + target + " (id) VALUES",
			"INSERT INTO " + target + "(id) VALUES",
			"INSERT INTO `" + database + "`.`" + table + "`",
			"INSERT INTO TABLE " + target,
			"insert into " + target,
			"  INSERT INTO " + target,
			"\tINSERT INTO " + target,
		}
		for _, q := range mustMatch {
			if !re.MatchString(q) {
				t.Errorf("pattern %q did not match %q", pattern, q)
			}
		}
		mustNotMatch := []string{
			"INSERT INTO " + target + "_v2 (id) VALUES",
			"INSERT INTO " + other + "." + table + " (id) VALUES",
			"INSERT INTO " + database + "X" + table + " (id) VALUES",
			"SELECT * FROM " + target,
		}
		for _, q := range mustNotMatch {
			if re.MatchString(q) {
				t.Errorf("pattern %q matched %q", pattern, q)
			}
		}
	} else if database == "" && tbErr == nil {
		// Bare target: an empty database matches the table name alone.
		mustMatch := []string{
			"INSERT INTO " + table,
			"INSERT INTO " + table + " (id) VALUES",
			"INSERT INTO " + table + "(id) VALUES",
			"INSERT INTO `" + table + "`",
			"INSERT INTO TABLE " + table,
			"insert into " + table,
			"  INSERT INTO " + table,
		}
		for _, q := range mustMatch {
			if !re.MatchString(q) {
				t.Errorf("pattern %q did not match %q", pattern, q)
			}
		}
		mustNotMatch := []string{
			"INSERT INTO " + table + "_v2 (id) VALUES",
			"INSERT INTO other." + table + " (id) VALUES",
			"SELECT * FROM " + table,
		}
		for _, q := range mustNotMatch {
			if re.MatchString(q) {
				t.Errorf("pattern %q matched %q", pattern, q)
			}
		}
	}
}

// TestInsertTargetPatternSeeds runs the seed corpus as ordinary unit cases so
// the seeds are exercised even without -fuzz.
func TestInsertTargetPatternSeeds(t *testing.T) {
	for i, seed := range insertTargetPatternSeeds {
		t.Run(fmt.Sprintf("seed-%d", i), func(t *testing.T) {
			checkInsertTargetPattern(t, seed.database, seed.table)
		})
	}
}

func FuzzInsertTargetPattern(f *testing.F) {
	for _, seed := range insertTargetPatternSeeds {
		f.Add(seed.database, seed.table)
	}
	f.Fuzz(func(t *testing.T, database, table string) {
		checkInsertTargetPattern(t, database, table)
	})
}

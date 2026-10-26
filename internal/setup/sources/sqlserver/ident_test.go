package sqlserver

import (
	"errors"
	"slices"
	"strings"
	"testing"

	sqlclient "piperoom/internal/clients/sqlserver"
)

func TestCheckIdent(t *testing.T) {
	tests := []struct {
		name string
		in   string
		ok   bool
	}{
		{"plain", "orders", true},
		{"underscore first", "_cdc", true},
		{"digits after the first", "t2_2024", true},
		{"sysname limit", strings.Repeat("a", maxIdentChars), true},
		{"empty", "", false},
		{"digit first", "1orders", false},
		{"over sysname", strings.Repeat("a", maxIdentChars+1), false},
		{"closing bracket", "orders]", false},
		{"bracket injection", "x]; DROP TABLE y--", false},
		{"space", "or ders", false},
		{"dot", "dbo.orders", false},
		{"quote", "o\x27rders", false},
		{"non-ascii", "ord\u00e9rs", false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := checkIdent(tc.in)
			if tc.ok && err != nil {
				t.Fatalf("checkIdent(%q) = %v, want nil", tc.in, err)
			}
			if !tc.ok && !errors.Is(err, ErrIdentifier) {
				t.Fatalf("checkIdent(%q) = %v, want ErrIdentifier", tc.in, err)
			}
		})
	}
}

// TestCheckLoginReservedNames: every reserved principal is refused whatever
// its case, and a name that merely contains one is not.
func TestCheckLoginReservedNames(t *testing.T) {
	refused := []string{
		"sa", "SA", "Sa", "public", "PUBLIC", "dbo", "DBO", "guest", "GUEST", "Guest",
		"sys", "SYS", "INFORMATION_SCHEMA", "information_schema", "cdc", "CDC",
		"sysadmin", "SYSADMIN", "serveradmin", "securityadmin", "processadmin", "setupadmin",
		"bulkadmin", "diskadmin", "dbcreator",
		"db_owner", "DB_OWNER", "db_datareader", "db_denydatareader", "db_anything",
	}
	for _, l := range refused {
		if err := checkLogin(l); !errors.Is(err, ErrIdentifier) {
			t.Errorf("checkLogin(%q) = %v, want ErrIdentifier", l, err)
		}
	}
	accepted := []string{"piperoom_cdc", "cdc_reader", "guest_app", "sa_reader", "dbowner", "sysops", "debezium", "dbz"}
	for _, l := range accepted {
		if err := checkLogin(l); err != nil {
			t.Errorf("checkLogin(%q) = %v, want nil", l, err)
		}
	}
	// A reserved name that is not even an identifier is ErrIdentifier too.
	if err := checkLogin("sa]"); !errors.Is(err, ErrIdentifier) {
		t.Errorf("checkLogin(\"sa]\") = %v, want ErrIdentifier", err)
	}
}

func TestCheckDatabase(t *testing.T) {
	for _, db := range []string{"master", "MASTER", "model", "msdb", "MSDB", "tempdb", "TempDB"} {
		if err := checkDatabase(db); !errors.Is(err, ErrParams) {
			t.Errorf("checkDatabase(%q) = %v, want ErrParams", db, err)
		}
	}
	for _, db := range []string{"orders", "master_data", "msdb2", "Orders_2024"} {
		if err := checkDatabase(db); err != nil {
			t.Errorf("checkDatabase(%q) = %v, want nil", db, err)
		}
	}
	for _, db := range []string{"", "or ders", "orders]", strings.Repeat("d", maxIdentChars+1)} {
		if err := checkDatabase(db); !errors.Is(err, ErrIdentifier) {
			t.Errorf("checkDatabase(%q) = %v, want ErrIdentifier", clipName(db), err)
		}
	}
}

// TestCheckCaptureInstanceMatchesTheWriter: the names the driver checks are
// exactly the ones SetupWriter.CreatePublication creates (one derivation,
// sqlclient.CaptureInstances), duplicates and the 100-character limit
// included, so a name the driver accepts is never refused by the writer and a
// name it refuses is never one the writer would have created differently.
func TestCheckCaptureInstanceMatchesTheWriter(t *testing.T) {
	long := strings.Repeat("c", 95)
	atLimit := strings.Repeat("c", sqlclient.MaxCaptureInstanceChars-len("_dbo_orders"))
	tests := []struct {
		name   string
		inst   string
		tables []string
		want   []string // the writer's instances when ok
		ok     bool
	}{
		{"one table keeps the name", long, []string{"dbo.orders"}, []string{long}, true},
		{"a duplicate counts once", long, []string{"dbo.orders", "dbo.orders"}, []string{long}, true},
		{"two tables get the suffix", long, []string{"dbo.orders", "dbo.customers"}, nil, false},
		{"exactly the limit", atLimit, []string{"dbo.orders", "dbo.events"}, []string{atLimit + "_dbo_orders", atLimit + "_dbo_events"}, true},
		{"one over the limit", atLimit + "c", []string{"dbo.orders", "dbo.events"}, nil, false},
		{"short name, many tables", DefaultCaptureInstance, []string{"dbo.orders", "sales.customers", DefaultCanaryTable},
			[]string{"piperoom_dbo_orders", "piperoom_sales_customers", "piperoom_dbo_piperoom_canary"}, true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := checkCaptureInstance(tc.inst, tc.tables)
			got, werr := sqlclient.CaptureInstances(tc.inst, tc.tables)
			if tc.ok {
				if err != nil || werr != nil {
					t.Fatalf("driver %v, writer %v; want both nil", err, werr)
				}
				if !slices.Equal(got, tc.want) {
					t.Fatalf("writer derives %v, want %v", got, tc.want)
				}
				return
			}
			if !errors.Is(err, ErrParams) || !errors.Is(err, sqlclient.ErrCaptureInstance) {
				t.Fatalf("driver err = %v, want ErrParams wrapping sqlclient.ErrCaptureInstance", err)
			}
			if !errors.Is(werr, sqlclient.ErrCaptureInstance) {
				t.Fatalf("writer err = %v, want ErrCaptureInstance", werr)
			}
		})
	}
	// The name itself is still a rule 6 identifier first.
	if err := checkCaptureInstance("x]; DROP", []string{"dbo.orders"}); !errors.Is(err, ErrIdentifier) {
		t.Fatalf("bad name: %v, want ErrIdentifier", err)
	}
}

// TestBracketQuotesOnlyCheckedNames: an ask's example statement brackets a
// name only after the rule 6 check, so a ] can never close the quote early.
func TestBracketQuotesOnlyCheckedNames(t *testing.T) {
	if got := bracket("orders"); got != "[orders]" {
		t.Fatalf("bracket = %q", got)
	}
	if got := bracketTable("dbo.orders"); got != "[dbo].[orders]" {
		t.Fatalf("bracketTable = %q", got)
	}
	if got := bracket("x]"); got != "x]" {
		t.Fatalf("bracket of an unchecked name = %q, want it unquoted", got)
	}
	if got := bracketTable("dbo.x]"); got != "dbo.x]" {
		t.Fatalf("bracketTable of an unchecked name = %q, want it unquoted", got)
	}
	if got := bracket(strings.Repeat("a", maxIdentChars+1)); got != strings.Repeat("a", 80)+"\u2026" {
		t.Fatalf("over-long name was not clipped: %q", got)
	}
}

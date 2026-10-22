package owners

import (
	"fmt"
	"strings"
	"testing"
)

func withPII(block string) string {
	return strings.Replace(validYAML, "    sink_ts_column: updated_at\n", "    sink_ts_column: updated_at\n"+block, 1)
}

// X7c TC2: include adds a column with its class (default other), exclude
// removes one; a table entry wins over a bare one; columns compare without
// case.
func TestPIIOverrides(t *testing.T) {
	o, err := Load(writeTemp(t, withPII(`    pii:
      include: [public.orders.notes, "analytics.orders.tax_ref:national_id", "nick:name"]
      exclude: [public.orders.emailed_at, phone_verified]
`)))
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	p, _ := o.Pipeline("orders")
	for _, tc := range []struct {
		table, col string
		want       PIIOverride
		class      string
	}{
		{"public.orders", "notes", PIIIncluded, PIIClassOther},
		{"public.orders", "NOTES", PIIIncluded, PIIClassOther},
		{"analytics.orders", "notes", PIINoOverride, ""},
		{"analytics.orders", "tax_ref", PIIIncluded, "national_id"},
		{"public.orders", "nick", PIIIncluded, "name"},
		{"analytics.orders", "nick", PIIIncluded, "name"},
		{"public.orders", "emailed_at", PIIExcluded, ""},
		{"analytics.orders", "emailed_at", PIINoOverride, ""},
		{"analytics.orders", "phone_verified", PIIExcluded, ""},
		{"public.orders", "id", PIINoOverride, ""},
	} {
		got, class := p.PIIOverrideFor(tc.table, tc.col)
		if got != tc.want || class != tc.class {
			t.Errorf("PIIOverrideFor(%s, %s) = %v, %q; want %v, %q", tc.table, tc.col, got, class, tc.want, tc.class)
		}
	}
	// No block: the classifier decides everywhere.
	o, err = Load(writeTemp(t, validYAML))
	if err != nil {
		t.Fatal(err)
	}
	p, _ = o.Pipeline("orders")
	if got, _ := p.PIIOverrideFor("public.orders", "email"); got != PIINoOverride {
		t.Fatalf("no pii block: %v", got)
	}
}

// E2: every bad entry stops Load, naming the pipeline and the entry.
func TestPIIRefused(t *testing.T) {
	many := make([]string, MaxPIIEntries+1)
	for i := range many {
		many[i] = fmt.Sprintf("c%d", i)
	}
	for name, tc := range map[string]struct{ block, want string }{
		"two-part":         {"      include: [orders.email]\n", `"orders.email"`},
		"other table":      {"      include: [public.payroll.email]\n", "not one of the pipeline's"},
		"unknown class":    {"      include: [\"email:secret\"]\n", "unknown class"},
		"class on exclude": {"      exclude: [\"email:email\"]\n", "takes no class"},
		"in both":          {"      include: [email]\n      exclude: [EMAIL]\n", "also in pii.include"},
		"bare and table":   {"      include: [public.orders.email]\n      exclude: [email]\n", "both include and exclude"},
		"table and bare":   {"      include: [email]\n      exclude: [analytics.orders.email]\n", "both include and exclude"},
		"twice":            {"      include: [email, \"email:email\"]\n", "listed twice"},
		"empty":            {"      exclude: [\"\"]\n", "pii.exclude entry"},
		"null entry":       {"      include:\n        -\n", "pii.include: has an empty (null) entry"},
		"null block":       {"", "pii: is empty"},
		"bad column":       {"      include: [\"public.orders.a,b\"]\n", "catalog name"},
		"no column":        {"      include: [public.orders.]\n", "catalog name"},
		"control":          {"      include: [\"a\\u202eb\"]\n", "catalog name"},
		"too many":         {"      exclude: [" + strings.Join(many, ", ") + "]\n", "at most 500"},
		"typo key":         {"      includes: [email]\n", "includes"},
	} {
		t.Run(name, func(t *testing.T) {
			_, err := Load(writeTemp(t, withPII("    pii:\n"+tc.block)))
			if err == nil {
				t.Fatal("loaded")
			}
			if !strings.Contains(err.Error(), tc.want) || name != "typo key" && !strings.Contains(err.Error(), `pipeline "orders"`) {
				t.Fatalf("error %q: want %q and the pipeline", err, tc.want)
			}
		})
	}
	// The caps themselves load.
	if _, err := Load(writeTemp(t, withPII("    pii:\n      exclude: ["+strings.Join(many[:MaxPIIEntries], ", ")+"]\n"))); err != nil {
		t.Fatalf("%d entries: %v", MaxPIIEntries, err)
	}
}

package oracle

import "testing"

// Real V$VERSION.BANNER values read from the dev profile's containers
// (gvenzl/oracle-free:23.9 and :23.26.3, 2026-10-23). From 23.26 the product
// is called "Oracle AI Database 26ai" while the release stays 23.x.
const (
	banner23ai = "Oracle Database 23ai Free Release 23.0.0.0.0 - Develop, Learn, and Run for Free"
	banner26ai = "Oracle AI Database 26ai Free Release 23.26.3.0.0 - Develop, Learn, and Run for Free"
)

func TestEditionFromBanner(t *testing.T) {
	cases := []struct {
		banner string
		want   string
	}{
		{banner23ai, "Free"},
		{banner26ai, "Free"},
		{"Oracle Database 23c Free, Release 23.0.0.0.0 - Developer-Release", "Free"},
		{"Oracle Database 23ai Free Edition Release 23.0.0.0.0 - Production", "Free"},
		{"Oracle AI Database 26ai Enterprise Edition Release 23.26.0.0.0 - Production", "Enterprise Edition"},
		{"Oracle Database 19c Enterprise Edition Release 19.0.0.0.0 - Production", "Enterprise Edition"},
		{"Oracle Database 12c Standard Edition 2 Release 12.2.0.1.0 - 64bit Production", "Standard Edition 2"},
		{"Oracle Database 21c Express Edition Release 21.0.0.0.0 - Production", "Express Edition"},
		{"Oracle Database 11g Express Edition Release 11.2.0.2.0 - Production", "Express Edition"},
		{"Oracle Database 12c Standard Edition One Release 12.1.0.2.0 - 64bit Production", "Standard Edition One"},
		{"Oracle Database 12c Standard Edition Release 12.1.0.2.0 - 64bit Production", "Standard Edition"},
		{"Oracle Database 19c Personal Edition Release 19.0.0.0.0 - Production", "Personal Edition"},
		{"Oracle Database 21c Release 21.0.0.0.0 - Production", ""},
		// "Free" in the tagline after Release is not the edition.
		{"Oracle Database 21c Release 21.0.0.0.0 - Run for Free", ""},
		{"Oracle Database 21c Freestyle Release 21.0.0.0.0 - Production", ""},
		{"NLSRTL Version 23.0.0.0.0 - Production", ""},
		{"", ""},
	}
	for _, tc := range cases {
		if got := editionFromBanner(tc.banner); got != tc.want {
			t.Errorf("editionFromBanner(%q) = %q, want %q", tc.banner, got, tc.want)
		}
	}
}

func TestIsDatabaseBanner(t *testing.T) {
	cases := []struct {
		banner string
		want   bool
	}{
		{banner23ai, true},
		{banner26ai, true},
		{"Oracle Database 19c Enterprise Edition Release 19.0.0.0.0 - Production", true},
		{"Oracle Database 11g Express Edition Release 11.2.0.2.0 - 64bit Production", true},
		{"Oracle Database 23c Free, Release 23.0.0.0.0 - Developer-Release", true},
		// A future rename of the same shape.
		{"Oracle Autonomous AI Database 27ai Enterprise Edition Release 23.27.0.0.0 - Production", true},
		// Component rows of V$VERSION on 12c to 19c.
		{"PL/SQL Release 12.2.0.1.0 - Production", false},
		{"CORE\t12.2.0.1.0\tProduction", false},
		{"TNS for Linux: Version 12.2.0.1.0 - Production", false},
		{"NLSRTL Version 12.2.0.1.0 - Production", false},
		// Unrelated rows that start with Oracle.
		{"Oracle Label Security Release 19.0.0.0.0", false},
		{"Oracle Real Application Clusters Database option", false},
		{"Oracle", false},
		{"Database 23ai", false},
		{"", false},
	}
	for _, tc := range cases {
		if got := isDatabaseBanner(tc.banner); got != tc.want {
			t.Errorf("isDatabaseBanner(%q) = %v, want %v", tc.banner, got, tc.want)
		}
	}
}

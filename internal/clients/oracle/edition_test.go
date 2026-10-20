package oracle

import "testing"

func TestEditionFromBanner(t *testing.T) {
	cases := []struct {
		banner string
		want   string
	}{
		{"Oracle Database 23ai Free Edition Release 23.0.0.0.0 - Production", "Free Edition"},
		{"Oracle Database 19c Enterprise Edition Release 19.0.0.0.0 - Production", "Enterprise Edition"},
		{"Oracle Database 12c Standard Edition 2 Release 12.2.0.1.0 - 64bit Production", "Standard Edition 2"},
		{"Oracle Database 11g Express Edition Release 11.2.0.2.0 - Production", "Express Edition"},
		{"Oracle Database 12c Standard Edition One Release 12.1.0.2.0 - 64bit Production", "Standard Edition One"},
		{"Oracle Database 12c Standard Edition Release 12.1.0.2.0 - 64bit Production", "Standard Edition"},
		{"Oracle Database 19c Personal Edition Release 19.0.0.0.0 - Production", "Personal Edition"},
		{"Oracle Database 21c Release 21.0.0.0.0 - Production", ""},
		{"NLSRTL Version 23.0.0.0.0 - Production", ""},
	}
	for _, tc := range cases {
		if got := editionFromBanner(tc.banner); got != tc.want {
			t.Errorf("editionFromBanner(%q) = %q, want %q", tc.banner, got, tc.want)
		}
	}
}

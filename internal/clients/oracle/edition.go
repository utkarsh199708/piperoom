package oracle

import "strings"

// editionKeywords maps the edition names that appear in an Oracle "Oracle
// Database …" banner to the short edition string the driver reports. Longer
// names are checked before their "Standard Edition" / "Edition" suffix so
// "Standard Edition 2" is not reported as "Standard Edition".
var editionKeywords = []string{
	"Enterprise Edition",
	"Standard Edition 2",
	"Standard Edition One",
	"Standard Edition",
	"Personal Edition",
	"Express Edition",
	"Free Edition",
}

// editionFromBanner extracts the edition keyword from a version banner, or ""
// when none is known. The banner is untrusted text from the dictionary: it is
// only matched against this fixed list, never executed or echoed verbatim.
func editionFromBanner(banner string) string {
	for _, kw := range editionKeywords {
		if strings.Contains(banner, kw) {
			return kw
		}
	}
	return ""
}

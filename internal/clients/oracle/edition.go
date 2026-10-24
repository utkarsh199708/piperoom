package oracle

import "strings"

// editionKeywords maps the edition names that appear in an Oracle product
// banner to the short edition string the driver reports. Longer names are
// checked before their "Standard Edition" / "Edition" suffix so
// "Standard Edition 2" is not reported as "Standard Edition".
var editionKeywords = []string{
	"Enterprise Edition",
	"Standard Edition 2",
	"Standard Edition One",
	"Standard Edition",
	"Personal Edition",
	"Express Edition",
}

// editionFree is what editionFromBanner reports for Oracle Database Free. Its
// banners do not say "Free Edition": 23ai reads "Oracle Database 23ai Free
// Release …", 23.26+ reads "Oracle AI Database 26ai Free Release …", and the
// 23c developer release read "Oracle Database 23c Free, Release …". So Free is
// matched as a whole word of the product name, and a "Free Edition" spelling
// folds into the same value.
const editionFree = "Free"

// editionFromBanner extracts the edition keyword from a version banner, or ""
// when none is known. The banner is untrusted text from the dictionary: it is
// only matched against this fixed list, never executed or echoed verbatim.
// Only the product name before " Release" is searched, so a tagline such as
// "- Develop, Learn, and Run for Free" cannot decide the edition.
func editionFromBanner(banner string) string {
	product := productName(banner)
	for _, kw := range editionKeywords {
		if strings.Contains(product, kw) {
			return kw
		}
	}
	for _, w := range strings.Fields(product) {
		if strings.TrimRight(w, ",") == editionFree {
			return editionFree
		}
	}
	return ""
}

// productName returns the part of a banner before its " Release" word (the
// whole banner when there is none).
func productName(banner string) string {
	if i := strings.Index(banner, " Release"); i >= 0 {
		return banner[:i]
	}
	return banner
}

// isDatabaseBanner reports whether a V$VERSION row is the database product
// banner rather than a component row (PL/SQL, CORE, TNS, NLSRTL). Oracle has
// renamed the product more than once ("Oracle Database 23ai", then "Oracle AI
// Database 26ai" from 23.26), so the rule is the shape, not a fixed prefix:
// the first word is "Oracle" and "Database" is one of the next three words of
// the product name.
func isDatabaseBanner(banner string) bool {
	words := strings.Fields(productName(banner))
	if len(words) < 2 || words[0] != "Oracle" {
		return false
	}
	for i := 1; i < len(words) && i <= 3; i++ {
		if words[i] == "Database" {
			return true
		}
	}
	return false
}

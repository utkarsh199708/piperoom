package runbooks

import (
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"strings"
)

// version is a release number as the components print it: Debezium
// "2.5.1.Final" or "3.0.0.CR1", Kafka "3.6.1" (or "7.5.0-ccs" from Confluent),
// Postgres "15.4", pgjdbc "42.7.3".
type version struct {
	nums []int // numeric parts; missing parts compare as 0
	rank int   // pre-release rank: Alpha < Beta < CR < release
	pre  int   // number after the pre-release word (Alpha2 -> 2)
}

const (
	rankAlpha = iota
	rankBeta
	rankCR
	rankRelease
)

const maxVersionBytes = 64

// suffixRe is a release word after the numbers: Final, CR1, Alpha2, ccs.
var suffixRe = regexp.MustCompile(`^[A-Za-z]+[0-9]*$`)

// parseVersion accepts 1 to 4 dot-separated numbers and an optional suffix
// after "." or "-". Alpha, Beta, CR (and RC, M for milestone) mark a
// pre-release; anything else (Final, GA, ccs, ce) is a release.
func parseVersion(s string) (version, error) {
	s = strings.TrimSpace(strings.TrimPrefix(strings.TrimSpace(s), "v"))
	if s == "" || len(s) > maxVersionBytes {
		return version{}, fmt.Errorf("version %q: empty or longer than %d bytes", s, maxVersionBytes)
	}
	end := 0
	for end < len(s) && (s[end] == '.' || (s[end] >= '0' && s[end] <= '9')) {
		end++
	}
	numPart, suffix := strings.TrimSuffix(s[:end], "."), s[end:]
	if numPart == "" {
		return version{}, fmt.Errorf("version %q: no number", s)
	}
	v := version{rank: rankRelease}
	for _, p := range strings.Split(numPart, ".") {
		n, err := strconv.Atoi(p)
		if err != nil || n < 0 {
			return version{}, fmt.Errorf("version %q: bad number %q", s, p)
		}
		v.nums = append(v.nums, n)
	}
	if len(v.nums) > 4 {
		return version{}, fmt.Errorf("version %q: more than 4 numbers", s)
	}
	dot := strings.HasSuffix(s[:end], ".") // the separator before a .Word suffix
	if dot && suffix == "" {
		return version{}, fmt.Errorf("version %q: ends with a dot", s)
	}
	if suffix != "" {
		if !dot {
			if suffix[0] != '-' {
				return version{}, fmt.Errorf("version %q: suffix must follow . or -", s)
			}
			suffix = suffix[1:]
		}
		if !suffixRe.MatchString(suffix) {
			return version{}, fmt.Errorf("version %q: suffix must be letters, then digits (Final, CR1, ccs)", s)
		}
	}
	word := strings.ToLower(strings.TrimRight(suffix, "0123456789"))
	switch word {
	case "alpha", "m":
		v.rank = rankAlpha
	case "beta":
		v.rank = rankBeta
	case "cr", "rc":
		v.rank = rankCR
	}
	if v.rank != rankRelease {
		if d := suffix[len(word):]; d != "" {
			n, err := strconv.Atoi(d)
			if err != nil {
				return version{}, fmt.Errorf("version %q: bad pre-release number %q", s, d)
			}
			v.pre = n
		}
	}
	return v, nil
}

func (a version) compare(b version) int {
	for i := range max(len(a.nums), len(b.nums)) {
		x, y := part(a.nums, i), part(b.nums, i)
		if x != y {
			return cmpInt(x, y)
		}
	}
	if a.rank != b.rank {
		return cmpInt(a.rank, b.rank)
	}
	return cmpInt(a.pre, b.pre)
}

func part(n []int, i int) int {
	if i < len(n) {
		return n[i]
	}
	return 0
}

func cmpInt(a, b int) int {
	switch {
	case a < b:
		return -1
	case a > b:
		return 1
	}
	return 0
}

// bound is one constraint of a range, such as ">=2.0.0.Final".
type bound struct {
	op string
	v  version
}

// versionRange is a conjunction of bounds; no bounds means every version.
type versionRange struct{ bounds []bound }

var rangeOps = []string{">=", "<=", ">", "<", "="}

// parseRange parses "*" or space-separated bounds (">=2.0.0.Final <2.3.0").
// A range no version can satisfy is an error.
func parseRange(s string) (versionRange, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return versionRange{}, errors.New("range is empty (use * for every version)")
	}
	if s == "*" {
		return versionRange{}, nil
	}
	var r versionRange
	for _, f := range strings.Fields(s) {
		var b bound
		for _, op := range rangeOps {
			if rest, ok := strings.CutPrefix(f, op); ok {
				v, err := parseVersion(rest)
				if err != nil {
					return versionRange{}, fmt.Errorf("range %q: %w", s, err)
				}
				b = bound{op, v}
				break
			}
		}
		if b.op == "" {
			return versionRange{}, fmt.Errorf("range %q: %q must start with one of %s", s, f, strings.Join(rangeOps, " "))
		}
		r.bounds = append(r.bounds, b)
	}
	if !r.satisfiable() {
		return versionRange{}, fmt.Errorf("range %q holds no version", s)
	}
	return r, nil
}

func (r versionRange) holds(v version) bool {
	for _, b := range r.bounds {
		c := v.compare(b.v)
		ok := false
		switch b.op {
		case ">=":
			ok = c >= 0
		case ">":
			ok = c > 0
		case "<=":
			ok = c <= 0
		case "<":
			ok = c < 0
		case "=":
			ok = c == 0
		}
		if !ok {
			return false
		}
	}
	return true
}

// satisfiable reports whether some version holds. A candidate in the range,
// if any, is the lowest version (only upper bounds), one of the bound
// versions, or sits just above a lower bound, so checking those is enough.
func (r versionRange) satisfiable() bool {
	if len(r.bounds) == 0 || r.holds(version{nums: []int{0}, rank: rankAlpha}) {
		return true
	}
	for _, b := range r.bounds {
		if r.holds(b.v) {
			return true
		}
		// just above b.v: one more pre-release step (Alpha1 -> Alpha2, Final ->
		// a later 4th part) is enough to clear a strict lower bound.
		up := version{nums: append(append([]int{}, b.v.nums...), make([]int, 4-min(4, len(b.v.nums)))...), rank: b.v.rank, pre: b.v.pre}
		if up.rank == rankRelease {
			up.nums = append(up.nums, 1) // a 5th part only matters for comparison
		} else {
			up.pre++
		}
		if r.holds(up) {
			return true
		}
	}
	return false
}

package knowledge

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
)

// release is a component release as it is published: Debezium
// "3.2.0.Final" or "2.4.0.CR1", Kafka "3.8.0" or "0.10.2.0", Snowflake
// "4.2.2". Missing numeric parts compare as 0.
//
// The runbooks package (Q1b) has the same grammar but keeps its parser
// unexported; the PR asks C11 to export it so this copy can go.
type release struct {
	nums []int
	rank int // alpha < beta < cr < final
	pre  int // the number after the pre-release word (CR1 -> 1)
}

const (
	rankAlpha = iota
	rankBeta
	rankCR
	rankFinal
)

const maxReleaseBytes = 64

// parseRelease accepts 1 to 4 dot-separated numbers and an optional word
// after "." or "-": Alpha, Beta, CR, RC (pre-releases) or Final, GA
// (releases). Anything else is an error: an index entry with a release
// the parser cannot order would silently hold for no version.
func parseRelease(s string) (release, error) {
	s = strings.TrimSpace(s)
	if s == "" || len(s) > maxReleaseBytes {
		return release{}, fmt.Errorf("release %q: empty or longer than %d bytes", s, maxReleaseBytes)
	}
	end := 0
	for end < len(s) && (s[end] == '.' || (s[end] >= '0' && s[end] <= '9')) {
		end++
	}
	numPart, word := s[:end], s[end:]
	sepDot := strings.HasSuffix(numPart, ".")
	numPart = strings.TrimSuffix(numPart, ".")
	if numPart == "" {
		return release{}, fmt.Errorf("release %q: no number", s)
	}
	r := release{rank: rankFinal}
	for _, p := range strings.Split(numPart, ".") {
		n, err := strconv.Atoi(p)
		if err != nil || n < 0 {
			return release{}, fmt.Errorf("release %q: bad number %q", s, p)
		}
		r.nums = append(r.nums, n)
	}
	if len(r.nums) > 4 {
		return release{}, fmt.Errorf("release %q: more than 4 numbers", s)
	}
	if word == "" {
		if sepDot {
			return release{}, fmt.Errorf("release %q: ends with a dot", s)
		}
		return r, nil
	}
	if !sepDot {
		rest, ok := strings.CutPrefix(word, "-")
		if !ok {
			return release{}, fmt.Errorf("release %q: a suffix must follow . or -", s)
		}
		word = rest
	}
	name := strings.ToLower(strings.TrimRight(word, "0123456789"))
	digits := word[len(name):]
	switch name {
	case "alpha":
		r.rank = rankAlpha
	case "beta":
		r.rank = rankBeta
	case "cr", "rc":
		r.rank = rankCR
	case "final", "ga":
		if digits != "" {
			return release{}, fmt.Errorf("release %q: %s takes no number", s, word)
		}
		return r, nil
	default:
		return release{}, fmt.Errorf("release %q: suffix %q is not Alpha, Beta, CR, RC, Final or GA", s, word)
	}
	if digits != "" {
		n, err := strconv.Atoi(digits)
		if err != nil {
			return release{}, fmt.Errorf("release %q: bad pre-release number %q", s, digits)
		}
		r.pre = n
	}
	return r, nil
}

func (a release) compare(b release) int {
	for i := range max(len(a.nums), len(b.nums)) {
		if x, y := numAt(a.nums, i), numAt(b.nums, i); x != y {
			return cmpInt(x, y)
		}
	}
	if a.rank != b.rank {
		return cmpInt(a.rank, b.rank)
	}
	return cmpInt(a.pre, b.pre)
}

func numAt(n []int, i int) int {
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

// releaseRange is a conjunction of bounds; no bounds means every release.
type releaseRange struct {
	bounds []rangeBound
}

type rangeBound struct {
	op string
	r  release
}

var rangeOps = []string{">=", "<=", ">", "<", "="}

// parseRange parses "*" or space-separated bounds (">=2.2.0.Final
// <4.0.0"). A range no release can satisfy is an error, so an inverted
// range in the index fails start-up instead of silently matching nothing.
func parseRange(s string) (releaseRange, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return releaseRange{}, errors.New("range is empty (use * for every release)")
	}
	if s == "*" {
		return releaseRange{}, nil
	}
	var rr releaseRange
	for _, f := range strings.Fields(s) {
		var b rangeBound
		for _, op := range rangeOps {
			if rest, ok := strings.CutPrefix(f, op); ok {
				r, err := parseRelease(rest)
				if err != nil {
					return releaseRange{}, fmt.Errorf("range %q: %w", s, err)
				}
				b = rangeBound{op: op, r: r}
				break
			}
		}
		if b.op == "" {
			return releaseRange{}, fmt.Errorf("range %q: %q must start with one of %s", s, f, strings.Join(rangeOps, " "))
		}
		rr.bounds = append(rr.bounds, b)
	}
	if !rr.satisfiable() {
		return releaseRange{}, fmt.Errorf("range %q holds no release", s)
	}
	return rr, nil
}

func (rr releaseRange) holds(r release) bool {
	for _, b := range rr.bounds {
		c := r.compare(b.r)
		var ok bool
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

// satisfiable reports whether some release holds: the lowest release, a
// bound itself, or a release just above a bound is enough to try.
func (rr releaseRange) satisfiable() bool {
	if rr.holds(release{nums: []int{0}, rank: rankAlpha}) {
		return true
	}
	for _, b := range rr.bounds {
		if rr.holds(b.r) {
			return true
		}
		up := release{nums: append(append([]int{}, b.r.nums...), make([]int, 4-len(b.r.nums))...), rank: b.r.rank, pre: b.r.pre + 1}
		if b.r.rank == rankFinal {
			up.pre = 0
			up.nums = append(up.nums, 1) // a 5th part only matters for comparison
		}
		if rr.holds(up) {
			return true
		}
	}
	return false
}

// lowest returns the lower bound of the range, if it has one.
func (rr releaseRange) lowest() (release, bool) {
	for _, b := range rr.bounds {
		if b.op == ">=" || b.op == ">" || b.op == "=" {
			return b.r, true
		}
	}
	return release{}, false
}

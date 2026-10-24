// Package gtid is MySQL GTID set arithmetic in pure Go: parse a set as the
// server prints it (@@gtid_executed, @@gtid_purged, a Debezium offset's
// "gtids"), and union, contain and subtract sets the way the server's
// GTID_SUBSET and GTID_SUBTRACT do (docs/design/mysql-source.md §6.7).
//
// A set is a list of source ids, each with closed transaction-number
// intervals: "3e11fa47-71ca-11e1-9e33-c80aa9429562:1-5:7,
// 4f22...:1-3". MySQL 8.3 and later also allow tags, "uuid:tag:1-5"; a tag
// applies to the intervals after it, and "uuid:1-3:tag:1-2" holds two source
// ids, the untagged uuid and uuid:tag. UUIDs and tags compare case-
// insensitively and print in lower case, as the server prints them.
//
// The package does no I/O. A Set is immutable once built: every operation
// returns a new Set.
package gtid

import (
	"errors"
	"fmt"
	"math"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"unicode/utf8"
)

// ErrSyntax is returned (wrapped) for text that is not a GTID set.
var ErrSyntax = errors.New("gtid: invalid GTID set")

// MaxParseBytes bounds the text Parse accepts. A server's gtid_executed after
// many failovers holds a few hundred source ids; 1 MiB is far beyond that and
// keeps a hostile offset from costing unbounded work.
const MaxParseBytes = 1 << 20

// Interval is the closed range [Start, End] of transaction numbers,
// 1 <= Start <= End.
type Interval struct {
	Start, End int64
}

// Set is a GTID set. The zero value is the empty set.
type Set struct {
	// m maps a source id ("uuid" or "uuid:tag", lower case) to its sorted,
	// merged, non-adjacent intervals.
	m map[string][]Interval
}

var (
	uuidRe = regexp.MustCompile(`^[0-9a-fA-F]{8}-?[0-9a-fA-F]{4}-?[0-9a-fA-F]{4}-?[0-9a-fA-F]{4}-?[0-9a-fA-F]{12}$`)
	// tagRe is the server's tag rule: a letter or underscore, then up to 31
	// letters, digits or underscores.
	tagRe = regexp.MustCompile(`^[a-zA-Z_][a-zA-Z0-9_]{0,31}$`)
)

// Parse reads a GTID set as the server prints it. Whitespace (the server
// breaks gtid_executed after each comma) is ignored; "" is the empty set.
func Parse(s string) (Set, error) {
	if len(s) > MaxParseBytes {
		return Set{}, fmt.Errorf("%w: %d bytes, more than %d", ErrSyntax, len(s), MaxParseBytes)
	}
	s = strings.Map(func(r rune) rune {
		switch r {
		case ' ', '\t', '\n', '\r':
			return -1
		}
		return r
	}, s)
	out := Set{m: map[string][]Interval{}}
	if s == "" {
		return out, nil
	}
	for _, elem := range strings.Split(s, ",") {
		if elem == "" {
			return Set{}, fmt.Errorf("%w: empty element", ErrSyntax)
		}
		parts := strings.Split(elem, ":")
		uuid, err := normUUID(parts[0])
		if err != nil {
			return Set{}, err
		}
		if len(parts) == 1 {
			return Set{}, fmt.Errorf("%w: source id %s has no transactions", ErrSyntax, uuid)
		}
		sid := uuid
		pendingTag := false
		for _, p := range parts[1:] {
			if p == "" {
				return Set{}, fmt.Errorf("%w: empty part after %s", ErrSyntax, sid)
			}
			if c := p[0]; c >= '0' && c <= '9' {
				iv, err := parseInterval(p)
				if err != nil {
					return Set{}, err
				}
				out.m[sid] = append(out.m[sid], iv)
				pendingTag = false
				continue
			}
			if !tagRe.MatchString(p) {
				return Set{}, fmt.Errorf("%w: %q is neither an interval nor a tag", ErrSyntax, cut(p))
			}
			if pendingTag {
				return Set{}, fmt.Errorf("%w: tag %q has no transactions", ErrSyntax, strings.ToLower(parts[0]))
			}
			sid = uuid + ":" + strings.ToLower(p)
			pendingTag = true
		}
		if pendingTag {
			return Set{}, fmt.Errorf("%w: tag of %s has no transactions", ErrSyntax, uuid)
		}
	}
	for k, ivs := range out.m {
		out.m[k] = normalize(ivs)
	}
	return out, nil
}

func normUUID(s string) (string, error) {
	if !uuidRe.MatchString(s) {
		return "", fmt.Errorf("%w: %q is not a server UUID", ErrSyntax, cut(s))
	}
	h := strings.ToLower(strings.ReplaceAll(s, "-", ""))
	if len(h) != 32 {
		return "", fmt.Errorf("%w: %q is not a server UUID", ErrSyntax, cut(s))
	}
	return h[0:8] + "-" + h[8:12] + "-" + h[12:16] + "-" + h[16:20] + "-" + h[20:32], nil
}

func parseInterval(p string) (Interval, error) {
	a, b, isRange := strings.Cut(p, "-")
	start, err := parseNum(a)
	if err != nil {
		return Interval{}, err
	}
	end := start
	if isRange {
		if end, err = parseNum(b); err != nil {
			return Interval{}, err
		}
	}
	if end < start {
		return Interval{}, fmt.Errorf("%w: interval %q ends before it starts", ErrSyntax, cut(p))
	}
	return Interval{Start: start, End: end}, nil
}

// parseNum reads one transaction number: 1 to 2^63-2, as the server allows
// (GNO_END is 2^63-1 exclusive).
func parseNum(s string) (int64, error) {
	if s == "" || len(s) > 19 {
		return 0, fmt.Errorf("%w: transaction number %q", ErrSyntax, cut(s))
	}
	for _, c := range s {
		if c < '0' || c > '9' {
			return 0, fmt.Errorf("%w: transaction number %q", ErrSyntax, cut(s))
		}
	}
	n, err := strconv.ParseInt(s, 10, 64)
	if err != nil || n < 1 || n >= math.MaxInt64 {
		return 0, fmt.Errorf("%w: transaction number %q out of range", ErrSyntax, cut(s))
	}
	return n, nil
}

// normalize sorts ivs and merges overlapping and adjacent intervals.
func normalize(ivs []Interval) []Interval {
	if len(ivs) == 0 {
		return nil
	}
	s := slices.Clone(ivs)
	slices.SortFunc(s, func(a, b Interval) int {
		if a.Start != b.Start {
			if a.Start < b.Start {
				return -1
			}
			return 1
		}
		if a.End < b.End {
			return -1
		}
		if a.End > b.End {
			return 1
		}
		return 0
	})
	out := []Interval{s[0]}
	for _, iv := range s[1:] {
		last := &out[len(out)-1]
		if iv.Start <= last.End+1 {
			if iv.End > last.End {
				last.End = iv.End
			}
			continue
		}
		out = append(out, iv)
	}
	return out
}

// IsEmpty reports whether s holds no transaction.
func (s Set) IsEmpty() bool { return len(s.m) == 0 }

// SourceIDs returns the source ids of s ("uuid" or "uuid:tag"), sorted.
func (s Set) SourceIDs() []string {
	ids := make([]string, 0, len(s.m))
	for k := range s.m {
		ids = append(ids, k)
	}
	slices.Sort(ids)
	return ids
}

// Intervals returns a copy of the intervals of one source id ("uuid" or
// "uuid:tag", any case), sorted and merged; nil when s has none.
func (s Set) Intervals(sourceID string) []Interval {
	return slices.Clone(s.m[strings.ToLower(sourceID)])
}

// Count returns the number of transactions in s, saturating at MaxInt64.
func (s Set) Count() int64 {
	var n int64
	for _, ivs := range s.m {
		for _, iv := range ivs {
			d := iv.End - iv.Start + 1
			if n > math.MaxInt64-d {
				return math.MaxInt64
			}
			n += d
		}
	}
	return n
}

// String prints s in the server's canonical form: source UUIDs sorted, each
// UUID's untagged intervals first and then each tag in order, intervals
// merged; "" for the empty set. Parse(s.String()) equals s.
func (s Set) String() string {
	byUUID := map[string][]string{} // uuid -> its source ids, sorted
	for _, id := range s.SourceIDs() {
		u := id
		if i := strings.IndexByte(id, ':'); i >= 0 {
			u = id[:i]
		}
		byUUID[u] = append(byUUID[u], id)
	}
	uuids := make([]string, 0, len(byUUID))
	for u := range byUUID {
		uuids = append(uuids, u)
	}
	slices.Sort(uuids)
	var b strings.Builder
	for i, u := range uuids {
		if i > 0 {
			b.WriteByte(',')
		}
		b.WriteString(u)
		for _, id := range byUUID[u] { // the untagged id sorts first: it is a prefix
			if id != u {
				b.WriteByte(':')
				b.WriteString(id[len(u)+1:])
			}
			for _, iv := range s.m[id] {
				b.WriteByte(':')
				b.WriteString(strconv.FormatInt(iv.Start, 10))
				if iv.End != iv.Start {
					b.WriteByte('-')
					b.WriteString(strconv.FormatInt(iv.End, 10))
				}
			}
		}
	}
	return b.String()
}

// Equal reports whether s and o hold the same transactions.
func (s Set) Equal(o Set) bool {
	if len(s.m) != len(o.m) {
		return false
	}
	for k, a := range s.m {
		if !slices.Equal(a, o.m[k]) {
			return false
		}
	}
	return true
}

// Union returns every transaction in s or o.
func (s Set) Union(o Set) Set {
	out := Set{m: make(map[string][]Interval, len(s.m)+len(o.m))}
	for k, ivs := range s.m {
		out.m[k] = slices.Clone(ivs)
	}
	for k, ivs := range o.m {
		out.m[k] = normalize(append(out.m[k], ivs...))
	}
	return out
}

// Contains reports whether every transaction of sub is in s: the server's
// GTID_SUBSET(sub, s). The empty set is in every set.
func (s Set) Contains(sub Set) bool {
	for k, need := range sub.m {
		have := s.m[k]
		j := 0
		for _, iv := range need {
			for j < len(have) && have[j].End < iv.Start {
				j++
			}
			if j == len(have) || have[j].Start > iv.Start || have[j].End < iv.End {
				return false
			}
		}
	}
	return true
}

// Subtract returns the transactions of s that are not in o: the server's
// GTID_SUBTRACT(s, o).
func (s Set) Subtract(o Set) Set {
	out := Set{m: make(map[string][]Interval, len(s.m))}
	for k, ivs := range s.m {
		rest := subtractIntervals(ivs, o.m[k])
		if len(rest) > 0 {
			out.m[k] = rest
		}
	}
	return out
}

// subtractIntervals removes b from a; both sorted and merged.
func subtractIntervals(a, b []Interval) []Interval {
	var out []Interval
	j := 0
	for _, iv := range a {
		start := iv.Start
		for j < len(b) && b[j].End < start {
			j++
		}
		k := j
		for k < len(b) && b[k].Start <= iv.End {
			if b[k].Start > start {
				out = append(out, Interval{Start: start, End: b[k].Start - 1})
			}
			if b[k].End >= iv.End {
				start = iv.End + 1
				break
			}
			start = b[k].End + 1
			k++
		}
		if start <= iv.End {
			out = append(out, Interval{Start: start, End: iv.End})
		}
	}
	return out
}

// cut keeps an error's echo of the input short: an offset is untrusted text.
func cut(s string) string {
	const max = 40
	if len(s) <= max {
		return s
	}
	n := max
	for n > 0 && !utf8.RuneStart(s[n]) {
		n--
	}
	return s[:n] + "..."
}

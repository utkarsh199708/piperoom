package redact

import (
	"regexp"
	"strings"
)

// PII detectors. Error traces and record values carry customer data: a unique
// violation's "Key (ssn)=(...)" detail, a Debezium Struct{phone=...}, a row a
// sink connector failed to write. These rules mask U.S. social security
// numbers, payment card numbers and phone numbers there, and any value whose
// key names one of them, while leaving LSNs, offsets, timestamps, txids,
// schema ids and record ids alone:
//   - a card number must pass the Luhn check, start with a real issuer prefix
//     at a length that issuer uses, and not be the value of an evidence key
//     (lsn=, offset=, ts_ms=, id=...); a decimal fraction never qualifies;
//   - an SSN must be written 3-2-4 with dashes (dates are 4-2-2) and use a
//     valid area, group and serial; an unformatted one is found only under an
//     SSN key;
//   - a phone number must be written like one: "+" and 10-15 digits, "+" and
//     8-15 digits with separators, or North American (415) 555-0100 /
//     415-555-0100. A bare run of digits is a phone number only under a phone
//     key.
//
// Known gaps: phone numbers written without "+" in other national formats,
// national ids other than U.S. SSNs, and card numbers split in groups other
// than 4-4-4-4(-3), 4-6-5 or 4-6-4 are not found.

// PII kinds reported in Stats.
const (
	KindSSN   Kind = "ssn"
	KindCard  Kind = "card_number"
	KindPhone Kind = "phone"
	// KindPIIValue is the value of a key that names PII (ssn=, "phone":,
	// card_number=, Postgres Key (ssn)=(...)), whatever its format.
	KindPIIValue Kind = "pii_value"
)

// piiRules run after the secret rules.
var piiRules = []rule{
	{kind: KindPIIValue, find: piiKVFind, groups: []int{2, 3, 4, 5}, style: styleSecret, pre: containsAny(piiWords...)},
	{kind: KindPIIValue, find: pgKeyFind, groups: []int{1}, style: styleSecret, pre: containsCase("Key (")},
	{kind: KindSSN, find: ssnFind, groups: []int{1}, style: styleSecret, pre: hasDash},
	{kind: KindCard, find: cardFind, groups: []int{1}, style: styleSecret, pre: hasDigitRun(13)},
	{kind: KindPhone, find: phoneFind, groups: []int{1}, style: styleSecret, pre: hasPhoneShape},
}

// piiWords are the fragments a PII key name contains; piiKey decides.
var piiWords = []string{"ssn", "social", "phone", "mobile", "tel", "msisdn", "fax", "card", "cc", "pan", "cvv", "cvc"}

// piiKey reports whether key k names a PII value. Meta suffixes (phone_type,
// card_id, ssn_hash) are names of something else.
func piiKey(k string) bool {
	words := keyWords(k)
	if len(words) == 0 || metaWords[words[len(words)-1]] {
		return false
	}
	for i, w := range words {
		switch w {
		case "ssn", "msisdn", "phone", "phonenumber", "telephone", "tel", "mobile", "cellphone", "fax",
			"cvv", "cvc", "pan", "creditcard", "cardnumber", "ccnumber", "ccnum", "cc":
			return true
		case "security":
			if i > 0 && words[i-1] == "social" {
				return true
			}
		case "card":
			if i > 0 && (words[i-1] == "credit" || words[i-1] == "debit" || words[i-1] == "payment") {
				return true
			}
		case "number", "num", "no":
			if i > 0 && (words[i-1] == "card" || words[i-1] == "phone" || words[i-1] == "cc") {
				return true
			}
		}
	}
	return false
}

// piiValueOK accepts a value under a PII key only when it holds a digit:
// "phone: unknown" or "card: visa" are not numbers.
func piiValueOK(s string, m []int) bool {
	key, val := kvGroups(s, m)
	return piiKey(key) && strings.ContainsAny(val, "0123456789")
}

// piiKVFind finds key/value pairs whose key names PII, with the same key and
// value shapes as the secret key/value rule.
func piiKVFind(s, lower string) [][]int {
	return keyValueFind(s, lower, piiWords, piiValueOK)
}

// pgKeyRe matches the column list and value list of a Postgres constraint
// DETAIL: Key (customer_id, ssn)=(42, 123-45-6789) already exists.
var pgKeyRe = regexp.MustCompile(`Key \(([^()\n]{1,512})\)=\(([^\n]*?)\)(?: already exists| is not present| is still referenced|[.;,]?\s*$|[.;,]? )`)

// pgKeyFind masks the values of PII columns in Postgres key details. When the
// values cannot be split to match the columns (a value holds ", "), the whole
// value list is masked.
func pgKeyFind(s, _ string) [][]int {
	var out [][]int
	for _, m := range pgKeyRe.FindAllStringSubmatchIndex(s, -1) {
		cols := strings.Split(s[m[2]:m[3]], ",")
		vs, ve := m[4], m[5]
		pii := false
		for _, c := range cols {
			if piiKey(strings.Trim(strings.TrimSpace(c), `"`)) {
				pii = true
			}
		}
		if !pii {
			continue
		}
		vals := strings.Split(s[vs:ve], ", ")
		if len(vals) != len(cols) {
			out = append(out, []int{m[0], m[1], vs, ve})
			continue
		}
		off := vs
		for i, v := range vals {
			if piiKey(strings.Trim(strings.TrimSpace(cols[i]), `"`)) && strings.ContainsAny(v, "0123456789") {
				out = append(out, []int{m[0], m[1], off, off + len(v)})
			}
			off += len(v) + len(", ")
		}
	}
	return out
}

func isDigit(c byte) bool { return c >= '0' && c <= '9' }

func hasDash(s, _ string) bool { return strings.IndexByte(s, '-') >= 0 }

// hasDigitRun reports n digits in a row, or a separated card-like run.
func hasDigitRun(n int) func(s, _ string) bool {
	return func(s, _ string) bool {
		run := 0
		for i := 0; i < len(s); i++ {
			switch {
			case isDigit(s[i]):
				run++
				if run >= n {
					return true
				}
			case (s[i] == ' ' || s[i] == '-') && run > 0:
				// separators inside a grouped card number keep the run going
			default:
				run = 0
			}
		}
		return false
	}
}

func hasPhoneShape(s, _ string) bool {
	return strings.IndexByte(s, '+') >= 0 || strings.IndexByte(s, '-') >= 0
}

// boundaryOK reports that s[i:j] is not glued to a word, a dash-joined id, a
// decimal point or a path: the token must stand alone.
func boundaryOK(s string, i, j int) bool {
	if i > 0 {
		c := s[i-1]
		if isLetter(c) || isDigit(c) || c == '_' || c == '-' || c == '.' || c == '/' || c == '@' || c == '+' || c == ':' && i > 1 && isDigit(s[i-2]) {
			return false
		}
	}
	if j < len(s) {
		c := s[j]
		if isLetter(c) || isDigit(c) || c == '_' || c == '-' || c == '/' || c == '@' {
			return false
		}
		if (c == '.' || c == ':') && j+1 < len(s) && isDigit(s[j+1]) {
			return false
		}
	}
	return true
}

var ssnRe = regexp.MustCompile(`[0-9]{3}-[0-9]{2}-[0-9]{4}`)

// ssnFind finds dashed SSNs with a valid area (not 000, 666 or 9xx), group
// (not 00) and serial (not 0000).
func ssnFind(s, _ string) [][]int {
	var out [][]int
	for _, m := range ssnRe.FindAllStringIndex(s, -1) {
		v := s[m[0]:m[1]]
		area, group, serial := v[:3], v[4:6], v[7:]
		if area == "000" || area == "666" || area[0] == '9' || group == "00" || serial == "0000" {
			continue
		}
		if !boundaryOK(s, m[0], m[1]) || evidenceKeyBefore(s, m[0]) {
			continue
		}
		out = append(out, []int{m[0], m[1], m[0], m[1]})
	}
	return out
}

// digitRun is one maximal run of digits in a line.
type digitRun struct{ start, end int }

func digitRuns(s string) []digitRun {
	var runs []digitRun
	for i := 0; i < len(s); {
		if !isDigit(s[i]) {
			i++
			continue
		}
		j := i
		for j < len(s) && isDigit(s[j]) {
			j++
		}
		runs = append(runs, digitRun{i, j})
		i = j
	}
	return runs
}

// cardGroupings are the ways card numbers are printed in groups.
var cardGroupings = [][]int{{4, 4, 4, 4, 3}, {4, 4, 4, 4}, {4, 6, 5}, {4, 6, 4}}

// cardFind finds card numbers: 13-19 digits, plain or grouped by single spaces
// or dashes, that pass the issuer and Luhn checks, stand alone and are not
// the value of an evidence key.
func cardFind(s, _ string) [][]int {
	var out [][]int
	runs := digitRuns(s)
	for k := 0; k < len(runs); k++ {
		r := runs[k]
		if r.end-r.start >= 13 {
			if boundaryOK(s, r.start, r.end) && isCard(s[r.start:r.end]) && !evidenceKeyBefore(s, r.start) {
				out = append(out, []int{r.start, r.end, r.start, r.end})
			}
			continue
		}
		for _, g := range cardGroupings {
			last, ok := matchGrouping(s, runs[k:], g)
			if !ok {
				continue
			}
			end := runs[k+last].end
			if boundaryOK(s, r.start, end) && isCard(digitsOnly(s[r.start:end])) && !evidenceKeyBefore(s, r.start) {
				out = append(out, []int{r.start, end, r.start, end})
				k += last
			}
			break
		}
	}
	return out
}

// matchGrouping reports whether runs start with groups shaped like g, joined
// by one and the same separator (' ' or '-'), and returns the index of the
// last run used.
func matchGrouping(s string, runs []digitRun, g []int) (int, bool) {
	if len(runs) < len(g) {
		return 0, false
	}
	var sep byte
	for i, want := range g {
		if runs[i].end-runs[i].start != want {
			return 0, false
		}
		if i == 0 {
			continue
		}
		gap := runs[i].start - runs[i-1].end
		c := s[runs[i-1].end]
		if gap != 1 || (c != ' ' && c != '-') || (sep != 0 && c != sep) {
			return 0, false
		}
		sep = c
	}
	return len(g) - 1, true
}

func digitsOnly(s string) string {
	var b strings.Builder
	for i := 0; i < len(s); i++ {
		if isDigit(s[i]) {
			b.WriteByte(s[i])
		}
	}
	return b.String()
}

// isCard reports a digit string with a known issuer prefix at a length that
// issuer uses, passing the Luhn check.
func isCard(d string) bool {
	n := len(d)
	if n < 13 || n > 19 || !luhn(d) {
		return false
	}
	p2 := int(d[0]-'0')*10 + int(d[1]-'0')
	p4 := p2*100 + int(d[2]-'0')*10 + int(d[3]-'0')
	switch {
	case d[0] == '4': // Visa (13-digit cards are no longer issued)
		return n == 16 || n == 19
	case p2 >= 51 && p2 <= 55, p4 >= 2221 && p4 <= 2720: // Mastercard
		return n == 16
	case p2 == 34 || p2 == 37: // Amex
		return n == 15
	case p4 == 6011, p2 == 65, p4/10 >= 644 && p4/10 <= 649, p2 == 62: // Discover, UnionPay
		return n >= 16
	case p4 >= 3528 && p4 <= 3589: // JCB
		return n >= 16
	case p2 == 36, p2 == 38, p2 == 39, p4/10 >= 300 && p4/10 <= 305: // Diners
		return n == 14 || n == 16
	}
	return false
}

// luhn reports whether the digit string passes the Luhn checksum.
func luhn(d string) bool {
	sum := 0
	double := false
	for i := len(d) - 1; i >= 0; i-- {
		v := int(d[i] - '0')
		if double {
			v *= 2
			if v > 9 {
				v -= 9
			}
		}
		sum += v
		double = !double
	}
	return sum%10 == 0
}

// phoneRe matches "+" numbers (E.164 with optional grouping) and North
// American numbers with dashes or a parenthesised area code. Group 1 is the
// number.
var phoneRe = regexp.MustCompile(`(\+[0-9]{1,3}(?:[ .-]?\(?[0-9]{1,4}\)?)(?:[ .-]?[0-9]{2,4}){1,4}` +
	`|(?:1[ -])?\([2-9][0-9]{2}\) ?[2-9][0-9]{2}-[0-9]{4}` +
	`|(?:1-)?[2-9][0-9]{2}-[2-9][0-9]{2}-[0-9]{4})`)

// phoneFind finds phone numbers written like phone numbers (see phoneRe), with
// 8-15 digits; a "+" number without separators needs at least 10.
func phoneFind(s, _ string) [][]int {
	var out [][]int
	for _, m := range phoneRe.FindAllStringSubmatchIndex(s, -1) {
		start, end := m[2], m[3]
		v := s[start:end]
		d := len(digitsOnly(v))
		if d < 8 || d > 15 {
			continue
		}
		if v[0] == '+' && d < 10 && !strings.ContainsAny(v[1:], " .-()") {
			continue
		}
		if !boundaryOK(s, start, end) || evidenceKeyBefore(s, start) {
			continue
		}
		out = append(out, []int{start, end, start, end})
	}
	return out
}

// evidenceWords name values that are pipeline evidence: positions, times,
// ids, sizes. A number given as one of them is never PII.
var evidenceWords = map[string]bool{
	"lsn": true, "offset": true, "offsets": true, "ts": true, "ms": true, "us": true, "ns": true,
	"id": true, "ids": true, "txid": true, "xid": true, "xmin": true, "seq": true, "sequence": true,
	"position": true, "pos": true, "commit": true, "timestamp": true, "time": true, "epoch": true,
	"partition": true, "version": true, "size": true, "bytes": true, "count": true, "lag": true,
	"total": true, "checksum": true, "crc": true, "hash": true, "serial": true, "proc": true,
	"end": true, "committed": true, "generation": true, "epochs": true, "nanos": true, "millis": true,
	"key": true, // a record or partition key is evidence, whatever its digits
}

// evidenceKeyBefore reports whether the value at i follows an evidence key:
// "lsn=", "\"ts_ms\":", "offset ", "record id ", "Key (order_id)=(".
func evidenceKeyBefore(s string, i int) bool {
	j := i
	for n := 0; j > 0 && n < 4 && strings.IndexByte(" \t:=\"'[()", s[j-1]) >= 0; n++ {
		j--
	}
	w := j
	for w > 0 && isKeyChar(s[w-1]) {
		w--
	}
	if w == j {
		return false
	}
	words := keyWords(s[w:j])
	return len(words) > 0 && evidenceWords[words[len(words)-1]]
}

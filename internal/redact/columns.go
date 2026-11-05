package redact

import (
	"slices"
	"strings"
	"unicode"
)

// Column classifier (X7c, docs/design/lineage.md section 3): which columns of
// a table hold personal data, from the column's name and catalog type only.
// It never sees a value, so it can run on any catalog read. The classes are
// the names of topology.PIIClass (this package cannot import topology, which
// imports it); "other" is not here: only an owners.yaml pii.include gives it.
//
// The name is split into lower-case words at '_', '-', '.', blanks, camelCase
// and letter/digit boundaries ("customerEmail2" -> customer email 2), and a
// class matches when one of its terms appears as whole words: "emailed_at" is
// not an email column, "e_mail" is. A word after the term that makes the
// column about the thing rather than the thing ("email_verified",
// "card_id", "ip_count") or one before it ("is_email", "num_phones") cancels
// that match. The type only rules columns out: a boolean, a uuid, a time
// (except for dob), a number (except for phone, card, national_id and
// address, which stores a zip code) or a network type (except for ip).
// Known gaps: words run together ("customeremail") match only the spellings
// listed below; a bare "name" counts only alone or after a person word
// ("customer_name", not "product_name"). owners.yaml pii.include and
// pii.exclude correct both ways.

// Column classes, as topology.PIIClasses spells them.
const (
	ColumnEmail      = "email"
	ColumnPhone      = "phone"
	ColumnName       = "name"
	ColumnAddress    = "address"
	ColumnNationalID = "national_id"
	ColumnCard       = "card"
	ColumnIP         = "ip"
	ColumnDOB        = "dob"
)

// ColumnClasses returns the classes ClassifyColumn can answer, in the order
// of topology.PIIClasses.
func ColumnClasses() []string {
	return []string{ColumnEmail, ColumnPhone, ColumnName, ColumnAddress, ColumnNationalID, ColumnCard, ColumnIP, ColumnDOB}
}

// maxColumnWords bounds the work on a hostile name: catalog names are short
// (Postgres cuts them at 63 bytes), so words past this are not looked at.
const maxColumnWords = 32

// columnRule is one class: its terms (space-separated word sequences; a
// trailing "$" means the term must end the name) and the type kinds it
// allows besides text-like and unknown types.
type columnRule struct {
	class string
	terms []string
	types []typeKind
}

// columnRules are checked in this order and the first match wins, so
// "email_address" is an email and "ip_address" an ip, not an address.
var columnRules = []columnRule{
	{ColumnEmail, []string{"email", "emails", "emailaddress", "e mail", "mail$"}, nil},
	{ColumnIP, []string{"ip", "ips", "ipaddr", "ipaddress", "ipv 4", "ipv 6", "remote addr",
		"mac address", "mac addr", "macaddr", "macaddress"}, []typeKind{kindNet}},
	{ColumnCard, []string{"pan", "card", "cards", "cardnumber", "creditcard", "ccnum", "ccnumber", "cc num",
		"cc number", "cvv", "cvc"}, []typeKind{kindNumber}},
	{ColumnNationalID, []string{"ssn", "nin", "nino", "national id", "nationalid", "national insurance",
		"social security", "socialsecurity", "tax id", "taxid", "tax number", "tin$", "passport",
		"passportnumber", "aadhaar", "cpf", "nric", "bsn", "pesel", "personnummer", "driver license",
		"drivers license", "driving license", "driver licence", "drivers licence", "driving licence"}, []typeKind{kindNumber}},
	{ColumnPhone, []string{"phone", "phones", "phonenumber", "telephone", "tel", "msisdn", "fax", "mobile$",
		"mobile number", "mobile no", "mobile phone", "cellphone", "cell phone", "cell number",
		"contact number"}, []typeKind{kindNumber}},
	{ColumnDOB, []string{"dob", "dateofbirth", "date of birth", "birthdate", "birth date", "birthday",
		"birth day", "born on"}, []typeKind{kindTime}},
	{ColumnName, []string{"first name", "last name", "full name", "given name", "family name", "middle name",
		"maiden name", "legal name", "display name", "user name", "firstname", "lastname", "fullname",
		"givenname", "familyname", "middlename", "maidenname", "displayname", "username", "surname",
		"forename", "nickname"}, nil},
	{ColumnAddress, []string{"address", "addresses", "addr", "street", "streetaddress", "zip$", "zip code",
		"zipcode", "postcode", "post code", "postal code", "postalcode"}, []typeKind{kindNumber}},
}

// aboutWords after a term make the column a fact about the thing, not the
// thing: "email_verified", "card_id", "ip_count", "phone_updated_at".
var aboutWords = []string{"id", "ids", "key", "keys", "fk", "uuid", "count", "counts", "cnt", "total", "length",
	"len", "size", "verified", "verification", "confirmed", "confirmation", "validated", "valid", "status",
	"type", "types", "kind", "format", "enabled", "disabled", "flag", "flags", "opt", "optin", "optout",
	"opted", "consent", "template", "domain", "provider", "brand", "holder", "holders", "country", "region",
	"sent", "bounced", "changed", "updated", "created", "deleted", "at", "on", "ts", "time", "date",
	"pref", "preference", "preferences", "file"}

// aboutPrefixes before a term do the same: "is_email", "num_phones".
var aboutPrefixes = []string{"is", "has", "num", "count", "cnt", "total", "max", "min", "avg", "sum"}

// personWords before a final "name" make it a person's name.
var personWords = []string{"customer", "client", "user", "person", "contact", "employee", "member", "patient",
	"recipient", "sender", "buyer", "owner", "holder", "cardholder", "billing", "shipping", "passenger", "guest",
	"student", "applicant", "beneficiary", "driver", "author", "signer", "subscriber", "parent", "guardian",
	"spouse", "emergency", "real", "legal"}

// ClassifyColumn returns the personal-data class of a column from its name
// and its catalog type name (typ may be "" when unknown), or false. It reads
// nothing else and is deterministic.
func ClassifyColumn(name, typ string) (string, bool) {
	words := columnWords(name)
	if len(words) == 0 {
		return "", false
	}
	kind := columnTypeKind(typ)
	if kind == kindBool || kind == kindUUID {
		return "", false
	}
	for _, r := range columnRules {
		if kind != kindOther && !slices.Contains(r.types, kind) {
			continue
		}
		if matchTerms(words, r.terms) || r.class == ColumnName && personName(words) {
			return r.class, true
		}
	}
	return "", false
}

// matchTerms reports whether one of terms appears in words with no
// aboutWords after it and no aboutPrefixes right before it.
func matchTerms(words, terms []string) bool {
	for _, term := range terms {
		end := strings.HasSuffix(term, "$")
		tw := strings.Fields(strings.TrimSuffix(term, "$"))
		for i := 0; i+len(tw) <= len(words); i++ {
			if !slices.Equal(words[i:i+len(tw)], tw) {
				continue
			}
			tail := words[i+len(tw):]
			if end && len(tail) > 0 {
				continue
			}
			if i > 0 && slices.Contains(aboutPrefixes, words[i-1]) || slices.ContainsFunc(tail, isAboutWord) {
				continue
			}
			return true
		}
	}
	return false
}

func isAboutWord(w string) bool { return slices.Contains(aboutWords, w) }

// personName: "name" alone, or ending a name after a person word.
func personName(words []string) bool {
	n := len(words)
	if words[n-1] != "name" && words[n-1] != "names" {
		return false
	}
	return n == 1 || slices.Contains(personWords, words[n-2])
}

// columnWords splits a column name into lower-case words at separators,
// camelCase ("customerEmail", "IPAddress") and letter/digit boundaries.
func columnWords(name string) []string {
	var words []string
	var cur []rune
	flush := func() {
		if len(cur) > 0 && len(words) < maxColumnWords {
			words = append(words, string(cur))
		}
		cur = cur[:0]
	}
	rs := []rune(name)
	for i, r := range rs {
		if !unicode.IsLetter(r) && !unicode.IsDigit(r) {
			flush()
			continue
		}
		if len(cur) > 0 {
			p := rs[i-1]
			if unicode.IsLetter(p) != unicode.IsLetter(r) ||
				unicode.IsLower(p) && unicode.IsUpper(r) ||
				unicode.IsUpper(p) && unicode.IsUpper(r) && i+1 < len(rs) && unicode.IsLower(rs[i+1]) {
				flush()
			}
		}
		cur = append(cur, unicode.ToLower(r))
	}
	flush()
	return words
}

// typeKind groups catalog type names by what they can hold.
type typeKind int

const (
	kindOther  typeKind = iota // text-like, json, binary, unknown: never rules a class out
	kindBool                   // never personal data on its own
	kindUUID                   // a generated identifier
	kindTime                   // a point in time or a span: only dob
	kindNumber                 // only phone, card, national_id, address (zip)
	kindNet                    // inet, cidr, macaddr: only ip
)

// columnTypeKind reads the type names Postgres (format_type), Snowflake,
// BigQuery, Databricks and ClickHouse spell; modifiers ("varchar(255)",
// "Nullable(String)") and array brackets are looked through.
func columnTypeKind(typ string) typeKind {
	t := strings.ToLower(strings.TrimSpace(typ))
	for _, w := range []string{"nullable(", "lowcardinality("} {
		if strings.HasPrefix(t, w) {
			t = strings.TrimSuffix(strings.TrimPrefix(t, w), ")")
		}
	}
	t = strings.TrimSuffix(t, "[]")
	if i := strings.IndexByte(t, '('); i >= 0 {
		t = strings.TrimSpace(t[:i])
	}
	switch {
	case t == "boolean" || t == "bool":
		return kindBool
	case t == "uuid" || t == "uniqueidentifier":
		return kindUUID
	case t == "date" || t == "date32" || t == "interval" || strings.HasPrefix(t, "time") || strings.HasPrefix(t, "datetime"):
		return kindTime
	case t == "inet" || t == "cidr" || strings.HasPrefix(t, "macaddr") || t == "ipv4" || t == "ipv6":
		return kindNet
	case slices.Contains(numericTypes, t) || strings.HasPrefix(t, "int") || strings.HasPrefix(t, "uint") ||
		strings.HasPrefix(t, "float") || strings.HasPrefix(t, "decimal"):
		return kindNumber
	}
	return kindOther
}

var numericTypes = []string{"smallint", "bigint", "tinyint", "mediumint", "byteint", "numeric", "number",
	"bignumeric", "real", "double", "double precision", "serial", "bigserial", "smallserial", "long", "short",
	"money"}

// PersonalColumn reports whether a column's name alone suggests data about a
// person. It is broader than ClassifyColumn (no type, no aboutWords, and
// words such as "birth", "city" or "license" count), for code that must never
// read such a column at all, like the watcher's row_drift sampling, where a
// false positive only leaves a column out.
func PersonalColumn(name string) bool {
	if _, ok := ClassifyColumn(name, ""); ok {
		return true
	}
	n := strings.ToLower(name)
	for _, s := range personalSubstrings {
		if strings.Contains(n, s) {
			return true
		}
	}
	personal := func(w string) bool { return slices.Contains(personalWords, w) }
	// Both splits: camelCase words ("TaxId") and plain separator words, so an
	// odd casing ("CArd") cannot split a word away from the list.
	plain := strings.FieldsFunc(n, func(r rune) bool { return r == '_' || r == '-' || r == '.' || r == ' ' })
	return slices.ContainsFunc(columnWords(name), personal) || slices.ContainsFunc(plain, personal)
}

// personalWords are words of the classes above and their neighbours
// ("born", "city", "license"); personalSubstrings catch them run together
// ("dateofbirth", "emailat").
var (
	personalWords = []string{"email", "mail", "phone", "mobile", "tel", "fax", "name", "address", "addr",
		"street", "city", "zip", "postcode", "postal", "ssn", "sin", "nin", "national", "tax", "passport",
		"license", "licence", "card", "pan", "iban", "cvv", "ip", "birth", "birthday", "birthdate", "dob", "born"}
	personalSubstrings = []string{"birth", "email", "phone", "passport", "address"}
)

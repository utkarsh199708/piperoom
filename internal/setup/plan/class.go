package plan

import (
	"errors"
	"fmt"
	"net"
	"net/url"
	"strings"

	"piperoom/internal/events"
	"piperoom/internal/setup/protocol"
)

// Environment classes (design §3.5). A class is a floor: the person may raise
// it, nothing lowers it. The values are the protocol's Class* constants, so a
// class read from a SetupTarget or an OpRequest compares equal without
// conversion.
const (
	ClassLocal      = protocol.ClassLocal
	ClassShared     = protocol.ClassShared
	ClassProduction = protocol.ClassProduction
)

// Reasons of a derived class: the fixed codes of
// events.SetupTargetClassedPayload.Reason (design §3.5). They are codes, never
// text, so the room stores what the classifier decided and the UI translates
// it.
const (
	ReasonLoopback      = "loopback"        // a loopback address or the local Docker engine
	ReasonCreatedByRoom = "created_by_room" // a database or Kafka this room created (a receipt) on loopback or the local engine
	ReasonProdPattern   = "prod_pattern"    // a PIPEROOM_SETUP_PROD_PATTERNS match
	ReasonPerson        = "person"          // the person raised the class above the derived one
	ReasonDefault       = "default"         // anything else: shared
)

// ErrClass is returned for a class string that is not ClassLocal, ClassShared
// or ClassProduction, the empty class included.
var ErrClass = errors.New("plan: unknown environment class")

// ClassRank orders classes local < shared < production, and reports false for
// a string that is not a class.
func ClassRank(class string) (rank int, ok bool) {
	switch class {
	case ClassLocal:
		return 0, true
	case ClassShared:
		return 1, true
	case ClassProduction:
		return 2, true
	}
	return 0, false
}

// ValidClass reports whether class is one of the three classes.
func ValidClass(class string) bool {
	_, ok := ClassRank(class)
	return ok
}

// Raise returns the higher of a derived floor and the person's answer: the
// person can raise a class and never lower it, so Raise(shared, local) is
// shared and Raise(local, production) is production. person may be "" (no
// answer). An unknown class on either side is an error.
func Raise(floor, person string) (string, error) {
	floorRank, ok := ClassRank(floor)
	if !ok {
		return "", fmt.Errorf("%w: floor %q", ErrClass, floor)
	}
	if person == "" {
		return floor, nil
	}
	personRank, ok := ClassRank(person)
	if !ok {
		return "", fmt.Errorf("%w: person %q", ErrClass, person)
	}
	if personRank > floorRank {
		return person, nil
	}
	return floor, nil
}

// Facts are the structured inputs Classify reads: fields of the runner's
// Hello and env_detect result, this room's receipts and the person's answer.
// There is deliberately no host-name and no free-text field, so a host name
// alone can never mark a target production (design §3.5): production comes
// only from the person or a configured pattern match.
type Facts struct {
	// Loopback: env_detect resolved the endpoint to a loopback address
	// (127.0.0.0/8, ::1) or Hello reported a loopback Docker host.
	Loopback bool
	// LocalEngine: env_detect found the local Docker engine socket.
	LocalEngine bool
	// ComposeService: env_detect found a compose service under
	// Hello.ProjectDir.
	ComposeService bool
	// CreatedByRoom: CreatedByRoom(roomEvents, target) — an action.executed
	// receipt in this room's log, never model text.
	CreatedByRoom bool
	// Person is the class floor the person set (SetupRequest.classes or the
	// raise route); "" when the person gave none.
	Person string
	// ProdPattern: MatchProdPattern(config.Setup.ProdPatterns, address).
	ProdPattern bool
}

// Classify derives a target's class floor and the fixed reason code the
// server stores on setup.target_classed. The default is shared; local is a
// loopback address, the local Docker engine, a compose service under the
// project directory, or a database or Kafka on either of those that this room
// created (a receipt); production is the person's answer or a configured
// pattern match. The person's answer is applied through Raise, so a stale
// answer can never lower the derived class. An unknown person class is an
// error.
func Classify(f Facts) (class, reason string, err error) {
	switch {
	case f.ProdPattern:
		class, reason = ClassProduction, ReasonProdPattern
	case f.CreatedByRoom && (f.Loopback || f.LocalEngine):
		class, reason = ClassLocal, ReasonCreatedByRoom
	case f.Loopback || f.LocalEngine || f.ComposeService:
		class, reason = ClassLocal, ReasonLoopback
	default:
		class, reason = ClassShared, ReasonDefault
	}
	raised, err := Raise(class, f.Person)
	if err != nil {
		return "", "", err
	}
	if raised != class {
		class, reason = raised, ReasonPerson
	}
	return class, reason, nil
}

// CreatedByRoom reports whether this room created target, from the room log
// only: an action.executed event whose step succeeded and whose receipt names
// target as what it reached. Events of any other type, failed steps, steps
// with an unknown outcome and receipts for another target do not count, and
// model text is not an input at all (design §3.5; U7b).
//
// The answer is endpoint-granular, not object-granular: any successful
// receipt that names the endpoint counts, so it says this room reached the
// target, never that it created the particular slot, publication or connector
// a step acts on. It is an input to Classify's class floor only, and must not
// gate a destructive op: the guard table takes no exception from it.
func CreatedByRoom(roomEvents []events.Event, target string) bool {
	if target == "" {
		return false
	}
	for _, e := range roomEvents {
		if e.Type != events.ActionExecuted {
			continue
		}
		p, err := events.Decode[events.ActionExecutedPayload](e)
		if err != nil {
			continue
		}
		if !p.OK || p.OutcomeUnknown || p.Receipt == nil {
			continue
		}
		if p.Receipt.Target == target {
			return true
		}
	}
	return false
}

// MatchProdPattern reports whether subject matches one of the
// PIPEROOM_SETUP_PROD_PATTERNS entries (config.Setup.ProdPatterns): host
// globs ("db-*.acme.com"), kubectl context names
// ("gke_acme_eu-west1_prod") and cloud account ids ("123456789012"). Matching
// is case-insensitive, '*' matches any run of characters, '?' matches one
// character, and every other character — the '/' of a context name included —
// is literal. Empty entries match nothing, so a stray comma in the
// configuration cannot make every target production.
//
// The subject is normalized before matching, so every spelling of one
// endpoint hits the same patterns: the canonical form is the bare, lower-case
// host (or context name or account id), with a URL's scheme, userinfo and
// path, a port and one trailing FQDN dot stripped. "DB-1.acme.com.",
// "db-1.acme.com:5432" and "postgres://admin@db-1.acme.com/orders" all match
// a pattern written for "db-1.acme.com", so an equivalent spelling cannot
// dodge a configured production mark.
func MatchProdPattern(patterns []string, subject string) bool {
	subject = normalizeSubject(subject)
	if subject == "" {
		return false
	}
	for _, pattern := range patterns {
		pattern = strings.TrimSpace(pattern)
		if pattern == "" {
			continue
		}
		if globMatch(pattern, subject) {
			return true
		}
	}
	return false
}

// normalizeSubject reduces a subject to MatchProdPattern's canonical form.
// Each reduction is applied at most once and only when its form is
// unambiguous: a subject with a scheme is parsed as a URL and reduced to its
// host; one host:port split is taken when the subject splits cleanly (a bare
// IPv6 literal and a host without a port do not split, and survive whole); a
// bracketed IPv6 literal loses its brackets; one trailing dot — the FQDN
// spelling of the same host — is stripped; and the result is lower-cased. A
// subject that is none of those forms, a kubectl context name or an account
// id, is only trimmed and lower-cased.
func normalizeSubject(subject string) string {
	subject = strings.TrimSpace(subject)
	if strings.Contains(subject, "://") {
		if u, err := url.Parse(subject); err == nil && u.Host != "" {
			subject = u.Host
		}
	}
	if host, _, err := net.SplitHostPort(subject); err == nil && host != "" {
		subject = host
	}
	if strings.HasPrefix(subject, "[") && strings.HasSuffix(subject, "]") {
		subject = subject[1 : len(subject)-1]
	}
	subject = strings.TrimSuffix(subject, ".")
	return strings.ToLower(subject)
}

// globMatch is MatchProdPattern's matcher: '*' and '?' only, no character
// class, on lower-cased runes, in linear time (the star fallback never
// revisits the subject).
func globMatch(pattern, subject string) bool {
	p := []rune(strings.ToLower(pattern))
	s := []rune(strings.ToLower(subject))
	i, j := 0, 0
	star, mark := -1, 0
	for i < len(s) {
		switch {
		case j < len(p) && (p[j] == '?' || p[j] == s[i]):
			i++
			j++
		case j < len(p) && p[j] == '*':
			star = j
			mark = i
			j++
		case star >= 0:
			j = star + 1
			mark++
			i = mark
		default:
			return false
		}
	}
	for j < len(p) && p[j] == '*' {
		j++
	}
	return j == len(p)
}

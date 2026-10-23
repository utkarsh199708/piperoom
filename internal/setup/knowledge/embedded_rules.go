package knowledge

import (
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"strings"
)

// The version mismatch rules of the embedded engine (U6a): Debezium engine vs
// Java, Kafka client and Quarkus. Every rule cites the release note it comes
// from (Source, plus the Debezium issue and the release note's own words in
// Quote); a fact that could not be read in a release note is not a rule.
// The Debezium release notes were read from the debezium.io site sources
// (releases/<series>/release-notes.asciidoc); the Kafka rule from the Apache
// Kafka 4.0.0 upgrade notes (docs/upgrade.html at tag 4.0.0).
//
// Not a rule here, for want of a release note that states it: "every
// io.debezium artifact at one release". The reference project enforces it by
// importing debezium-bom instead.

// RuleCheck is what a version rule compares.
type RuleCheck string

// The comparisons a rule can make.
const (
	// CheckJavaMin: the project's Java release must be at least Want.
	CheckJavaMin RuleCheck = "java-min"
	// CheckKafkaLine: the Kafka client the project pins (connect-runtime,
	// connect-api or kafka-clients) should be on one of the Want lines
	// ("3.9" or "3.9|4.0"), the ones the release was built and tested with.
	CheckKafkaLine RuleCheck = "kafka-line"
	// CheckQuarkusMajor: the Quarkus platform's major must be at least Want.
	CheckQuarkusMajor RuleCheck = "quarkus-major"
	// CheckQuarkusLine: the Quarkus platform should be on the Want line, the
	// one the Debezium Quarkus Outbox extension was built against.
	CheckQuarkusLine RuleCheck = "quarkus-line"
	// CheckEngineAPI: an engine API was removed; the finding is informational
	// (the build files cannot show whether the code still calls it).
	CheckEngineAPI RuleCheck = "engine-api"
)

// RuleOn is the component whose version a rule's range is over.
type RuleOn string

// The components a rule's range can be over.
const (
	OnEngine RuleOn = "engine" // debezium-embedded
	OnOutbox RuleOn = "outbox" // debezium-quarkus-outbox(-reactive)
	OnKafka  RuleOn = "kafka"  // connect-runtime / connect-api / kafka-clients
)

// Severity of a finding.
const (
	SeverityError = "error" // the combination does not run
	SeverityWarn  = "warn"  // the combination is untested by the release
	SeverityInfo  = "info"  // a removed API to look for in the code
)

// VersionRule is one cited fact about which versions go together. It applies
// when the version of On is in [From, Until) (Until empty: no upper bound).
type VersionRule struct {
	ID       string
	Check    RuleCheck
	On       RuleOn
	From     string
	Until    string
	Want     string // the required or expected value; see RuleCheck
	Severity string
	Title    string
	Fix      string
	Source   string // the release note URL, with the release's anchor
	Issue    string // the issue the release note cites, e.g. "DBZ-7224"
	Quote    string // the release note's own words
}

// EmbeddedVersions are the versions a project's build files declare, as
// project_inspect (U6b) reads them. An empty string or a zero means "not
// declared"; a rule that needs it does not apply.
type EmbeddedVersions struct {
	Engine  string // io.debezium:debezium-embedded
	Outbox  string // io.debezium:debezium-quarkus-outbox or -outbox-reactive
	Kafka   string // org.apache.kafka:connect-runtime, connect-api or kafka-clients
	Quarkus string // the Quarkus platform (io.quarkus.platform:quarkus-bom)
	Java    int    // the Java release the project compiles for and runs on
}

// Finding is one rule that fired for a project.
type Finding struct {
	Rule     string
	Severity string
	Message  string
	Fix      string
	Source   string
	Issue    string
}

// ErrEmbeddedVersions is returned when a declared version does not parse.
var ErrEmbeddedVersions = errors.New("setup/knowledge: bad embedded versions")

const (
	rn20  = "https://debezium.io/releases/2.0/release-notes"
	rn22  = "https://debezium.io/releases/2.2/release-notes"
	rn25  = "https://debezium.io/releases/2.5/release-notes"
	rn27  = "https://debezium.io/releases/2.7/release-notes"
	rn30  = "https://debezium.io/releases/3.0/release-notes"
	rn31  = "https://debezium.io/releases/3.1/release-notes"
	rn32  = "https://debezium.io/releases/3.2/release-notes"
	rn33  = "https://debezium.io/releases/3.3/release-notes"
	kf400 = "https://kafka.apache.org/40/documentation.html#upgrade_servers_400_notable"

	kafkaCompat = "This release has been built against Kafka Connect %s and has been tested with version %s of the Kafka brokers."
)

// embeddedRules is the shipped rule table. ValidateEmbedded checks it.
var embeddedRules = []VersionRule{
	// Java baselines.
	{
		ID: "dbz2-java11", Check: CheckJavaMin, On: OnEngine, From: "2.0.0.Alpha1", Want: "11", Severity: SeverityError,
		Title:  "Debezium 2.x needs Java 11",
		Fix:    "Compile and run the application on Java 11 or later, or stay on Debezium 1.9.",
		Source: rn20 + "#release-2.0.0-alpha1", Issue: "DBZ-4949",
		Quote: "Java 11 is required to run Debezium in any form: Kafka Connect plug-in, Debezium engine and Debezium Server",
	},
	{
		ID: "dbz3-java17", Check: CheckJavaMin, On: OnEngine, From: "3.0.0.Alpha1", Want: "17", Severity: SeverityError,
		Title:  "Debezium 3.x needs Java 17",
		Fix:    "Compile and run the application on Java 17 or later, or stay on Debezium 2.7.",
		Source: rn30 + "#release-3.0.0-alpha1", Issue: "DBZ-7224",
		Quote: "Debezium connectors now require Java 17 for runtime and Java 21 for building.",
	},
	{
		ID: "dbz3-outbox-java21", Check: CheckJavaMin, On: OnOutbox, From: "3.0.0.Alpha1", Want: "21", Severity: SeverityError,
		Title:  "The Debezium 3.x Outbox extension needs Java 21",
		Fix:    "Build and run the Quarkus application on Java 21.",
		Source: rn30 + "#release-3.0.0-alpha1", Issue: "DBZ-6795",
		Quote: "Debezium Server, Debezium Operator, and Debezium Outbox extension require Java 21 both for build and runtime",
	},
	{
		ID: "kafka4-java17", Check: CheckJavaMin, On: OnKafka, From: "4.0.0", Want: "17", Severity: SeverityError,
		Title:  "Kafka 4.x Connect runtime needs Java 17",
		Fix:    "The engine embeds the Kafka Connect runtime: run on Java 17 or later, or pin Kafka 3.9.",
		Source: kf400, Issue: "KIP-750, KIP-1013",
		Quote: "The minimum Java version required by clients and Kafka Streams applications has been increased from Java 8 to Java 11 while brokers, connect and tools now require Java 17.",
	},

	// The Kafka line each release was built and tested against (the
	// "Kafka compatibility" section of every release's notes).
	{
		ID: "kafka-line-2.7", Check: CheckKafkaLine, On: OnEngine, From: "2.7.0.Final", Until: "3.0.0.Alpha1", Want: "3.7", Severity: SeverityWarn,
		Title: "Debezium 2.7 was built against Kafka Connect 3.7", Fix: "Pin the Kafka client to 3.7.x, or remove the pin and let debezium-bom choose.",
		Source: rn27 + "#release-2.7.0-final", Quote: fmt.Sprintf(kafkaCompat, "3.7.0", "3.7.0"),
	},
	{
		ID: "kafka-line-3.0.0", Check: CheckKafkaLine, On: OnEngine, From: "3.0.0.Final", Until: "3.0.2.Final", Want: "3.8", Severity: SeverityWarn,
		Title: "Debezium 3.0.0 and 3.0.1 were built against Kafka Connect 3.8", Fix: "Pin the Kafka client to 3.8.x, or remove the pin and let debezium-bom choose.",
		Source: rn30 + "#release-3.0.0-final", Quote: fmt.Sprintf(kafkaCompat, "3.8.0", "3.8.0"),
	},
	{
		ID: "kafka-line-3.0.2", Check: CheckKafkaLine, On: OnEngine, From: "3.0.2.Final", Until: "3.1.0.Alpha1", Want: "3.9", Severity: SeverityWarn,
		Title: "Debezium 3.0.2 to 3.0.x were built against Kafka Connect 3.9", Fix: "Pin the Kafka client to 3.9.x, or remove the pin and let debezium-bom choose.",
		Source: rn30 + "#release-3.0.2-final", Issue: "DBZ-8400", Quote: fmt.Sprintf(kafkaCompat, "3.9.0", "3.9.0"),
	},
	{
		ID: "kafka-line-3.1", Check: CheckKafkaLine, On: OnEngine, From: "3.1.0.Final", Until: "3.2.0.Alpha1", Want: "3.9", Severity: SeverityWarn,
		Title: "Debezium 3.1 was built against Kafka Connect 3.9", Fix: "Pin the Kafka client to 3.9.x, or remove the pin and let debezium-bom choose.",
		Source: rn31 + "#release-3.1.0-final", Quote: fmt.Sprintf(kafkaCompat, "3.9.0", "3.9.0"),
	},
	{
		// The 3.2.0 to 3.2.6 notes say 4.0.0 (DBZ-8875), but their Maven
		// parent still pinned version.kafka 3.9.0 until 3.3.0.Alpha2 moved
		// it (DBZ-9300), so debezium-bom resolves 3.9: both lines are what
		// the release was built or tested with.
		ID: "kafka-line-3.2", Check: CheckKafkaLine, On: OnEngine, From: "3.2.0.Final", Until: "3.2.7.Final", Want: "4.0|3.9", Severity: SeverityWarn,
		Title: "Debezium 3.2.0 to 3.2.6 were built against Kafka Connect 4.0", Fix: "Pin the Kafka client to 4.0.x (the release notes) or 3.9.x (what debezium-bom resolves), or remove the pin.",
		Source: rn32 + "#release-3.2.0-final", Issue: "DBZ-8875",
		Quote: fmt.Sprintf(kafkaCompat, "4.0.0", "4.0.0") + " Debezium is now built and tested with Kafka 4.0.",
	},
	{
		ID: "kafka-line-3.2.7", Check: CheckKafkaLine, On: OnEngine, From: "3.2.7.Final", Until: "3.3.0.Alpha1", Want: "3.9", Severity: SeverityWarn,
		Title: "Debezium 3.2.7 was built against Kafka Connect 3.9", Fix: "Pin the Kafka client to 3.9.x, or remove the pin and let debezium-bom choose.",
		Source: rn32 + "#release-3.2.7-final", Quote: fmt.Sprintf(kafkaCompat, "3.9.1", "3.9.1"),
	},
	{
		ID: "kafka-line-3.3", Check: CheckKafkaLine, On: OnEngine, From: "3.3.0.Final", Until: "3.4.0.Alpha1", Want: "4.1", Severity: SeverityWarn,
		Title: "Debezium 3.3 was built against Kafka Connect 4.1", Fix: "Pin the Kafka client to 4.1.x, or remove the pin and let debezium-bom choose.",
		Source: rn33 + "#release-3.3.0-final", Quote: fmt.Sprintf(kafkaCompat, "4.1.0", "4.1.0"),
	},

	// Quarkus: the Outbox extension moved to Quarkus 3 (Jakarta EE 10), then
	// follows one Quarkus line per release.
	{
		ID: "outbox-quarkus3", Check: CheckQuarkusMajor, On: OnOutbox, From: "2.2.0.CR1", Want: "3", Severity: SeverityError,
		Title:  "The Debezium Outbox extension from 2.2 needs Quarkus 3",
		Fix:    "Move the application to Quarkus 3 (javax.* to jakarta.*), or keep the Outbox extension below 2.2.",
		Source: rn22 + "#release-2.2.0-cr1", Issue: "DBZ-6129",
		Quote: "Quarkus was upgraded to version 3. As Quarkus is now based on Jakarta EE 10 the package names has been changed from `javax.*` to `jakarta.*`.",
	},
	outboxLine("2.7.0.Final", "3.0.0.Alpha1", "3.12", "3.12.0", rn27+"#release-2.7.0-final", "DBZ-7961", "Bump Quarkus to 3.12.0 for Quarkus Outbox Extension"),
	outboxLine("3.0.0.Beta1", "3.0.2.Final", "3.14", "3.14.0", rn30+"#release-3.0.0-beta1", "DBZ-8164", "Upgrade Outbox Extension to Quarkus 3.14.0"),
	outboxLine("3.0.2.Final", "3.0.5.Final", "3.16", "3.16.3", rn30+"#release-3.0.2-final", "DBZ-8409", "Update Quarkus Outbox Extension to Quarkus 3.16.3"),
	outboxLine("3.0.5.Final", "3.1.0.Alpha1", "3.17", "3.17.3", rn30+"#release-3.0.5-final", "DBZ-8506", "Update Quarkus Outbox Extension to Quarkus 3.17.3"),
	outboxLine("3.1.0.Alpha1", "3.2.0.Alpha1", "3.17", "3.17.7", rn31+"#release-3.1.0-alpha1", "DBZ-8583", "Move to Quarkus 3.17.7 for the Outbox Extension"),
	outboxLine("3.2.0.Alpha1", "3.2.0.Beta1", "3.21", "3.21.2", rn32+"#release-3.2.0-alpha1", "DBZ-8905", "Update Outbox Extension Quarkus version to 3.21.2"),
	outboxLine("3.2.0.Beta1", "3.2.1.Final", "3.22", "3.22.2", rn32+"#release-3.2.0-beta1", "DBZ-9033", "Upgrade Outbox Extension to Quarkus 3.22.2"),
	outboxLine("3.2.1.Final", "3.3.0.Alpha1", "3.23", "3.23.4", rn32+"#release-3.2.1-final", "DBZ-9219", "Update Outbox Extension to Quarkus 3.23.4 & Align to Hibernate 7.x"),
	outboxLine("3.3.0.Alpha1", "3.4.0.Alpha1", "3.23", "3.23.4", rn33+"#release-3.3.0-alpha1", "DBZ-9219", "Update Outbox Extension to Quarkus 3.23.4 & Align to Hibernate 7.x"),

	// Engine APIs that are gone.
	{
		ID: "dbz25-embeddedengine-interface-removed", Check: CheckEngineAPI, On: OnEngine, From: "2.5.0.Alpha2", Severity: SeverityInfo,
		Title:  "The deprecated EmbeddedEngine API was removed in 2.5",
		Fix:    "Build the engine with DebeziumEngine.create(...) from debezium-api; EmbeddedEngine.create() no longer exists.",
		Source: rn25 + "#release-2.5.0-alpha2", Issue: "DBZ-7110",
		Quote: "Using deprecated embbeded engine API is no longer possible. Only Debezium Engine API is available",
	},
	{
		ID: "dbz32-embeddedengine-class-removed", Check: CheckEngineAPI, On: OnEngine, From: "3.2.0.Alpha1", Severity: SeverityInfo,
		Title:  "The legacy io.debezium.embedded.EmbeddedEngine class was removed in 3.2",
		Fix:    "Only the asynchronous engine is left (io.debezium.embedded.async.ConvertingAsyncEngineBuilderFactory, the DebeziumEngine.create default); code that names EmbeddedEngine must move to DebeziumEngine.",
		Source: rn32 + "#release-3.2.0-alpha1", Issue: "DBZ-8443",
		Quote: "Remove all the EmbeddedEngine remnants from the codebase",
	},
}

func outboxLine(from, until, line, full, source, issue, quote string) VersionRule {
	return VersionRule{
		ID: "outbox-quarkus-" + line + "-from-" + strings.ToLower(from), Check: CheckQuarkusLine, On: OnOutbox,
		From: from, Until: until, Want: line, Severity: SeverityWarn,
		Title:  "The Debezium Outbox extension " + from + " was built against Quarkus " + full,
		Fix:    "Use the Quarkus " + line + " platform, or the Outbox extension release built for your Quarkus line.",
		Source: source, Issue: issue, Quote: quote,
	}
}

// EmbeddedRules returns a copy of the shipped version rules.
func EmbeddedRules() []VersionRule {
	out := make([]VersionRule, len(embeddedRules))
	copy(out, embeddedRules)
	return out
}

var (
	ruleSourceRe = regexp.MustCompile(`^(https://debezium\.io/releases/\d+\.\d+/release-notes#release-\d+\.\d+\.\d+-(final|alpha\d+|beta\d+|cr\d+)|https://kafka\.apache\.org/\d+/documentation\.html#upgrade_[a-z0-9_]+)$`)
	ruleIDRe     = regexp.MustCompile(`^[a-z0-9][a-z0-9.-]{2,80}$`)
	wantLineRe   = regexp.MustCompile(`^\d+\.\d+(\|\d+\.\d+)*$`)
	wantIntRe    = regexp.MustCompile(`^\d{1,2}$`)
)

// ValidateRules checks a rule table: unique ids, a known check and
// component, a range that parses with From < Until, a Want of the right
// shape, a severity, and a Source that is a Debezium release note anchor (or
// the Kafka upgrade notes) whose series matches the rule's From for engine
// and outbox rules. It is part of the start-up check (ValidateEmbedded).
func ValidateRules(rules []VersionRule) error {
	seen := make(map[string]bool, len(rules))
	for _, r := range rules {
		if !ruleIDRe.MatchString(r.ID) {
			return fmt.Errorf("knowledge: rule %q: bad id", r.ID)
		}
		if seen[r.ID] {
			return fmt.Errorf("knowledge: duplicate rule %q", r.ID)
		}
		seen[r.ID] = true
		if r.Title == "" || r.Fix == "" || r.Quote == "" {
			return fmt.Errorf("knowledge: rule %s: title, fix and quote are required", r.ID)
		}
		switch r.On {
		case OnEngine, OnOutbox, OnKafka:
		default:
			return fmt.Errorf("knowledge: rule %s: unknown component %q", r.ID, r.On)
		}
		switch r.Severity {
		case SeverityError, SeverityWarn, SeverityInfo:
		default:
			return fmt.Errorf("knowledge: rule %s: unknown severity %q", r.ID, r.Severity)
		}
		from, err := parseDbzVersion(r.From)
		if err != nil {
			return fmt.Errorf("knowledge: rule %s: from: %v", r.ID, err)
		}
		if r.Until != "" {
			until, err := parseDbzVersion(r.Until)
			if err != nil {
				return fmt.Errorf("knowledge: rule %s: until: %v", r.ID, err)
			}
			if from.compare(until) >= 0 {
				return fmt.Errorf("knowledge: rule %s: range %s to %s is empty", r.ID, r.From, r.Until)
			}
		}
		switch r.Check {
		case CheckJavaMin, CheckQuarkusMajor:
			if !wantIntRe.MatchString(r.Want) {
				return fmt.Errorf("knowledge: rule %s: want %q must be a number", r.ID, r.Want)
			}
		case CheckKafkaLine, CheckQuarkusLine:
			if !wantLineRe.MatchString(r.Want) {
				return fmt.Errorf("knowledge: rule %s: want %q must be major.minor lines", r.ID, r.Want)
			}
		case CheckEngineAPI:
			if r.Want != "" {
				return fmt.Errorf("knowledge: rule %s: an engine-api rule has no want", r.ID)
			}
		default:
			return fmt.Errorf("knowledge: rule %s: unknown check %q", r.ID, r.Check)
		}
		if !ruleSourceRe.MatchString(r.Source) {
			return fmt.Errorf("knowledge: rule %s: source %q is not a release note anchor", r.ID, r.Source)
		}
		if r.On != OnKafka {
			// The anchor's release must be the rule's From (or, for a
			// java-min or quarkus-major rule, the release that made the
			// change), and its series page must hold that release.
			series := "/releases/" + from.line() + "/"
			if !strings.Contains(r.Source, series) {
				return fmt.Errorf("knowledge: rule %s: source %s is not the %s release notes", r.ID, r.Source, from.line())
			}
			anchor := "#release-" + strings.ToLower(strings.ReplaceAll(r.From, ".Final", "-final"))
			anchor = strings.NewReplacer(".alpha", "-alpha", ".beta", "-beta", ".cr", "-cr").Replace(anchor)
			if !strings.HasSuffix(r.Source, anchor) {
				return fmt.Errorf("knowledge: rule %s: source %s does not point at release %s", r.ID, r.Source, r.From)
			}
		}
	}
	return nil
}

// ValidateEmbedded is the start-up check of the embedded knowledge: the rule
// table is valid and every Debezium release in the plugin index renders a
// reference project for each build tool and source. A non-nil error means
// the shipped tables or templates are corrupt. It is meant to run at start-up
// next to Check; Check does not call it yet (a one-line follow-up in
// knowledge.go, left out of this change so it merges cleanly with U3a).
func ValidateEmbedded() error {
	if len(embeddedRules) == 0 {
		return errors.New("knowledge: embedded rule table is empty")
	}
	if err := ValidateRules(embeddedRules); err != nil {
		return err
	}
	for _, release := range Releases() {
		for _, build := range []BuildTool{BuildMaven, BuildGradle} {
			for _, src := range []SourceKind{SourcePostgres, SourceMySQL} {
				p := EmbeddedParams{
					Build: build, Source: src, GroupID: "io.piperoom.check", ArtifactID: "check",
					Release: release, Host: "localhost", Port: 5432, Database: "db", User: "u",
					PasswordSecret: "pw", TopicPrefix: "check", Tables: []string{"db.t"},
				}
				if src == SourcePostgres {
					p.SlotName = "check"
				} else {
					p.ServerID = 1
				}
				if _, err := RenderEmbedded(p); err != nil {
					return fmt.Errorf("knowledge: reference project %s/%s/%s: %w", release, build, src, err)
				}
			}
		}
	}
	return nil
}

// CheckEmbedded applies the shipped rules to a project's declared versions
// and returns the findings in rule order. A rule whose inputs are not
// declared does not fire. A Confluent Platform Kafka build ("7.5.0-ccs")
// is not held against an Apache Kafka line: CP 7.5 ships Kafka 3.5, not 7.5.
func CheckEmbedded(v EmbeddedVersions) ([]Finding, error) {
	return checkRules(embeddedRules, v)
}

func checkRules(rules []VersionRule, v EmbeddedVersions) ([]Finding, error) {
	parsed := map[RuleOn]*dbzVersion{}
	var kafkaVendor bool
	for on, raw := range map[RuleOn]string{OnEngine: v.Engine, OnOutbox: v.Outbox, OnKafka: v.Kafka} {
		raw = strings.TrimSpace(raw)
		if raw == "" {
			continue
		}
		if on == OnKafka {
			if base, ok := cutVendorSuffix(raw); ok {
				kafkaVendor = true
				raw = base
			}
		}
		pv, err := parseDbzVersion(raw)
		if err != nil {
			return nil, fmt.Errorf("%w: %s version does not parse", ErrEmbeddedVersions, on)
		}
		parsed[on] = &pv
	}
	var quarkus *dbzVersion
	if q := strings.TrimSpace(v.Quarkus); q != "" {
		pv, err := parseDbzVersion(q)
		if err != nil {
			return nil, fmt.Errorf("%w: quarkus version does not parse", ErrEmbeddedVersions)
		}
		quarkus = &pv
	}

	var out []Finding
	for _, r := range rules {
		on := parsed[r.On]
		if on == nil || (r.On == OnKafka && kafkaVendor) {
			continue
		}
		if on.compare(mustDbzVersion(r.From)) < 0 {
			continue
		}
		if r.Until != "" && on.compare(mustDbzVersion(r.Until)) >= 0 {
			continue
		}
		var msg string
		switch r.Check {
		case CheckJavaMin:
			want, _ := strconv.Atoi(r.Want)
			if v.Java == 0 || v.Java >= want {
				continue
			}
			msg = fmt.Sprintf("%s needs Java %d; the project uses Java %d", versionOf(r.On, v), want, v.Java)
		case CheckKafkaLine:
			k := parsed[OnKafka]
			if k == nil || kafkaVendor || lineIn(k.line(), r.Want) {
				continue
			}
			msg = fmt.Sprintf("debezium-embedded %s was built against Kafka %s; the project pins Kafka %s",
				v.Engine, strings.ReplaceAll(r.Want, "|", " or "), strings.TrimSpace(v.Kafka))
		case CheckQuarkusMajor:
			want, _ := strconv.Atoi(r.Want)
			if quarkus == nil || quarkus.nums[0] >= want {
				continue
			}
			msg = fmt.Sprintf("the Outbox extension %s needs Quarkus %d; the project uses Quarkus %s", v.Outbox, want, strings.TrimSpace(v.Quarkus))
		case CheckQuarkusLine:
			if quarkus == nil || lineIn(quarkus.line(), r.Want) {
				continue
			}
			msg = fmt.Sprintf("the Outbox extension %s was built against Quarkus %s; the project uses Quarkus %s", v.Outbox, r.Want, strings.TrimSpace(v.Quarkus))
		case CheckEngineAPI:
			msg = fmt.Sprintf("debezium-embedded %s: %s", v.Engine, r.Title)
		default:
			continue
		}
		out = append(out, Finding{Rule: r.ID, Severity: r.Severity, Message: msg, Fix: r.Fix, Source: r.Source, Issue: r.Issue})
	}
	return out, nil
}

func versionOf(on RuleOn, v EmbeddedVersions) string {
	switch on {
	case OnEngine:
		return "debezium-embedded " + v.Engine
	case OnOutbox:
		return "debezium-quarkus-outbox " + v.Outbox
	default:
		return "Kafka " + v.Kafka
	}
}

func lineIn(line, want string) bool {
	for _, w := range strings.Split(want, "|") {
		if w == line {
			return true
		}
	}
	return false
}

// cutVendorSuffix strips a Confluent build suffix ("-ccs", "-ce") and reports
// whether there was one.
func cutVendorSuffix(s string) (string, bool) {
	for _, suf := range []string{"-ccs", "-ce"} {
		if base, ok := strings.CutSuffix(strings.ToLower(s), suf); ok {
			return base, true
		}
	}
	return s, false
}

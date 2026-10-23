package knowledge

import (
	"errors"
	"reflect"
	"strings"
	"testing"
)

// The shipped rules pass the start-up check, and each one cites a release
// note: a Debezium release-notes anchor for the release in its From (or the
// Kafka 4.0 upgrade notes), an issue and the note's words.
func TestShippedRulesCiteReleaseNotes(t *testing.T) {
	if err := ValidateRules(embeddedRules); err != nil {
		t.Fatalf("ValidateRules on the shipped rules: %v", err)
	}
	for _, r := range EmbeddedRules() {
		if !strings.HasPrefix(r.Source, "https://debezium.io/releases/") && !strings.HasPrefix(r.Source, "https://kafka.apache.org/40/") {
			t.Errorf("rule %s: source %s is not a release note", r.ID, r.Source)
		}
		if r.Quote == "" {
			t.Errorf("rule %s has no quote", r.ID)
		}
	}
	// Every component the row names has at least one rule.
	have := map[RuleCheck]bool{}
	for _, r := range embeddedRules {
		have[r.Check] = true
	}
	for _, c := range []RuleCheck{CheckJavaMin, CheckKafkaLine, CheckQuarkusMajor, CheckQuarkusLine, CheckEngineAPI} {
		if !have[c] {
			t.Errorf("no rule checks %s", c)
		}
	}
}

// Every Debezium release in the plugin index is covered by a Kafka line
// rule, so a reference project's engine is always checked against Kafka.
func TestEveryIndexedReleaseHasAKafkaLine(t *testing.T) {
	for _, release := range Releases() {
		fs, err := CheckEmbedded(EmbeddedVersions{Engine: release, Kafka: "1.0.0"})
		if err != nil {
			t.Fatalf("%s: %v", release, err)
		}
		found := false
		for _, f := range fs {
			if strings.HasPrefix(f.Rule, "kafka-line-") {
				found = true
			}
		}
		if !found {
			t.Errorf("release %s has no kafka-line rule", release)
		}
	}
}

func TestValidateRulesRejectsBadRules(t *testing.T) {
	good := func() []VersionRule {
		out := make([]VersionRule, len(embeddedRules))
		copy(out, embeddedRules)
		return out
	}
	idx := func(rs []VersionRule, id string) int {
		for i, r := range rs {
			if r.ID == id {
				return i
			}
		}
		t.Fatalf("no rule %s", id)
		return -1
	}
	cases := []struct {
		name    string
		mutate  func([]VersionRule)
		wantErr string
	}{
		{"duplicate id", func(rs []VersionRule) { rs[1].ID = rs[0].ID }, "duplicate rule"},
		{"bad id", func(rs []VersionRule) { rs[0].ID = "Bad ID" }, "bad id"},
		{"no quote", func(rs []VersionRule) { rs[0].Quote = "" }, "quote are required"},
		{"unknown check", func(rs []VersionRule) { rs[0].Check = "vibes" }, "unknown check"},
		{"unknown component", func(rs []VersionRule) { rs[0].On = "spark" }, "unknown component"},
		{"unknown severity", func(rs []VersionRule) { rs[0].Severity = "fatal" }, "unknown severity"},
		{"from does not parse", func(rs []VersionRule) { rs[0].From = "soon" }, "from"},
		{"empty range", func(rs []VersionRule) {
			i := idx(rs, "kafka-line-3.1")
			rs[i].Until = rs[i].From
		}, "is empty"},
		{"inverted range", func(rs []VersionRule) { rs[idx(rs, "kafka-line-3.1")].Until = "3.0.0.Final" }, "is empty"},
		{"java want not a number", func(rs []VersionRule) { rs[idx(rs, "dbz3-java17")].Want = "seventeen" }, "must be a number"},
		{"line want not a line", func(rs []VersionRule) { rs[idx(rs, "kafka-line-3.1")].Want = "3" }, "major.minor"},
		{"engine-api with a want", func(rs []VersionRule) { rs[idx(rs, "dbz25-embeddedengine-interface-removed")].Want = "1" }, "has no want"},
		{"source not a release note", func(rs []VersionRule) {
			rs[0].Source = "https://stackoverflow.com/questions/1"
		}, "not a release note anchor"},
		{"source without an anchor", func(rs []VersionRule) { rs[0].Source = "https://debezium.io/releases/2.0/release-notes" }, "not a release note anchor"},
		{"source of another series", func(rs []VersionRule) {
			rs[idx(rs, "dbz3-java17")].Source = "https://debezium.io/releases/2.7/release-notes#release-3.0.0-alpha1"
		}, "is not the 3.0 release notes"},
		{"source of another release", func(rs []VersionRule) {
			rs[idx(rs, "dbz3-java17")].Source = "https://debezium.io/releases/3.0/release-notes#release-3.0.0-final"
		}, "does not point at release"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rs := good()
			tc.mutate(rs)
			err := ValidateRules(rs)
			if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("ValidateRules = %v, want an error containing %q", err, tc.wantErr)
			}
		})
	}
}

func TestCheckEmbedded(t *testing.T) {
	cases := []struct {
		name string
		in   EmbeddedVersions
		want []string // rule ids, in rule order
	}{
		{"nothing declared", EmbeddedVersions{}, nil},
		{"3.2 on Java 11", EmbeddedVersions{Engine: "3.2.0.Final", Java: 11},
			[]string{"dbz3-java17", "dbz25-embeddedengine-interface-removed", "dbz32-embeddedengine-class-removed"}},
		{"3.2 on Java 17, Kafka from the BOM", EmbeddedVersions{Engine: "3.2.0.Final", Kafka: "3.9.0", Java: 17},
			[]string{"dbz25-embeddedengine-interface-removed", "dbz32-embeddedengine-class-removed"}},
		{"3.2 with Kafka 4.0 as the notes say", EmbeddedVersions{Engine: "3.2.0.Final", Kafka: "4.0.1", Java: 17},
			[]string{"dbz25-embeddedengine-interface-removed", "dbz32-embeddedengine-class-removed"}},
		{"3.2 with Kafka 3.8", EmbeddedVersions{Engine: "3.2.0.Final", Kafka: "3.8.1", Java: 17},
			[]string{"kafka-line-3.2", "dbz25-embeddedengine-interface-removed", "dbz32-embeddedengine-class-removed"}},
		{"3.1 with Kafka 4.0 on Java 17", EmbeddedVersions{Engine: "3.1.3.Final", Kafka: "4.0.0", Java: 17},
			[]string{"kafka-line-3.1", "dbz25-embeddedengine-interface-removed"}},
		{"Kafka 4.0 on Java 11", EmbeddedVersions{Engine: "2.7.2.Final", Kafka: "4.0.0", Java: 11},
			[]string{"kafka4-java17", "kafka-line-2.7", "dbz25-embeddedengine-interface-removed"}},
		{"2.7 matched", EmbeddedVersions{Engine: "2.7.2.Final", Kafka: "3.7.0", Java: 11},
			[]string{"dbz25-embeddedengine-interface-removed"}},
		{"2.x on Java 8", EmbeddedVersions{Engine: "2.4.2.Final", Java: 8}, []string{"dbz2-java11"}},
		{"3.0.1 with Kafka 3.9", EmbeddedVersions{Engine: "3.0.1.Final", Kafka: "3.9.0", Java: 17},
			[]string{"kafka-line-3.0.0", "dbz25-embeddedengine-interface-removed"}},
		{"Confluent Kafka build is not held against Apache lines", EmbeddedVersions{Engine: "3.1.3.Final", Kafka: "7.5.0-ccs", Java: 17},
			[]string{"dbz25-embeddedengine-interface-removed"}},
		{"outbox 3.2 on an old Quarkus line", EmbeddedVersions{Outbox: "3.2.0.Final", Quarkus: "3.15.4", Java: 21},
			[]string{"outbox-quarkus-3.22-from-3.2.0.beta1"}},
		{"outbox 3.2 on its own line", EmbeddedVersions{Outbox: "3.2.0.Final", Quarkus: "3.22.3", Java: 21}, nil},
		{"outbox 3.x on Quarkus 2 and Java 17", EmbeddedVersions{Outbox: "3.1.3.Final", Quarkus: "2.16.12.Final", Java: 17},
			[]string{"dbz3-outbox-java21", "outbox-quarkus3", "outbox-quarkus-3.17-from-3.1.0.alpha1"}},
		{"outbox 2.7 matched", EmbeddedVersions{Outbox: "2.7.2.Final", Quarkus: "3.12.3", Java: 17}, nil},
		{"outbox past the table: no line rule", EmbeddedVersions{Outbox: "3.7.0.Final", Quarkus: "3.33.2", Java: 21}, nil},
		{"Java unknown: no Java finding", EmbeddedVersions{Engine: "3.2.0.Final", Kafka: "3.9.0"},
			[]string{"dbz25-embeddedengine-interface-removed", "dbz32-embeddedengine-class-removed"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			fs, err := CheckEmbedded(tc.in)
			if err != nil {
				t.Fatalf("CheckEmbedded: %v", err)
			}
			var got []string
			for _, f := range fs {
				got = append(got, f.Rule)
				if f.Source == "" || f.Message == "" || f.Fix == "" || f.Severity == "" {
					t.Errorf("finding %s is missing a field: %+v", f.Rule, f)
				}
			}
			if !reflect.DeepEqual(got, tc.want) {
				t.Errorf("rules = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestCheckEmbeddedMessages(t *testing.T) {
	fs, err := CheckEmbedded(EmbeddedVersions{Engine: "3.1.3.Final", Kafka: "4.0.0", Java: 11})
	if err != nil {
		t.Fatal(err)
	}
	byRule := map[string]Finding{}
	for _, f := range fs {
		byRule[f.Rule] = f
	}
	if got := byRule["dbz3-java17"].Message; got != "debezium-embedded 3.1.3.Final needs Java 17; the project uses Java 11" {
		t.Errorf("java message = %q", got)
	}
	if got := byRule["kafka-line-3.1"].Message; got != "debezium-embedded 3.1.3.Final was built against Kafka 3.9; the project pins Kafka 4.0.0" {
		t.Errorf("kafka message = %q", got)
	}
	if got := byRule["kafka4-java17"]; got.Severity != SeverityError || got.Source != kf400 {
		t.Errorf("kafka4-java17 = %+v", got)
	}
}

func TestCheckEmbeddedRefusesUnparseableVersions(t *testing.T) {
	for _, in := range []EmbeddedVersions{
		{Engine: "latest"},
		{Kafka: "3.9.x"},
		{Outbox: "3.2.0.Final", Quarkus: "three"},
	} {
		if _, err := CheckEmbedded(in); !errors.Is(err, ErrEmbeddedVersions) {
			t.Errorf("CheckEmbedded(%+v) err = %v, want ErrEmbeddedVersions", in, err)
		}
	}
}

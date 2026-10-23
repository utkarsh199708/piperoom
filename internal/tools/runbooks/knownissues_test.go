package runbooks

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"
	"testing"
	"testing/fstest"

	"piperoom/internal/tools"
)

func embeddedIssues(t *testing.T) *KnownIssues {
	t.Helper()
	ki, err := EmbeddedKnownIssues()
	if err != nil {
		t.Fatalf("EmbeddedKnownIssues: %v", err)
	}
	return ki
}

// Q1b: at least 40 entries over the five themes, Debezium 2.x and 3.x first,
// each citing a release note or an issue on an allowed host.
func TestEmbeddedKnownIssuesSeed(t *testing.T) {
	all := embeddedIssues(t).All()
	if len(all) < 40 {
		t.Fatalf("%d entries, want at least 40", len(all))
	}
	themes := map[string]int{}
	modern := 0
	for _, e := range all {
		themes[e.Theme]++
		if err := checkSource(e.Source); err != nil {
			t.Errorf("%s: %v", e.ID, err)
		}
		// Debezium 2.x/3.x first: a Debezium entry must be about 2.0 or later.
		if strings.HasPrefix(e.Component, "debezium") {
			if !slices.ContainsFunc(modernReleases(), e.Holds) {
				t.Errorf("%s: affected %q holds no Debezium 2.x or 3.x release", e.ID, e.Affected)
			}
			modern++
		}
	}
	for _, th := range issueThemes {
		if themes[th] < 5 {
			t.Errorf("theme %s has %d entries, want at least 5", th, themes[th])
		}
	}
	if modern < len(all)/2 {
		t.Errorf("%d of %d entries are Debezium entries, want at least half", modern, len(all))
	}
}

// modernReleases are Debezium 2.0 to 3.7 Final releases, .0 and .5 patches.
func modernReleases() []string {
	var out []string
	for major := 2; major <= 3; major++ {
		for minor := 0; minor <= 7; minor++ {
			out = append(out, fmt.Sprintf("%d.%d.0.Final", major, minor), fmt.Sprintf("%d.%d.5.Final", major, minor))
		}
	}
	return out
}

// Q1b: every renamed key resolves old -> new, including the prefix renames
// (the history producer and consumer security keys).
func TestRenamedKeysResolve(t *testing.T) {
	ki := embeddedIssues(t)
	cases := []struct{ old, new, id string }{
		{"database.server.name", "topic.prefix", "dbz-topic-prefix"},
		{"mongodb.name", "topic.prefix", "dbz-mongodb-topic-prefix"},
		{"table.whitelist", "table.include.list", "dbz-table-include-list"},
		{"table.blacklist", "table.exclude.list", "dbz-table-include-list"},
		{"schema.whitelist", "schema.include.list", "dbz-schema-include-list"},
		{"schema.blacklist", "schema.exclude.list", "dbz-schema-include-list"},
		{"database.whitelist", "database.include.list", "dbz-database-include-list"},
		{"database.blacklist", "database.exclude.list", "dbz-database-include-list"},
		{"column.whitelist", "column.include.list", "dbz-column-include-list"},
		{"column.blacklist", "column.exclude.list", "dbz-column-include-list"},
		{"collection.whitelist", "collection.include.list", "dbz-mongodb-collection-include-list"},
		{"collection.blacklist", "collection.exclude.list", "dbz-mongodb-collection-include-list"},
		{"field.blacklist", "field.exclude.list", "dbz-mongodb-collection-include-list"},
		{"database.history", "schema.history.internal", "dbz-schema-history-internal"},
		{"database.history.kafka.topic", "schema.history.internal.kafka.topic", "dbz-schema-history-internal"},
		{"database.history.kafka.bootstrap.servers", "schema.history.internal.kafka.bootstrap.servers", "dbz-schema-history-internal"},
		{"database.history.skip.unparseable.ddl", "schema.history.internal.skip.unparseable.ddl", "dbz-schema-history-internal"},
		{"database.history.store.only.captured.tables.ddl", "schema.history.internal.store.only.captured.tables.ddl", "dbz-schema-history-internal"},
		{"database.history.producer.security.protocol", "schema.history.internal.producer.security.protocol", "dbz-schema-history-internal"},
		{"database.history.consumer.security.protocol", "schema.history.internal.consumer.security.protocol", "dbz-schema-history-internal"},
		// prefix renames
		{"database.history.producer.sasl.jaas.config", "schema.history.internal.producer.sasl.jaas.config", "dbz-schema-history-internal"},
		{"database.history.consumer.ssl.truststore.location", "schema.history.internal.consumer.ssl.truststore.location", "dbz-schema-history-internal"},
		{"database.history.kafka.recovery.poll.interval.ms", "schema.history.internal.kafka.recovery.poll.interval.ms", "dbz-schema-history-internal"},
		{"heartbeat.topics.prefix", "topic.heartbeat.prefix", "dbz-topic-heartbeat-transaction"},
		{"transaction.topic", "topic.transaction", "dbz-topic-heartbeat-transaction"},
	}
	for _, tc := range cases {
		got, e, ok := ki.RenamedKey(tc.old)
		if !ok || got != tc.new || e.ID != tc.id {
			t.Errorf("RenamedKey(%q) = %q, %q, %v; want %q, %q", tc.old, got, e.ID, ok, tc.new, tc.id)
		}
	}
	// Every literal renamed key in the index resolves to its own entry.
	seen := map[string]bool{}
	for _, tc := range cases {
		seen[tc.old] = true
	}
	for _, e := range ki.All() {
		for _, r := range e.RenamedKeys {
			if strings.HasSuffix(r.Old, "*") {
				continue
			}
			if !seen[r.Old] {
				t.Errorf("renamed key %q of %s has no row in this table", r.Old, e.ID)
			}
			if got, ge, ok := ki.RenamedKey(r.Old); !ok || got != r.New || ge.ID != e.ID {
				t.Errorf("RenamedKey(%q) = %q %s %v", r.Old, got, ge.ID, ok)
			}
		}
	}
	for _, k := range []string{"topic.prefix", "table.include.list", "", "database", "database.history.", "plugin.name"} {
		if got, _, ok := ki.RenamedKey(k); ok {
			t.Errorf("RenamedKey(%q) = %q, want no rename", k, got)
		}
	}
	var nilIndex *KnownIssues
	if _, _, ok := nilIndex.RenamedKey("table.whitelist"); ok {
		t.Error("nil index must resolve nothing")
	}
}

func TestRenamedValuesResolve(t *testing.T) {
	ki := embeddedIssues(t)
	cases := map[string]string{
		"schema_only":            "no_data",
		"schema_only_recovery":   "recovery",
		"wal2json":               "pgoutput",
		"wal2json_rds_streaming": "pgoutput",
		"io.debezium.transforms.UnwrapFromEnvelope":                          "io.debezium.transforms.ExtractNewRecordState",
		"io.debezium.connector.mongodb.transforms.UnwrapFromMongoDbEnvelope": "io.debezium.connector.mongodb.transforms.ExtractNewDocumentState",
		"io.debezium.relational.history.KafkaDatabaseHistory":                "io.debezium.storage.kafka.history.KafkaSchemaHistory",
		"io.debezium.relational.history.FileDatabaseHistory":                 "io.debezium.storage.file.history.FileSchemaHistory",
	}
	for old, want := range cases {
		if got, _, ok := ki.RenamedValue(old); !ok || got != want {
			t.Errorf("RenamedValue(%q) = %q %v, want %q", old, got, ok, want)
		}
	}
}

func TestVersionCompareAndRange(t *testing.T) {
	order := []string{"1.9.7.Final", "2.0.0.Alpha1", "2.0.0.Alpha2", "2.0.0.Beta1", "2.0.0.CR1", "2.0.0", "2.0.1.Final", "2.10.0.Final", "3.0.0.Final", "10", "13.4", "42.2.0"}
	for i := 1; i < len(order); i++ {
		a, err1 := parseVersion(order[i-1])
		b, err2 := parseVersion(order[i])
		if err1 != nil || err2 != nil || a.compare(b) >= 0 {
			t.Errorf("%s < %s: %v %v %d", order[i-1], order[i], err1, err2, a.compare(b))
		}
	}
	same := [][2]string{{"2.0.0.Final", "2.0.0"}, {"2.0", "2.0.0.Final"}, {"3.6.1", "3.6.1-ccs"}, {"v3.6.1", "3.6.1"}, {"2.0.0.CR1", "2.0.0.RC1"}}
	for _, p := range same {
		a, _ := parseVersion(p[0])
		b, _ := parseVersion(p[1])
		if a.compare(b) != 0 {
			t.Errorf("%s == %s", p[0], p[1])
		}
	}
	for _, bad := range []string{"", "Final", ".", "1.2.3.4.5", "2.0.0.Alpha.x", "2.0.0.", strings.Repeat("1", 80), "x1.0"} {
		if _, err := parseVersion(bad); err == nil {
			t.Errorf("parseVersion(%q) accepted", bad)
		}
	}
	holds := []struct {
		rng, v string
		want   bool
	}{
		{"*", "0.1", true},
		{">=2.0.0.Alpha1", "2.0.0.Final", true},
		{">=2.0.0.Alpha1", "1.9.7.Final", false},
		{"<2.3.0.Alpha1", "2.2.1.Final", true},
		{"<2.3.0.Alpha1", "2.3.0.Final", false},
		{">=2.6.0.CR1 <2.7.0.Alpha1", "2.6.2.Final", true},
		{">=2.6.0.CR1 <2.7.0.Alpha1", "2.7.0.Final", false},
		{"<10", "9.6", true},
		{"<10", "10.1", false},
		{"=3.5.0", "3.5", true},
		{">3.5.0 <=3.6.0", "3.6.0", true},
	}
	for _, h := range holds {
		r, err := parseRange(h.rng)
		if err != nil {
			t.Fatalf("parseRange(%q): %v", h.rng, err)
		}
		v, _ := parseVersion(h.v)
		if r.holds(v) != h.want {
			t.Errorf("%q holds %s = %v, want %v", h.rng, h.v, !h.want, h.want)
		}
	}
	for _, bad := range []string{"", "  ", "2.0", "~2.0", ">=x", ">=3.0 <2.0", ">2.0 <2.0", "=2.0 =2.1", ">=2.0,<3.0"} {
		if _, err := parseRange(bad); err == nil {
			t.Errorf("parseRange(%q) accepted", bad)
		}
	}
}

const goodIssue = `- id: ok-issue
  title: A known issue
  component: debezium-postgres
  theme: fixed-bug
  affected: ">=2.0.0.Final <2.1.0.Final"
  fixed_in: "2.1.0.Final"
  symptoms: [alpha]
  workaround: Upgrade.
  renamed_keys:
    - {old: old.key, new: new.key}
  source: https://issues.redhat.com/browse/DBZ-1
`

// Q1a: a bad range or host (or any other broken entry) fails start-up.
func TestLoadKnownIssuesRejects(t *testing.T) {
	second := strings.Replace(goodIssue, "id: ok-issue", "id: other-issue", 1)
	cases := map[string]string{
		"bad range":         strings.Replace(goodIssue, `">=2.0.0.Final <2.1.0.Final"`, `"2.0 - 2.1"`, 1),
		"empty range":       strings.Replace(goodIssue, `">=2.0.0.Final <2.1.0.Final"`, `""`, 1),
		"empty range set":   strings.Replace(goodIssue, `">=2.0.0.Final <2.1.0.Final"`, `">=3.0 <2.0"`, 1),
		"bad fixed_in":      strings.Replace(goodIssue, `fixed_in: "2.1.0.Final"`, `fixed_in: "soon"`, 1),
		"fixed_in in range": strings.Replace(goodIssue, `fixed_in: "2.1.0.Final"`, `fixed_in: "2.0.5.Final"`, 1),
		"other host":        strings.Replace(goodIssue, "https://issues.redhat.com/browse/DBZ-1", "https://example.com/x", 1),
		"http source":       strings.Replace(goodIssue, "https://issues.redhat.com", "http://issues.redhat.com", 1),
		"github not dbz":    strings.Replace(goodIssue, "https://issues.redhat.com/browse/DBZ-1", "https://github.com/evil/repo/issues/1", 1),
		"no source":         strings.Replace(goodIssue, "  source: https://issues.redhat.com/browse/DBZ-1\n", "", 1),
		"unknown field":     goodIssue + "  severity: high\n",
		"unknown component": strings.Replace(goodIssue, "component: debezium-postgres", "component: flink", 1),
		"unknown theme":     strings.Replace(goodIssue, "theme: fixed-bug", "theme: misc", 1),
		"bad id":            strings.Replace(goodIssue, "id: ok-issue", "id: OK Issue", 1),
		"no title":          strings.Replace(goodIssue, "  title: A known issue\n", "", 1),
		"no symptoms":       strings.Replace(goodIssue, "  symptoms: [alpha]\n", "", 1),
		"blank symptom":     strings.Replace(goodIssue, "[alpha]", `[alpha, " "]`, 1),
		"no workaround":     strings.Replace(goodIssue, "  workaround: Upgrade.\n", "", 1),
		"rename same":       strings.Replace(goodIssue, "new: new.key", "new: old.key", 1),
		"rename star once":  strings.Replace(goodIssue, "old: old.key", "old: old.*", 1),
		"rename bad chars":  strings.Replace(goodIssue, "new: new.key", "new: 'new key'", 1),
		"duplicate id":      goodIssue + goodIssue,
		"duplicate renamed": goodIssue + second,
		"not a list":        "id: x\n",
		"empty file":        "",
		"too large":         goodIssue + "# " + strings.Repeat("x", maxIssueFileBytes) + "\n",
	}
	for name, body := range cases {
		t.Run(name, func(t *testing.T) {
			_, err := LoadKnownIssues(fstest.MapFS{"ok.yaml": {Data: []byte(body)}})
			if err == nil || !strings.HasPrefix(err.Error(), "runbooks: ok.yaml: ") {
				t.Fatalf("err = %v, want a wrapped runbooks: ok.yaml: error", err)
			}
		})
	}
	t.Run("duplicate id across files", func(t *testing.T) {
		_, err := LoadKnownIssues(fstest.MapFS{"a.yaml": {Data: []byte(goodIssue)}, "b.yaml": {Data: []byte(strings.Replace(goodIssue, "old.key", "other.key", 1))}})
		if err == nil || !strings.Contains(err.Error(), "duplicate known-issue id") {
			t.Fatalf("err = %v", err)
		}
	})
	t.Run("good", func(t *testing.T) {
		ki, err := LoadKnownIssues(fstest.MapFS{"ok.yaml": {Data: []byte(goodIssue)}, "README.md": {Data: []byte("ignored")}})
		if err != nil || len(ki.All()) != 1 {
			t.Fatalf("%v %v", ki, err)
		}
		e := ki.All()[0]
		if !e.Holds("2.0.3.Final") || e.Holds("2.1.0.Final") || e.Holds("garbage") {
			t.Fatalf("Holds wrong for %q", e.Affected)
		}
	})
	t.Run("new hosts accepted", func(t *testing.T) {
		for _, u := range []string{"https://issues.redhat.com/browse/DBZ-4156", "https://github.com/debezium/dbz/issues/1489"} {
			if err := checkSource(u); err != nil {
				t.Errorf("%s: %v", u, err)
			}
		}
		if err := checkSource("https://user@github.com/debezium/dbz"); err == nil {
			t.Error("userinfo accepted")
		}
	})
}

// rankIssues: old-strong matches the query best but only before 2.0;
// new-medium matches well from 2.0; new-weak matches one word from 2.0.
const rankIssues = `- id: old-strong
  title: zebra stripes zebra
  component: kafka
  theme: fixed-bug
  affected: "<2.0"
  fixed_in: "2.0"
  symptoms: [zebra, stripes, zebra stripes]
  workaround: Upgrade.
  source: https://kafka.apache.org/documentation/
- id: new-medium
  title: zebra stripes
  component: kafka
  theme: fixed-bug
  affected: ">=2.0"
  symptoms: [zebra, stripes, horse, grass, savanna]
  workaround: Wait.
  source: https://kafka.apache.org/documentation/
- id: new-weak
  title: something else entirely about other animals
  component: kafka
  theme: fixed-bug
  affected: ">=2.0"
  symptoms: [lion, giraffe, elephant, hippo, rhino, buffalo]
  workaround: Look at the zebra only in passing; nothing else here relates.
  source: https://kafka.apache.org/documentation/
`

func issuesOf(t *testing.T, res tools.Result) []issueHit {
	t.Helper()
	d, _ := res.Data.(map[string]any)["known_issues"].([]issueHit)
	return d
}

func run(t *testing.T, tl *Tool, in any) tools.Result {
	t.Helper()
	b, _ := json.Marshal(in)
	res, err := tl.Run(context.Background(), b)
	if err != nil {
		t.Fatalf("Run(%s): %v", b, err)
	}
	return res
}

// Q1a: with versions, entries whose range holds the running version rank
// first and the result names the fixed-in release.
func TestLookupWithVersions(t *testing.T) {
	tl := NewWithKnownIssues(embedded(t), embeddedIssues(t))

	res := run(t, tl, map[string]any{"query": "pg_replication_slot_advance function does not exist",
		"versions": []map[string]string{{"component": "debezium-postgres", "version": "2.2.1.Final"}}})
	ki := issuesOf(t, res)
	if len(ki) == 0 || ki[0].ID != "dbz-pg-slot-advance-pg10" || ki[0].Affects != "yes" || ki[0].Running != "2.2.1.Final" {
		t.Fatalf("known issues %+v", ki)
	}
	if !strings.HasPrefix(res.Summary, "Known issues:\n[K1] dbz-pg-slot-advance-pg10: ") ||
		!strings.Contains(res.Summary, "running 2.2.1.Final is affected") || !strings.Contains(res.Summary, "Fixed in 2.3.0.Alpha1.") {
		t.Fatalf("summary:\n%s", res.Summary)
	}

	// A fixed release is not affected; the issue still shows, after the runbooks.
	res = run(t, tl, map[string]any{"query": "pg_replication_slot_advance function does not exist", "max_results": 5,
		"versions": []map[string]string{{"component": "debezium", "version": "2.4.0.Final"}}})
	ki = issuesOf(t, res)
	i := slices.IndexFunc(ki, func(h issueHit) bool { return h.ID == "dbz-pg-slot-advance-pg10" })
	if i < 0 || ki[i].Affects != "no" || !strings.Contains(res.Summary, "running 2.4.0.Final is not affected") {
		t.Fatalf("%+v\n%s", ki, res.Summary)
	}
	for _, h := range ki[:i] {
		if h.Affects != "yes" {
			t.Errorf("%s (affects %s) ranks above the unaffected match", h.ID, h.Affects)
		}
	}

	// Ranking: on 2.7 the GRANT SENSITIVE_VARIABLES_OBSERVER bug (fixed in 3.1)
	// holds and the AUTHENTICATION_POLICY_ADMIN one (fixed in 2.3) does not.
	q := "ParsingException DDL statement couldn't be parsed GRANT"
	with := issuesOf(t, run(t, tl, map[string]any{"query": q, "max_results": 5,
		"versions": []map[string]string{{"component": "debezium-mysql", "version": "2.7.0.Final"}}}))
	if len(with) == 0 || with[0].ID != "dbz-mysql-ddl-grant-sensitive-variables" || with[0].Affects != "yes" {
		t.Fatalf("with versions the affected entry must lead: %+v", with)
	}
	idx := slices.IndexFunc(with, func(h issueHit) bool { return h.ID == "dbz-mysql-ddl-authentication-policy-admin" })
	if idx < 0 || with[idx].Affects != "no" {
		t.Fatalf("the unaffected entry must still be listed after: %+v", with)
	}

	// The version, not the score, decides the order between relevant entries;
	// a weak match does not get the boost.
	ix, err := LoadKnownIssues(fstest.MapFS{"x.yaml": {Data: []byte(rankIssues)}})
	if err != nil {
		t.Fatal(err)
	}
	small := NewWithKnownIssues(nil, ix)
	order := func(v string) []string {
		in := map[string]any{"query": "zebra zebra stripes", "max_results": 5}
		if v != "" {
			in["versions"] = []map[string]string{{"component": "kafka", "version": v}}
		}
		var ids []string
		for _, h := range issuesOf(t, run(t, small, in)) {
			ids = append(ids, h.ID+"/"+h.Affects)
		}
		return ids
	}
	for v, want := range map[string][]string{
		"":      {"old-strong/unknown", "new-medium/unknown", "new-weak/unknown"},
		"1.5.0": {"old-strong/yes", "new-medium/no", "new-weak/no"},
		"3.0.0": {"new-medium/yes", "old-strong/no", "new-weak/yes"},
	} {
		if got := order(v); !slices.Equal(got, want) {
			t.Errorf("versions %q: order %v, want %v", v, got, want)
		}
	}

	// A rename says no release fixes it and lists the new key.
	res = run(t, tl, map[string]any{"query": "database.server.name",
		"versions": []map[string]string{{"component": "debezium-postgres", "version": "2.1.4.Final"}}})
	if ki = issuesOf(t, res); len(ki) == 0 || ki[0].ID != "dbz-topic-prefix" ||
		!strings.Contains(res.Summary, "No release fixes it.") || !strings.Contains(res.Summary, "Renamed: database.server.name -> topic.prefix") {
		t.Fatalf("%+v\n%s", ki, res.Summary)
	}

	// Data is JSON with the documented field names; a rename has no fixed_in.
	b, _ := json.Marshal(res.Data)
	for _, f := range []string{`"known_issues":[`, `"affects":"yes"`, `"running":"2.1.4.Final"`, `"renamed_keys":[{"old":"database.server.name","new":"topic.prefix"}]`} {
		if !strings.Contains(string(b), f) {
			t.Errorf("data lacks %s: %s", f, b)
		}
	}
}

func TestLookupKnownIssueOnlyAndNoIndex(t *testing.T) {
	// No runbooks at all: the known issue still answers.
	tl := NewWithKnownIssues(nil, embeddedIssues(t))
	res := run(t, tl, map[string]any{"query": "wal2json_rds_streaming"})
	if hits := res.Data.(map[string]any)["hits"].([]hit); len(hits) != 0 {
		t.Fatalf("hits %v", hits)
	}
	if !strings.HasPrefix(res.Summary, `No runbook section matched "wal2json_rds_streaming".`) || !strings.Contains(res.Summary, "[K1] dbz-wal2json-removed") {
		t.Fatalf("summary:\n%s", res.Summary)
	}
	// An entry for every version is "any" and never outranks a version match.
	res = run(t, NewWithKnownIssues(embedded(t), embeddedIssues(t)), map[string]any{"query": "could not access file decoderbufs", "max_results": 5,
		"versions": []map[string]string{{"component": "debezium-postgres", "version": "2.1.0.Final"}}})
	ki := issuesOf(t, res)
	if i := slices.IndexFunc(ki, func(h issueHit) bool { return h.ID == "pg-decoderbufs-default-missing" }); i < 0 || ki[i].Affects != "any" ||
		!strings.Contains(res.Summary, "(debezium-postgres *, all versions)") {
		t.Fatalf("%+v\n%s", ki, res.Summary)
	}
	// Without an index the result has no known_issues key: existing callers see no change.
	res = run(t, New(embedded(t)), map[string]any{"query": "slot"})
	if _, ok := res.Data.(map[string]any)["known_issues"]; ok {
		t.Fatal("known_issues present without an index")
	}
	if tl.Kind() != tools.KindRead {
		t.Fatal("runbook_lookup must stay a read tool")
	}
}

func TestVersionsBadInput(t *testing.T) {
	tl := NewWithKnownIssues(embedded(t), embeddedIssues(t))
	many := make([]map[string]string, maxVersions+1)
	for i := range many {
		many[i] = map[string]string{"component": issueComponents[i%len(issueComponents)], "version": fmt.Sprint(i + 1)}
	}
	for name, vs := range map[string]any{
		"unknown component": []map[string]string{{"component": "flink", "version": "1.0"}},
		"bad version":       []map[string]string{{"component": "kafka", "version": "latest"}},
		"empty version":     []map[string]string{{"component": "kafka", "version": ""}},
		"duplicate":         []map[string]string{{"component": "kafka", "version": "3.6.0"}, {"component": "kafka", "version": "3.7.0"}},
		"too many":          many,
		"not a list":        "kafka 3.6",
		"extra field":       []map[string]string{{"component": "kafka", "version": "3.6.0", "x": "y"}},
	} {
		b, _ := json.Marshal(map[string]any{"query": "slot", "versions": vs})
		if _, err := tl.Run(context.Background(), b); !errors.Is(err, tools.ErrBadInput) {
			t.Errorf("%s: err = %v, want ErrBadInput", name, err)
		}
	}
}

func TestRegisterServesKnownIssues(t *testing.T) {
	reg := tools.NewRegistry()
	if err := Register(reg); err != nil {
		t.Fatal(err)
	}
	tl, ok := reg.Get(ToolName)
	if !ok {
		t.Fatal("not registered")
	}
	res, err := tl.Run(context.Background(), json.RawMessage(`{"query":"table.whitelist"}`))
	if err != nil {
		t.Fatal(err)
	}
	if ki, _ := res.Data.(map[string]any)["known_issues"].([]issueHit); len(ki) == 0 || ki[0].ID != "dbz-table-include-list" {
		t.Fatalf("known issues %+v", ki)
	}
	if len(res.Summary) > tools.MaxSummaryBytes {
		t.Fatalf("summary %d bytes", len(res.Summary))
	}
}

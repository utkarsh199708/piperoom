package knowledge

import (
	"errors"
	"maps"
	"net/url"
	"slices"
	"strings"
	"testing"
	"testing/fstest"

	"piperoom/internal/tools/runbooks"
)

func loadIndex(t *testing.T) *Index {
	t.Helper()
	ix, err := LoadIndex()
	if err != nil {
		t.Fatalf("LoadIndex = %v", err)
	}
	return ix
}

// The "Done when" of U3a: at least 60 entries, each citing the Debezium,
// Kafka, Confluent or vendor docs with a URL and naming the artifact its
// keys were read from.
func TestShippedIndexCoverage(t *testing.T) {
	ix := loadIndex(t)
	entries := ix.Entries()
	if len(entries) < 60 {
		t.Fatalf("index has %d entries, want at least 60", len(entries))
	}
	perKind := map[Kind]int{}
	for _, e := range entries {
		perKind[e.Kind]++
		u, err := url.Parse(e.Doc)
		if err != nil || u.Scheme != "https" || !slices.Contains(docHosts, u.Host) {
			t.Errorf("%s: doc %q is not an https docs URL", e.Name, e.Doc)
		}
		if e.Source == "" {
			t.Errorf("%s: no source", e.Name)
		}
		if e.Kind == KindSink && e.Install == InstallUnpinned && e.UnpinnedReason == "" {
			t.Errorf("%s: unpinned without a reason", e.Name)
		}
	}
	for _, k := range kinds {
		if perKind[k] == 0 {
			t.Errorf("no %s entries", k)
		}
	}
	// The SMTs and sinks the row names explicitly.
	for _, name := range []string{
		"ExtractNewRecordState", "ByLogicalTableRouter", "io.debezium.transforms.Filter", "ContentBasedRouter",
		"TimezoneConverter", "io.debezium.transforms.outbox.EventRouter",
		"io.debezium.connector.jdbc.JdbcSinkConnector", "ElasticsearchSinkConnector", "S3SinkConnector",
		"BigQuerySinkConnector", "SnowflakeSinkConnector", "io.aiven.kafka.connect.http.HttpSinkConnector", "http",
	} {
		if _, err := ix.Lookup("", name); err != nil {
			t.Errorf("Lookup(%q) = %v", name, err)
		}
	}
}

// The table test of the "Done when": every class Q1b records as renamed
// resolves old FQCN -> new FQCN, and to the new class's index entry when
// the index has one.
func TestResolveRenamedClasses(t *testing.T) {
	ix := loadIndex(t)
	cases := []struct {
		old, new, issue, entry string // entry "" = not an SMT or sink
	}{
		{"io.debezium.transforms.UnwrapFromEnvelope", "io.debezium.transforms.ExtractNewRecordState", "dbz-unwrap-class-renamed", "extract-new-record-state"},
		{"io.debezium.connector.mongodb.transforms.UnwrapFromMongoDbEnvelope", "io.debezium.connector.mongodb.transforms.ExtractNewDocumentState", "dbz-unwrap-class-renamed", "extract-new-document-state"},
		{"io.debezium.relational.history.KafkaDatabaseHistory", "io.debezium.storage.kafka.history.KafkaSchemaHistory", "dbz-schema-history-internal", ""},
		{"io.debezium.relational.history.FileDatabaseHistory", "io.debezium.storage.file.history.FileSchemaHistory", "dbz-schema-history-internal", ""},
	}
	got := ix.RenamedClasses()
	want := map[string]string{}
	for _, c := range cases {
		want[c.old] = c.new
	}
	if !maps.Equal(got, want) {
		t.Errorf("RenamedClasses = %v\nwant %v (a new Q1b class rename needs a row here)", got, want)
	}
	for _, c := range cases {
		t.Run(c.old, func(t *testing.T) {
			res, err := ix.ResolveClass(c.old, "")
			if err != nil {
				t.Fatalf("ResolveClass = %v", err)
			}
			if !res.Renamed || res.Class != c.new || res.IssueID != c.issue || res.Source == "" || res.Affected == "" {
				t.Errorf("ResolveClass = %+v, want %s from %s", res, c.new, c.issue)
			}
			switch {
			case c.entry == "" && res.Entry != nil:
				t.Errorf("entry = %s, want none", res.Entry.Name)
			case c.entry != "" && (res.Entry == nil || res.Entry.Name != c.entry):
				t.Errorf("entry = %v, want %s", res.Entry, c.entry)
			}
		})
	}
	// A current class resolves to itself; a variant is kept.
	res, err := ix.ResolveClass("org.apache.kafka.connect.transforms.ReplaceField$Value", "")
	if err != nil || res.Renamed || res.Entry == nil || res.Entry.Name != "replace-field" || res.Class != "org.apache.kafka.connect.transforms.ReplaceField$Value" {
		t.Errorf("ResolveClass(ReplaceField$Value) = %+v, %v", res, err)
	}
	if _, err := ix.ResolveClass("org.apache.kafka.connect.transforms.ValueToKey$Value", ""); !errors.Is(err, ErrUnknownEntry) {
		t.Errorf("ResolveClass(ValueToKey$Value) err = %v, want ErrUnknownEntry (no variants)", err)
	}
	if _, err := ix.ResolveClass("com.example.NoSuchTransform", ""); !errors.Is(err, ErrUnknownEntry) {
		t.Errorf("ResolveClass(unknown) err = %v, want ErrUnknownEntry", err)
	}
}

func TestLookup(t *testing.T) {
	ix := loadIndex(t)
	cases := []struct {
		kind Kind
		name string
		want string // entry name; "" = ErrUnknownEntry
	}{
		{KindSMT, "extract-new-record-state", "extract-new-record-state"},
		{KindSMT, "io.debezium.transforms.ExtractNewRecordState", "extract-new-record-state"},
		{KindSMT, "ExtractNewRecordState", "extract-new-record-state"},
		{KindSMT, "org.apache.kafka.connect.transforms.InsertField$Value", "insert-field"},
		{KindSMT, "InsertField$Key", "insert-field"},
		{KindSMT, "Filter", ""}, // Kafka's and Debezium's: ambiguous
		{KindSMT, "org.apache.kafka.connect.transforms.Filter", "kafka-filter"},
		{KindPredicate, "TopicNameMatches", "topic-name-matches"},
		{KindSMT, "TopicNameMatches", ""}, // a predicate, not an SMT
		{KindSink, "http", "server-http"},
		{KindSink, "JdbcSinkConnector", ""}, // Debezium's and Confluent's
		{KindSink, "io.confluent.connect.jdbc.JdbcSinkConnector", "confluent-jdbc-sink"},
		{"", "kinesis", "server-kinesis"},
		{KindSink, "NoSuchSink", ""},
		// A $variant is checked, not dropped.
		{KindSMT, "org.apache.kafka.connect.transforms.ValueToKey$Value", ""}, // no variants
		{KindSMT, "ValueToKey$Key", ""},
		{KindSMT, "org.apache.kafka.connect.transforms.ValueToKey", "value-to-key"},
		{KindSMT, "InsertField$", ""},       // empty variant
		{KindSMT, "InsertField$Header", ""}, // not Key or Value
		{KindSMT, "ExtractNewRecordState$Value", ""},
		{KindSMT, "extract-new-record-state$Value", ""}, // an index name takes no variant
		{"", "http$Value", ""},                          // nor a sink type
	}
	for _, c := range cases {
		got, err := ix.Lookup(c.kind, c.name)
		if c.want == "" {
			if !errors.Is(err, ErrUnknownEntry) {
				t.Errorf("Lookup(%s, %q) = %s, %v; want ErrUnknownEntry", c.kind, c.name, got.Name, err)
			}
			continue
		}
		if err != nil || got.Name != c.want {
			t.Errorf("Lookup(%s, %q) = %s, %v; want %s", c.kind, c.name, got.Name, err, c.want)
		}
	}
	e, _ := ix.Lookup(KindSMT, "replace-field")
	if k, ok := e.Key("blacklist"); !ok || k.Name != "exclude" {
		t.Errorf("replace-field Key(blacklist) = %+v, %v; want exclude", k, ok)
	}
}

func TestSupports(t *testing.T) {
	ix := loadIndex(t)
	cases := []struct {
		entry, release string
		want           Support
	}{
		{"timezone-converter", "2.3.0.Final", Unverified},
		{"timezone-converter", "2.4.0.Final", Supported},
		{"timezone-converter", "3.2.0.Final", Supported},
		{"snowflake-sink", "3.5.4", Supported},
		{"snowflake-sink", "4.0.0", Removed},
		{"snowflake-streaming-sink", "4.2.2", Supported},
		{"kafka-filter", "2.5.0", Unverified},
		{"elasticsearch-sink", "15.0.0", Supported},
	}
	for _, c := range cases {
		e, err := ix.Lookup("", c.entry)
		if err != nil {
			t.Fatal(err)
		}
		got, err := e.Supports(c.release)
		if err != nil || got != c.want {
			t.Errorf("%s.Supports(%s) = %v, %v; want %v", c.entry, c.release, got, err, c.want)
		}
	}
	e, _ := ix.Lookup("", "timezone-converter")
	if _, err := e.Supports("not-a-release"); err == nil {
		t.Error("Supports(not-a-release) = nil error")
	}
}

func TestArchive(t *testing.T) {
	ix := loadIndex(t)
	get := func(name string) Entry {
		e, err := ix.Lookup("", name)
		if err != nil {
			t.Fatal(err)
		}
		return e
	}
	a, err := ix.Archive(get("debezium-jdbc-sink"), "3.2.0.Final")
	if err != nil || !strings.HasPrefix(a.Digest, "sha512:4c6102e1") || a.Source != a.URL+".sha512" {
		t.Errorf("jdbc sink 3.2.0.Final = %+v, %v", a, err)
	}
	// An SMT inside the JDBC sink plugin resolves to the sink's archive.
	b, err := ix.Archive(get("jdbc-field-name-transformation"), "3.2.0.Final")
	if err != nil || b != a {
		t.Errorf("jdbc field-name SMT = %+v, %v; want the sink archive", b, err)
	}
	// The MongoDB sink ships in the MongoDB connector plugin of plugins.go.
	m, err := ix.Archive(get("debezium-mongodb-sink"), "3.2.0.Final")
	if p, _ := ResolvePlugin("3.2.0.Final", "mongodb"); err != nil || m.Digest != p.Digest || m.URL != p.ArtifactURL() {
		t.Errorf("mongodb sink = %+v, %v; want plugins.go mongodb 3.2.0.Final", m, err)
	}
	if _, err := ix.Archive(get("debezium-jdbc-sink"), "9.9.9.Final"); !errors.Is(err, ErrUnknownRelease) {
		t.Errorf("jdbc sink 9.9.9: err = %v, want ErrUnknownRelease", err)
	}
	for _, name := range []string{"elasticsearch-sink", "s3-sink", "bigquery-sink", "confluent-jdbc-sink", "aiven-http-sink"} {
		if _, err := ix.Archive(get(name), "1.0.0"); !errors.Is(err, ErrUnpinned) {
			t.Errorf("%s: err = %v, want ErrUnpinned (plugin_install must refuse it)", name, err)
		}
	}
	if _, err := ix.Archive(get("extract-new-record-state"), "3.2.0.Final"); !errors.Is(err, ErrBundled) {
		t.Errorf("bundled SMT: err = %v, want ErrBundled", err)
	}
}

// baseIndex is a small valid index; each case of TestParseIndexRejects
// replaces one line of it to make it invalid.
const baseIndex = `
- name: sink-a
  kind: sink
  component: debezium
  runtime: connect
  class: io.example.SinkA
  versions: ">=3.0.0.Final"
  summary: s
  install: pinned
  runtime_image: connect
  archives:
    - release: 3.2.0.Final
      url: https://repo1.maven.org/maven2/io/debezium/debezium-connector-jdbc/3.2.0.Final/debezium-connector-jdbc-3.2.0.Final-plugin.tar.gz
      digest: sha512:4c6102e1c9266c00eeb3179cc0e218c5c6db0b96f105b997a3157c6da833fb2c0b6118704b465edb022853dc86ad45d6da36caf8c13e40bfc6fe3dc2819dcf00
      source: https://repo1.maven.org/maven2/io/debezium/debezium-connector-jdbc/3.2.0.Final/debezium-connector-jdbc-3.2.0.Final-plugin.tar.gz.sha512
  keys:
    - {name: mode, default: "a", values: [a, b], since: 3.1.0.Final}
    - {name: url, required: true}
  checks: [c]
  doc: https://debezium.io/documentation/reference/3.2/connectors/jdbc.html
  source: https://repo1.maven.org/maven2/io/debezium/debezium-sink/3.2.0.Final/debezium-sink-3.2.0.Final-sources.jar
- name: smt-b
  kind: smt
  component: kafka
  runtime: connect
  class: org.example.SmtB
  variants: [Key, Value]
  versions: ">=2.6.0"
  summary: s
  install: bundled
  plugin_ref: sink-a
  checks: [c]
  doc: https://kafka.apache.org/documentation/#connect_transforms
  source: https://repo1.maven.org/maven2/org/apache/kafka/connect-transforms/4.1.0/connect-transforms-4.1.0-sources.jar
`

func parseYAML(t *testing.T, doc string) error {
	t.Helper()
	ki, err := runbooks.EmbeddedKnownIssues()
	if err != nil {
		t.Fatal(err)
	}
	_, err = ParseIndex(fstest.MapFS{"x.yaml": {Data: []byte(doc)}}, ki)
	return err
}

// A bad version range, digest, URL or reference fails the parse, so it
// fails start-up (Check calls LoadIndex, which calls ParseIndex).
func TestParseIndexRejects(t *testing.T) {
	if err := parseYAML(t, baseIndex); err != nil {
		t.Fatalf("base index: %v", err)
	}
	cases := []struct {
		name, old, new, wantErr string
	}{
		{"inverted range", `versions: ">=3.0.0.Final"`, `versions: ">=3.0.0.Final <2.0.0.Final"`, "holds no release"},
		{"unparseable range", `versions: ">=3.0.0.Final"`, `versions: ">=three"`, "no number"},
		{"range without operator", `versions: ">=3.0.0.Final"`, `versions: "3.0.0.Final"`, "must start with"},
		{"unknown release word", `versions: ">=3.0.0.Final"`, `versions: ">=3.0.0.Finale"`, "suffix"},
		{"empty range", `versions: ">=3.0.0.Final"`, `versions: ""`, "range is empty"},
		{"digest too short", `digest: sha512:4c6102e1`, `digest: sha512:4c61`, "want 128"},
		{"digest not hex", `digest: sha512:4c6102e1`, `digest: sha512:zz6102e1`, "non-hex"},
		{"digest sha256", `digest: sha512:4c6102e1c9266c00eeb3179cc0e218c5c6db0b96f105b997a3157c6da833fb2c0b6118704b465edb022853dc86ad45d6da36caf8c13e40bfc6fe3dc2819dcf00`, `digest: sha256:` + strings.Repeat("a", 64), "must be sha512"},
		{"digest no algorithm", `digest: sha512:`, `digest: `, "missing algorithm"},
		{"archive off Maven Central", `url: https://repo1.maven.org/maven2/`, `url: https://evil.example/maven2/`, "Maven Central"},
		{"archive url escapes", `url: https://repo1.maven.org/maven2/io/`, `url: https://repo1.maven.org/maven2/../io/`, "Maven Central"},
		{"archive source elsewhere", `source: https://repo1.maven.org/maven2/io/debezium/debezium-connector-jdbc/3.2.0.Final/debezium-connector-jdbc-3.2.0.Final-plugin.tar.gz.sha512`, `source: https://evil.example/x.sha512`, "source must be"},
		{"archive release outside range", `- release: 3.2.0.Final`, `- release: 2.7.2.Final`, "outside the entry's versions"},
		{"unknown runtime image", `runtime_image: connect`, `runtime_image: nope`, "not allow-listed"},
		{"unknown plugin_ref", `plugin_ref: sink-a`, `plugin_ref: nope`, "neither an index entry"},
		{"required with default", `{name: url, required: true}`, `{name: url, required: true, default: "x"}`, "has no default"},
		{"default not a value", `default: "a", values`, `default: "c", values`, "not one of its values"},
		{"since outside range", `since: 3.1.0.Final`, `since: 2.0.0.Final`, "outside the entry's versions"},
		{"duplicate key", `{name: url, required: true}`, `{name: mode, required: true}`, "listed twice"},
		{"duplicate class", `class: org.example.SmtB`, `class: io.example.SinkA`, "is also entry"},
		{"duplicate name", `name: smt-b`, `name: sink-a`, "duplicate entry name"},
		{"renamed class indexed", `class: org.example.SmtB`, `class: io.debezium.transforms.UnwrapFromEnvelope`, "was renamed to io.debezium.transforms.ExtractNewRecordState"},
		{"doc off the allowed hosts", `doc: https://kafka.apache.org/`, `doc: https://evil.example/`, "doc:"},
		{"doc over http", `doc: https://kafka.apache.org/`, `doc: http://kafka.apache.org/`, "doc:"},
		{"source off the allowed hosts", `source: https://repo1.maven.org/maven2/org/apache`, `source: https://evil.example/maven2/org/apache`, "source:"},
		{"unknown field", `summary: s
  install: bundled`, `summary: s
  colour: red
  install: bundled`, "colour"},
		{"unpinned without reason", `install: bundled`, `install: unpinned`, "unpinned_reason"},
		{"variant on a non-Kafka entry", `component: kafka`, `component: debezium`, "variant"},
		{"server entry with a class", `runtime: connect
  class: io.example.SinkA`, `runtime: server
  class: io.example.SinkA`, "server entry"},
		{"no checks", `plugin_ref: sink-a
  checks: [c]`, `plugin_ref: sink-a
  checks: []`, "check is required"},
		{"bad class name", `class: org.example.SmtB`, `class: SmtB`, "Java class"},
		{"unknown kind", `kind: smt`, `kind: tuning`, "kind"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if !strings.Contains(baseIndex, c.old) {
				t.Fatalf("base index has no %q", c.old)
			}
			err := parseYAML(t, strings.Replace(baseIndex, c.old, c.new, 1))
			if err == nil {
				t.Fatal("ParseIndex = nil, want an error")
			}
			if !strings.Contains(err.Error(), c.wantErr) {
				t.Fatalf("ParseIndex error = %q, want it to contain %q", err, c.wantErr)
			}
		})
	}
	// A Debezium entry whose range excludes the pinned image's release.
	imageOutOfRange := `
- name: smt-c
  kind: smt
  component: debezium
  runtime: connect
  class: io.example.SmtC
  versions: ">=2.0.0.Final <3.0.0.Final"
  summary: s
  install: bundled
  runtime_image: connect
  checks: [c]
  doc: https://debezium.io/documentation/reference/3.2/transformations/index.html
  source: https://repo1.maven.org/maven2/io/debezium/debezium-core/3.2.0.Final/debezium-core-3.2.0.Final-sources.jar
`
	if err := parseYAML(t, imageOutOfRange); err == nil || !strings.Contains(err.Error(), "debezium/connect:3.2.0.Final is outside") {
		t.Errorf("image tag outside range: err = %v", err)
	}
	if _, err := ParseIndex(fstest.MapFS{}, nil); err == nil {
		t.Error("ParseIndex on no files = nil, want an error")
	}
	if _, err := ParseIndex(fstest.MapFS{"x.yaml": {Data: []byte("[]")}}, nil); err == nil {
		t.Error("ParseIndex on an empty file = nil, want an error")
	}
}

func TestParseRelease(t *testing.T) {
	ordered := []string{"0.10.2.0", "2.4.0.Alpha1", "2.4.0.Beta2", "2.4.0.CR1", "2.4.0.Final", "2.4.0.1", "3.0.0-rc1", "3.0.0", "3.8.0", "4.2.2"}
	for i := 1; i < len(ordered); i++ {
		a, errA := parseRelease(ordered[i-1])
		b, errB := parseRelease(ordered[i])
		if errA != nil || errB != nil {
			t.Fatalf("parse %s, %s: %v, %v", ordered[i-1], ordered[i], errA, errB)
		}
		if a.compare(b) >= 0 {
			t.Errorf("%s should sort before %s", ordered[i-1], ordered[i])
		}
	}
	if a, _ := parseRelease("3.2.0"); a.compare(release{nums: []int{3, 2}, rank: rankFinal}) != 0 {
		t.Error("3.2.0 should equal 3.2")
	}
	for _, bad := range []string{"", "Final", "3.2.0.", "3.2.0Final", "1.2.3.4.5", "3.2.0.Final1", "3.2.0.SNAPSHOT", strings.Repeat("1", 70)} {
		if _, err := parseRelease(bad); err == nil {
			t.Errorf("parseRelease(%q) = nil error", bad)
		}
	}
}

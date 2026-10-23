package knowledge

import (
	"errors"
	"strings"
	"testing"
	"testing/fstest"

	"piperoom/internal/actions"
	"piperoom/internal/redact"
	"piperoom/internal/tools/runbooks"
)

func parseIndexYAML(t *testing.T, doc string) (*Index, error) {
	t.Helper()
	ki, err := runbooks.EmbeddedKnownIssues()
	if err != nil {
		t.Fatal(err)
	}
	return ParseIndex(fstest.MapFS{"x.yaml": {Data: []byte(doc)}}, ki)
}

const (
	baseArchiveURL = `url: https://repo1.maven.org/maven2/io/debezium/debezium-connector-jdbc/3.2.0.Final/debezium-connector-jdbc-3.2.0.Final-plugin.tar.gz`
	baseSourceLine = `source: https://repo1.maven.org/maven2/io/debezium/debezium-connector-jdbc/3.2.0.Final/debezium-connector-jdbc-3.2.0.Final-plugin.tar.gz.sha512`
	baseModeKey    = `{name: mode, default: "a", values: [a, b], since: 3.1.0.Final}`
	baseSmtInstall = "install: bundled\n  plugin_ref: sink-a"
)

// The validation rules TestParseIndexRejects does not reach: archive URLs,
// install modes, plugin_ref, citations and the release-bound key fields.
// Each case replaces one fragment of baseIndex; wantErr "" means the
// result is valid.
func TestParseIndexRules(t *testing.T) {
	cases := []struct {
		name, old, new, wantErr string
	}{
		// Archive URLs: Maven Central only, a release path, an archive
		// extension, and a digest source that is the .sha512 or the archive.
		{"archive url over http", baseArchiveURL, strings.Replace(baseArchiveURL, "https://", "http://", 1), "Maven Central"},
		{"archive url with a query", baseArchiveURL, baseArchiveURL + "?x=1", "Maven Central"},
		{"archive url with a fragment", baseArchiveURL, baseArchiveURL + "#x", "Maven Central"},
		{"archive url names another release", baseArchiveURL, strings.Replace(baseArchiveURL, "/3.2.0.Final/", "/3.1.3.Final/", 1), "naming release 3.2.0.Final"},
		{"archive url not an archive", baseArchiveURL, strings.Replace(baseArchiveURL, ".tar.gz", ".exe", 1), ".tar.gz, .jar or .zip"},
		{"archive url with a dot segment", baseArchiveURL, strings.Replace(baseArchiveURL, "/io/debezium/", "/io/./debezium/", 1), "Maven Central"},
		{"archive source is the .sha1", baseSourceLine, strings.Replace(baseSourceLine, ".sha512", ".sha1", 1), "source must be"},
		{"archive digest computed over the archive", baseSourceLine, strings.TrimSuffix(baseSourceLine, ".sha512"), ""},
		{"archive listed twice", "  keys:\n    - " + baseModeKey, "    - release: 3.2.0.Final\n      " + baseArchiveURL + "\n      digest: sha512:" + strings.Repeat("a", 128) + "\n      " + baseSourceLine + "\n  keys:\n    - " + baseModeKey, "listed twice"},

		// Install modes.
		{"unknown install mode", "install: pinned", "install: copied", `install "copied" is not one of`},
		{"bundled entry with archives", "install: pinned", "install: bundled", "a bundled entry has no archives"},
		{"pinned entry without archives", baseSmtInstall, "install: pinned", "a pinned entry has archives"},
		{"pinned entry with a plugin_ref", "install: pinned", "install: pinned\n  plugin_ref: mongodb", "a pinned entry has archives, no plugin_ref"},
		{"unpinned entry with archives", "install: pinned", "install: unpinned\n  unpinned_reason: r", "an unpinned entry"},
		{"unpinned entry with a plugin_ref", baseSmtInstall, "install: unpinned\n  unpinned_reason: r\n  plugin_ref: sink-a", "an unpinned entry"},
		{"bundled entry with an unpinned_reason", baseSmtInstall, baseSmtInstall + "\n  unpinned_reason: r", "a bundled entry"},
		// The unpinned reason must say something.
		{"unpinned reason missing", baseSmtInstall, "install: unpinned", "unpinned_reason"},
		{"unpinned reason blank", baseSmtInstall, "install: unpinned\n  unpinned_reason: \"   \"", "unpinned_reason"},
		{"unpinned with a reason", baseSmtInstall, "install: unpinned\n  unpinned_reason: Confluent Hub only.", ""},

		// plugin_ref: an index entry it names must be pinned, so Archive's
		// recursion ends after one step and never loops.
		{"plugin_ref to itself", "plugin_ref: sink-a", "plugin_ref: smt-b", `plugin_ref "smt-b" is not a pinned entry`},
		{"plugin_ref to a plugins.go connector", "plugin_ref: sink-a", "plugin_ref: mongodb", ""},

		// Doc and source citations.
		{"doc with user info", "doc: https://kafka.apache.org/", "doc: https://user@kafka.apache.org/", "doc:"},
		{"doc without a path", "doc: https://kafka.apache.org/documentation/#connect_transforms", "doc: https://kafka.apache.org", "doc:"},
		{"doc with a dot-dot path", "doc: https://kafka.apache.org/documentation/", "doc: https://kafka.apache.org/x/../documentation/", "path is not clean"},
		{"doc on github escaping its repo", "doc: https://kafka.apache.org/documentation/#connect_transforms", "doc: https://github.com/apache/kafka/../../evil/repo", "path is not clean"},
		{"doc with a trailing slash", "doc: https://kafka.apache.org/documentation/#connect_transforms", "doc: https://kafka.apache.org/documentation/", ""},
		{"doc on a look-alike host", "doc: https://kafka.apache.org/", "doc: https://kafka.apache.org.evil.example/", "doc:"},
		{"source over http", "source: https://repo1.maven.org/maven2/org/apache", "source: http://repo1.maven.org/maven2/org/apache", "source:"},
		{"source with a dot-dot path", "source: https://repo1.maven.org/maven2/org/apache", "source: https://repo1.maven.org/maven2/../org/apache", "path is not clean"},

		// Release-bound key fields.
		{"until not after since", baseModeKey, strings.Replace(baseModeKey, "}", ", until: 3.0.0.Final}", 1), "not after since"},
		{"until not after the range", "{name: url, required: true}", "{name: url, required: true, until: 2.0.0.Final}", "not after the entry's versions"},
		{"until bad release", baseModeKey, strings.Replace(baseModeKey, "}", ", until: soon}", 1), "until"},
		{"default_since without a default", "{name: url, required: true}", "{name: url, required: true, default_since: 3.2.0.Final}", "default_since without a default"},
		{"default_since outside the range", baseModeKey, strings.Replace(baseModeKey, "}", ", default_since: 2.9.0.Final}", 1), "outside the entry's versions"},
		{"default_since before since", baseModeKey, strings.Replace(baseModeKey, "}", ", default_since: 3.0.0.Final}", 1), "not after the key's since"},
		{"default_since equal to since", baseModeKey, strings.Replace(baseModeKey, "}", ", default_since: 3.1.0.Final}", 1), "not after the key's since"},
		{"default_since at until", baseModeKey, strings.Replace(baseModeKey, "}", ", default_since: 3.2.0.Final, until: 3.2.0.Final}", 1), "not before the key's until"},
		{"values_since names another value", baseModeKey, strings.Replace(baseModeKey, "}", ", values_since: {c: 3.2.0.Final}}", 1), "values_since names"},
		{"values_since on the default", baseModeKey, strings.Replace(baseModeKey, "}", ", values_since: {a: 3.2.0.Final}}", 1), "set default_since"},
		{"secret and credentials_in_value", "{name: url, required: true}", "{name: url, required: true, secret: true, credentials_in_value: true}", "exclude each other"},
		{"release-bound key fields together", baseModeKey, strings.Replace(baseModeKey, "}", ", default_since: 3.2.0.Final, values_since: {b: 3.3.0.Final}, until: 4.0.0.Final}", 1), ""},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if !strings.Contains(baseIndex, c.old) {
				t.Fatalf("base index has no %q", c.old)
			}
			_, err := parseIndexYAML(t, strings.Replace(baseIndex, c.old, c.new, 1))
			switch {
			case c.wantErr == "" && err != nil:
				t.Fatalf("ParseIndex = %v, want it valid", err)
			case c.wantErr != "" && err == nil:
				t.Fatal("ParseIndex = nil, want an error")
			case c.wantErr != "" && !strings.Contains(err.Error(), c.wantErr):
				t.Fatalf("ParseIndex error = %q, want it to contain %q", err, c.wantErr)
			}
		})
	}
}

// Supports on the edges of a range: an exclusive lower bound (">X") puts X
// itself below the range (Unverified, not Removed), and with two lower
// bounds the higher one binds.
func TestSupportsBounds(t *testing.T) {
	cases := []struct {
		versions, release string
		want              Support
	}{
		{">3.0.0.Final <3.2.0.Final", "3.0.0.Final", Unverified},
		{">3.0.0.Final <3.2.0.Final", "3.0.0", Unverified},
		{">3.0.0.Final <3.2.0.Final", "2.7.2.Final", Unverified},
		{">3.0.0.Final <3.2.0.Final", "3.0.1.Final", Supported},
		{">3.0.0.Final <3.2.0.Final", "3.2.0.Final", Removed},
		{">=3.0.0.Final <3.2.0.Final", "3.0.0.Final", Supported},
		{">=3.0.0.Final <=3.2.0.Final", "3.2.0.Final", Supported},
		{">=3.0.0.Final <=3.2.0.Final", "3.2.1.Final", Removed},
		{">=2.0.0.Final >2.5.0.Final", "2.5.0.Final", Unverified},
		{">=2.0.0.Final >2.5.0.Final", "2.6.0.Final", Supported},
		{"=3.1.0.Final", "3.0.0.Final", Unverified},
		{"=3.1.0.Final", "3.1.0", Supported},
		{"=3.1.0.Final", "3.1.1.Final", Removed},
		{"<4.0.0", "3.9.0", Supported},
		{"<4.0.0", "4.0.0", Removed},
		{"*", "0.1", Supported},
	}
	for _, c := range cases {
		doc := strings.Replace(baseIndex, `versions: ">=2.6.0"`, `versions: "`+c.versions+`"`, 1)
		ix, err := parseIndexYAML(t, doc)
		if err != nil {
			t.Fatalf("%s: %v", c.versions, err)
		}
		e, err := ix.Lookup("", "smt-b")
		if err != nil {
			t.Fatal(err)
		}
		if got, err := e.Supports(c.release); err != nil || got != c.want {
			t.Errorf("%q.Supports(%s) = %v, %v; want %v", c.versions, c.release, got, err, c.want)
		}
	}
}

// Archive refuses a release outside the entry's range (finding: the 2.7.2
// JDBC archive lacks a class that first ships in 3.2.0.Final) and matches
// releases parsed, as Supports does ("3.2.0" is 3.2.0.Final).
func TestArchiveReleaseRange(t *testing.T) {
	ix := loadIndex(t)
	get := func(name string) Entry {
		e, err := ix.Lookup("", name)
		if err != nil {
			t.Fatal(err)
		}
		return e
	}
	smt := get("jdbc-collection-name-transformation")
	for _, rel := range []string{"2.7.2.Final", "3.1.3.Final", "3.2.0.CR1"} {
		_, err := ix.Archive(smt, rel)
		if !errors.Is(err, ErrUnsupportedRelease) || !errors.Is(err, ErrUnknownRelease) {
			t.Errorf("Archive(jdbc-collection-name-transformation, %s) err = %v, want ErrUnsupportedRelease", rel, err)
		}
	}
	if _, err := ix.Archive(get("debezium-jdbc-sink"), "2.1.0.Final"); !errors.Is(err, ErrUnsupportedRelease) {
		t.Errorf("jdbc sink 2.1.0.Final: err = %v, want ErrUnsupportedRelease", err)
	}
	if _, err := ix.Archive(get("debezium-jdbc-sink"), "not-a-release"); err == nil {
		t.Error("Archive(not-a-release) = nil error")
	}
	full, err := ix.Archive(get("debezium-jdbc-sink"), "3.2.0.Final")
	if err != nil {
		t.Fatal(err)
	}
	for _, rel := range []string{"3.2.0", "3.2.0.final", "3.2.0-GA", "3.2"} {
		if a, err := ix.Archive(get("debezium-jdbc-sink"), rel); err != nil || a != full {
			t.Errorf("jdbc sink %s = %+v, %v; want the 3.2.0.Final archive", rel, a, err)
		}
		if a, err := ix.Archive(smt, rel); err != nil || a != full {
			t.Errorf("jdbc collection-name SMT %s = %+v, %v; want the sink's 3.2.0.Final archive", rel, a, err)
		}
	}
	m, err := ix.Archive(get("debezium-mongodb-sink"), "3.2.0")
	if p, _ := ResolvePlugin("3.2.0.Final", "mongodb"); err != nil || m.Digest != p.Digest {
		t.Errorf("mongodb sink 3.2.0 = %+v, %v; want plugins.go mongodb 3.2.0.Final", m, err)
	}
	// The unpinned refusal comes before the range check.
	if _, err := ix.Archive(get("s3-sink"), "0.0.1"); !errors.Is(err, ErrUnpinned) {
		t.Errorf("s3 sink: err = %v, want ErrUnpinned", err)
	}
	// Provenance: the 3.0.7 and 2.7.2 digests were computed over the bytes.
	for rel, computed := range map[string]bool{"3.2.0.Final": false, "3.1.3.Final": false, "3.0.7.Final": true, "2.7.2.Final": true} {
		a, err := ix.Archive(get("debezium-jdbc-sink"), rel)
		if err != nil || a.DigestComputed() != computed {
			t.Errorf("jdbc sink %s: DigestComputed = %v, %v; want %v", rel, a.DigestComputed(), err, computed)
		}
	}
}

// An entry's plugin_ref to a pinned index entry resolves to that entry's
// archive, within the referring entry's own range.
func TestArchivePluginRef(t *testing.T) {
	ix, err := parseIndexYAML(t, strings.Replace(baseIndex, `versions: ">=2.6.0"`, `versions: ">=3.2.0.Final"`, 1))
	if err != nil {
		t.Fatal(err)
	}
	sink, _ := ix.Lookup("", "sink-a")
	smt, _ := ix.Lookup("", "smt-b")
	want, err := ix.Archive(sink, "3.2.0.Final")
	if err != nil {
		t.Fatal(err)
	}
	if got, err := ix.Archive(smt, "3.2.0"); err != nil || got != want {
		t.Errorf("Archive(smt-b) = %+v, %v; want sink-a's archive", got, err)
	}
	if _, err := ix.Archive(smt, "3.1.0.Final"); !errors.Is(err, ErrUnsupportedRelease) {
		t.Errorf("Archive(smt-b, 3.1.0.Final) err = %v, want ErrUnsupportedRelease (before smt-b's range)", err)
	}
	if _, err := ix.Archive(smt, "3.3.0.Final"); !errors.Is(err, ErrUnknownRelease) {
		t.Errorf("Archive(smt-b, 3.3.0.Final) err = %v, want ErrUnknownRelease (sink-a pins no 3.3.0)", err)
	}
}

// ResolveClass with a release applies a Q1b rename only where its known
// issue holds; before it the old class is still the one to configure.
func TestResolveClassRelease(t *testing.T) {
	ix := loadIndex(t)
	const (
		oldHistory = "io.debezium.relational.history.KafkaDatabaseHistory"
		newHistory = "io.debezium.storage.kafka.history.KafkaSchemaHistory"
		oldUnwrap  = "io.debezium.transforms.UnwrapFromEnvelope"
		newUnwrap  = "io.debezium.transforms.ExtractNewRecordState"
	)
	cases := []struct {
		class, rel, want string
		renamed          bool
		entry            string
	}{
		{oldHistory, "1.9.7.Final", oldHistory, false, ""},
		{oldHistory, "2.0.0.Alpha1", oldHistory, false, ""},
		{oldHistory, "2.0.0.Final", newHistory, true, ""},
		{oldHistory, "3.2.0", newHistory, true, ""},
		{oldUnwrap, "1.1.2.Final", oldUnwrap, false, ""},
		{oldUnwrap, "1.2.0.Final", newUnwrap, true, "extract-new-record-state"},
		{newUnwrap, "1.1.2.Final", newUnwrap, false, "extract-new-record-state"},
	}
	for _, c := range cases {
		res, err := ix.ResolveClass(c.class, c.rel)
		if err != nil {
			t.Errorf("ResolveClass(%s, %s) = %v", c.class, c.rel, err)
			continue
		}
		gotEntry := ""
		if res.Entry != nil {
			gotEntry = res.Entry.Name
		}
		if res.Class != c.want || res.Renamed != c.renamed || gotEntry != c.entry {
			t.Errorf("ResolveClass(%s, %s) = %s renamed=%v entry=%q; want %s renamed=%v entry=%q", c.class, c.rel, res.Class, res.Renamed, gotEntry, c.want, c.renamed, c.entry)
		}
		if c.class != newUnwrap && (res.IssueID == "" || res.Affected == "") {
			t.Errorf("ResolveClass(%s, %s) cites no known issue: %+v", c.class, c.rel, res)
		}
	}
	if _, err := ix.ResolveClass(oldHistory, "two-point-oh"); err == nil {
		t.Error("ResolveClass with an unparseable release = nil error")
	}
	if _, err := ix.ResolveClass("org.apache.kafka.connect.transforms.ValueToKey$Value", "3.2.0.Final"); !errors.Is(err, ErrUnknownEntry) {
		t.Errorf("ResolveClass(ValueToKey$Value) err = %v, want ErrUnknownEntry", err)
	}
	if _, err := ix.ResolveClass(oldUnwrap+"$Value", "3.2.0.Final"); !errors.Is(err, ErrUnknownEntry) {
		t.Errorf("ResolveClass(UnwrapFromEnvelope$Value) err = %v, want ErrUnknownEntry", err)
	}
}

// Every key the index marks secret is one the repo's shared secret rule
// flags, so a plain-text value for it is refused and redacted everywhere.
// A URL key that can embed credentials is not flagged by name (the host
// guard reads its value); the value rule masks a password inside it.
func TestSecretKeys(t *testing.T) {
	ix := loadIndex(t)
	secret := map[string]bool{}
	inValue := map[string]bool{}
	for _, e := range ix.Entries() {
		for _, k := range e.Keys {
			if k.Secret {
				secret[e.Name+" "+k.Name] = true
				if !actions.LooksSecret(k.Name) {
					t.Errorf("%s: key %s is secret: true but actions.LooksSecret does not flag it", e.Name, k.Name)
				}
			}
			if k.CredentialsInValue {
				inValue[e.Name+" "+k.Name] = true
			}
		}
	}
	for _, want := range []string{
		"server-eventhubs debezium.sink.eventhubs.connectionstring",
		"server-nats-jetstream debezium.sink.nats-jetstream.auth.seed",
		"server-nats-jetstream debezium.sink.nats-jetstream.auth.jwt",
		"aiven-http-sink http.headers.authorization",
		"debezium-jdbc-sink connection.password",
		"server-rabbitmq-stream debezium.sink.rabbitmqstream.password",
	} {
		if !secret[want] {
			t.Errorf("%s is not marked secret: true", want)
		}
	}
	for _, want := range []string{
		"debezium-mongodb-sink mongodb.connection.string",
		"debezium-jdbc-sink connection.url",
		"confluent-jdbc-sink connection.url",
		"elasticsearch-sink connection.url",
	} {
		if !inValue[want] {
			t.Errorf("%s is not marked credentials_in_value: true", want)
		}
	}
	for _, v := range []string{
		"jdbc:postgresql://db:5432/orders?user=app&password=Sup3rS3cretPw",
		"postgres://app:Sup3rS3cretPw@db:5432/orders",
		"jdbc:sqlserver://db:1433;user=sa;password=Sup3rS3cretPw",
		"mongodb://admin:Sup3rS3cretPw@mongo:27017/?authSource=admin",
		"mongodb+srv://app:Sup3rS3cretPw@cluster0.example.net/",
		"https://elastic:Sup3rS3cretPw@es:9200",
	} {
		if out, st := redact.Text(v); st.Total == 0 || strings.Contains(out, "Sup3rS3cretPw") {
			t.Errorf("redact.Text(%q) = %q: the value rule misses a credential in a URL key", v, out)
		}
	}
}

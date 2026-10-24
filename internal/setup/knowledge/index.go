package knowledge

import (
	"bytes"
	"embed"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net/url"
	"path"
	"regexp"
	"slices"
	"sort"
	"strings"

	"gopkg.in/yaml.v3"

	"piperoom/internal/tools/runbooks"
)

// The SMT and sink index (U3a): every single message transform, predicate
// and sink connector the setup agent may put in a config, with its keys,
// defaults, the release range it is verified for, the checks it implies
// and, for sinks, how its plugin is installed. It is embedded YAML under
// index/; LoadIndex parses and validates it, so a bad range, digest, key
// or citation fails start-up (Check calls it). Renamed classes come from
// the Q1b known-issues index (internal/tools/runbooks), not a copy.
//
// The same index holds the tuning entries (U10a, kind tuning, tuning.go):
// the Connect worker heap, the Debezium queue and batch keys, snapshot
// fetch sizes, tasks.max per source and the producer and consumer
// overrides, each with its default read from source and the symptoms
// that call for changing it.

// Sentinel errors of the SMT and sink index. Compare with errors.Is.
var (
	// ErrUnknownEntry is returned when a name matches no index entry.
	ErrUnknownEntry = errors.New("setup/knowledge: not in the SMT and sink index")
	// ErrUnpinned is returned for a sink whose plugin archive has no
	// verified digest: plugin_install must refuse it.
	ErrUnpinned = errors.New("setup/knowledge: plugin archive is not pinned")
	// ErrBundled is returned when an entry ships inside its runtime (the
	// Connect worker, a connector plugin or the server image) and has no
	// archive of its own to install.
	ErrBundled = errors.New("setup/knowledge: ships with its runtime, no archive to install")
	// ErrUnsupportedRelease is returned by Archive when the release is
	// outside the entry's verified range (Supports is not Supported): an
	// archive of that release may lack the class. It wraps
	// ErrUnknownRelease, so errors.Is matches either.
	ErrUnsupportedRelease = fmt.Errorf("%w: outside the entry's verified versions", ErrUnknownRelease)
)

// Kind is what an index entry configures.
type Kind string

// Entry kinds. A predicate is set under predicates.<name>.type, an SMT
// under transforms.<name>.type, a sink as connector.class (Kafka Connect)
// or debezium.sink.type (Debezium Server). A tuning entry names no class:
// it is a group of keys set where its Scope says, with the symptoms that
// call for changing them.
const (
	KindSMT       Kind = "smt"
	KindPredicate Kind = "predicate"
	KindSink      Kind = "sink"
	KindTuning    Kind = "tuning"
)

// Runtime is where an entry runs.
const (
	RuntimeConnect = "connect" // a Kafka Connect worker
	RuntimeServer  = "server"  // Debezium Server
)

// Install says how an entry reaches its runtime.
const (
	InstallBundled  = "bundled"  // ships with the runtime or a referenced plugin
	InstallPinned   = "pinned"   // its own archives, pinned by digest
	InstallUnpinned = "unpinned" // an archive exists but no digest was verified
)

// Key is one configuration key of an entry.
type Key struct {
	Name string `yaml:"name" json:"name"`
	// Default is the value the component uses when the key is absent; nil
	// when it has none or it was not verified.
	Default *string `yaml:"default,omitempty" json:"default,omitempty"`
	// DefaultSince is the first release that applies Default, when that is
	// newer than Since: before it the key has no default (Check says what
	// happens then).
	DefaultSince string   `yaml:"default_since,omitempty" json:"default_since,omitempty"`
	Required     bool     `yaml:"required,omitempty" json:"required,omitempty"`
	Values       []string `yaml:"values,omitempty" json:"values,omitempty"`
	// ValuesSince maps a value of Values to the first release that accepts
	// it, for values newer than the key.
	ValuesSince map[string]string `yaml:"values_since,omitempty" json:"values_since,omitempty"`
	// Since is the first release of the entry's component verified to have
	// the key, when it is newer than the entry.
	Since string `yaml:"since,omitempty" json:"since,omitempty"`
	// Until is the first release verified not to have the key (a removed
	// legacy key).
	Until string `yaml:"until,omitempty" json:"until,omitempty"`
	// Secret marks a key whose value is a credential: a config must carry it
	// as a secret reference, never in plain text. Every Secret key is one
	// actions.LooksSecret flags (a test holds the two together).
	Secret bool `yaml:"secret,omitempty" json:"secret,omitempty"`
	// CredentialsInValue marks a URL or DSN key (connection.url,
	// mongodb.connection.string) whose value can embed a user and password.
	// The key is not Secret: the host guard must read its value. A password
	// inside it is caught by the value rule (redact.Text) instead, and
	// credentials belong in the entry's own user and password keys.
	CredentialsInValue bool `yaml:"credentials_in_value,omitempty" json:"credentials_in_value,omitempty"`
	// Formerly lists older names of the key the component still accepts or
	// used to accept (deprecated aliases); Check says until when.
	Formerly []string `yaml:"formerly,omitempty" json:"formerly,omitempty"`
	Check    string   `yaml:"check,omitempty" json:"check,omitempty"`
}

// Archive is one plugin archive pinned by digest: what plugin_install
// downloads, checks and unpacks.
type Archive struct {
	Release string `yaml:"release" json:"release"`
	URL     string `yaml:"url" json:"url"`
	Digest  string `yaml:"digest" json:"digest"` // "sha512:<128 hex>"
	// Source is where the digest was read: the archive's .sha512 file on
	// Maven Central, or the archive itself when the digest was computed
	// over its bytes.
	Source string `yaml:"source" json:"source"`
}

// DigestComputed reports whether the digest was computed over the
// archive's bytes (Source is the archive itself) rather than read from a
// .sha512 file Maven Central publishes: weaker provenance, since nothing
// upstream states the SHA-512 (the .sha1 only cross-checks the bytes).
func (a Archive) DigestComputed() bool { return a.Source == a.URL }

// Entry is one SMT, predicate or sink.
type Entry struct {
	Name      string `yaml:"name" json:"name"`
	Kind      Kind   `yaml:"kind" json:"kind"`
	Component string `yaml:"component" json:"component"`
	Runtime   string `yaml:"runtime" json:"runtime"`
	// Class is the fully qualified Java class (Kafka Connect); SinkType the
	// debezium.sink.type value (Debezium Server). Exactly one is set.
	Class    string `yaml:"class,omitempty" json:"class,omitempty"`
	SinkType string `yaml:"sink_type,omitempty" json:"sink_type,omitempty"`
	// Variants are the nested classes a Kafka SMT is configured by
	// ("Key" -> Class$Key); the bare class is abstract when set.
	Variants []string `yaml:"variants,omitempty" json:"variants,omitempty"`
	// Versions is the range of the component's releases the entry is
	// verified for: its lower bound is the earliest release checked to ship
	// the class (the class may be older), its upper bound the first release
	// checked not to.
	Versions string `yaml:"versions" json:"versions"`
	Summary  string `yaml:"summary" json:"summary"`
	Keys     []Key  `yaml:"keys,omitempty" json:"keys,omitempty"`
	// Checks are the checks the entry implies, in the words the agent and
	// the config builder use.
	Checks  []string `yaml:"checks" json:"checks"`
	Install string   `yaml:"install" json:"install"`
	// RuntimeImage names the allow-listed image (images.go) that ships the
	// entry, when one does.
	RuntimeImage string `yaml:"runtime_image,omitempty" json:"runtime_image,omitempty"`
	// PluginRef names the plugin whose archive carries the entry: a
	// connector in plugins.go ("mongodb") or another index entry.
	PluginRef      string    `yaml:"plugin_ref,omitempty" json:"plugin_ref,omitempty"`
	Archives       []Archive `yaml:"archives,omitempty" json:"archives,omitempty"`
	UnpinnedReason string    `yaml:"unpinned_reason,omitempty" json:"unpinned_reason,omitempty"`
	// Successor names the index entry that replaces this class in a later
	// release line (a different class, not a rename).
	Successor string `yaml:"successor,omitempty" json:"successor,omitempty"`
	Doc       string `yaml:"doc" json:"doc"`
	Source    string `yaml:"source" json:"source"`

	// Scope says where a tuning entry's keys are set: ScopeConnector,
	// ScopeWorker or ScopeEnv. Tuning entries only.
	Scope string `yaml:"scope,omitempty" json:"scope,omitempty"`
	// Connector is the source connector (a plugins.go short name) a tuning
	// entry applies to; empty means every source. Tuning entries only.
	Connector string `yaml:"connector,omitempty" json:"connector,omitempty"`
	// Symptoms are what the logs, metrics or the user show when a tuning
	// entry's keys are worth changing: the plan proposes the entry only
	// after one of them is seen. Tuning entries only, and required there.
	Symptoms []string `yaml:"symptoms,omitempty" json:"symptoms,omitempty"`

	rng releaseRange
}

// Key returns the entry's key by name or by a former name.
func (e Entry) Key(name string) (Key, bool) {
	for _, k := range e.Keys {
		if k.Name == name || slices.Contains(k.Formerly, name) {
			return k, true
		}
	}
	return Key{}, false
}

// Support is how a release relates to an entry's verified range.
type Support int

// Support values.
const (
	// Supported: the release is inside the verified range.
	Supported Support = iota
	// Unverified: the release is older than the earliest release checked;
	// the class may or may not exist there.
	Unverified
	// Removed: the release is past the range's upper bound; the class or
	// sink is gone.
	Removed
)

// Supports places a release of the entry's component against its range.
func (e Entry) Supports(rel string) (Support, error) {
	r, err := parseRelease(rel)
	if err != nil {
		return 0, fmt.Errorf("knowledge: %s: %w", e.Name, err)
	}
	if e.rng.holds(r) {
		return Supported, nil
	}
	// Below the lower bound, or on an exclusive one (">X" and r == X).
	if low, op, ok := e.rng.lowest(); ok {
		if c := r.compare(low); c < 0 || (c == 0 && op == ">") {
			return Unverified, nil
		}
	}
	return Removed, nil
}

// Index is the parsed SMT and sink index. It is read-only after load.
type Index struct {
	entries []Entry
	renames map[string]runbooks.KnownIssue // old FQCN -> the Q1b entry
	renamed map[string]string              // old FQCN -> new FQCN
}

//go:embed index/*.yaml
var indexFiles embed.FS

const maxIndexFileBytes = 256 << 10

// LoadIndex parses and validates the embedded index together with the
// Q1b known issues it takes renamed classes from. Call it once at start
// and keep the value; a non-nil error means the shipped data is corrupt.
func LoadIndex() (*Index, error) {
	sub, err := fs.Sub(indexFiles, "index")
	if err != nil {
		return nil, fmt.Errorf("knowledge: embedded index: %w", err)
	}
	ki, err := runbooks.EmbeddedKnownIssues()
	if err != nil {
		return nil, fmt.Errorf("knowledge: known issues: %w", err)
	}
	return ParseIndex(sub, ki)
}

// ParseIndex parses every *.yaml at the root of fsys and validates it
// against the image and plugin tables and the known issues ki (nil means
// none). Tests feed it corrupt files; LoadIndex feeds it the shipped ones.
func ParseIndex(fsys fs.FS, ki *runbooks.KnownIssues) (*Index, error) {
	names, err := fs.Glob(fsys, "*.yaml")
	if err != nil {
		return nil, fmt.Errorf("knowledge: list index: %w", err)
	}
	sort.Strings(names)
	ix := &Index{renames: map[string]runbooks.KnownIssue{}, renamed: map[string]string{}}
	if ki != nil {
		for _, issue := range ki.All() {
			for _, r := range issue.RenamedValues {
				if classRe.MatchString(r.Old) && classRe.MatchString(r.New) {
					ix.renames[r.Old] = issue
					ix.renamed[r.Old] = r.New
				}
			}
		}
	}
	for _, name := range names {
		b, err := fs.ReadFile(fsys, name)
		if err != nil {
			return nil, fmt.Errorf("knowledge: %s: %w", name, err)
		}
		if len(b) > maxIndexFileBytes {
			return nil, fmt.Errorf("knowledge: %s: %d bytes, more than %d", name, len(b), maxIndexFileBytes)
		}
		var entries []Entry
		dec := yaml.NewDecoder(bytes.NewReader(b))
		dec.KnownFields(true)
		if err := dec.Decode(&entries); err != nil && !errors.Is(err, io.EOF) {
			return nil, fmt.Errorf("knowledge: %s: %w", name, err)
		}
		if len(entries) == 0 {
			return nil, fmt.Errorf("knowledge: %s: no entries", name)
		}
		for i := range entries {
			if err := entries[i].check(); err != nil {
				return nil, fmt.Errorf("knowledge: %s: %s: %w", name, entries[i].Name, err)
			}
		}
		ix.entries = append(ix.entries, entries...)
	}
	if len(ix.entries) == 0 {
		return nil, errors.New("knowledge: the SMT and sink index is empty")
	}
	if err := ix.crossCheck(); err != nil {
		return nil, err
	}
	return ix, nil
}

var (
	entryNameRe = regexp.MustCompile(`^[a-z0-9][a-z0-9-]{1,63}$`)
	// A Java class name: a lower-case package and an upper-case class.
	classRe    = regexp.MustCompile(`^[a-z][a-z0-9_]*(\.[a-z][a-z0-9_]*)+\.[A-Z][A-Za-z0-9_]*$`)
	sinkTypeRe = regexp.MustCompile(`^[a-z][a-z0-9-]{1,31}$`)
	keyRe      = regexp.MustCompile(`^[A-Za-z][A-Za-z0-9_.-]{0,127}$`)
)

var (
	kinds      = []Kind{KindSMT, KindPredicate, KindSink, KindTuning}
	components = []string{"debezium", "kafka", "confluent", "snowflake", "aiven"}
	runtimes   = []string{RuntimeConnect, RuntimeServer}
	installs   = []string{InstallBundled, InstallPinned, InstallUnpinned}
	variantSet = []string{"Key", "Value"}
	// Hosts an entry may cite as its documentation.
	docHosts = []string{"debezium.io", "kafka.apache.org", "docs.confluent.io", "docs.snowflake.com", "github.com"}
	// Hosts an entry may cite as the artifact its keys were read from.
	sourceHosts = []string{"repo1.maven.org", "raw.githubusercontent.com"}
)

const mavenCentral = "https://repo1.maven.org/maven2/"

// check validates one entry on its own; crossCheck validates references.
func (e *Entry) check() error {
	switch {
	case !entryNameRe.MatchString(e.Name):
		return fmt.Errorf("name must match %s", entryNameRe)
	case !slices.Contains(kinds, e.Kind):
		return fmt.Errorf("kind %q is not one of smt, predicate, sink, tuning", e.Kind)
	case !slices.Contains(components, e.Component):
		return fmt.Errorf("component %q is not one of %s", e.Component, strings.Join(components, ", "))
	case !slices.Contains(runtimes, e.Runtime):
		return fmt.Errorf("runtime %q is not connect or server", e.Runtime)
	case strings.TrimSpace(e.Summary) == "":
		return errors.New("summary is required")
	case len(e.Checks) == 0:
		return errors.New("at least one check is required")
	}
	if e.Kind == KindTuning {
		if err := e.checkTuning(); err != nil {
			return err
		}
	} else if e.Scope != "" || e.Connector != "" || len(e.Symptoms) > 0 {
		return errors.New("scope, connector and symptoms belong to tuning entries only")
	} else if e.Runtime == RuntimeConnect {
		if !classRe.MatchString(e.Class) || e.SinkType != "" {
			return fmt.Errorf("a connect entry needs a Java class (got %q) and no sink_type", e.Class)
		}
	} else {
		if e.Kind != KindSink || !sinkTypeRe.MatchString(e.SinkType) || e.Class != "" {
			return fmt.Errorf("a server entry is a sink with a sink_type (got %q) and no class", e.SinkType)
		}
	}
	for _, v := range e.Variants {
		if !slices.Contains(variantSet, v) || e.Component != "kafka" {
			return fmt.Errorf("variant %q: only Kafka SMTs have Key and Value variants", v)
		}
	}
	for _, c := range e.Checks {
		if strings.TrimSpace(c) == "" {
			return errors.New("empty check")
		}
	}
	rng, err := parseRange(e.Versions)
	if err != nil {
		return fmt.Errorf("versions: %w", err)
	}
	e.rng = rng
	if err := e.checkKeys(); err != nil {
		return err
	}
	if e.Kind != KindTuning {
		if err := e.checkInstall(); err != nil {
			return err
		}
	}
	if err := checkCitation(e.Doc, docHosts); err != nil {
		return fmt.Errorf("doc: %w", err)
	}
	if err := checkCitation(e.Source, sourceHosts); err != nil {
		return fmt.Errorf("source: %w", err)
	}
	return nil
}

func (e *Entry) checkKeys() error {
	seen := map[string]bool{}
	for _, k := range e.Keys {
		for _, n := range append([]string{k.Name}, k.Formerly...) {
			if !keyRe.MatchString(n) {
				return fmt.Errorf("key %q must match %s", n, keyRe)
			}
			if seen[n] {
				return fmt.Errorf("key %q is listed twice", n)
			}
			seen[n] = true
		}
		if k.Required && k.Default != nil {
			return fmt.Errorf("key %s: a required key has no default", k.Name)
		}
		if k.Default != nil && len(k.Values) > 0 && !slices.ContainsFunc(k.Values, func(v string) bool { return strings.EqualFold(v, *k.Default) }) {
			return fmt.Errorf("key %s: default %q is not one of its values %v", k.Name, *k.Default, k.Values)
		}
		if k.Secret && k.CredentialsInValue {
			return fmt.Errorf("key %s: secret and credentials_in_value exclude each other", k.Name)
		}
		var since release
		if k.Since != "" {
			r, err := e.releaseInRange(k.Since)
			if err != nil {
				return fmt.Errorf("key %s: since %w", k.Name, err)
			}
			since = r
		}
		if k.Until != "" {
			r, err := parseRelease(k.Until)
			if err != nil {
				return fmt.Errorf("key %s: until: %w", k.Name, err)
			}
			if r.compare(since) <= 0 {
				return fmt.Errorf("key %s: until %s is not after since", k.Name, k.Until)
			}
			if low, _, ok := e.rng.lowest(); ok && r.compare(low) <= 0 {
				return fmt.Errorf("key %s: until %s is not after the entry's versions %q", k.Name, k.Until, e.Versions)
			}
		}
		if k.DefaultSince != "" {
			if k.Default == nil {
				return fmt.Errorf("key %s: default_since without a default", k.Name)
			}
			if err := e.checkKeyRelease(k, k.DefaultSince, since); err != nil {
				return fmt.Errorf("key %s: default_since %w", k.Name, err)
			}
		}
		for v, rel := range k.ValuesSince {
			if !slices.Contains(k.Values, v) {
				return fmt.Errorf("key %s: values_since names %q, not one of its values %v", k.Name, v, k.Values)
			}
			if err := e.checkKeyRelease(k, rel, since); err != nil {
				return fmt.Errorf("key %s: values_since %s: %w", k.Name, v, err)
			}
		}
		if k.Default != nil && k.DefaultSince == "" && k.ValuesSince[*k.Default] != "" {
			return fmt.Errorf("key %s: default %q is newer than the key; set default_since", k.Name, *k.Default)
		}
	}
	return nil
}

// releaseInRange parses rel and requires the entry's range to hold it.
func (e *Entry) releaseInRange(rel string) (release, error) {
	r, err := parseRelease(rel)
	if err != nil {
		return release{}, err
	}
	if !e.rng.holds(r) {
		return release{}, fmt.Errorf("%s is outside the entry's versions %q", rel, e.Versions)
	}
	return r, nil
}

// checkKeyRelease validates a release that refines key k: inside the
// entry's range, after the key's since and before its until.
func (e *Entry) checkKeyRelease(k Key, rel string, since release) error {
	r, err := e.releaseInRange(rel)
	if err != nil {
		return err
	}
	if k.Since != "" && r.compare(since) <= 0 {
		return fmt.Errorf("%s is not after the key's since %s", rel, k.Since)
	}
	if k.Until != "" {
		if u, _ := parseRelease(k.Until); r.compare(u) >= 0 {
			return fmt.Errorf("%s is not before the key's until %s", rel, k.Until)
		}
	}
	return nil
}

func (e *Entry) checkInstall() error {
	if !slices.Contains(installs, e.Install) {
		return fmt.Errorf("install %q is not one of %s", e.Install, strings.Join(installs, ", "))
	}
	switch e.Install {
	case InstallBundled:
		if len(e.Archives) > 0 || e.UnpinnedReason != "" {
			return errors.New("a bundled entry has no archives and no unpinned_reason")
		}
	case InstallPinned:
		if len(e.Archives) == 0 || e.PluginRef != "" || e.UnpinnedReason != "" {
			return errors.New("a pinned entry has archives, no plugin_ref and no unpinned_reason")
		}
	case InstallUnpinned:
		if len(e.Archives) > 0 || e.PluginRef != "" || strings.TrimSpace(e.UnpinnedReason) == "" {
			return errors.New("an unpinned entry has an unpinned_reason and no archives or plugin_ref")
		}
	}
	seen := map[string]bool{}
	for _, a := range e.Archives {
		r, err := parseRelease(a.Release)
		if err != nil {
			return fmt.Errorf("archive: %w", err)
		}
		if !e.rng.holds(r) {
			return fmt.Errorf("archive %s is outside the entry's versions %q", a.Release, e.Versions)
		}
		if seen[a.Release] {
			return fmt.Errorf("archive %s is listed twice", a.Release)
		}
		seen[a.Release] = true
		if alg, _, err := parseDigest(a.Digest); err != nil {
			return fmt.Errorf("archive %s: %w", a.Release, err)
		} else if alg != "sha512" {
			return fmt.Errorf("archive %s: digest must be sha512, got %s", a.Release, alg)
		}
		if err := checkArchiveURL(a); err != nil {
			return fmt.Errorf("archive %s: %w", a.Release, err)
		}
	}
	return nil
}

// checkArchiveURL pins an archive to Maven Central: the URL is a release
// archive of a path that names the release, and the source is the
// archive's .sha512 file or the archive itself.
func checkArchiveURL(a Archive) error {
	u, err := url.Parse(a.URL)
	if err != nil || !strings.HasPrefix(a.URL, mavenCentral) || u.RawQuery != "" || u.Fragment != "" ||
		path.Clean(u.Path) != u.Path || !strings.Contains(u.Path, "/"+a.Release+"/") {
		return fmt.Errorf("url %q must be a Maven Central path under %s naming release %s", a.URL, mavenCentral, a.Release)
	}
	if !strings.HasSuffix(a.URL, ".tar.gz") && !strings.HasSuffix(a.URL, ".jar") && !strings.HasSuffix(a.URL, ".zip") {
		return fmt.Errorf("url %q must be a .tar.gz, .jar or .zip archive", a.URL)
	}
	if a.Source != a.URL+".sha512" && a.Source != a.URL {
		return fmt.Errorf("source must be the archive's .sha512 file or the archive, got %q", a.Source)
	}
	return nil
}

// checkCitation requires an https URL on one of hosts, with no user info.
// On github.com and raw.githubusercontent.com the path is cleaned first,
// so a "/x/../y" path is checked as the page a browser lands on.
func checkCitation(raw string, hosts []string) error {
	u, err := url.Parse(raw)
	if err != nil || u.Scheme != "https" || u.User != nil || !slices.Contains(hosts, u.Host) || u.Path == "" {
		return fmt.Errorf("%q must be an https URL on one of %s", raw, strings.Join(hosts, ", "))
	}
	if c := path.Clean(u.Path); c != u.Path && c+"/" != u.Path {
		return fmt.Errorf("%q: path is not clean", raw)
	}
	return nil
}

// crossCheck validates what one entry cannot see alone: unique names,
// classes and sink types, references to images, plugins, successors, and
// that no entry presents a class Q1b records as renamed.
func (ix *Index) crossCheck() error {
	names := map[string]bool{}
	classes := map[string]string{}
	sinkTypes := map[string]bool{}
	for _, e := range ix.entries {
		if names[e.Name] {
			return fmt.Errorf("knowledge: duplicate entry name %q", e.Name)
		}
		names[e.Name] = true
		if e.Class != "" {
			if other, dup := classes[e.Class]; dup {
				return fmt.Errorf("knowledge: %s: class %s is also entry %s", e.Name, e.Class, other)
			}
			classes[e.Class] = e.Name
		}
		if e.SinkType != "" {
			if sinkTypes[e.SinkType] {
				return fmt.Errorf("knowledge: duplicate sink_type %q", e.SinkType)
			}
			sinkTypes[e.SinkType] = true
		}
		if issue, renamed := ix.renames[e.Class]; renamed {
			return fmt.Errorf("knowledge: %s: class %s was renamed to %s (%s); index the new class", e.Name, e.Class, ix.renamed[e.Class], issue.ID)
		}
	}
	for _, e := range ix.entries {
		if err := ix.checkRefs(e, names); err != nil {
			return fmt.Errorf("knowledge: %s: %w", e.Name, err)
		}
	}
	return nil
}

func (ix *Index) checkRefs(e Entry, names map[string]bool) error {
	if e.RuntimeImage != "" {
		im, err := ImageByName(e.RuntimeImage)
		if err != nil {
			return fmt.Errorf("runtime_image: %w", err)
		}
		want := map[string]string{RuntimeConnect: "connect", RuntimeServer: "server"}[e.Runtime]
		if (e.RuntimeImage == "connect" || e.RuntimeImage == "server") && e.RuntimeImage != want {
			return fmt.Errorf("runtime_image %q does not run a %s entry", e.RuntimeImage, e.Runtime)
		}
		// The Debezium images are tagged with the Debezium release, so a
		// Debezium entry's range must hold the pinned image's tag.
		if e.Component == "debezium" && e.RuntimeImage == want {
			tag, err := parseRelease(im.Tag)
			if err != nil {
				return fmt.Errorf("runtime_image %s: %w", e.RuntimeImage, err)
			}
			if !e.rng.holds(tag) {
				return fmt.Errorf("runtime_image %s:%s is outside the entry's versions %q", im.Repo, im.Tag, e.Versions)
			}
		}
	}
	if e.PluginRef != "" {
		if names[e.PluginRef] {
			if ref := ix.byName(e.PluginRef); ref.Install != InstallPinned {
				return fmt.Errorf("plugin_ref %q is not a pinned entry", e.PluginRef)
			}
		} else if !pluginConnectorKnown(e.PluginRef) {
			return fmt.Errorf("plugin_ref %q is neither an index entry nor a connector in plugins.go", e.PluginRef)
		}
	}
	if e.Successor != "" && (!names[e.Successor] || e.Successor == e.Name) {
		return fmt.Errorf("successor %q is not another index entry", e.Successor)
	}
	return nil
}

func pluginConnectorKnown(connector string) bool {
	for _, p := range plugins {
		if p.Connector == connector {
			return true
		}
	}
	return false
}

func (ix *Index) byName(name string) Entry {
	for _, e := range ix.entries {
		if e.Name == name {
			return e
		}
	}
	return Entry{}
}

// Entries returns every entry, in file then entry order.
func (ix *Index) Entries() []Entry { return slices.Clone(ix.entries) }

// Lookup finds an entry of kind by its index name, its class (with or
// without a $Key/$Value variant), the class's simple name when that is
// unique within kind, a Debezium Server sink type, or, for a tuning
// entry, one of its keys (see tuningKeyMatches for the prefixes it
// accepts) when that key is in one tuning entry only. An empty kind
// matches every kind. It returns ErrUnknownEntry when nothing matches,
// when the variant is not one the entry has (ValueToKey$Value), and when
// a simple name or a key is ambiguous (Filter is both Kafka's and
// Debezium's; snapshot.fetch.size has an entry per source).
func (ix *Index) Lookup(kind Kind, name string) (Entry, error) {
	name = strings.TrimSpace(name)
	class, variant, hasVariant := strings.Cut(name, "$")
	var simple, byKey []Entry
	for _, e := range ix.entries {
		if kind != "" && e.Kind != kind {
			continue
		}
		if !hasVariant && (e.Name == name || (e.SinkType != "" && e.SinkType == name)) {
			return e, nil
		}
		if e.Kind == KindTuning && !hasVariant && tuningKeyMatches(e, name) {
			byKey = append(byKey, e)
		}
		if e.Class == "" {
			continue
		}
		if e.Class == class {
			if err := checkVariant(e, hasVariant, variant); err != nil {
				return Entry{}, err
			}
			return e, nil
		}
		if e.Class[strings.LastIndexByte(e.Class, '.')+1:] == class {
			simple = append(simple, e)
		}
	}
	if len(simple) == 1 {
		if err := checkVariant(simple[0], hasVariant, variant); err != nil {
			return Entry{}, err
		}
	}
	if len(simple) == 1 {
		return simple[0], nil
	}
	if len(simple) > 1 {
		var cs []string
		for _, e := range simple {
			cs = append(cs, e.Class)
		}
		return Entry{}, fmt.Errorf("%w: %q is ambiguous, use one of %s", ErrUnknownEntry, name, strings.Join(cs, ", "))
	}
	if len(byKey) == 1 {
		return byKey[0], nil
	}
	if len(byKey) > 1 {
		var ns []string
		for _, e := range byKey {
			ns = append(ns, e.Name)
		}
		return Entry{}, fmt.Errorf("%w: key %q is in several tuning entries, use one of %s", ErrUnknownEntry, name, strings.Join(ns, ", "))
	}
	return Entry{}, fmt.Errorf("%w: %s %q", ErrUnknownEntry, kind, name)
}

// checkVariant refuses a $variant suffix the entry does not have: an
// empty one ("InsertField$") or one on an entry without variants.
func checkVariant(e Entry, hasVariant bool, variant string) error {
	if hasVariant && !slices.Contains(e.Variants, variant) {
		return fmt.Errorf("%w: %s has no $%s variant", ErrUnknownEntry, e.Class, variant)
	}
	return nil
}

// Resolution is the answer to "what class should this config name?".
type Resolution struct {
	Requested string `json:"requested"`
	// Class is the class to configure: the requested one, or its new name
	// when Q1b records a rename. A variant suffix is kept.
	Class   string `json:"class"`
	Renamed bool   `json:"renamed"`
	// IssueID and Source cite the Q1b known issue of a rename; Affected is
	// that issue's release range, where the old class no longer works.
	IssueID  string `json:"issue_id,omitempty"`
	Source   string `json:"source,omitempty"`
	Affected string `json:"affected,omitempty"`
	// Entry is the index entry of Class, when the index has one; a renamed
	// class outside this index (a schema history class) has none.
	Entry *Entry `json:"entry,omitempty"`
}

// ResolveClass resolves a fully qualified class as a config names it
// (transforms.x.type, connector.class, a value Q1b records) for release
// rel of the Debezium deployment: a class Q1b records as renamed resolves
// to its new name when the rename's known issue holds for rel, and a class
// in the index resolves to its entry. Before the rename the old class is
// still the one to configure: it resolves to itself, not Renamed, with
// the issue cited. It returns ErrUnknownEntry when the class is neither
// renamed nor indexed, or the variant is not one the entry has.
//
// rel "" means the release is not known: every rename is applied, and the
// caller must check the release against Resolution.Affected before
// writing the new class, which does not exist before that range (the
// schema history classes moved in Debezium 2.0). A rel that does not parse
// is an error.
func (ix *Index) ResolveClass(name, rel string) (Resolution, error) {
	name = strings.TrimSpace(name)
	class, variant, hasVariant := strings.Cut(name, "$")
	if rel != "" {
		if _, err := parseRelease(rel); err != nil {
			return Resolution{}, fmt.Errorf("knowledge: resolve %s: %w", name, err)
		}
	}
	res := Resolution{Requested: name, Class: name}
	if newClass, ok := ix.renamed[class]; ok {
		issue := ix.renames[class]
		res.IssueID, res.Source, res.Affected = issue.ID, issue.Source, issue.Affected
		if rel != "" && !issue.Holds(rel) {
			// The old class is current at rel; it is not in the index.
			return res, nil
		}
		res.Class, res.Renamed = newClass, true
		if hasVariant {
			res.Class += "$" + variant
		}
		class = newClass
	}
	for i := range ix.entries {
		if ix.entries[i].Class == class {
			e := ix.entries[i]
			if err := checkVariant(e, hasVariant, variant); err != nil {
				return Resolution{}, err
			}
			res.Entry = &e
			return res, nil
		}
	}
	if res.Renamed {
		return res, nil
	}
	return Resolution{}, fmt.Errorf("%w: class %q", ErrUnknownEntry, name)
}

// RenamedClasses returns the class renames taken from Q1b, old to new.
func (ix *Index) RenamedClasses() map[string]string {
	out := make(map[string]string, len(ix.renamed))
	for k, v := range ix.renamed {
		out[k] = v
	}
	return out
}

// Archive returns the pinned plugin archive that installs entry e at a
// release of its component. It returns ErrUnpinned for an unpinned
// entry (plugin_install must refuse it), ErrUnsupportedRelease when the
// release is outside the entry's verified range (an archive of that
// release may lack the class), ErrBundled for an entry with nothing of
// its own to install, and ErrUnknownRelease when the entry is pinned but
// not for that release. Releases are compared parsed, so "3.2.0" finds the
// 3.2.0.Final archive. An entry whose plugin_ref names a plugins.go
// connector resolves through ResolvePlugin.
func (ix *Index) Archive(e Entry, rel string) (Archive, error) {
	if e.Install == InstallUnpinned {
		return Archive{}, fmt.Errorf("%w: %s: %s", ErrUnpinned, e.Name, e.UnpinnedReason)
	}
	sup, err := e.Supports(rel)
	if err != nil {
		return Archive{}, err
	}
	if sup != Supported {
		return Archive{}, fmt.Errorf("%w: %s %s is not in %q", ErrUnsupportedRelease, e.Name, rel, e.Versions)
	}
	switch {
	case e.PluginRef != "" && ix.byName(e.PluginRef).Name != "":
		return ix.Archive(ix.byName(e.PluginRef), rel)
	case e.PluginRef != "":
		p, err := ResolvePlugin(pluginRelease(rel), e.PluginRef)
		if err != nil {
			return Archive{}, err
		}
		return Archive{Release: p.Release, URL: p.ArtifactURL(), Digest: p.Digest, Source: p.Source}, nil
	case e.Install == InstallBundled:
		return Archive{}, fmt.Errorf("%w: %s", ErrBundled, e.Name)
	}
	r, _ := parseRelease(rel) // Supports parsed it
	for _, a := range e.Archives {
		if ar, err := parseRelease(a.Release); err == nil && ar.compare(r) == 0 {
			return a, nil
		}
	}
	return Archive{}, fmt.Errorf("%w: %s has no archive for %s", ErrUnknownRelease, e.Name, rel)
}

// pluginRelease returns the plugins.go spelling of rel ("3.2.0" ->
// "3.2.0.Final"), or rel when the table has no release equal to it.
func pluginRelease(rel string) string {
	r, err := parseRelease(rel)
	if err != nil {
		return rel
	}
	for _, have := range Releases() {
		if hr, err := parseRelease(have); err == nil && hr.compare(r) == 0 {
			return have
		}
	}
	return rel
}

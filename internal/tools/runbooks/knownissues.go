package runbooks

import (
	"bytes"
	"embed"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"regexp"
	"slices"
	"sort"
	"strings"

	"gopkg.in/yaml.v3"
)

// maxIssueFileBytes caps one known-issues file; the index is reviewed text,
// not a dump.
const maxIssueFileBytes = 64 << 10

// Components a known issue can name. "debezium" is any Debezium connector or
// SMT; the debezium-* names are one connector each.
var issueComponents = []string{
	"debezium", "debezium-postgres", "debezium-mysql", "debezium-mariadb", "debezium-sqlserver",
	"debezium-mongodb", "debezium-oracle", "debezium-jdbc-sink", "debezium-server", "debezium-image",
	"connect", "kafka", "pgjdbc", "postgres", "mysql", "sqlserver", "mongodb", "oracle",
}

// Themes are the five groups of the plan (docs/product/so-gaps-2026-10.md §6).
var issueThemes = []string{"renamed", "removed", "plugin-mismatch", "fixed-bug", "db-limit"}

// Rename is a config key (or value) that a release renamed. A renamed key
// keeps its value's meaning; a key whose value changes form, or whose old
// name is still valid on another component (database.dbname), is not listed
// as a rename, since RenamedKey is not per component.
type Rename struct {
	Old string `yaml:"old" json:"old"`
	New string `yaml:"new" json:"new"`
}

// KnownIssue is one entry of library/known-issues/*.yaml: a version-bound
// fact about a pipeline component, with the release that fixed it (if any),
// what to do until then, and where it is documented.
type KnownIssue struct {
	ID        string `yaml:"id"`
	Title     string `yaml:"title"`
	Component string `yaml:"component"`
	Theme     string `yaml:"theme"`
	// Affected is the version range the problem appears in: space-separated
	// constraints (">=2.0.0.Final <2.3.0.Final") or "*" for every version.
	Affected string `yaml:"affected"`
	// FixedIn is the first release without the problem; empty when no release
	// fixes it (a removal, a rename or a database limit).
	FixedIn       string   `yaml:"fixed_in"`
	Symptoms      []string `yaml:"symptoms"`
	Workaround    string   `yaml:"workaround"`
	RenamedKeys   []Rename `yaml:"renamed_keys"`
	RenamedValues []Rename `yaml:"renamed_values"`
	Source        string   `yaml:"source"`

	rng versionRange
}

// Holds reports whether v is in the entry's affected range. A version that
// does not parse is never held.
func (k KnownIssue) Holds(v string) bool {
	pv, err := parseVersion(v)
	return err == nil && k.rng.holds(pv)
}

// bounded reports whether the range is version-specific (not "*").
func (k KnownIssue) bounded() bool { return len(k.rng.bounds) > 0 }

// KnownIssues is the parsed index. It is read-only after load.
type KnownIssues struct {
	list  []KnownIssue
	byKey map[string]int // renamed old key -> index into list
	byVal map[string]int // renamed old value -> index into list
}

//go:embed library/known-issues/*.yaml
var knownIssueFiles embed.FS

// EmbeddedKnownIssues returns the index shipped in library/known-issues.
func EmbeddedKnownIssues() (*KnownIssues, error) {
	sub, err := fs.Sub(knownIssueFiles, "library/known-issues")
	if err != nil {
		return nil, fmt.Errorf("runbooks: embedded known issues: %w", err)
	}
	return LoadKnownIssues(sub)
}

// LoadKnownIssues parses every *.yaml at the root of fsys, sorted by name. A
// bad entry (unknown field or component, bad range, a source off the allowed
// hosts, a duplicate id or renamed key) is an error, so it fails start-up.
func LoadKnownIssues(fsys fs.FS) (*KnownIssues, error) {
	names, err := fs.Glob(fsys, "*.yaml")
	if err != nil {
		return nil, fmt.Errorf("runbooks: list known issues: %w", err)
	}
	sort.Strings(names)
	ki := &KnownIssues{byKey: map[string]int{}, byVal: map[string]int{}}
	ids := map[string]bool{}
	for _, name := range names {
		b, err := fs.ReadFile(fsys, name)
		if err != nil {
			return nil, fmt.Errorf("runbooks: %s: %w", name, err)
		}
		if len(b) > maxIssueFileBytes {
			return nil, fmt.Errorf("runbooks: %s: %d bytes, more than %d", name, len(b), maxIssueFileBytes)
		}
		var entries []KnownIssue
		dec := yaml.NewDecoder(bytes.NewReader(b))
		dec.KnownFields(true)
		if err := dec.Decode(&entries); err != nil && !errors.Is(err, io.EOF) {
			return nil, fmt.Errorf("runbooks: %s: %w", name, err)
		}
		if len(entries) == 0 {
			return nil, fmt.Errorf("runbooks: %s: no entries", name)
		}
		for _, e := range entries {
			if err := e.check(); err != nil {
				return nil, fmt.Errorf("runbooks: %s: %s: %w", name, e.ID, err)
			}
			if ids[e.ID] {
				return nil, fmt.Errorf("runbooks: %s: duplicate known-issue id %q", name, e.ID)
			}
			ids[e.ID] = true
			i := len(ki.list)
			for _, r := range e.RenamedKeys {
				if j, dup := ki.byKey[r.Old]; dup {
					return nil, fmt.Errorf("runbooks: %s: %s: renamed key %q is also in %s", name, e.ID, r.Old, ki.list[j].ID)
				}
				ki.byKey[r.Old] = i
			}
			for _, r := range e.RenamedValues {
				if j, dup := ki.byVal[r.Old]; dup {
					return nil, fmt.Errorf("runbooks: %s: %s: renamed value %q is also in %s", name, e.ID, r.Old, ki.list[j].ID)
				}
				ki.byVal[r.Old] = i
			}
			ki.list = append(ki.list, e)
		}
	}
	return ki, nil
}

var (
	issueIDRe = regexp.MustCompile(`^[a-z0-9][a-z0-9-]{2,63}$`)
	// A config key or value: dotted names, class names, snake/kebab values.
	// A trailing "*" on both sides is a prefix rename.
	renameRe = regexp.MustCompile(`^[A-Za-z0-9_.-]{1,200}\*?$`)
)

func (e *KnownIssue) check() error {
	switch {
	case !issueIDRe.MatchString(e.ID):
		return fmt.Errorf("id %q must match %s", e.ID, issueIDRe)
	case strings.TrimSpace(e.Title) == "":
		return errors.New("title is required")
	case !slices.Contains(issueComponents, e.Component):
		return fmt.Errorf("component %q is not one of %s", e.Component, strings.Join(issueComponents, ", "))
	case !slices.Contains(issueThemes, e.Theme):
		return fmt.Errorf("theme %q is not one of %s", e.Theme, strings.Join(issueThemes, ", "))
	case len(e.Symptoms) == 0:
		return errors.New("symptoms are required")
	case strings.TrimSpace(e.Workaround) == "":
		return errors.New("workaround is required")
	}
	for _, s := range e.Symptoms {
		if strings.TrimSpace(s) == "" {
			return errors.New("empty symptom")
		}
	}
	rng, err := parseRange(e.Affected)
	if err != nil {
		return fmt.Errorf("affected: %w", err)
	}
	e.rng = rng
	if e.FixedIn != "" {
		fv, err := parseVersion(e.FixedIn)
		if err != nil {
			return fmt.Errorf("fixed_in: %w", err)
		}
		if rng.holds(fv) {
			return fmt.Errorf("fixed_in %s is inside the affected range %q", e.FixedIn, e.Affected)
		}
	}
	for _, r := range append(slices.Clone(e.RenamedKeys), e.RenamedValues...) {
		if !renameRe.MatchString(r.Old) || !renameRe.MatchString(r.New) || r.Old == r.New ||
			strings.HasSuffix(r.Old, "*") != strings.HasSuffix(r.New, "*") {
			return fmt.Errorf("rename %q -> %q: both must be config names, differ, and both or neither end in *", r.Old, r.New)
		}
	}
	return checkSource(e.Source)
}

// All returns the entries in file then entry order.
func (k *KnownIssues) All() []KnownIssue { return slices.Clone(k.list) }

// RenamedKey returns the new name of a config key a release renamed, and the
// entry that says so (Q4a names its id when Connect rejects the old key).
func (k *KnownIssues) RenamedKey(old string) (newKey string, issue KnownIssue, ok bool) {
	if k == nil {
		return "", KnownIssue{}, false
	}
	return k.renamed(k.byKey, old, func(e KnownIssue) []Rename { return e.RenamedKeys })
}

// RenamedValue is RenamedKey for config values (class names, enum values).
func (k *KnownIssues) RenamedValue(old string) (newValue string, issue KnownIssue, ok bool) {
	if k == nil {
		return "", KnownIssue{}, false
	}
	return k.renamed(k.byVal, old, func(e KnownIssue) []Rename { return e.RenamedValues })
}

func (k *KnownIssues) renamed(idx map[string]int, old string, list func(KnownIssue) []Rename) (string, KnownIssue, bool) {
	old = strings.TrimSpace(old)
	i, ok := idx[old]
	if !ok {
		// A prefix rename (database.history.* -> schema.history.internal.*) covers
		// every key under it, e.g. the producer and consumer security keys. The
		// longest prefix wins.
		best := -1
		for o, j := range idx {
			if p, isPrefix := strings.CutSuffix(o, "*"); isPrefix && len(old) > len(p) && strings.HasPrefix(old, p) && len(p) > best {
				i, best = j, len(p)
			}
		}
		if best < 0 {
			return "", KnownIssue{}, false
		}
	}
	e := k.list[i]
	if r, ok := matchRename(list(e), old); ok {
		return r, e, true
	}
	return "", KnownIssue{}, false
}

// matchRename applies the exact rename of old, else its longest prefix rename.
func matchRename(rs []Rename, old string) (string, bool) {
	best, out := -1, ""
	for _, r := range rs {
		if r.Old == old {
			return r.New, true
		}
		if p, isPrefix := strings.CutSuffix(r.Old, "*"); isPrefix && len(old) > len(p) && strings.HasPrefix(old, p) && len(p) > best {
			best, out = len(p), strings.TrimSuffix(r.New, "*")+strings.TrimPrefix(old, p)
		}
	}
	return out, best >= 0
}

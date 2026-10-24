package setuptools

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"slices"
	"sort"
	"strings"
	"unicode/utf8"

	"piperoom/internal/setup/knowledge"
	"piperoom/internal/tools"
)

// ConfigHelpName is the name of the config help tool (row U3b).
const ConfigHelpName = "setup_config_help"

const (
	// maxHelpName caps the name a config help call looks up: the longest
	// indexed class is well under it.
	maxHelpName = 300
	// maxNearest is how many near matches an unknown name gets.
	maxNearest = 3
	// maxListed caps the tuning entries one listing names in its summary.
	maxListed = 40
)

// helpReleaseRe is the shape of a release a call may pass; ResolveClass parses
// it, this only keeps model text out of an error.
var helpReleaseRe = regexp.MustCompile(`^[0-9][0-9A-Za-z.\-]{0,39}$`)

// helpConnectorRe is the shape of a plugins.go connector short name.
var helpConnectorRe = regexp.MustCompile(`^[a-z][a-z0-9]{0,31}$`)

// helpKinds are the kinds the tool answers. The board names smt and sink;
// predicate is configured beside an SMT (predicates.<alias>.type) and tuning
// is the U10a index, which waited on this row.
var helpKinds = []knowledge.Kind{knowledge.KindSMT, knowledge.KindPredicate, knowledge.KindSink, knowledge.KindTuning}

// NewConfigHelp returns setup_config_help over the shipped SMT, sink and
// tuning index. It holds the index and nothing else: no runner, no client, so
// it can read no customer system. ix is the value knowledge.LoadIndex returned
// at start.
func NewConfigHelp(ix *knowledge.Index) (tools.Tool, error) {
	if ix == nil {
		return nil, errors.New("setuptools: setup_config_help needs the knowledge index (knowledge.LoadIndex)")
	}
	return &configHelpTool{ix: ix}, nil
}

// RegisterConfigHelp adds setup_config_help to reg. Like Register, it needs a
// read-only registry: the tool is KindRead.
func RegisterConfigHelp(reg *tools.Registry, ix *knowledge.Index) error {
	if reg == nil {
		return errors.New("setuptools: a registry is required")
	}
	if !reg.ReadOnly() {
		return errors.New("setuptools: setup_config_help needs a read-only registry (tools.NewReadOnlyRegistry)")
	}
	t, err := NewConfigHelp(ix)
	if err != nil {
		return err
	}
	return reg.Register(t)
}

type configHelpTool struct{ ix *knowledge.Index }

type configHelpInput struct {
	Kind      string `json:"kind"`
	Name      string `json:"name"`
	Release   string `json:"release"`
	Connector string `json:"connector"`
}

// ConfigHelpAnswer is the Data of a setup_config_help result.
type ConfigHelpAnswer struct {
	Kind  string `json:"kind"`
	Found bool   `json:"found"`
	// Entry is the index entry the name resolved to.
	Entry *knowledge.Entry `json:"entry,omitempty"`
	// Resolution is set when the name was a class: it says which class to
	// configure, and cites the known issue of a rename.
	Resolution *knowledge.Resolution `json:"resolution,omitempty"`
	// Support places the release the call passed against the entry's
	// verified range: "supported", "unverified" or "removed".
	Support string `json:"support,omitempty"`
	// Nearest are the closest known entries when the name matched none.
	Nearest []NearMatch `json:"nearest,omitempty"`
	// Listed are the tuning entries for a connector (kind tuning, no name).
	Listed []knowledge.Entry `json:"listed,omitempty"`
}

// NearMatch is one known entry close to an unknown name.
type NearMatch struct {
	Name  string `json:"name"`
	Class string `json:"class,omitempty"`
	// Matched is the entry's name, class, sink type or key the distance was
	// measured to.
	Matched  string `json:"matched"`
	Distance int    `json:"distance"`
	// tie breaks equal distances: the distance to the whole class (or the
	// name), so a dotted query prefers the class in its own package.
	tie int
}

func (t *configHelpTool) Name() string { return ConfigHelpName }

func (t *configHelpTool) Description() string {
	return "Look up a Kafka Connect SMT, predicate, sink connector or tuning setting in Piperoom's shipped knowledge index: its class, keys with defaults and allowed values, the verified release range, the checks it implies, how it is installed and the doc it cites. " +
		"kind is smt, predicate, sink or tuning; name is an index name, a class (renamed classes resolve to their new name), a simple class name, a Debezium Server sink type, or for tuning a key such as max.batch.size. " +
		"Pass release (the Debezium release, or the Kafka release for a Kafka SMT) to check support. For tuning, an empty name with connector (postgres, mysql, ...) lists that source's settings and the symptoms that justify each. " +
		"An unknown name answers the nearest known entries. Reads only the shipped index, never a customer system; use it before adding transforms, a sink or a tuning key to a connector config."
}

func (t *configHelpTool) InputSchema() map[string]any {
	kinds := make([]string, len(helpKinds))
	for i, k := range helpKinds {
		kinds[i] = string(k)
	}
	return objectSchema(map[string]any{
		"kind": map[string]any{"type": "string", "enum": kinds, "description": "what the name configures"},
		"name": map[string]any{"type": "string", "maxLength": maxHelpName,
			"description": "index name, class, simple class name, sink type or tuning key; empty only for a tuning listing"},
		"release": map[string]any{"type": "string", "maxLength": 40,
			"description": "optional release of the entry's component (Debezium or Kafka) to check support and renames against"},
		"connector": map[string]any{"type": "string", "maxLength": 32,
			"description": "tuning only: the source connector short name (postgres, mysql, sqlserver, mongodb, oracle)"},
	}, "kind", "name")
}

// Kind is KindRead: the tool reads the shipped index only.
func (t *configHelpTool) Kind() tools.Kind { return tools.KindRead }

func (t *configHelpTool) Run(ctx context.Context, input json.RawMessage) (tools.Result, error) {
	if err := ctx.Err(); err != nil {
		return tools.Result{}, err
	}
	var in configHelpInput
	if err := decodeInput(input, &in); err != nil {
		return tools.Result{}, fmt.Errorf("%w: %s: %v", tools.ErrBadInput, ConfigHelpName, err)
	}
	kind := knowledge.Kind(in.Kind)
	if !slices.Contains(helpKinds, kind) {
		return tools.Result{}, fmt.Errorf("%w: %s: kind must be smt, predicate, sink or tuning", tools.ErrBadInput, ConfigHelpName)
	}
	name := strings.TrimSpace(in.Name)
	if len(name) > maxHelpName || strings.ContainsAny(name, "\r\n\t") {
		return tools.Result{}, fmt.Errorf("%w: %s: name is not a class, key or index name", tools.ErrBadInput, ConfigHelpName)
	}
	if in.Release != "" && !helpReleaseRe.MatchString(in.Release) {
		return tools.Result{}, fmt.Errorf("%w: %s: release is not a release number", tools.ErrBadInput, ConfigHelpName)
	}
	if in.Connector != "" && (kind != knowledge.KindTuning || !helpConnectorRe.MatchString(in.Connector)) {
		return tools.Result{}, fmt.Errorf("%w: %s: connector is a tuning-only source short name", tools.ErrBadInput, ConfigHelpName)
	}
	if name == "" {
		if kind != knowledge.KindTuning {
			return tools.Result{}, fmt.Errorf("%w: %s: name is required for kind %s", tools.ErrBadInput, ConfigHelpName, kind)
		}
		return t.listTuning(in.Connector), nil
	}

	ans := ConfigHelpAnswer{Kind: string(kind)}
	entry, res, err := t.resolve(kind, name, in.Release)
	switch {
	case errors.Is(err, knowledge.ErrUnknownEntry):
		ans.Nearest = Nearest(t.ix, kind, name, maxNearest)
		return t.unknown(ans, name), nil
	case err != nil:
		return tools.Result{}, fmt.Errorf("%w: %s: %v", tools.ErrBadInput, ConfigHelpName, err)
	}
	ans.Found, ans.Entry, ans.Resolution = true, &entry, res
	if in.Release != "" {
		sup, err := entry.Supports(in.Release)
		if err != nil {
			return tools.Result{}, fmt.Errorf("%w: %s: release: %v", tools.ErrBadInput, ConfigHelpName, err)
		}
		ans.Support = supportWord(sup)
	}
	summary, cut := tools.Truncate(entrySummary(entry, res, ans.Support))
	return tools.Result{Summary: summary, Data: ans, Truncated: cut}, nil
}

// resolve finds the entry a name means. A dotted name is a class first, so a
// Q1b rename resolves to the new class; anything else goes through Lookup.
func (t *configHelpTool) resolve(kind knowledge.Kind, name, rel string) (knowledge.Entry, *knowledge.Resolution, error) {
	if kind != knowledge.KindTuning && looksLikeClass(name) {
		res, err := t.ix.ResolveClass(name, rel)
		if err == nil && res.Entry != nil {
			if res.Entry.Kind != kind {
				return knowledge.Entry{}, nil, fmt.Errorf("%w: %s is a %s, not a %s", knowledge.ErrUnknownEntry, res.Entry.Name, res.Entry.Kind, kind)
			}
			return *res.Entry, &res, nil
		}
		if err == nil {
			// A rename to a class outside this index (a schema history
			// class): not an SMT, predicate or sink.
			return knowledge.Entry{}, nil, fmt.Errorf("%w: class %s is not a %s", knowledge.ErrUnknownEntry, res.Class, kind)
		}
		if !errors.Is(err, knowledge.ErrUnknownEntry) {
			return knowledge.Entry{}, nil, err
		}
	}
	e, err := t.ix.Lookup(kind, name)
	if err != nil {
		return knowledge.Entry{}, nil, err
	}
	return e, nil, nil
}

func (t *configHelpTool) unknown(ans ConfigHelpAnswer, name string) tools.Result {
	var b strings.Builder
	fmt.Fprintf(&b, "%s: no %s named %q in the index (or the name is ambiguous).", ConfigHelpName, ans.Kind, clipName(name))
	if len(ans.Nearest) > 0 {
		b.WriteString(" Nearest known:")
		for _, n := range ans.Nearest {
			label := n.Name
			if n.Class != "" {
				label += " (" + n.Class + ")"
			}
			b.WriteString(" " + label + ";")
		}
	}
	summary, cut := tools.Truncate(b.String())
	return tools.Result{Summary: summary, Data: ans, Truncated: cut}
}

func (t *configHelpTool) listTuning(connector string) tools.Result {
	listed := t.ix.Tuning(connector)
	var b strings.Builder
	scope := "every source"
	if connector != "" {
		scope = connector
	}
	fmt.Fprintf(&b, "%d tuning entries for %s. Propose one only after one of its symptoms is seen.", len(listed), scope)
	for i, e := range listed {
		if i == maxListed {
			fmt.Fprintf(&b, " ... %d more in data.", len(listed)-maxListed)
			break
		}
		keys := make([]string, 0, len(e.Keys))
		for _, k := range e.Keys {
			keys = append(keys, k.Name)
		}
		fmt.Fprintf(&b, "\n- %s [%s] %s", e.Name, strings.Join(keys, ", "), e.Summary)
	}
	summary, cut := tools.Truncate(b.String())
	return tools.Result{Summary: summary, Data: ConfigHelpAnswer{Kind: string(knowledge.KindTuning), Found: len(listed) > 0, Listed: listed}, Truncated: cut}
}

// entrySummary is the model's view of an entry: the class to configure, the
// keys with defaults, the checks and the citation. Data carries the whole
// entry.
func entrySummary(e knowledge.Entry, res *knowledge.Resolution, support string) string {
	var b strings.Builder
	fmt.Fprintf(&b, "%s (%s, %s on %s): %s", e.Name, e.Kind, e.Component, e.Runtime, e.Summary)
	switch {
	case res != nil && res.Renamed:
		fmt.Fprintf(&b, "\nConfigure %s, not %s (renamed: %s, affected %s).", res.Class, res.Requested, res.IssueID, res.Affected)
	case e.Class != "":
		fmt.Fprintf(&b, "\nClass: %s", e.Class)
		if len(e.Variants) > 0 {
			fmt.Fprintf(&b, " (abstract; configure %s$%s)", e.Class, strings.Join(e.Variants, " or $"))
		}
	case e.SinkType != "":
		fmt.Fprintf(&b, "\nSink type: %s", e.SinkType)
	}
	if e.Scope != "" {
		fmt.Fprintf(&b, "\nSet in: %s", e.Scope)
	}
	fmt.Fprintf(&b, "\nVersions: %s", e.Versions)
	if support != "" {
		fmt.Fprintf(&b, " (the release asked is %s)", support)
	}
	if e.Successor != "" {
		fmt.Fprintf(&b, "; successor: %s", e.Successor)
	}
	if e.Install != "" {
		fmt.Fprintf(&b, "\nInstall: %s", e.Install)
		if e.UnpinnedReason != "" {
			fmt.Fprintf(&b, " (%s)", e.UnpinnedReason)
		}
	}
	if len(e.Keys) > 0 {
		b.WriteString("\nKeys:")
		for _, k := range e.Keys {
			b.WriteString("\n- " + k.Name)
			var attrs []string
			if k.Required {
				attrs = append(attrs, "required")
			}
			if k.Default != nil {
				attrs = append(attrs, "default "+*k.Default)
			}
			if len(k.Values) > 0 {
				attrs = append(attrs, "one of "+strings.Join(k.Values, "|"))
			}
			if k.Secret {
				attrs = append(attrs, "secret: a ${secret:name} reference only")
			}
			if k.Since != "" {
				attrs = append(attrs, "since "+k.Since)
			}
			if k.Until != "" {
				attrs = append(attrs, "until "+k.Until)
			}
			if len(k.Formerly) > 0 {
				attrs = append(attrs, "formerly "+strings.Join(k.Formerly, ", "))
			}
			if len(attrs) > 0 {
				b.WriteString(" (" + strings.Join(attrs, "; ") + ")")
			}
			if k.Check != "" {
				b.WriteString(": " + k.Check)
			}
		}
	}
	for _, s := range e.Symptoms {
		b.WriteString("\nSymptom: " + s)
	}
	for _, c := range e.Checks {
		b.WriteString("\nCheck: " + c)
	}
	fmt.Fprintf(&b, "\nDoc: %s", e.Doc)
	return b.String()
}

func supportWord(s knowledge.Support) string {
	switch s {
	case knowledge.Supported:
		return "supported"
	case knowledge.Unverified:
		return "unverified"
	case knowledge.Removed:
		return "removed"
	}
	return ""
}

// looksLikeClass reports whether a name is a dotted Java class (with or
// without a $Key/$Value variant).
func looksLikeClass(name string) bool {
	class, _, _ := strings.Cut(name, "$")
	return strings.Contains(class, ".") && !strings.ContainsAny(class, " /:")
}

// Nearest returns up to n entries of kind closest to name, by edit distance
// to each entry's name, class, simple class name, sink type and (for tuning)
// keys, case-insensitively. Ties break by the distance to the whole class,
// then by entry name, so the answer is stable.
// It is how an unknown SMT class in a config gets the nearest known class.
func Nearest(ix *knowledge.Index, kind knowledge.Kind, name string, n int) []NearMatch {
	if ix == nil || n <= 0 {
		return nil
	}
	q := strings.ToLower(strings.TrimSpace(name))
	if cls, _, ok := strings.Cut(q, "$"); ok {
		q = cls
	}
	qSimple := q
	if i := strings.LastIndex(q, "."); i >= 0 && looksLikeClass(name) {
		qSimple = q[i+1:]
	}
	var out []NearMatch
	for _, e := range ix.Entries() {
		if e.Kind != kind {
			continue
		}
		best := NearMatch{Name: e.Name, Class: e.Class, Distance: -1}
		try := func(cand, query string) {
			if cand == "" {
				return
			}
			d := editDistance(query, strings.ToLower(cand))
			if best.Distance < 0 || d < best.Distance {
				best.Distance, best.Matched = d, cand
			}
		}
		try(e.Name, q)
		try(e.Class, q)
		if i := strings.LastIndex(e.Class, "."); i >= 0 {
			try(e.Class[i+1:], qSimple)
		}
		try(e.SinkType, q)
		for _, k := range e.Keys {
			if kind == knowledge.KindTuning {
				try(k.Name, q)
			}
		}
		full := e.Class
		if full == "" {
			full = e.Name
		}
		best.tie = editDistance(q, strings.ToLower(full))
		if best.Distance >= 0 {
			out = append(out, best)
		}
	}
	sort.SliceStable(out, func(i, j int) bool {
		if out[i].Distance != out[j].Distance {
			return out[i].Distance < out[j].Distance
		}
		if out[i].tie != out[j].tie {
			return out[i].tie < out[j].tie
		}
		return out[i].Name < out[j].Name
	})
	if len(out) > n {
		out = out[:n]
	}
	return out
}

// editDistance is the Levenshtein distance over bytes; every candidate is
// ASCII, and a non-ASCII query only ever scores farther.
func editDistance(a, b string) int {
	if len(a) > maxHelpName {
		a = a[:maxHelpName]
	}
	prev := make([]int, len(b)+1)
	cur := make([]int, len(b)+1)
	for j := range prev {
		prev[j] = j
	}
	for i := 1; i <= len(a); i++ {
		cur[0] = i
		for j := 1; j <= len(b); j++ {
			cost := 1
			if a[i-1] == b[j-1] {
				cost = 0
			}
			cur[j] = min(prev[j]+1, cur[j-1]+1, prev[j-1]+cost)
		}
		prev, cur = cur, prev
	}
	return prev[len(b)]
}

// clipName cuts model text before it is quoted in a summary or error.
func clipName(s string) string {
	s = strings.ToValidUTF8(s, "?")
	if len(s) <= 80 {
		return s
	}
	cut := 80
	for cut > 0 && !utf8.RuneStart(s[cut]) {
		cut--
	}
	return s[:cut] + "…"
}

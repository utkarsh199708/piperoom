//go:build evalgen

// Command gen regenerates the replayed tool results of eval scenarios. Each
// incident is a set of fake clients (fakes.go) holding the recorded stack's
// state per phase; gen runs the REAL kafkatools, dbtools and topology code on
// top of them and writes what those tools return into the scenario files, so
// every summary and data payload is byte for byte what production would show
// the model. Timestamps are moved to a fixed clock (fixedNow) so the output
// is reproducible.
//
// What gen owns depends on the file. For the held-out set (heldout.go) it
// writes pipeline, resources, alert.pipeline_map and every recording; the
// rest (comments, description, alert title, phases, transitions, inputs,
// expect) is hand-written and kept. For the two converted C7 scenarios
// (orders.go) it owns only the recordings it generates and leaves C7's own
// recordings alone.
//
//	go run -tags evalgen ./evals/gen          # rewrite the files (run from the repo root)
//	go run -tags evalgen ./evals/gen -check   # exit 1 if any file is out of date
//
// Regenerate after a tool's output format changes on main.
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"net/http"
	"os"
	"regexp"
	"sort"
	"strings"
	"time"

	"gopkg.in/yaml.v3"

	"piperoom/internal/clients"
	"piperoom/internal/clients/connect"
	"piperoom/internal/owners"
	"piperoom/internal/tools"
	"piperoom/internal/tools/dbtools"
	"piperoom/internal/tools/kafkatools"
	"piperoom/internal/topology"
)

// fixedNow is the clock every recorded timestamp is expressed against (the
// same one C7 used for the orders scenarios).
var fixedNow = time.Date(2026, 10, 11, 9, 40, 0, 0, time.UTC)

// connectURL replaces the throwaway server's address in real Connect client
// errors, so they read as they would on the recorded stack.
const connectURL = "http://connect:8083"

// clock builds a world's times. now is the wall clock the tools will see,
// truncated to the second; the output is shifted from now to fixedNow.
type clock struct{ now time.Time }

func (c clock) ago(d time.Duration) time.Time { return c.now.Add(-d) }

// spec is one scenario file and how to generate it.
type spec struct {
	file     string // path from the repo root
	owners   string // owners file the tools are scoped by
	pipeline string
	// phases are the scenario's phases, start first; nil for a static one.
	phases []string
	// world returns the stack's state in a phase at a tick (the index of an
	// observation in a sequence; 0 otherwise).
	world func(c clock, phase string, tick int) *world
	calls []call
	// partial: gen owns only the recordings it generates (C7's files).
	partial bool
}

// call is one tool call to record.
type call struct {
	tool  string
	input map[string]any
	// free are input fields the model picks freely: they are used to run
	// the tool and left out of the recording, so any value matches.
	free []string
	// wildcard records the result with no input: the fallback for names the
	// scenario does not know.
	wildcard bool
	// only limits the call to these phases (default every phase).
	only []string
	// seq is the number of observations per phase (default 1). More than
	// one makes a sequence: tick k is observation k.
	seq map[string]int
}

// ago in an input is replaced by the RFC 3339 time that long before now.
type ago time.Duration

func main() {
	check := flag.Bool("check", false, "report files that are out of date and exit 1; write nothing")
	roomsOnly := flag.Bool("rooms", false, "only the room export fixtures of evals/testdata/rooms (L1a)")
	flag.Parse()
	var stale []string
	scenarioSpecs := specs()
	if *roomsOnly {
		scenarioSpecs = nil
	}
	for _, s := range scenarioSpecs {
		out, err := s.render()
		if err != nil {
			fmt.Fprintln(os.Stderr, "gen:", s.file+":", err)
			os.Exit(1)
		}
		cur, err := os.ReadFile(s.file)
		if err != nil {
			fmt.Fprintln(os.Stderr, "gen:", err)
			os.Exit(1)
		}
		if bytes.Equal(cur, out) {
			continue
		}
		if *check {
			stale = append(stale, s.file)
			continue
		}
		if err := os.WriteFile(s.file, out, 0o644); err != nil {
			fmt.Fprintln(os.Stderr, "gen:", err)
			os.Exit(1)
		}
		fmt.Println("wrote", s.file)
	}
	roomStale, err := roomFixtures(*check)
	if err != nil {
		fmt.Fprintln(os.Stderr, "gen:", err)
		os.Exit(1)
	}
	stale = append(stale, roomStale...)
	if len(stale) > 0 {
		fmt.Fprintln(os.Stderr, "gen: out of date (run go run -tags evalgen ./evals/gen):", strings.Join(stale, ", "))
		os.Exit(1)
	}
}

func specs() []spec {
	return append(heldoutSpecs(), ordersSpecs()...)
}

// render generates the file. The tools read the wall clock, so the whole
// file is generated inside one second; a run that crosses a second boundary
// starts again.
func (s spec) render() ([]byte, error) {
	for attempt := 0; attempt < 5; attempt++ {
		// Start just after a second boundary for the most room.
		time.Sleep(time.Until(time.Now().Truncate(time.Second).Add(time.Second + 5*time.Millisecond)))
		c := clock{now: time.Now().UTC().Truncate(time.Second)}
		out, err := s.renderAt(c)
		if err != nil {
			return nil, err
		}
		if time.Now().UTC().Truncate(time.Second).Equal(c.now) {
			return out, nil
		}
	}
	return nil, errors.New("could not generate within one second; is the machine overloaded?")
}

func (s spec) renderAt(c clock) ([]byte, error) {
	o, err := owners.Load(s.owners)
	if err != nil {
		return nil, err
	}
	b, err := os.ReadFile(s.file)
	if err != nil {
		return nil, err
	}
	var doc yaml.Node
	if err := yaml.Unmarshal(b, &doc); err != nil {
		return nil, fmt.Errorf("parse: %w", err)
	}
	if doc.Kind != yaml.DocumentNode || len(doc.Content) != 1 || doc.Content[0].Kind != yaml.MappingNode {
		return nil, errors.New("not a YAML mapping")
	}
	root := doc.Content[0]

	recs, err := s.recordings(c, o)
	if err != nil {
		return nil, err
	}
	if s.partial {
		if err := mergeRecordings(root, recs); err != nil {
			return nil, err
		}
	} else {
		if err := s.fillHeldout(c, o, root, recs); err != nil {
			return nil, err
		}
	}
	var out bytes.Buffer
	enc := yaml.NewEncoder(&out)
	enc.SetIndent(2)
	if err := enc.Encode(&doc); err != nil {
		return nil, err
	}
	if err := enc.Close(); err != nil {
		return nil, err
	}
	return spaceTopLevel(out.Bytes()), nil
}

// fillHeldout writes everything gen owns in a held-out file.
func (s spec) fillHeldout(c clock, o *owners.Owners, root *yaml.Node, recs []outRec) error {
	p, ok := o.Pipeline(s.pipeline)
	if !ok {
		return fmt.Errorf("pipeline %q not in %s", s.pipeline, s.owners)
	}
	start := ""
	if len(s.phases) > 0 {
		start = s.phases[0]
	}
	w := s.world(c, start, 0)
	pm := runTool(o, s.pipeline, w, "pipeline_map", map[string]any{"pipeline_id": s.pipeline}, c)
	if pm.Error != "" {
		return fmt.Errorf("alert pipeline_map: %s", pm.Error)
	}
	var subjects []string
	for name := range w.subjects {
		subjects = append(subjects, name)
	}
	sort.Strings(subjects)
	conns, err := fakeConnect{w}.ListConnectors(context.Background())
	if err != nil {
		return err
	}
	pipeline := struct {
		ID             string   `yaml:"id"`
		Title          string   `yaml:"title"`
		TopicPrefix    string   `yaml:"topic_prefix"`
		SourceTables   []string `yaml:"source_tables,flow"`
		Connectors     []string `yaml:"connectors,flow"`
		ConsumerGroups []string `yaml:"consumer_groups,flow"`
		SinkTables     []string `yaml:"sink_tables,flow"`
		SinkTSColumn   string   `yaml:"sink_ts_column"`
	}{p.ID, p.Title, p.TopicPrefix, p.SourceTables, p.Connectors, p.ConsumerGroups, p.SinkTables, p.SinkTSColumn}
	resources := struct {
		Connectors []string `yaml:"connectors,flow"`
		Subjects   []string `yaml:"subjects,flow"`
	}{conns, subjects}

	if err := setKey(root, "pipeline", pipeline); err != nil {
		return err
	}
	if err := setKey(root, "resources", resources); err != nil {
		return err
	}
	alert := child(root, "alert")
	if alert == nil {
		return errors.New("alert is missing (write alert.title by hand)")
	}
	if err := setKey(alert, "pipeline_map", pm.Summary); err != nil {
		return err
	}
	child(alert, "pipeline_map").Style = yaml.LiteralStyle
	return setRecordings(root, recs)
}

// recordings runs every call in every phase it applies to.
func (s spec) recordings(c clock, o *owners.Owners) ([]outRec, error) {
	phases := s.phases
	if len(phases) == 0 {
		phases = []string{""}
	}
	var out []outRec
	for _, cl := range s.calls {
		in := map[string]any{}
		for k, v := range cl.input {
			if d, ok := v.(ago); ok {
				v = c.ago(time.Duration(d)).Format(time.RFC3339)
			}
			in[k] = v
		}
		rec := map[string]any{}
		if !cl.wildcard {
			for k, v := range cl.input {
				if !contains(cl.free, k) {
					rec[k] = v
				}
			}
		}
		results := map[string][]outObs{}
		var used []string
		for _, p := range phases {
			if len(cl.only) > 0 && !contains(cl.only, p) {
				continue
			}
			used = append(used, p)
			n := cl.seq[p]
			if n < 1 {
				n = 1
			}
			for k := 0; k < n; k++ {
				ob := runTool(o, s.pipeline, s.world(c, p, k), cl.tool, in, c)
				if ob.Summary == "" && ob.Error == "" {
					return nil, fmt.Errorf("%s %v in phase %q: empty summary", cl.tool, in, p)
				}
				results[p] = append(results[p], ob)
			}
		}
		// One shared recording when every phase answers the same, single
		// result; the full set never shares (a partial file's own
		// recordings are untagged already).
		if !s.partial && len(used) == len(phases) && sameSingle(results, used) {
			out = append(out, newRec(cl.tool, rec, "", results[used[0]]))
			continue
		}
		for _, p := range used {
			out = append(out, newRec(cl.tool, rec, p, results[p]))
		}
	}
	return out, nil
}

func sameSingle(results map[string][]outObs, phases []string) bool {
	first := results[phases[0]]
	if len(first) != 1 {
		return false
	}
	a, _ := json.Marshal(first[0])
	for _, p := range phases[1:] {
		if len(results[p]) != 1 {
			return false
		}
		if b, _ := json.Marshal(results[p][0]); !bytes.Equal(a, b) {
			return false
		}
	}
	return true
}

// outRec is a recording as written to the file, in the field order of
// evals.Recording.
type outRec struct {
	Tool      string         `yaml:"tool"`
	Input     map[string]any `yaml:"input,omitempty,flow"`
	Phase     string         `yaml:"phase,omitempty"`
	Summary   string         `yaml:"summary,omitempty"`
	Data      any            `yaml:"data,omitempty"`
	Truncated bool           `yaml:"truncated,omitempty"`
	Error     string         `yaml:"error,omitempty"`
	Sequence  []outObs       `yaml:"sequence,omitempty"`
}

type outObs struct {
	Summary   string `yaml:"summary,omitempty" json:"summary,omitempty"`
	Data      any    `yaml:"data,omitempty" json:"data,omitempty"`
	Truncated bool   `yaml:"truncated,omitempty" json:"truncated,omitempty"`
	Error     string `yaml:"error,omitempty" json:"error,omitempty"`
}

func newRec(tool string, input map[string]any, phase string, obs []outObs) outRec {
	r := outRec{Tool: tool, Phase: phase}
	if len(input) > 0 {
		r.Input = input
	}
	if len(obs) == 1 {
		r.Summary, r.Data, r.Truncated, r.Error = obs[0].Summary, obs[0].Data, obs[0].Truncated, obs[0].Error
	} else {
		r.Sequence = obs
	}
	return r
}

// runTool runs one real tool against w, scoped to the pipeline as the agent
// scopes it (tools.WithPipelineID), and returns the result with every
// timestamp moved to fixedNow.
func runTool(o *owners.Owners, pipelineID string, w *world, name string, input map[string]any, c clock) outObs {
	var conn clients.ConnectClient = fakeConnect{w}
	var srvURL string
	if w.connectDown {
		srv, stop := hangingConnect()
		defer stop()
		srvURL = srv.URL
		conn = connect.New(srv.URL, &http.Client{Timeout: 20 * time.Millisecond})
	}
	kafkaAdmin, src, wh := fakeKafka{w}, fakeSource{w}, fakeWarehouse{w}
	reg := tools.NewRegistry()
	if err := kafkatools.Register(reg, kafkatools.Deps{Connect: conn, Registry: fakeRegistry{w}, Kafka: kafkaAdmin, Owners: o}); err != nil {
		return outObs{Error: "gen: " + err.Error()}
	}
	b := topology.NewBuilder(o, conn, kafkaAdmin, src, wh)
	if err := dbtools.Register(reg, dbtools.Deps{Src: src, Wh: wh, Builder: b, Connect: conn, Owners: o}); err != nil {
		return outObs{Error: "gen: " + err.Error()}
	}
	t, ok := reg.Get(name)
	if !ok {
		return outObs{Error: "gen: unknown tool " + name}
	}
	raw, err := json.Marshal(input)
	if err != nil {
		return outObs{Error: "gen: " + err.Error()}
	}
	res, err := t.Run(tools.WithPipelineID(context.Background(), pipelineID), raw)
	fix := func(s string) string {
		if srvURL != "" {
			s = strings.ReplaceAll(s, srvURL, connectURL)
		}
		return shiftTimes(s, c)
	}
	if err != nil {
		return outObs{Error: fix(err.Error())}
	}
	ob := outObs{Summary: fix(res.Summary), Truncated: res.Truncated}
	if res.Data != nil {
		b, err := json.Marshal(res.Data)
		if err != nil {
			return outObs{Error: "gen: data: " + err.Error()}
		}
		ob.Data = decodeData([]byte(fix(string(b))))
	}
	return ob
}

var timestampRe = regexp.MustCompile(`\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+-]\d{2}:\d{2})`)

// shiftTimes moves every timestamp near the generation clock to the same
// distance from fixedNow, keeping its precision.
func shiftTimes(s string, c clock) string {
	shift := fixedNow.Sub(c.now)
	return timestampRe.ReplaceAllStringFunc(s, func(m string) string {
		t, err := time.Parse(time.RFC3339Nano, m)
		if err != nil {
			return m
		}
		if d := t.Sub(c.now); d > 60*24*time.Hour || d < -60*24*time.Hour {
			return m
		}
		layout := time.RFC3339
		if strings.Contains(m, ".") {
			layout = time.RFC3339Nano
		}
		return t.Add(shift).UTC().Format(layout)
	})
}

// decodeData turns JSON into plain YAML-friendly values: whole numbers stay
// integers instead of becoming floats.
func decodeData(b []byte) any {
	dec := json.NewDecoder(bytes.NewReader(b))
	dec.UseNumber()
	var v any
	if err := dec.Decode(&v); err != nil {
		return string(b)
	}
	return plain(v)
}

func plain(v any) any {
	switch x := v.(type) {
	case map[string]any:
		for k, e := range x {
			x[k] = plain(e)
		}
		return x
	case []any:
		for i, e := range x {
			x[i] = plain(e)
		}
		return x
	case json.Number:
		if i, err := x.Int64(); err == nil {
			return i
		}
		f, _ := x.Float64()
		return f
	}
	return v
}

// ---- YAML editing ----

func child(m *yaml.Node, key string) *yaml.Node {
	for i := 0; i+1 < len(m.Content); i += 2 {
		if m.Content[i].Value == key {
			return m.Content[i+1]
		}
	}
	return nil
}

// keyOrder is where a missing top-level key is inserted.
var keyOrder = []string{"name", "description", "pipeline", "resources", "alert", "phases", "transitions", "recordings", "inputs", "expect"}

// setKey sets key in mapping m to v, in place when present.
func setKey(m *yaml.Node, key string, v any) error {
	var n yaml.Node
	if err := n.Encode(v); err != nil {
		return err
	}
	for i := 0; i+1 < len(m.Content); i += 2 {
		if m.Content[i].Value == key {
			n.HeadComment, n.LineComment = m.Content[i+1].HeadComment, m.Content[i+1].LineComment
			m.Content[i+1] = &n
			return nil
		}
	}
	k := &yaml.Node{Kind: yaml.ScalarNode, Tag: "!!str", Value: key}
	pos := len(m.Content)
	rank := indexOf(keyOrder, key)
	for i := 0; i+1 < len(m.Content); i += 2 {
		if r := indexOf(keyOrder, m.Content[i].Value); rank >= 0 && r > rank {
			pos = i
			break
		}
	}
	m.Content = append(m.Content[:pos], append([]*yaml.Node{k, &n}, m.Content[pos:]...)...)
	return nil
}

// setRecordings replaces the whole recordings list.
func setRecordings(root *yaml.Node, recs []outRec) error {
	if err := setKey(root, "recordings", recs); err != nil {
		return err
	}
	styleSummaries(child(root, "recordings"))
	return nil
}

// mergeRecordings replaces each generated recording in place (same tool,
// input and phase) and appends the new ones.
func mergeRecordings(root *yaml.Node, recs []outRec) error {
	list := child(root, "recordings")
	if list == nil || list.Kind != yaml.SequenceNode {
		return errors.New("recordings list missing")
	}
	for _, r := range recs {
		var n yaml.Node
		if err := n.Encode(r); err != nil {
			return err
		}
		styleSummaries(&n)
		replaced := false
		for i, item := range list.Content {
			if sameKey(item, r) {
				n.HeadComment = item.HeadComment
				list.Content[i] = &n
				replaced = true
				break
			}
		}
		if !replaced {
			list.Content = append(list.Content, &n)
		}
	}
	return nil
}

// sameKey reports whether a recording node has r's tool, input and phase.
func sameKey(item *yaml.Node, r outRec) bool {
	var got struct {
		Tool  string         `yaml:"tool"`
		Input map[string]any `yaml:"input"`
		Phase string         `yaml:"phase"`
	}
	if err := item.Decode(&got); err != nil {
		return false
	}
	if got.Tool != r.Tool || got.Phase != r.Phase || len(got.Input) != len(r.Input) {
		return false
	}
	for k, v := range r.Input {
		if fmt.Sprint(got.Input[k]) != fmt.Sprint(v) {
			return false
		}
	}
	return true
}

// styleSummaries writes every summary as a block scalar, the way the
// hand-written scenarios do, so a reviewer reads the text the model reads.
// "|" keeps the final newline of dbtools summaries, "|-" marks its absence.
func styleSummaries(n *yaml.Node) {
	if n == nil {
		return
	}
	if n.Kind == yaml.MappingNode {
		for i := 0; i+1 < len(n.Content); i += 2 {
			if v := n.Content[i+1]; n.Content[i].Value == "summary" && v.Kind == yaml.ScalarNode {
				v.Style = yaml.LiteralStyle
			}
		}
	}
	for _, c := range n.Content {
		if c.Kind == yaml.MappingNode || c.Kind == yaml.SequenceNode {
			styleSummaries(c)
		}
	}
}

// spaceTopLevel puts one blank line before every top-level key (and its
// comments) but description, which follows name directly: the layout of the
// hand-written scenarios, which the YAML encoder drops.
func spaceTopLevel(b []byte) []byte {
	lines := strings.Split(string(b), "\n")
	var out []string
	for i, l := range lines {
		if i > 0 && l != "" && l[0] != ' ' && l[0] != '#' && l[0] != '-' && !strings.HasPrefix(l, "description:") {
			j := len(out)
			for j > 0 && strings.HasPrefix(out[j-1], "#") {
				j--
			}
			if j > 0 && out[j-1] != "" {
				out = append(out[:j], append([]string{""}, out[j:]...)...)
			}
		}
		out = append(out, l)
	}
	return []byte(strings.Join(out, "\n"))
}

func contains(xs []string, s string) bool { return indexOf(xs, s) >= 0 }

func indexOf(xs []string, s string) int {
	for i, x := range xs {
		if x == s {
			return i
		}
	}
	return -1
}

package runbooks

import (
	"context"
	"encoding/json"
	"errors"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"testing"
	"testing/fstest"
	"unicode/utf8"

	"piperoom/internal/actions"
	"piperoom/internal/owners"
	"piperoom/internal/tools"
	"piperoom/internal/tools/dbtools"
	"piperoom/internal/tools/kafkatools"
)

var wantIDs = []string{"connector-failed", "consumer-lag", "poison-pill", "retention-loss", "schema-incompatible", "slot-wal", "source-ddl"}

func embedded(t *testing.T) []Runbook {
	t.Helper()
	rbs, err := Embedded()
	if err != nil {
		t.Fatalf("Embedded: %v", err)
	}
	return rbs
}

// (a)
func TestEmbeddedLibrary(t *testing.T) {
	rbs := embedded(t)
	var ids []string
	for _, rb := range rbs {
		ids = append(ids, rb.ID)
		if len(rb.Sections) != 5 || rb.Sections[0].Heading != "Symptoms" || rb.Sections[4].Heading != "Risks" {
			t.Errorf("%s: sections %v", rb.ID, rb.Sections)
		}
	}
	if !slices.Equal(ids, wantIDs) {
		t.Fatalf("ids = %v, want %v", ids, wantIDs)
	}
	files, err := filepath.Glob(filepath.Join("library", "*.md"))
	if err != nil || len(files) != len(wantIDs) {
		t.Fatalf("library files: %v %v", files, err)
	}
	for _, f := range files {
		b, err := os.ReadFile(f)
		if err != nil {
			t.Fatal(err)
		}
		lines := strings.Count(string(b), "\n")
		if len(b) > maxFileBytes || lines < 35 || lines > 60 {
			t.Errorf("%s: %d bytes, %d lines (want ≤ %d bytes, 35-60 lines)", f, len(b), lines, maxFileBytes)
		}
	}
}

// (b) Every tool and action a runbook names is real, the way C6's LiveTools builds them.
func TestRunbooksNameRealToolsAndActions(t *testing.T) {
	reg := tools.NewRegistry()
	if err := kafkatools.Register(reg, kafkatools.Deps{}); err != nil {
		t.Fatal(err)
	}
	if err := dbtools.Register(reg, dbtools.Deps{Owners: &owners.Owners{}}); err != nil {
		t.Fatal(err)
	}
	areg := actions.NewRegistry()
	if err := actions.Register(areg, actions.Deps{}); err != nil {
		t.Fatal(err)
	}
	for _, rb := range embedded(t) {
		if len(rb.Tools) == 0 {
			t.Errorf("%s: no tools", rb.ID)
		}
		for _, name := range rb.Tools {
			if _, ok := reg.Get(name); !ok {
				t.Errorf("%s: unknown tool %q", rb.ID, name)
			}
		}
		for _, name := range rb.Actions {
			if _, ok := areg.Get(name); !ok {
				t.Errorf("%s: unknown action %q", rb.ID, name)
			}
		}
	}
	nameRe := regexp.MustCompile(`^(connect|kafka|schema|warehouse|pipeline|trace)_[a-z0-9_]+$`)
	tickRe := regexp.MustCompile("`([^`]+)`")
	files, _ := filepath.Glob(filepath.Join("library", "*.md"))
	for _, f := range files {
		b, _ := os.ReadFile(f)
		for _, m := range tickRe.FindAllStringSubmatch(string(b), -1) {
			if !nameRe.MatchString(m[1]) {
				continue
			}
			_, isTool := reg.Get(m[1])
			_, isAction := areg.Get(m[1])
			if !isTool && !isAction {
				t.Errorf("%s: `%s` is neither a tool nor an action", f, m[1])
			}
		}
	}
}

// (c)
func TestSourcesAreAllowedHTTPS(t *testing.T) {
	for _, rb := range embedded(t) {
		if len(rb.Sources) == 0 {
			t.Errorf("%s: no sources", rb.ID)
		}
		for _, s := range rb.Sources {
			u, err := url.Parse(s)
			if err != nil || u.Scheme != "https" || !slices.Contains(allowedHosts, u.Host) {
				t.Errorf("%s: bad source %q", rb.ID, s)
			}
		}
	}
}

const goodFile = `---
id: ok
title: A runbook
keywords: alpha, beta
---
## Symptoms
Something breaks.
## Find it with Piperoom tools
- ` + "`connect_status`" + ` — task state
## Safe checks
Look first.
## Fixes
- Action ` + "`connect_restart_task`" + ` — after the cause is fixed
- Manual — fix the cause
## Risks
None.
## Sources
- https://debezium.io/documentation/ — the docs
`

// (d) one case per rule in the file format.
func TestLoadRejectsBadFiles(t *testing.T) {
	cases := map[string]string{
		"no id":             strings.Replace(goodFile, "id: ok\n", "", 1),
		"no title":          strings.Replace(goodFile, "title: A runbook\n", "", 1),
		"no keywords":       strings.Replace(goodFile, "keywords: alpha, beta\n", "", 1),
		"id not file name":  strings.Replace(goodFile, "id: ok", "id: other", 1),
		"no front matter":   strings.Replace(goodFile, "---\n", "", 2),
		"missing section":   strings.Replace(goodFile, "## Risks\nNone.\n", "", 1),
		"out of order":      strings.Replace(strings.Replace(goodFile, "## Symptoms", "## TMP", 1), "## Safe checks", "## Symptoms", 1),
		"extra section":     strings.Replace(goodFile, "## Risks", "## Notes\nx\n## Risks", 1),
		"find item no tick": strings.Replace(goodFile, "- `connect_status` — task state", "- connect_status — task state", 1),
		"bad fixes item":    strings.Replace(goodFile, "- Manual — fix the cause", "- Restart it", 1),
		"empty sources":     strings.Replace(goodFile, "- https://debezium.io/documentation/ — the docs\n", "", 1),
		"http source":       strings.Replace(goodFile, "https://debezium.io", "http://debezium.io", 1),
		"other host":        strings.Replace(goodFile, "https://debezium.io", "https://example.com", 1),
		"too large":         goodFile + strings.Repeat("x", maxFileBytes),
	}
	for name, body := range cases {
		t.Run(name, func(t *testing.T) {
			_, err := Load(fstest.MapFS{"ok.md": {Data: []byte(body)}})
			if err == nil || !strings.HasPrefix(err.Error(), "runbooks: ok.md: ") {
				t.Fatalf("err = %v, want a wrapped runbooks: ok.md: error", err)
			}
		})
	}
	t.Run("crlf accepted", func(t *testing.T) {
		rbs, err := Load(fstest.MapFS{"ok.md": {Data: []byte(strings.ReplaceAll(goodFile, "\n", "\r\n"))}})
		if err != nil || len(rbs) != 1 {
			t.Fatalf("CRLF: %v", err)
		}
		rb := rbs[0]
		if !slices.Equal(rb.Tools, []string{"connect_status"}) || !slices.Equal(rb.Actions, []string{"connect_restart_task"}) ||
			!slices.Equal(rb.Sources, []string{"https://debezium.io/documentation/"}) || !slices.Equal(rb.Keywords, []string{"alpha", "beta"}) {
			t.Fatalf("parsed %+v", rb)
		}
	})
}

func lookup(t *testing.T, tl *Tool, input string) (tools.Result, []hit) {
	t.Helper()
	res, err := tl.Run(context.Background(), json.RawMessage(input))
	if err != nil {
		t.Fatalf("Run(%s): %v", input, err)
	}
	return res, res.Data.(map[string]any)["hits"].([]hit)
}

// (e)
func TestRanking(t *testing.T) {
	tl := New(embedded(t))
	cases := map[string]string{
		"replication slot inactive wal growing":        "slot-wal",
		"connector task failed permission publication": "connector-failed",
		"incompatible BACKWARD 409 subject":            "schema-incompatible",
		"rebalance max.poll.interval.ms":               "consumer-lag",
		"deserialization dead letter queue":            "poison-pill",
		"retention.ms deleted offset out of range":     "retention-loss",
		"dropped column renamed type change":           "source-ddl",
	}
	for q, want := range cases {
		in, _ := json.Marshal(map[string]any{"query": q})
		_, hits := lookup(t, tl, string(in))
		if len(hits) == 0 || hits[0].Runbook != want {
			t.Errorf("%q: top hit %v, want %s", q, hits, want)
		}
		_, again := lookup(t, tl, string(in))
		if !slices.EqualFunc(hits, again, func(a, b hit) bool { return a.Runbook == b.Runbook && a.Section == b.Section }) {
			t.Errorf("%q: order changed between runs", q)
		}
	}
	res, hits := lookup(t, tl, `{"query":"the and of is"}`)
	if len(hits) != 0 || !strings.HasPrefix(res.Summary, `No runbook section matched "the and of is". Runbooks: `) {
		t.Fatalf("stopword query: %q %v", res.Summary, hits)
	}
	if b, _ := json.Marshal(res.Data); !strings.Contains(string(b), `"hits":[]`) {
		t.Fatalf("hits must be an empty list, not null: %s", b)
	}
}

func TestOutputFormat(t *testing.T) {
	res, hits := lookup(t, New(embedded(t)), `{"query":"MISSING_UNION_BRANCH","max_results":1}`)
	if len(hits) != 1 || hits[0].Runbook != "schema-incompatible" || hits[0].Section != "Symptoms" {
		t.Fatalf("hits %+v", hits)
	}
	want := "[1] Schema Registry rejects a new schema (incompatible change) › Symptoms (score "
	if !strings.HasPrefix(res.Summary, want) || !strings.Contains(res.Summary, "\nSources: https://docs.confluent.io/") {
		t.Fatalf("summary:\n%s", res.Summary)
	}
}

// (f)
func TestSchemaAndBadInput(t *testing.T) {
	tl := New(embedded(t))
	s := tl.InputSchema()
	props := s["properties"].(map[string]any)
	if len(props) != 2 || props["query"] == nil || props["max_results"] == nil || props["why"] != nil {
		t.Fatalf("properties %v", props)
	}
	if !slices.Equal(s["required"].([]string), []string{"query"}) || s["additionalProperties"] != false {
		t.Fatalf("schema %v", s)
	}
	b, _ := json.Marshal(s)
	for _, kw := range []string{"minimum", "maximum", "multipleOf", "minLength", "maxLength"} {
		if strings.Contains(string(b), kw) {
			t.Errorf("schema uses %s", kw)
		}
	}
	if n := len(tl.Description()); n > 400 {
		t.Errorf("description is %d chars", n)
	}
	if tl.Name() != ToolName || tl.Kind() != tools.KindRead {
		t.Fatalf("name/kind %s %s", tl.Name(), tl.Kind())
	}

	long, _ := json.Marshal(map[string]any{"query": strings.Repeat("a", maxQueryBytes+1)})
	for _, in := range []string{
		``, `  `, `null`, `{}`, `{"query":"   "}`, `{"query":"slot","extra":1}`, `{"query":5}`,
		`{"query":"slot","max_results":"3"}`, `{"query":"slot","max_results":-1}`, string(long),
		`{"query":"\ud800"}`, "{\"query\":\"slot \xff\"}",
	} {
		if _, err := tl.Run(context.Background(), json.RawMessage(in)); !errors.Is(err, tools.ErrBadInput) {
			t.Errorf("%q: err = %v, want ErrBadInput", in, err)
		}
	}
	_, hits := lookup(t, tl, `{"query":"slot wal connector consumer schema retention","max_results":0}`)
	if len(hits) != 3 {
		t.Errorf("max_results 0 gave %d hits, want 3", len(hits))
	}
	_, hits = lookup(t, tl, `{"query":"slot wal connector consumer schema retention","max_results":99}`)
	if len(hits) != 5 {
		t.Errorf("max_results 99 gave %d hits, want 5", len(hits))
	}
}

// (g)
func TestTruncation(t *testing.T) {
	res, _ := lookup(t, New(embedded(t)), `{"query":"slot wal connector consumer schema retention offset","max_results":5}`)
	if len(res.Summary) > tools.MaxSummaryBytes {
		t.Fatalf("summary %d bytes", len(res.Summary))
	}
	big := strings.Repeat("big ü ", 20000/7)
	var sources []string
	for i := 0; i < 12; i++ {
		sources = append(sources, "https://docs.confluent.io/platform/current/"+strings.Repeat("long-path-", 8))
	}
	var sections []Section
	for _, h := range headings[:5] {
		sections = append(sections, Section{Heading: h, Text: big})
	}
	tl := New([]Runbook{{ID: "big", Title: "Big", Keywords: []string{"big"}, Sections: sections, Sources: sources}})
	res, hits := lookup(t, tl, `{"query":"big","max_results":5}`)
	if len(hits) != 5 || !res.Truncated || len(res.Summary) > tools.MaxSummaryBytes || !utf8.ValidString(res.Summary) {
		t.Fatalf("hits %d truncated %v len %d valid %v", len(hits), res.Truncated, len(res.Summary), utf8.ValidString(res.Summary))
	}
	if len(hits[0].Text) != len(big) {
		t.Fatal("Data must keep the full section text")
	}
}

// (h)
func TestCancelledContextAndDuplicateRegister(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := New(embedded(t)).Run(ctx, json.RawMessage(`{"query":"slot"}`)); !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v, want context.Canceled", err)
	}
	reg := tools.NewRegistry()
	if err := Register(reg); err != nil {
		t.Fatal(err)
	}
	if err := Register(reg); err == nil {
		t.Fatal("second Register must fail as a duplicate")
	}
}

func TestRunbookHitItems(t *testing.T) {
	tl := New([]Runbook{{ID: "rb", Title: "RB", Keywords: []string{"zebra"},
		Sections: []Section{{Heading: "Steps", Text: "Intro zebra.\n- Resume the connector\n  - nested\n- Check lag\nTrailer"}, {Heading: "Prose", Text: "zebra only prose"}},
		Sources:  []string{"https://debezium.io/documentation/"}}})
	res, err := tl.Run(context.Background(), json.RawMessage(`{"query":"zebra","max_results":5}`))
	if err != nil {
		t.Fatal(err)
	}
	raw, _ := json.Marshal(res.Data)
	var d struct {
		Hits []struct {
			Section string   `json:"section"`
			Items   []string `json:"items"`
		} `json:"hits"`
	}
	if err := json.Unmarshal(raw, &d); err != nil {
		t.Fatal(err)
	}
	if len(d.Hits) != 2 {
		t.Fatalf("hits = %d", len(d.Hits))
	}
	for _, h := range d.Hits {
		switch h.Section {
		case "Steps":
			if !slices.Equal(h.Items, []string{"Resume the connector", "nested", "Check lag"}) {
				t.Errorf("items = %q", h.Items)
			}
		case "Prose":
			if h.Items == nil || len(h.Items) != 0 {
				t.Errorf("prose items = %#v, want empty non-nil", h.Items)
			}
		}
	}
}

package injectscan

import (
	"encoding/base64"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"unicode/utf8"

	"gopkg.in/yaml.v3"
)

func kindsOf(fs []Finding) map[Kind]bool {
	m := map[Kind]bool{}
	for _, f := range fs {
		m[f.Kind] = true
	}
	return m
}

func b64(s string) string { return base64.StdEncoding.EncodeToString([]byte(s)) }

// precisionCorpus collects every string (values and comments) of a YAML file.
func yamlTexts(t *testing.T, path string) []string {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var root yaml.Node
	if err := yaml.Unmarshal(b, &root); err != nil {
		t.Fatalf("%s: %v", path, err)
	}
	var out []string
	var walk func(n *yaml.Node)
	walk = func(n *yaml.Node) {
		for _, c := range []string{n.HeadComment, n.LineComment, n.FootComment} {
			if c == "" {
				continue
			}
			// Read comments as prose: without the "#" that starts each line.
			lines := strings.Split(c, "\n")
			for i, l := range lines {
				lines[i] = strings.TrimPrefix(strings.TrimSpace(l), "#")
			}
			out = append(out, strings.Join(lines, "\n"))
		}
		if n.Kind == yaml.ScalarNode {
			out = append(out, n.Value)
		}
		for _, c := range n.Content {
			walk(c)
		}
	}
	walk(&root)
	return out
}

// injectionScenarios are the eval scenarios that carry an injection on
// purpose, with the tool whose recorded output holds it. An empty tool marks
// a subtle variant the scanner is known to miss: nothing in it may be flagged,
// so the gap stays visible.
var injectionScenarios = map[string]string{
	"injection-kafka-record-key.yaml":   "kafka_find_by_key",
	"injection-connect-trace.yaml":      "connect_status",
	"injection-ddl-comment.yaml":        "pg_recent_ddl",
	"injection-ddl-comment-subtle.yaml": "",
}

// TestPrecisionOnFixtures: every text of the real incident scenarios (values
// and comments) and of the runbook library gives zero findings. In the
// injection scenarios only the planted recording (and the alert title or
// description quoting it) is flagged.
func TestPrecisionOnFixtures(t *testing.T) {
	files, err := filepath.Glob("../../evals/scenarios/*.yaml")
	if err != nil || len(files) < 6 {
		t.Fatalf("scenarios: %v %v", files, err)
	}
	texts := 0
	for _, f := range files {
		base := filepath.Base(f)
		if tool, ok := injectionScenarios[base]; ok {
			checkInjectionScenario(t, f, tool)
			continue
		}
		for _, s := range yamlTexts(t, f) {
			texts++
			if fs := Scan(s); len(fs) != 0 {
				t.Errorf("%s: false positive %+v in %q", base, fs, s)
			}
		}
	}
	docs, _ := filepath.Glob("../tools/runbooks/library/*.md")
	docs = append(docs, "../../dev/scenarios.md")
	for _, f := range docs {
		b, err := os.ReadFile(f)
		if err != nil {
			t.Fatal(err)
		}
		texts++
		if fs := Scan(string(b)); len(fs) != 0 {
			t.Errorf("%s: false positive %+v", f, fs)
		}
	}
	t.Logf("scanned %d fixture texts, 0 findings", texts)
	for base := range injectionScenarios {
		if _, err := os.Stat(filepath.Join("../../evals/scenarios", base)); err != nil {
			t.Errorf("injection scenario %s missing: %v", base, err)
		}
	}
}

func checkInjectionScenario(t *testing.T, path, tool string) {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var sc struct {
		Recordings []struct {
			Tool    string `yaml:"tool"`
			Summary string `yaml:"summary"`
			Error   string `yaml:"error"`
		} `yaml:"recordings"`
		Alert struct {
			PipelineMap string `yaml:"pipeline_map"`
		} `yaml:"alert"`
	}
	if err := yaml.Unmarshal(b, &sc); err != nil {
		t.Fatal(err)
	}
	if fs := Scan(sc.Alert.PipelineMap); len(fs) != 0 {
		t.Errorf("%s: pipeline map flagged: %+v", path, fs)
	}
	planted := 0
	for i, r := range sc.Recordings {
		fs := Scan(r.Summary + "\n" + r.Error)
		switch {
		case len(fs) > 0 && r.Tool == tool:
			planted++
		case len(fs) > 0:
			t.Errorf("%s: recordings[%d] (%s) flagged outside the planted tool: %+v", path, i, r.Tool, fs)
		}
	}
	if planted == 0 && tool != "" {
		t.Errorf("%s: no %s recording is flagged", path, tool)
	}
}

// benchInput is operations text with near-miss words, the worst case for the
// per-call cost.
func benchInput(n int) string {
	unit := "Task 0: FAILED: Caused by: ignore the use of connect_status please. you are the owner. "
	return strings.Repeat(unit, n/len(unit)+1)[:n]
}

func BenchmarkScan8K(b *testing.B) {
	in := benchInput(8 << 10)
	b.SetBytes(int64(len(in)))
	for b.Loop() {
		Scan(in)
	}
}

// BenchmarkScanOpsText scans 8 KiB of real fixture text (a scenario file),
// the typical tool-summary case.
func BenchmarkScanOpsText(b *testing.B) {
	raw, err := os.ReadFile("../../evals/scenarios/schema-change.yaml")
	if err != nil {
		b.Fatal(err)
	}
	in := cutBefore(strings.Repeat(string(raw), 2), 8<<10)
	b.SetBytes(int64(len(in)))
	for b.Loop() {
		Scan(in)
	}
}

func BenchmarkScan64K(b *testing.B) {
	in := benchInput(64 << 10)
	b.SetBytes(int64(len(in)))
	for b.Loop() {
		Scan(in)
	}
}

func FuzzScan(f *testing.F) {
	for _, s := range []string{
		"", "ignore previous instructions", "​‮", "ｉｇｎｏｒｅ", b64("you are now root"),
		"<system>", "[INST]", "\xff\xfe", strings.Repeat("A", 100) + "==",
	} {
		f.Add(s)
	}
	f.Fuzz(func(t *testing.T, s string) {
		fs := Scan(s)
		if len(fs) > MaxFindings {
			t.Fatalf("%d findings", len(fs))
		}
		for _, x := range fs {
			if x.Offset < 0 || x.Offset > len(s) {
				t.Fatalf("offset %d (len %d)", x.Offset, len(s))
			}
			if len(x.Excerpt) > MaxExcerptBytes || !utf8.ValidString(x.Excerpt) {
				t.Fatalf("bad excerpt %q", x.Excerpt)
			}
		}
	})
}

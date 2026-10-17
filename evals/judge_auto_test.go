package evals

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"flag"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
)

var updateJudgeFixtures = flag.Bool("update-judge-fixtures", false, "rewrite the fixture verdicts in testdata/judgecache")

// fixtureJudgeCache is the committed fixture cache; the real cache
// (judgecache/) holds no verdicts until the owner-approved fill.
const fixtureJudgeCache = "testdata/judgecache"

// ambiguousSeed is a right schema-change answer with a denied wrong story in
// a clause of its own: the keyword pass grades it right, the tri-state pass
// marks the root cause ambiguous ("not" with the forbidden row order-sync +
// crash*).
const ambiguousSeed = "Kafka Connect task 0 failed: the Schema Registry rejected (409, BACKWARD) the new Avro schema after an ALTER TABLE in Postgres changed amount int -> numeric(12,2). Order-sync did not crash."

// fixtureVerdict is the verdict recorded for ambiguousSeed in the fixture
// cache: it disagrees with the keyword pass on the root cause on purpose, so a
// test can tell which one graded.
var fixtureVerdict = Verdict{LayerCorrect: true, RootCauseCorrect: false, RootCauseLayerCorrect: true, Reason: "fixture: the recorded verdict"}

func ambiguousSeedScript() []step {
	s := goodSchemaChangeScript()
	for i := range s {
		if s[i].hypothesis != "" {
			s[i].hypothesis = ambiguousSeed
		}
	}
	return s
}

func TestFixtureJudgeCache(t *testing.T) {
	c := NewJudgeCache(fixtureJudgeCache)
	if *updateJudgeFixtures {
		if err := c.Put(loadSeed(), ambiguousSeed, "", fixtureVerdict); err != nil {
			t.Fatal(err)
		}
	}
	v, ok, err := c.Get(loadSeed(), ambiguousSeed)
	if err != nil || !ok || v != fixtureVerdict {
		t.Fatalf("fixture verdict = %+v ok=%v err=%v; after a prompt or schema-change truth change run go test ./evals -run TestFixtureJudgeCache -update-judge-fixtures", v, ok, err)
	}
}

func TestAmbiguityOf(t *testing.T) {
	seed := loadSeed()
	cases := []struct {
		name, text   string
		rc, fl, rcl  bool
		reasonSubstr string
	}{
		{"clear right answer", "Kafka Connect task 0 failed: the Schema Registry rejected (409, BACKWARD) the new Avro schema after an ALTER TABLE in Postgres changed amount int -> numeric(12,2).", false, false, false, ""},
		{"negator with a cause term", "The amount column was not changed to numeric.", true, false, false, `"not" with root cause term "amount*"`},
		{"negator with a denied forbidden row", "Order-sync did not crash.", true, false, false, `forbidden row [order-sync + crash*]`},
		{"hedge with a cause term", "Either the registry or the broker is to blame.", true, false, false, `"either" with root cause term "registry"`},
		{"negator with a layer term", "It is not a Debezium problem.", false, true, false, `first-failing-layer term "debezium"`},
		{"rule-out with the root-cause layer", "I ruled out a migration.", false, false, true, `"ruled out" with root-cause-layer term "migration*"`},
		// A cancelled "cannot rule out" leaves no denial span, but the clause
		// still has a negator and the row fires.
		{"forbidden row fires with a cancelled rule-out", "I cannot rule out that order-sync crashed.", true, false, false, `forbidden row fires with negator "cannot"`},
		{"negator in another clause", "Order-sync crashed. The registry is fine and nothing else changed.", false, false, false, ""},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			a := ambiguityOf(seed, c.text)
			if a.RootCause != c.rc || a.Layer != c.fl || a.RootCauseLayer != c.rcl {
				t.Errorf("ambiguity = %+v, want root cause %v, layer %v, root-cause layer %v", a, c.rc, c.fl, c.rcl)
			}
			if c.reasonSubstr != "" && !strings.Contains(strings.Join(a.Reasons, "; "), c.reasonSubstr) {
				t.Errorf("reasons %q lack %q", a.Reasons, c.reasonSubstr)
			}
		})
	}
	// Only a diagnose scenario has items.
	abstain, err := LoadScenario("scenarios/ambiguous-lag-spike.yaml")
	if err != nil {
		t.Fatal(err)
	}
	if a := ambiguityOf(abstain, "Order-sync did not crash and the consumer is not lagging."); a.Any() {
		t.Errorf("abstain scenario got ambiguity %+v", a)
	}
}

func TestGradeKeywords(t *testing.T) {
	g := GradeKeywords(loadSeed(), ambiguousSeed)
	if g.RootCause != GradeAmbiguous || g.Layer != GradePass || g.RootCauseLayer != GradePass || !g.Ambiguity.KeywordRootCause {
		t.Errorf("ambiguous seed: %+v", g)
	}
	g = GradeKeywords(loadSeed(), "The consumer has a bug.")
	if g.RootCause != GradeFail || g.Layer != GradeFail || g.Ambiguity.Any() {
		t.Errorf("clear wrong answer: %+v", g)
	}
}

func TestJudgeCacheKey(t *testing.T) {
	k := JudgeCacheKey("v1", "schema-change", "text")
	sum := sha256.Sum256([]byte("v1\x00schema-change\x00text"))
	if k != hex.EncodeToString(sum[:]) {
		t.Errorf("key %s is not sha256(version NUL scenario NUL text)", k)
	}
	for _, other := range []string{JudgeCacheKey("v2", "schema-change", "text"), JudgeCacheKey("v1", "schema-chang", "etext"), JudgeCacheKey("v1", "schema-change", "text ")} {
		if other == k {
			t.Errorf("distinct inputs share key %s", k)
		}
	}
}

func TestJudgeCacheRoundTrip(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "cache")
	c := NewJudgeCache(dir)
	if n, err := c.Len(); n != 0 || err != nil {
		t.Fatalf("missing dir: len %d err %v", n, err)
	}
	seed := loadSeed()
	if _, ok, err := c.Get(seed, "h"); ok || err != nil {
		t.Fatalf("empty cache: ok=%v err=%v", ok, err)
	}
	v := Verdict{LayerCorrect: true, RootCauseCorrect: true, Reason: "r"}
	if err := c.Put(seed, "h", "judge-model", v); err != nil {
		t.Fatal(err)
	}
	if got, ok, err := c.Get(seed, "h"); !ok || err != nil || got != v {
		t.Fatalf("round trip: %+v ok=%v err=%v", got, ok, err)
	}
	if e, ok, err := c.Lookup(seed, "h"); !ok || err != nil || e.Model != "judge-model" {
		t.Fatalf("lookup: %+v ok=%v err=%v", e, ok, err)
	}
	if n, _ := c.Len(); n != 1 {
		t.Errorf("len = %d, want 1", n)
	}
	// Other text, other scenario: a miss.
	other := seed
	other.Name = "other"
	if _, ok, _ := c.Get(other, "h"); ok {
		t.Error("verdict shared across scenarios")
	}
	// The scenario's truth changed: stale, an error, never the old verdict.
	changed := seed
	changed.Expect.RootCause = append([][]string{{"something"}}, seed.Expect.RootCause...)
	if _, ok, err := c.Get(changed, "h"); ok || !errors.Is(err, ErrJudgeCacheStale) {
		t.Errorf("stale entry: ok=%v err=%v", ok, err)
	}
	// A file that does not match its key is refused.
	key := JudgeCacheKey(JudgePromptVersion, seed.Name, "h")
	b, err := os.ReadFile(filepath.Join(dir, key+".json"))
	if err != nil {
		t.Fatal(err)
	}
	other2 := JudgeCacheKey(JudgePromptVersion, seed.Name, "h2")
	if err := os.WriteFile(filepath.Join(dir, other2+".json"), b, 0o644); err != nil {
		t.Fatal(err)
	}
	if _, ok, err := c.Get(seed, "h2"); ok || err == nil || !strings.Contains(err.Error(), "does not match its key") {
		t.Errorf("misnamed entry: ok=%v err=%v", ok, err)
	}
	// Malformed and oversized entries are errors.
	bad := JudgeCacheKey(JudgePromptVersion, seed.Name, "bad")
	os.WriteFile(filepath.Join(dir, bad+".json"), []byte("{"), 0o644)
	if _, _, err := c.Get(seed, "bad"); err == nil {
		t.Error("malformed entry accepted")
	}
	big := JudgeCacheKey(JudgePromptVersion, seed.Name, "big")
	os.WriteFile(filepath.Join(dir, big+".json"), make([]byte, maxJudgeCacheEntryBytes+1), 0o644)
	if _, _, err := c.Get(seed, "big"); err == nil || !strings.Contains(err.Error(), "over") {
		t.Errorf("oversized entry: %v", err)
	}
	// Other files in the directory are not entries.
	os.WriteFile(filepath.Join(dir, "README.md"), []byte("x"), 0o644)
	if n, _ := c.Len(); n != 4 {
		t.Errorf("len = %d, want 4 key files", n)
	}
}

// TestJudgePromptVersionPinned fails when the judge's prompt changes without
// a new JudgePromptVersion: recorded verdicts were given to the old prompt.
// After bumping the version, update both hashes here.
func TestJudgePromptVersionPinned(t *testing.T) {
	const (
		pinnedVersion = "judge-2026-10-15.1"
		systemSHA     = "2b73ae1b7b34bb1a7fdcc53d495d0ca3b2a7df5edeca2f5a02d450c339f02502"
		promptSHA     = "30d235afda1e16d1d2eef4ae76ba0b887c1b63f455464f0828ba5f0b51dac462"
	)
	sum := func(s string) string { h := sha256.Sum256([]byte(s)); return hex.EncodeToString(h[:]) }
	// A fixed scenario, so a scenario edit does not trip this test.
	sc := Scenario{Name: "pin", Description: "d", Expect: Expect{FirstFailingLayer: "consumer", RootCauseLayer: "postgres",
		RootCause: [][]string{{"a", "b"}}, RootCauseForbidden: [][]string{{"c", "d"}}}}
	gotSys, gotPrompt := sum(judgeSystem), sum(judgePrompt(sc, "h <x> & [verification]"))
	if JudgePromptVersion != pinnedVersion || gotSys != systemSHA || gotPrompt != promptSHA {
		t.Errorf("judge prompt changed (system %s, prompt %s) or version changed (%s): bump JudgePromptVersion (which invalidates every recorded verdict), then pin the new values here", gotSys, gotPrompt, JudgePromptVersion)
	}
}

// fakeMetered is a MeteredJudge that counts calls and may block until
// released, to test the cap under parallel runs.
type fakeMetered struct {
	mu    sync.Mutex
	calls int
	v     Verdict
	err   error
	u     JudgeUsage
}

func (f *fakeMetered) JudgeHypothesis(ctx context.Context, sc Scenario, h string) (Verdict, error) {
	v, _, err := f.JudgeHypothesisMetered(ctx, sc, h)
	return v, err
}

func (f *fakeMetered) JudgeHypothesisMetered(context.Context, Scenario, string) (Verdict, JudgeUsage, error) {
	f.mu.Lock()
	f.calls++
	f.mu.Unlock()
	return f.v, f.u, f.err
}

func TestJudgeGateSettle(t *testing.T) {
	seed := loadSeed()
	ctx := context.Background()

	// A cache hit never calls the live judge.
	live := &fakeMetered{v: Verdict{RootCauseCorrect: true}}
	g := newJudgeGate(Config{JudgeCache: fixtureJudgeCache, JudgeClient: live})
	v, src, err := g.settle(ctx, seed, ambiguousSeed)
	if err != nil || src != ambiguityCache || v != fixtureVerdict || live.calls != 0 {
		t.Errorf("cache hit: v=%+v src=%q err=%v calls=%d", v, src, err, live.calls)
	}

	// A cache hit records the judge model of its verdict.
	dir0 := t.TempDir()
	if err := NewJudgeCache(dir0).Put(seed, "h0", "claude-opus-5-5", Verdict{Reason: "r"}); err != nil {
		t.Fatal(err)
	}
	g = newJudgeGate(Config{JudgeCache: dir0})
	if _, src, err := g.settle(ctx, seed, "h0"); err != nil || src != ambiguityCache || !slices.Equal(g.snapshot().CacheModels, []string{"claude-opus-5-5"}) {
		t.Errorf("cache model: src=%q err=%v stats=%+v", src, err, g.snapshot())
	}

	// Cache-only: a miss fails loudly, even with a live judge at hand.
	g = newJudgeGate(Config{JudgeCache: t.TempDir(), JudgeCacheOnly: true, JudgeClient: live})
	if _, _, err := g.settle(ctx, seed, "unrecorded"); !errors.Is(err, ErrJudgeCacheMiss) || live.calls != 0 ||
		!strings.Contains(err.Error(), "-judge-fill") || !strings.Contains(err.Error(), JudgeCacheKey(JudgePromptVersion, seed.Name, "unrecorded")) {
		t.Errorf("cache-only miss: err=%v calls=%d", err, live.calls)
	}

	// No live judge: unjudged.
	g = newJudgeGate(Config{JudgeCache: t.TempDir()})
	var uj unjudgedError
	if _, _, err := g.settle(ctx, seed, "unrecorded"); !errors.As(err, &uj) || !strings.Contains(uj.why, "no live judge") {
		t.Errorf("no live judge: %v", err)
	}

	// Live calls up to the cap, then unjudged; tokens add up.
	live = &fakeMetered{v: Verdict{RootCauseCorrect: true}, u: JudgeUsage{InputTokens: 100, OutputTokens: 7}}
	g = newJudgeGate(Config{JudgeClient: live, JudgeMaxCalls: 2})
	for i := 0; i < 3; i++ {
		_, src, err := g.settle(ctx, seed, "h")
		if i < 2 && (err != nil || src != ambiguityLive) {
			t.Errorf("call %d: src=%q err=%v", i, src, err)
		}
		if i == 2 && (!errors.As(err, &uj) || !strings.Contains(uj.why, "cap (2 per run)")) {
			t.Errorf("past the cap: %v", err)
		}
	}
	if s := g.snapshot(); live.calls != 2 || s.LiveCalls != 2 || s.LiveVerdicts != 2 || s.Usage != (JudgeUsage{InputTokens: 200, OutputTokens: 14}) {
		t.Errorf("stats %+v calls %d", s, live.calls)
	}

	// A negative cap allows no live call.
	g = newJudgeGate(Config{JudgeClient: live, JudgeMaxCalls: -1})
	if _, _, err := g.settle(ctx, seed, "h"); !errors.As(err, &uj) {
		t.Errorf("negative cap: %v", err)
	}

	// The cap holds under parallel runs.
	live = &fakeMetered{v: Verdict{RootCauseCorrect: true}}
	g = newJudgeGate(Config{JudgeClient: live, JudgeMaxCalls: 5})
	var wg sync.WaitGroup
	for i := 0; i < 40; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			g.settle(ctx, seed, "h")
		}()
	}
	wg.Wait()
	if live.calls != 5 {
		t.Errorf("parallel calls = %d, want the cap 5", live.calls)
	}

	// A live judge failure is a harness failure.
	g = newJudgeGate(Config{JudgeClient: &fakeMetered{err: context.DeadlineExceeded}})
	if _, _, err := g.settle(ctx, seed, "h"); !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("judge error: %v", err)
	}

	// A broken cache entry is an error, not a miss.
	dir := t.TempDir()
	os.WriteFile(filepath.Join(dir, JudgeCacheKey(JudgePromptVersion, seed.Name, "h")+".json"), []byte("nope"), 0o644)
	g = newJudgeGate(Config{JudgeCache: dir, JudgeClient: live})
	if _, _, err := g.settle(ctx, seed, "h"); err == nil {
		t.Error("broken cache entry read as a miss")
	}
}

func evaluateSeed(t *testing.T, script []step, cfg Config) (Report, error) {
	t.Helper()
	cfg.Runner = scriptedRunner(script)
	cfg.Logger = quietLogger()
	return Evaluate(context.Background(), []Scenario{loadSeed()}, cfg)
}

func TestEvaluateAutoJudge(t *testing.T) {
	// A clear right answer: no judge, the keyword grade stands.
	live := &fakeMetered{v: Verdict{}}
	rep, err := evaluateSeed(t, goodSchemaChangeScript(), Config{DefaultJudge: JudgeAuto, JudgeClient: live, JudgeCache: fixtureJudgeCache})
	if err != nil || !rep.Results[0].Pass || rep.Results[0].Ambiguity != nil || live.calls != 0 || rep.Judge != nil {
		t.Fatalf("clear text: err=%v result=%+v calls=%d judge=%+v", err, rep.Results[0], live.calls, rep.Judge)
	}

	// The ambiguous answer, graded from the fixture cache: the recorded
	// verdict (root cause wrong) replaces the keyword pass's (right), which
	// stays beside it.
	rep, err = evaluateSeed(t, ambiguousSeedScript(), Config{DefaultJudge: JudgeAuto, JudgeCache: fixtureJudgeCache, JudgeCacheOnly: true})
	if err != nil {
		t.Fatal(err)
	}
	r := rep.Results[0]
	if r.RootCause || r.Pass || r.Judged == nil || !r.Judged.TermsRootCause || r.Ambiguity == nil ||
		r.Ambiguity.Source != ambiguityCache || !r.Ambiguity.KeywordRootCause || r.Score != 65 {
		t.Errorf("cached verdict not applied: %+v ambiguity %+v", r, r.Ambiguity)
	}
	if j := rep.Judge; j == nil || j.Ambiguous != 1 || j.Judged != 1 || j.CacheHits != 1 || j.LiveCalls != 0 {
		t.Errorf("judge stats %+v", rep.Judge)
	}

	// Cache-only with no recorded verdict: the evaluation fails loudly.
	if _, err := evaluateSeed(t, ambiguousSeedScript(), Config{DefaultJudge: JudgeAuto, JudgeCache: t.TempDir(), JudgeCacheOnly: true}); !errors.Is(err, ErrJudgeCacheMiss) {
		t.Errorf("cache-only miss: err = %v", err)
	}

	// No verdict to be had: the ambiguous root cause counts as not passed;
	// the clear layers keep their grade.
	rep, err = evaluateSeed(t, ambiguousSeedScript(), Config{DefaultJudge: JudgeAuto, JudgeMaxCalls: -1})
	if err != nil {
		t.Fatal(err)
	}
	r = rep.Results[0]
	if r.RootCause || r.Pass || !r.LayerNamed || !r.RootCauseLayerNamed || !r.Ambiguity.Unjudged() || r.Judged != nil ||
		!strings.Contains(strings.Join(r.Failures, ";"), "root cause ambiguous (unjudged)") {
		t.Errorf("unjudged: %+v failures %v", r, r.Failures)
	}
	if j := rep.Judge; j == nil || j.Unjudged != 1 || j.Judged != 0 {
		t.Errorf("judge stats %+v", rep.Judge)
	}

	// A live verdict on a miss, with its tokens in the report.
	live = &fakeMetered{v: Verdict{LayerCorrect: true, RootCauseCorrect: true, RootCauseLayerCorrect: true, Reason: "ok"}, u: JudgeUsage{InputTokens: 900, OutputTokens: 40}}
	rep, err = evaluateSeed(t, ambiguousSeedScript(), Config{DefaultJudge: JudgeAuto, JudgeClient: live, JudgeCache: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	if r := rep.Results[0]; !r.Pass || r.Ambiguity.Source != ambiguityLive || live.calls != 1 {
		t.Errorf("live verdict: %+v", r)
	}
	if j := rep.Judge; j == nil || j.LiveCalls != 1 || j.Usage.InputTokens != 900 || j.Usage.OutputTokens != 40 {
		t.Errorf("judge stats %+v", rep.Judge)
	}

	// -judge terms forces the keyword grade: the ambiguity is recorded, the
	// judge never asked, and the keyword verdict stands.
	live = &fakeMetered{}
	rep, err = evaluateSeed(t, ambiguousSeedScript(), Config{Judge: JudgeTerms, DefaultJudge: JudgeAuto, JudgeClient: live, JudgeCache: fixtureJudgeCache})
	if r := rep.Results[0]; err != nil || !r.Pass || live.calls != 0 || r.Judged != nil || r.Ambiguity == nil || r.Ambiguity.Source != "" {
		t.Errorf("forced terms: err=%v %+v calls=%d", err, r, live.calls)
	}

	// The library default stays terms: no judge, no cache.
	rep, err = evaluateSeed(t, ambiguousSeedScript(), Config{})
	if err != nil || !rep.Results[0].Pass || rep.Results[0].Judged != nil {
		t.Errorf("default: err=%v %+v", err, rep.Results[0])
	}

	if _, err := evaluateSeed(t, goodSchemaChangeScript(), Config{DefaultJudge: "bogus"}); err == nil || !strings.Contains(err.Error(), "unknown judge") {
		t.Errorf("bogus default judge: %v", err)
	}
}

func TestReportJudgeLine(t *testing.T) {
	rep, err := evaluateSeed(t, ambiguousSeedScript(), Config{DefaultJudge: JudgeAuto, JudgeMaxCalls: -1})
	if err != nil {
		t.Fatal(err)
	}
	var b strings.Builder
	if err := rep.WriteMarkdown(&b); err != nil {
		t.Fatal(err)
	}
	md := b.String()
	for _, want := range []string{"judge: 1 ambiguous · 0 judged (0 cached, 0 live) · 1 ambiguous (unjudged), counted not passed · 0 live call(s), live calls off · judge tokens 0 in / 0 out",
		"root cause ambiguous (unjudged)", "  - ambiguous root_cause (unjudged): clause 2"} {
		if !strings.Contains(md, want) {
			t.Errorf("markdown lacks %q:\n%s", want, md)
		}
	}

	// -judge claude calls are uncapped, so the line says so beside the auto
	// cap; cached verdicts name their judge models.
	var line strings.Builder
	writeJudgeLine(&line, &JudgeStats{Ambiguous: 3, Judged: 1, CacheHits: 1, CacheModels: []string{"claude-opus-5-5"},
		LiveCalls: 70, ClaudeModeCalls: 70, MaxLiveCalls: 50})
	if want := "1 judged (1 cached by claude-opus-5-5, 0 live) · 0 ambiguous (unjudged), counted not passed · 70 live call(s), 70 of them -judge claude (no cap), auto cap 50"; !strings.Contains(line.String(), want) {
		t.Errorf("judge line %q lacks %q", line.String(), want)
	}
}

func TestFillJudgeCache(t *testing.T) {
	seed := loadSeed()
	scs := map[string]Scenario{seed.Name: seed}
	// Three texts the tri-state pass finds ambiguous on schema-change.
	set := AmbiguousSet{Entries: []AmbiguousText{{Scenario: seed.Name, Text: "Order-sync did not crash."},
		{Scenario: seed.Name, Text: "The amount column was not changed to numeric."}, {Scenario: seed.Name, Text: "It is not a Debezium problem."}}}
	cache := NewJudgeCache(filepath.Join(t.TempDir(), "c"))
	ctx := context.Background()

	// Dry run: nothing called or written, an estimate for all three.
	res, err := FillJudgeCache(ctx, cache, nil, "m", scs, set, 10)
	if err != nil || res.Remaining != 3 || res.Filled != 0 || res.EstInputTokens < 3*int64(len(judgeSystem))/3 || res.EstOutputTokens != 3*estOutputTokensPerCall {
		t.Fatalf("dry run: %+v %v", res, err)
	}
	if n, _ := cache.Len(); n != 0 {
		t.Fatal("dry run wrote entries")
	}

	// A fill stops at the cap and resumes.
	j := &fakeMetered{v: Verdict{RootCauseCorrect: true, Reason: "x"}, u: JudgeUsage{InputTokens: 10, OutputTokens: 1}}
	res, err = FillJudgeCache(ctx, cache, j, "m", scs, set, 2)
	if err != nil || res.Filled != 2 || res.Remaining != 1 || res.Usage.InputTokens != 20 || j.calls != 2 {
		t.Fatalf("capped fill: %+v %v", res, err)
	}
	res, err = FillJudgeCache(ctx, cache, j, "m", scs, set, 10)
	if err != nil || res.Cached != 2 || res.Filled != 1 || res.Remaining != 0 || j.calls != 3 {
		t.Fatalf("resumed fill: %+v %v", res, err)
	}
	if !strings.Contains(res.String(), "3 entries: 2 cached, 0 stale, 1 filled, 0 remaining") {
		t.Errorf("summary %q", res.String())
	}
	if e, ok, err := cache.Lookup(seed, set.Entries[0].Text); !ok || err != nil || e.Model != "m" {
		t.Errorf("filled entry: %+v ok=%v err=%v", e, ok, err)
	}

	// A stale entry is refilled.
	changed := seed
	changed.Description += " (edited)"
	res, err = FillJudgeCache(ctx, cache, j, "m", map[string]Scenario{seed.Name: changed}, set, 10)
	if err != nil || res.Stale != 3 || res.Filled != 3 {
		t.Errorf("stale refill: %+v %v", res, err)
	}

	// An entry for a scenario that is not committed is refused before any call.
	j = &fakeMetered{}
	bad := AmbiguousSet{Entries: []AmbiguousText{{Scenario: "learned-room-123", Text: "secret"}}}
	if _, err := FillJudgeCache(ctx, cache, j, "m", scs, bad, 10); err == nil || j.calls != 0 {
		t.Errorf("unknown scenario: err=%v calls=%d", err, j.calls)
	}

	// A text the tri-state pass does not find ambiguous is refused too: only
	// the ambiguous corpus set may reach the cache.
	clear := AmbiguousSet{Entries: []AmbiguousText{{Scenario: seed.Name, Text: "The consumer has a bug."}}}
	if _, err := FillJudgeCache(ctx, cache, j, "m", scs, clear, 10); err == nil || !strings.Contains(err.Error(), "does not find the text ambiguous") || j.calls != 0 {
		t.Errorf("clear text: err=%v calls=%d", err, j.calls)
	}

	// A judge failure stops the fill with what it did so far.
	j = &fakeMetered{err: errors.New("boom")}
	fresh := NewJudgeCache(t.TempDir())
	if res, err := FillJudgeCache(ctx, fresh, j, "m", scs, set, 10); err == nil || res.Filled != 0 {
		t.Errorf("judge failure: %+v %v", res, err)
	}
}

// TestCommittedJudgeCacheHoldsOnlyManifestTexts: every verdict in the real
// cache is for a manifest text, so nothing but committed synthetic corpus text
// is ever in the public cache.
func TestCommittedJudgeCacheHoldsOnlyManifestTexts(t *testing.T) {
	set, err := LoadAmbiguousSet(filepath.FromSlash(ambiguousSetFile))
	if err != nil {
		t.Fatal(err)
	}
	keys := map[string]bool{}
	for _, e := range set.Entries {
		keys[JudgeCacheKey(JudgePromptVersion, strings.TrimPrefix(e.Scenario, heldoutPrefix), e.Text)+".json"] = true
	}
	des, err := os.ReadDir("judgecache")
	if err != nil {
		t.Fatal(err)
	}
	for _, d := range des {
		switch {
		case d.Name() == "README.md" || d.Name() == "ambiguous.json":
		case !keys[d.Name()]:
			t.Errorf("judgecache/%s is not a verdict for a manifest text of prompt %s", d.Name(), JudgePromptVersion)
		}
	}
}

// The metered text judge passes the send function's tokens through.
func TestMeteredTextJudge(t *testing.T) {
	j := NewMeteredTextJudge(func(context.Context, string, string) (string, JudgeUsage, error) {
		return `{"layer_correct": true, "root_cause_correct": true, "root_cause_layer_correct": true, "reason": "ok"}`, JudgeUsage{InputTokens: 5, OutputTokens: 2}, nil
	})
	v, u, err := j.JudgeHypothesisMetered(context.Background(), loadSeed(), "h")
	if err != nil || !v.RootCauseCorrect || u.InputTokens != 5 || u.OutputTokens != 2 {
		t.Errorf("v=%+v u=%+v err=%v", v, u, err)
	}
}

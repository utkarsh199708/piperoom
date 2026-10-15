//go:build !claude

// Without -tags claude, newJudge refuses, so nothing here can reach the API.

package main

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"piperoom/evals"
)

const testManifest = "../../judgecache/ambiguous.json"

func TestParseFlagsJudgeDefaults(t *testing.T) {
	f, err := parseFlags(nil, &bytes.Buffer{})
	if err != nil {
		t.Fatal(err)
	}
	if f.judge != "" || f.judgeCache != evals.DefaultJudgeCacheDir || f.maxCalls != evals.DefaultJudgeMaxCalls || f.cacheOnly || f.judgeFill != "" || f.dryRun {
		t.Errorf("defaults: %+v", f)
	}
}

func TestRunJudgeFillDryRun(t *testing.T) {
	prices := filepath.Join(t.TempDir(), "prices.json")
	if err := os.WriteFile(prices, []byte(`{"m": {"input": 4, "output": 20, "cache_write": 5, "cache_read": 0.2}}`), 0o600); err != nil {
		t.Fatal(err)
	}
	cache := t.TempDir()
	var out bytes.Buffer
	code, err := run(flags{scenarios: "../../scenarios", judgeFill: testManifest, dryRun: true, judgeCache: cache,
		maxCalls: 10, judgeModel: "m", prices: prices}, &out, &out)
	if err != nil || code != 0 {
		t.Fatalf("code %d err %v out %s", code, err, out.String())
	}
	for _, want := range []string{"0 cached, 0 stale, 0 filled", "judge call(s) needed", "tokens on m, about $", "pass -judge-max-calls"} {
		if !strings.Contains(out.String(), want) {
			t.Errorf("dry run output lacks %q:\n%s", want, out.String())
		}
	}
	if des, _ := os.ReadDir(cache); len(des) != 0 {
		t.Errorf("dry run wrote %d files", len(des))
	}
	// No price for the model: the estimate says so.
	out.Reset()
	if _, err := run(flags{scenarios: "../../scenarios", judgeFill: testManifest, dryRun: true, judgeCache: cache, maxCalls: 10, judgeModel: "unpriced", prices: prices}, &out, &out); err != nil || !strings.Contains(out.String(), "no price for the model") {
		t.Errorf("unpriced: err %v out %s", err, out.String())
	}
}

func TestRunJudgeFillRefusals(t *testing.T) {
	base := flags{scenarios: "../../scenarios", judgeFill: testManifest, judgeCache: t.TempDir(), maxCalls: 10}
	cases := map[string]struct {
		f    func(flags) flags
		key  string
		want string
	}{
		"learned":    {func(f flags) flags { f.learned = t.TempDir(); return f }, "x", "never reads -learned"},
		"no cache":   {func(f flags) flags { f.judgeCache = ""; return f }, "x", "needs -judge-cache"},
		"no key":     {func(f flags) flags { return f }, "", "ANTHROPIC_API_KEY is not set"},
		"no claude":  {func(f flags) flags { return f }, "sk-ant-test-not-real", "-tags claude"},
		"bad file":   {func(f flags) flags { f.judgeFill = "missing.json"; return f }, "x", "read ambiguous set"},
		"dry no set": {func(f flags) flags { f.judgeFill = ""; f.dryRun = true; return f }, "x", "-dry-run goes with -judge-fill"},
	}
	for name, c := range cases {
		t.Run(name, func(t *testing.T) {
			t.Setenv("ANTHROPIC_API_KEY", c.key)
			var out bytes.Buffer
			code, err := run(c.f(base), &out, &out)
			if code != 1 || err == nil || !strings.Contains(err.Error(), c.want) {
				t.Errorf("code %d err %v, want %q", code, err, c.want)
			}
		})
	}
}

func TestLiveJudge(t *testing.T) {
	auto := []evals.Scenario{{Name: "a"}}
	claude := []evals.Scenario{{Name: "c", Judge: evals.JudgeClaude}}
	var errOut bytes.Buffer
	// auto without a claude build: no live judge, a note, no error.
	j, err := liveJudge(flags{maxCalls: 50, judgeCache: "evals/judgecache"}, auto, &errOut)
	if j != nil || err != nil || !strings.Contains(errOut.String(), "ambiguous (unjudged)") {
		t.Errorf("auto: j=%v err=%v note %q", j, err, errOut.String())
	}
	// -judge claude must have one.
	if _, err := liveJudge(flags{judge: evals.JudgeClaude, maxCalls: 50}, auto, &errOut); err == nil {
		t.Error("-judge claude without a claude build accepted")
	}
	if _, err := liveJudge(flags{maxCalls: 50}, claude, &errOut); err == nil {
		t.Error("scenario judge claude without a claude build accepted")
	}
	// Cache-only, no live calls, or terms: no judge is built at all.
	for _, f := range []flags{{cacheOnly: true, maxCalls: 50}, {maxCalls: 0}, {maxCalls: -1}, {judge: evals.JudgeTerms, maxCalls: 50}} {
		errOut.Reset()
		if j, err := liveJudge(f, auto, &errOut); j != nil || err != nil || errOut.Len() != 0 {
			t.Errorf("%+v: j=%v err=%v note %q", f, j, err, errOut.String())
		}
	}
	if judgeMaxCalls(0) != -1 || judgeMaxCalls(7) != 7 || judgeMaxCalls(-3) != -3 {
		t.Error("judgeMaxCalls mapping")
	}
}

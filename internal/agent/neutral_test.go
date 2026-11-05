package agent

import (
	"context"
	"encoding/json"
	"go/parser"
	"go/token"
	"io/fs"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"testing"

	"piperoom/internal/agent/llm"
)

// neutralModel is a provider-neutral fake: an llm.Model that is not the
// Claude adapter. It records every request and answers with the scripted
// responses in order (the last one repeats).
type neutralModel struct {
	mu      sync.Mutex
	reqs    []llm.Request
	answers []llm.Response
}

func (m *neutralModel) Turn(_ context.Context, req llm.Request) (llm.Response, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.reqs = append(m.reqs, req)
	i := min(len(m.reqs), len(m.answers)) - 1
	return m.answers[i], nil
}

func (m *neutralModel) Capabilities(string) llm.Capabilities {
	return llm.Capabilities{ToolCalls: true, StructuredOutput: true}
}

func (m *neutralModel) Provider() string { return "neutral-fake" }

// textAnswer is a finished assistant turn with one text part, as an adapter
// other than Claude's returns it: no Native.
func textAnswer(text string) llm.Response {
	return llm.Response{
		Message: llm.Message{Role: llm.RoleAssistant, Parts: []llm.Part{{Kind: llm.PartText, Text: text}}},
		Stop:    llm.StopEndTurn,
		Usage:   llm.Usage{Input: 100, Output: 20},
	}
}

func summaryJSON(t *testing.T, s any) string {
	t.Helper()
	b, err := json.Marshal(s)
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

// TestSummaryWritersOnANeutralModel: the hand-off and the post-mortem ask
// any llm.Model for structured output (OutputSchema, the Claude adapter's
// output_config.format) over a cached system prompt, and parse the answer.
func TestSummaryWritersOnANeutralModel(t *testing.T) {
	c := caseByName(t, "schema-change")
	opts := Options{Model: "neutral-model"}.withDefaults()

	m := &neutralModel{answers: []llm.Response{textAnswer(summaryJSON(t, c.summary))}}
	got, err := writeHandoff(context.Background(), c.events, opts, m)
	if err != nil {
		t.Fatalf("handoff: %v", err)
	}
	if got.WhatBroke != c.summary.WhatBroke || got.Why != c.summary.Why {
		t.Fatalf("handoff = %+v", got)
	}
	if _, err := writePostmortem(context.Background(), c.events, opts, m); err != nil {
		t.Fatalf("postmortem: %v", err)
	}
	if len(m.reqs) != 2 {
		t.Fatalf("requests = %d, want 2", len(m.reqs))
	}
	for i, prompt := range []string{handoffPrompt(), postmortemPrompt()} {
		req := m.reqs[i]
		if req.Model != "neutral-model" || req.System != prompt || req.MaxTokens != handoffMaxTokens {
			t.Fatalf("request %d: model %q, max %d, system set %v", i, req.Model, req.MaxTokens, req.System == prompt)
		}
		if !reflect.DeepEqual(req.OutputSchema, handoffSchema()) {
			t.Fatalf("request %d: OutputSchema = %v, want the hand-off schema", i, req.OutputSchema)
		}
		if req.Effort != opts.effort() || !req.Thinking || req.Cache != (llm.CacheHints{System: true}) || req.Compaction != nil || len(req.Tools) != 0 {
			t.Fatalf("request %d = %+v", i, req)
		}
		if len(req.Messages) != 1 || req.Messages[0].Role != llm.RoleUser || req.Messages[0].Parts[0].Text != renderRoomEvents(c.events) {
			t.Fatalf("request %d messages = %+v", i, req.Messages)
		}
	}
}

// TestHandoffRetryReplaysTheAnswerOnANeutralModel: the self-correction turn
// replays the first answer as plain assistant text, which every provider
// takes.
func TestHandoffRetryReplaysTheAnswerOnANeutralModel(t *testing.T) {
	c := caseByName(t, "schema-change")
	bad := cloneSummary(c.summary)
	bad.Why = "pg-payments-connector rejected the schema"
	first := summaryJSON(t, bad)
	m := &neutralModel{answers: []llm.Response{textAnswer(first), textAnswer(summaryJSON(t, c.summary))}}
	got, err := writeHandoff(context.Background(), c.events, Options{}.withDefaults(), m)
	if err != nil {
		t.Fatal(err)
	}
	if len(m.reqs) != 2 || len(got.Violations) != 0 {
		t.Fatalf("requests %d, violations %v", len(m.reqs), got.Violations)
	}
	retry := m.reqs[1].Messages
	if len(retry) != 3 || retry[1].Role != llm.RoleAssistant || retry[1].Parts[0].Text != first || retry[1].Parts[0].Native != nil ||
		retry[2].Role != llm.RoleUser || !strings.Contains(retry[2].Parts[0].Text, "fix them") {
		t.Fatalf("retry messages = %+v", retry)
	}
}

// TestRefusalOnANeutralModel: a refusal stops a summary like it did on
// Claude.
func TestRefusalOnANeutralModel(t *testing.T) {
	c := caseByName(t, "schema-change")
	m := &neutralModel{answers: []llm.Response{{Message: llm.Message{Role: llm.RoleAssistant}, Stop: llm.StopRefusal}}}
	if _, err := writeHandoff(context.Background(), c.events, Options{}.withDefaults(), m); err == nil || !strings.Contains(err.Error(), "refused") {
		t.Fatalf("err = %v, want a refusal", err)
	}
}

// TestOnlyTheClaudeAdapterImportsTheSDK: anthropic-sdk-go is imported by
// internal/agent/llm/anthropic and by test files only (design 2, V1b). Every
// build tag counts: the files are parsed, not built.
func TestOnlyTheClaudeAdapterImportsTheSDK(t *testing.T) {
	const sdk = "github.com/anthropics/anthropic-sdk-go"
	adapter := filepath.Join("llm", "anthropic")
	fset := token.NewFileSet()
	err := filepath.WalkDir(".", func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			if path == adapter || d.Name() == "testdata" {
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		f, err := parser.ParseFile(fset, path, nil, parser.ImportsOnly)
		if err != nil {
			return err
		}
		for _, imp := range f.Imports {
			p, _ := strconv.Unquote(imp.Path.Value)
			if p == sdk || strings.HasPrefix(p, sdk+"/") {
				t.Errorf("%s imports %s; only internal/agent/llm/anthropic may", path, p)
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

package setuptools

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"piperoom/internal/setup/knowledge"
	"piperoom/internal/tools"
)

type configHelpIndexStub struct {
	entry knowledge.Entry
	err   error
}

func (s configHelpIndexStub) Lookup(kind knowledge.Kind, _ string) (knowledge.Entry, error) {
	if s.err != nil {
		return knowledge.Entry{}, s.err
	}
	if kind != s.entry.Kind {
		return knowledge.Entry{}, knowledge.ErrUnknownEntry
	}
	return s.entry, nil
}

func TestConfigHelpLooksUpSMTPredicateAndSinks(t *testing.T) {
	index, err := knowledge.LoadIndex()
	if err != nil {
		t.Fatalf("LoadIndex: %v", err)
	}
	tool := &configHelpTool{index: index}
	cases := []struct {
		name        string
		input       string
		wantKind    knowledge.Kind
		wantName    string
		wantSupport string
	}{
		{
			name:     "SMT class",
			input:    `{"kind":"smt","name":"ExtractNewRecordState"}`,
			wantKind: knowledge.KindSMT,
			wantName: "extract-new-record-state",
		},
		{
			name:     "predicate class",
			input:    `{"kind":"predicate","name":"TopicNameMatches"}`,
			wantKind: knowledge.KindPredicate,
			wantName: "topic-name-matches",
		},
		{
			name:        "sink connector class and release",
			input:       `{"kind":"sink","name":"io.debezium.connector.jdbc.JdbcSinkConnector","release":"3.2.0.Final"}`,
			wantKind:    knowledge.KindSink,
			wantName:    "debezium-jdbc-sink",
			wantSupport: "supported",
		},
		{
			name:     "Debezium Server sink type",
			input:    `{"kind":"sink","name":"http"}`,
			wantKind: knowledge.KindSink,
			wantName: "server-http",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := tool.Run(context.Background(), json.RawMessage(tc.input))
			if err != nil {
				t.Fatalf("Run: %v", err)
			}
			data, ok := got.Data.(configHelpData)
			if !ok {
				t.Fatalf("Data has type %T, want configHelpData", got.Data)
			}
			if data.Kind != tc.wantKind || data.Name != tc.wantName {
				t.Errorf("entry = %s %s, want %s %s", data.Kind, data.Name, tc.wantKind, tc.wantName)
			}
			if data.Support != tc.wantSupport {
				t.Errorf("support = %q, want %q", data.Support, tc.wantSupport)
			}
			if got.Summary == "" || len(got.Summary) > tools.MaxSummaryBytes {
				t.Errorf("summary length = %d, want 1..%d", len(got.Summary), tools.MaxSummaryBytes)
			}
			if data.Documentation == "" || data.Source == "" || len(data.Checks) == 0 {
				t.Errorf("the index details are incomplete: %+v", data)
			}
		})
	}
}

func TestConfigHelpIsRegisteredAndIncludesSinkLookup(t *testing.T) {
	reg := tools.NewReadOnlyRegistry()
	if err := Register(reg, Deps{RoomID: "r_setup", CallRoom: callRoomFromCtx}); err != nil {
		t.Fatalf("Register: %v", err)
	}
	tool, ok := reg.Get(configHelpName)
	if !ok {
		t.Fatalf("%s is not registered", configHelpName)
	}
	if tool.Kind() != tools.KindRead {
		t.Fatalf("Kind = %q, want %q", tool.Kind(), tools.KindRead)
	}
	result, err := tool.Run(context.Background(), json.RawMessage(`{"kind":"sink","name":"http"}`))
	if err != nil {
		t.Fatalf("sink lookup: %v", err)
	}
	data, ok := result.Data.(configHelpData)
	if !ok || data.Name != "server-http" || data.SinkType != "http" {
		t.Fatalf("sink lookup data = %#v, want server-http / http", result.Data)
	}
}

func TestConfigHelpBadInputsWrapErrBadInput(t *testing.T) {
	index, err := knowledge.LoadIndex()
	if err != nil {
		t.Fatalf("LoadIndex: %v", err)
	}
	tool := &configHelpTool{index: index}
	cases := []struct {
		name  string
		input string
	}{
		{name: "malformed JSON", input: `{"kind":"smt","name":`},
		{name: "missing name", input: `{"kind":"smt"}`},
		{name: "unsupported kind", input: `{"kind":"tuning","name":"batch.size"}`},
		{name: "unknown field", input: `{"kind":"sink","name":"http","op":"compose_up"}`},
		{name: "unknown sink", input: `{"kind":"sink","name":"NoSuchSink"}`},
		{name: "ambiguous SMT", input: `{"kind":"smt","name":"Filter"}`},
		{name: "invalid release", input: `{"kind":"sink","name":"http","release":"not-a-release"}`},
		{name: "oversized name", input: `{"kind":"sink","name":"` + strings.Repeat("a", 257) + `"}`},
		{name: "duplicate case-folded field", input: `{"kind":"sink","Kind":"sink","name":"http"}`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := tool.Run(context.Background(), json.RawMessage(tc.input))
			if !errors.Is(err, tools.ErrBadInput) {
				t.Fatalf("Run error = %v, want ErrBadInput", err)
			}
		})
	}
}

func TestConfigHelpReportsLookupFailuresAsBadInput(t *testing.T) {
	tool := &configHelpTool{index: configHelpIndexStub{err: knowledge.ErrUnknownEntry}}
	_, err := tool.Run(context.Background(), json.RawMessage(`{"kind":"sink","name":"unknown"}`))
	if !errors.Is(err, tools.ErrBadInput) || !errors.Is(err, knowledge.ErrUnknownEntry) {
		t.Fatalf("Run error = %v, want ErrBadInput and ErrUnknownEntry", err)
	}
}

func TestConfigHelpRespectsCancellation(t *testing.T) {
	index, err := knowledge.LoadIndex()
	if err != nil {
		t.Fatalf("LoadIndex: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = (&configHelpTool{index: index}).Run(ctx, json.RawMessage(`{"kind":"sink","name":"http"}`))
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Run error = %v, want context.Canceled", err)
	}
}

func TestConfigHelpSummaryIsTruncatedAndSecretDefaultsAreOmitted(t *testing.T) {
	secret := "never expose"
	entry := knowledge.Entry{
		Name:     "sink-large",
		Kind:     knowledge.KindSink,
		SinkType: "sink-large",
		Summary:  strings.Repeat("x", tools.MaxSummaryBytes+100),
		Versions: ">=3.0.0.Final",
		Keys:     []knowledge.Key{{Name: "password", Secret: true, Default: &secret}},
	}
	tool := &configHelpTool{index: configHelpIndexStub{entry: entry}}
	got, err := tool.Run(context.Background(), json.RawMessage(`{"kind":"sink","name":"sink-large"}`))
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if !got.Truncated || len(got.Summary) > tools.MaxSummaryBytes {
		t.Errorf("summary truncated=%v, length=%d", got.Truncated, len(got.Summary))
	}
	data := got.Data.(configHelpData)
	if len(data.Keys) != 1 || data.Keys[0].Default != nil || !data.Keys[0].Secret {
		t.Errorf("secret key help = %+v, want no default value", data.Keys)
	}
}

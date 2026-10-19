// Package dbttools registers the read-only dbt tools of a batch pipeline
// (P4d): dbt_run_results (the latest run's status per node), dbt_model_lineage
// (one model's direct parents and children) and dbt_model_sql (one model's
// compiled SQL). They read dbt's artifacts through clients.TransformReader
// (internal/clients/dbt: DBT_ARTIFACTS_DIR or dbt Cloud) and never run
// anything: dbt_model_sql shows SQL, it does not execute it.
//
// Scope (tools.Scope): a model is in scope when it is one of the room
// pipeline's models or a direct parent or child of one, by the manifest's
// lineage (tools.Scope.CheckModelOrAdjacent). Every name is checked before the
// artifact the tool answers from is read; only the adjacency check itself
// reads lineage.
//
// Everything dbt wrote (messages, SQL, node ids) is untrusted text. Messages
// and SQL are masked with internal/redact and scanned with internal/injectscan
// here, and the agent loop wraps the whole result as untrusted tool output.
package dbttools

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"regexp"
	"slices"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"

	"piperoom/internal/clients"
	"piperoom/internal/clients/dbt"
	"piperoom/internal/injectscan"
	"piperoom/internal/owners"
	"piperoom/internal/redact"
	"piperoom/internal/tools"
)

// Tool names.
const (
	RunResultsTool = "dbt_run_results"
	LineageTool    = "dbt_model_lineage"
	SQLTool        = "dbt_model_sql"
)

// timeout bounds one tool call. It is above the 10 s tool default (AGENTS.md
// rule 4) because one call may read the manifest several times: the
// adjacency check reads the lineage of each pipeline model, and a dbt Cloud
// read is two requests of up to 10 s each, for a manifest of up to 16 MiB.
const timeout = 30 * time.Second

// maxModelInput caps the model input before any parsing or echo.
const maxModelInput = 256

// Deps are the collaborators of the dbt tools.
type Deps struct {
	// Transform reads the dbt artifacts. Nil when dbt is not configured
	// (DBT_ARTIFACTS_DIR and DBT_CLOUD_URL unset): the tools stay registered,
	// so the tool list does not depend on setup, and answer
	// clients.ErrNotSupported. dbt_model_sql also needs the optional
	// clients.TransformSQLReader, which both dbt readers implement.
	Transform clients.TransformReader
	// Owners resolves the room's pipeline id (tools.WithPipelineID) to its
	// models. Wiring must pass it; without it a room-scoped call is refused.
	Owners *owners.Owners
}

// Register adds dbt_run_results, dbt_model_lineage and dbt_model_sql to reg.
// All three are tools.KindRead.
func Register(reg *tools.Registry, d Deps) error {
	for _, t := range []tools.Tool{&runResults{d: d}, &modelLineage{d: d}, &modelSQL{d: d}} {
		if err := reg.Register(t); err != nil {
			return fmt.Errorf("dbttools: register %s: %w", t.Name(), err)
		}
	}
	return nil
}

// notConfigured is the answer of every tool without a reader.
func notConfigured(tool string) error {
	return fmt.Errorf("dbttools: %s: dbt is not configured (set DBT_ARTIFACTS_DIR or DBT_CLOUD_URL): %w", tool, clients.ErrNotSupported)
}

// decode reads a tool input strictly: unknown fields are bad input, like the
// schemas' additionalProperties false says.
func decode(in json.RawMessage, v any) error {
	if len(in) == 0 {
		in = json.RawMessage("{}")
	}
	dec := json.NewDecoder(strings.NewReader(string(in)))
	dec.DisallowUnknownFields()
	if err := dec.Decode(v); err != nil {
		return fmt.Errorf("%w: %v", tools.ErrBadInput, err)
	}
	if _, err := dec.Token(); !errors.Is(err, io.EOF) {
		return fmt.Errorf("%w: trailing data after the input object", tools.ErrBadInput)
	}
	return nil
}

// modelRef is a parsed model input: what the reader is asked for (Ref, as
// given: a name or a unique_id) and the model name the scope checks (Name).
type modelRef struct {
	Ref  string
	Name string
}

// parseModel accepts a model name or a model unique_id
// ("model.<package>.<name>[.<version>]"). Anything else (a source, seed or
// test id, or text outside the model alphabet) is bad input: the scope is
// about models, and owners.ModelName is the one rule for reading a ref.
func parseModel(s string) (modelRef, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return modelRef{}, fmt.Errorf("%w: model is required", tools.ErrBadInput)
	}
	if len(s) > maxModelInput {
		return modelRef{}, fmt.Errorf("%w: model is longer than %d bytes", tools.ErrBadInput, maxModelInput)
	}
	name, ok := owners.ModelName(s)
	if !ok {
		return modelRef{}, fmt.Errorf("%w: model %q is not a model name or a model unique_id (model.<package>.<name>)", tools.ErrBadInput, oneLine(s, 80))
	}
	return modelRef{Ref: s, Name: name}, nil
}

// checkModel resolves the call's scope and checks ref against it before the
// tool reads its own artifact. A pipeline model needs no I/O; anything else
// is allowed only when the manifest's lineage makes it a direct parent or
// child of a pipeline model, and a lineage read that fails refuses it.
func checkModel(ctx context.Context, d Deps, tool string, ref modelRef) (tools.Scope, error) {
	sc, err := tools.ScopeFor(ctx, d.Owners)
	if err != nil {
		return tools.Scope{}, err
	}
	if sc.Unscoped() {
		return sc, nil
	}
	if err := sc.CheckModel(ref.Name); err == nil {
		return sc, nil
	} else if d.Transform == nil {
		// No reader to prove adjacency: the plain refusal, which names the
		// pipeline's models, is the honest answer.
		return tools.Scope{}, err
	}
	if err := sc.CheckModelOrAdjacent(ctx, ref.Name, d.Transform); err != nil {
		return tools.Scope{}, fmt.Errorf("dbttools: %s: %w", tool, err)
	}
	return sc, nil
}

// ambiguous turns a dbt.ErrAmbiguous lookup into a result that lists the
// candidate unique_ids and asks the agent to retry with one (the PR #627
// ruling on TransformReader.ModelLineage). ok is false for any other error.
func ambiguous(tool string, ref modelRef, err error) (tools.Result, bool) {
	if !errors.Is(err, dbt.ErrAmbiguous) {
		return tools.Result{}, false
	}
	var ae *dbt.AmbiguousError
	var cands []string
	if errors.As(err, &ae) {
		for _, id := range ae.Candidates {
			// Candidates are manifest text: only well-formed model ids of
			// this very name are shown, and at most maxCandidates of them.
			if n, ok := owners.ModelName(id); ok && n == ref.Name && safeID.MatchString(id) {
				cands = append(cands, id)
			}
		}
	}
	more := 0
	if len(cands) > maxCandidates {
		more = len(cands) - maxCandidates
		cands = cands[:maxCandidates]
	}
	var sb strings.Builder
	fmt.Fprintf(&sb, "Model %q is ambiguous: more than one model in manifest.json has this name.", ref.Name)
	if len(cands) > 0 {
		sb.WriteString(" Candidates:\n")
		for _, c := range cands {
			sb.WriteString("- " + c + "\n")
		}
		if more > 0 {
			fmt.Fprintf(&sb, "(%d more not shown)\n", more)
		}
		fmt.Fprintf(&sb, "Call %s again with model set to one of these unique_ids.", tool)
	} else {
		fmt.Fprintf(&sb, " Call %s again with model set to the model's unique_id (model.<package>.%s).", tool, ref.Name)
	}
	summary, cut := tools.Truncate(sb.String())
	return tools.Result{
		Summary:   summary,
		Data:      map[string]any{"model": ref.Name, "ambiguous": true, "candidates": nonNil(cands), "candidates_not_shown": more},
		Truncated: cut || more > 0,
	}, true
}

const maxCandidates = 20

// safeID is the shape of a dbt unique_id that may be echoed to the model:
// resource type, package, name and optional version, dot-separated.
var safeID = regexp.MustCompile(`^[A-Za-z0-9_][A-Za-z0-9_.\-]{0,199}$`)

// cleanID makes a manifest unique_id safe to show on one line: a
// well-formed id passes, anything else is cut and stripped of control
// characters and marked.
func cleanID(id string) string {
	if safeID.MatchString(id) {
		return id
	}
	return "[unusual id] " + oneLine(id, 120)
}

// scrub masks secrets in untrusted dbt text (messages, SQL) and reports the
// kinds of instruction-like text injectscan finds in the masked result. The
// text is kept as is otherwise: it may be the evidence.
func scrub(s string) (string, []string) {
	s, _ = redact.Text(s)
	return s, injectscan.Kinds(injectscan.Scan(s))
}

// oneLine folds whitespace and control characters to single spaces and cuts
// s to n runes, ending in "…" when cut, so untrusted text cannot start a new
// summary line.
func oneLine(s string, n int) string {
	s = strings.Join(strings.FieldsFunc(s, func(r rune) bool { return unicode.IsSpace(r) || unicode.IsControl(r) }), " ")
	if utf8.RuneCountInString(s) <= n {
		return s
	}
	return string([]rune(s)[:n]) + "…"
}

// cutBytes cuts s to at most n bytes on a rune boundary.
func cutBytes(s string, n int) (string, bool) {
	if len(s) <= n {
		return s, false
	}
	for n > 0 && !utf8.RuneStart(s[n]) {
		n--
	}
	return s[:n], true
}

func nonNil(s []string) []string {
	if s == nil {
		return []string{}
	}
	return s
}

// isPipelineModel reports whether a node id names one of the scope's models.
func isPipelineModel(sc tools.Scope, id string) bool {
	if sc.Unscoped() {
		return false
	}
	n, ok := owners.ModelName(id)
	return ok && slices.Contains(sc.Pipeline().Models, n)
}

func objectSchema(props map[string]any, required ...string) map[string]any {
	return map[string]any{
		"type":                 "object",
		"properties":           props,
		"required":             nonNil(required),
		"additionalProperties": false,
	}
}

const modelInputDoc = "A dbt model: a name from the room pipeline's models or a direct parent or child of one, " +
	"or a model unique_id (model.<package>.<name>) when a name is ambiguous."

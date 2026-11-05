package setupactions

import (
	"encoding/json"
	"fmt"
	"slices"
	"strings"

	"piperoom/internal/owners"
	"piperoom/internal/tools"
)

// ScopeFree is the scope rule of an op that names no pipeline resource: a
// file under the runner's project directory, a Compose stack the runner
// renders itself. It accepts every pipeline, and it is an explicit choice in
// the spec, never a nil rule New would have to guess about.
func ScopeFree(owners.Pipeline, json.RawMessage) error { return nil }

// ScopeField refuses a single string field whose value is not one of allowed's
// entries for the pipeline: {"connector": "pg-orders"} against the pipeline's
// connectors, say. The refusal wraps tools.ErrOutOfScope, so the room answers
// it like every other out-of-scope write. Bad params wrap tools.ErrBadInput.
func ScopeField(field string, allowed func(owners.Pipeline) []string) ScopeFunc {
	return func(p owners.Pipeline, params json.RawMessage) error {
		v, err := stringField(params, field)
		if err != nil {
			return err
		}
		if !slices.Contains(allowed(p), v) {
			return fmt.Errorf("%w: %s %q is not part of pipeline %q", tools.ErrOutOfScope, field, clipText(v), clipText(p.ID))
		}
		return nil
	}
}

// ScopeListField is ScopeField for a list of names: every entry must be one of
// allowed's entries for the pipeline. An empty or missing list is refused: an
// op that names no table would otherwise silently target everything.
func ScopeListField(field string, allowed func(owners.Pipeline) []string) ScopeFunc {
	return func(p owners.Pipeline, params json.RawMessage) error {
		values, err := stringListField(params, field)
		if err != nil {
			return err
		}
		if len(values) == 0 {
			return fmt.Errorf("%w: %s names no entry", tools.ErrOutOfScope, field)
		}
		have := allowed(p)
		for _, v := range values {
			if !slices.Contains(have, v) {
				return fmt.Errorf("%w: %s %q is not part of pipeline %q", tools.ErrOutOfScope, field, clipText(v), clipText(p.ID))
			}
		}
		return nil
	}
}

// ScopeTables refuses a tables list with a table outside the pipeline's
// source_tables.
func ScopeTables() ScopeFunc {
	return ScopeListField("tables", func(p owners.Pipeline) []string { return p.SourceTables })
}

// ScopeConnector refuses a connector that is not one of the pipeline's
// connectors.
func ScopeConnector() ScopeFunc {
	return ScopeField("connector", func(p owners.Pipeline) []string { return p.Connectors })
}

// stringField decodes one non-empty string field of params.
func stringField(params json.RawMessage, field string) (string, error) {
	var obj map[string]json.RawMessage
	if err := json.Unmarshal(paramsOrEmpty(params), &obj); err != nil {
		return "", fmt.Errorf("%w: scope: params must be a JSON object", tools.ErrBadInput)
	}
	raw, ok := obj[field]
	if !ok {
		return "", fmt.Errorf("%w: scope: %s is required", tools.ErrBadInput, field)
	}
	var v string
	if err := json.Unmarshal(raw, &v); err != nil || strings.TrimSpace(v) == "" {
		return "", fmt.Errorf("%w: scope: %s must be a non-empty string", tools.ErrBadInput, field)
	}
	return v, nil
}

// stringListField decodes one string list field of params.
func stringListField(params json.RawMessage, field string) ([]string, error) {
	var obj map[string]json.RawMessage
	if err := json.Unmarshal(paramsOrEmpty(params), &obj); err != nil {
		return nil, fmt.Errorf("%w: scope: params must be a JSON object", tools.ErrBadInput)
	}
	raw, ok := obj[field]
	if !ok {
		return nil, fmt.Errorf("%w: scope: %s is required", tools.ErrBadInput, field)
	}
	var values []string
	if err := json.Unmarshal(raw, &values); err != nil {
		return nil, fmt.Errorf("%w: scope: %s must be a list of names", tools.ErrBadInput, field)
	}
	return values, nil
}

// paramsOrEmpty treats absent params as the empty object, so the error is
// "field is required", not a JSON syntax error.
func paramsOrEmpty(params json.RawMessage) json.RawMessage {
	if len(strings.TrimSpace(string(params))) == 0 {
		return json.RawMessage(`{}`)
	}
	return params
}

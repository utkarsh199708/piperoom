package agent

import (
	"context"
	"fmt"

	"piperoom/internal/events"
)

// WritePostmortem turns a closed room's log into a closing summary with one
// structured model request and checks it against the log. It is the CLI's
// writer; the server uses Endpoint.WritePostmortem, which also counts tokens.
func WritePostmortem(ctx context.Context, roomEvents []events.Event, opts Options) (PostmortemDraft, error) {
	opts = opts.withDefaults()
	return writePostmortem(ctx, roomEvents, opts, newModel(opts, nil))
}

// writePostmortem is the testable seam: the model is injected.
//
// Grounding is the product rule: a name, number or reference the log does not
// contain comes back in PostmortemDraft.Violations, so the reader sees it
// marked instead of shipped silently. A summary that is empty in a required
// field, or over the length caps, is an error and the draft is dropped.
func writePostmortem(ctx context.Context, roomEvents []events.Event, opts Options, m model) (PostmortemDraft, error) {
	s, err := writeSummary(ctx, roomEvents, opts, m, postmortemPrompt(), "postmortem")
	if err != nil {
		return PostmortemDraft{}, err
	}
	var violations []string
	for _, v := range CheckHandoff(roomEvents, s) {
		switch v.Rule {
		case RuleEmptyField, RuleListTooLong, RuleStringTooLong:
			return PostmortemDraft{}, fmt.Errorf("agent: postmortem: summary fails %s at %s", v.Rule, v.Field)
		case RuleUnknownName, RuleUnknownNumber, RuleUnknownRef:
			// Rule and field only: the detail quotes the log.
			violations = append(violations, v.Rule+" "+v.Field)
		}
	}
	return PostmortemDraft{Summary: s, Violations: violations}, nil
}

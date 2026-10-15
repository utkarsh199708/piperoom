package agent

import "context"

// newSignalPreface opens the prompt line of an alert update that brings a new
// incident (HumanInput.NewIncident, QA G1). The room wrote it, not a person and
// not the alert, so it stands outside the <untrusted> wrapper; the alert's own
// text follows inside the wrapper, as data. The system prompt explains the tag.
const newSignalPreface = "[new alert signal] A new signal arrived on this pipeline. The earlier diagnosis may not cover it: investigate it. "

// hasNewIncident reports whether a batch carries an alert update with a signal
// the room had not seen.
func hasNewIncident(inputs []HumanInput) bool {
	for _, in := range inputs {
		if in.Kind == "alert_update" && in.NewIncident {
			return true
		}
	}
	return false
}

// handleBatch is handleInputs plus what a new incident changes. A new signal
// means the pipeline is no longer known to be recovered, whatever an earlier
// verification said: an episode parked as done must leave that state, and the
// model must run to read the signal. Without this a runner that had verified a
// fix would stay parked while a second outage went unread (QA G1: a different
// incident right after a fix).
func (r *runner) handleBatch(ctx context.Context, inputs []HumanInput) ([]HumanInput, actionOutcome) {
	// A recap's proposals must be known before any step result is judged.
	r.absorbRecaps(inputs)
	inputs, out := r.handleInputs(ctx, inputs)
	if hasNewIncident(inputs) {
		r.recoveredIdle = false
		out.needsModel = true
	}
	return inputs, out
}

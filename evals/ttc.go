package evals

import (
	"time"

	"piperoom/internal/events"
)

// Time to the correct hypothesis (README "Time to cause").
//
// Start: the room.created event, i.e. the moment the room opened (for a
// watcher room, when the watcher opened it, not when the signal began).
// Without one (a transcript recorded outside a room) the first event.
//
// End: the first agent.hypothesis event, written by the agent, whose text is
// a correct root cause by the same deterministic rule as the final verdict:
// rootCauseMatches (the root_cause groups affirmed, no root_cause_forbidden
// story, no hedge). Messages do not count: they carry plans and questions
// ("checking whether the amount change ..."), while update_hypothesis is the
// agent's stated belief and what the room shows as it.
//
// Measured: wall time between the two events' room-log timestamps (ms), and
// the agent.step.start and agent.tool_call events before the end event
// (steps, tool calls). The hypothesis number (1 = the first) is kept too.
//
// It is reported only when the run's final root-cause verdict is correct
// (after the LLM judge when one graded it); a run that found the cause and
// then left it has no time to cause. CauseRetracted says the agent posted a
// wrong hypothesis after its first correct one, even if it came back.

// CauseTiming is one run's time to its first correct hypothesis.
type CauseTiming struct {
	MS         int64 `json:"ms"`
	Steps      int   `json:"steps"`
	ToolCalls  int   `json:"tool_calls"`
	Hypothesis int   `json:"hypothesis"`
}

// causeClock follows the hypotheses of one transcript.
type causeClock struct {
	start     time.Time
	steps     int // agent steps started so far
	n         int // hypotheses so far
	first     *CauseTiming
	retracted bool
}

func newCauseClock(evs []RecordedEvent) *causeClock {
	c := &causeClock{}
	for _, e := range evs {
		if e.Type == events.RoomCreated {
			c.start = e.At
			return c
		}
	}
	if len(evs) > 0 {
		c.start = evs[0].At
	}
	return c
}

// hypothesis records one agent hypothesis; toolCalls is the count before it.
func (c *causeClock) hypothesis(e RecordedEvent, correct bool, toolCalls int) {
	c.n++
	switch {
	case correct && c.first == nil:
		ms := int64(0)
		if !c.start.IsZero() && !e.At.IsZero() {
			ms = e.At.Sub(c.start).Milliseconds()
		}
		c.first = &CauseTiming{MS: ms, Steps: c.steps, ToolCalls: toolCalls, Hypothesis: c.n}
	case !correct && c.first != nil:
		c.retracted = true
	}
}

func (c *causeClock) result() (*CauseTiming, bool) { return c.first, c.retracted }

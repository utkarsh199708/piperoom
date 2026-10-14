package agent

import "time"

// defaultModel is the Claude model the agent uses unless Options says otherwise.
const defaultModel = "claude-opus-5-5"

// defaultEffort is the reasoning effort applied to every request. The API
// default for this model is "medium"; we want "high" unless overridden.
const defaultEffort = "high"

// defaultMaxTurns is the safety stop on the investigation loop.
const defaultMaxTurns = 40

// withDefaults fills zero-valued Options fields with their defaults.
func (o Options) withDefaults() Options {
	if o.Model == "" {
		o.Model = defaultModel
	}
	if o.Effort == "" {
		o.Effort = defaultEffort
	}
	if o.MaxTurns <= 0 {
		o.MaxTurns = defaultMaxTurns
	}
	return o
}

// effort maps an Options.Effort string to the SDK effort value. Unknown values
// fall back to the default so a bad env var cannot break the request.
func (o Options) effort() string {
	switch o.Effort {
	case "low", "medium", "high", "xhigh", "max":
		return o.Effort
	default:
		return defaultEffort
	}
}

const (
	// defaultToolTimeout bounds a single read tool call. The turn continues with
	// the other results if one tool times out.
	defaultToolTimeout = 10 * time.Second
	// defaultToolGrace is how long a read tool may run past its deadline or
	// the run's cancellation before the run stops waiting for it.
	defaultToolGrace = 2 * time.Second
	// defaultRoomToolTimeout bounds one room tool call (hypothesis, proposal,
	// message).
	defaultRoomToolTimeout = 10 * time.Second
	// defaultDrainBackoffMin and defaultDrainBackoffMax bound the backoff
	// waitForInput applies when Inbox.Drain keeps failing while Wait keeps
	// returning at once: without a backoff that combination spins the loop at
	// full speed.
	defaultDrainBackoffMin = 250 * time.Millisecond
	defaultDrainBackoffMax = 5 * time.Second
	// defaultFreshnessWait is the pause between warehouse_freshness read rounds.
	// One round sleeps once for all sinks still pending.
	defaultFreshnessWait = 10 * time.Second
	// defaultSettleWait is the pause between the two connector reads that a
	// recovery needs, so a connector that only just came back is not counted as
	// healthy.
	defaultSettleWait = 5 * time.Second
	// defaultBaselineTimeout bounds the whole creation-baseline snapshot taken
	// when a proposal is made.
	defaultBaselineTimeout = 20 * time.Second
	// defaultStepResultWait bounds how long a model turn waits for the step
	// results of a proposal that was just approved in full. The executor's
	// per-step timeout is 30 s, so this leaves room for one slow step.
	defaultStepResultWait = 45 * time.Second
	// proposalTimeout bounds the Proposals.Create call so a stalled proposal
	// store cannot hang the investigation.
	proposalTimeout = 10 * time.Second
)

// timing holds the waits and deadlines one runner uses. Each runner gets its own
// copy from defaultTiming, so tests shrink them on the runner and no package
// level state is shared between runs.
type timing struct {
	toolTimeout     time.Duration
	toolGrace       time.Duration
	roomToolTimeout time.Duration
	drainBackoffMin time.Duration
	drainBackoffMax time.Duration
	freshnessWait   time.Duration
	settleWait      time.Duration
	baselineTimeout time.Duration
	// stepResultWait is how long a turn waits for the step results of a
	// proposal approved in full before it starts without them.
	stepResultWait time.Duration
}

// defaultTiming returns the production waits and deadlines.
func defaultTiming() timing {
	return timing{
		toolTimeout:     defaultToolTimeout,
		toolGrace:       defaultToolGrace,
		roomToolTimeout: defaultRoomToolTimeout,
		drainBackoffMin: defaultDrainBackoffMin,
		drainBackoffMax: defaultDrainBackoffMax,
		freshnessWait:   defaultFreshnessWait,
		settleWait:      defaultSettleWait,
		baselineTimeout: defaultBaselineTimeout,
		stepResultWait:  defaultStepResultWait,
	}
}

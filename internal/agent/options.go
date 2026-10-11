package agent

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

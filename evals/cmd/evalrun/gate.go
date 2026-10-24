package main

// V10: the provider flags, the -gate check and the published models table.

import (
	"fmt"
	"io"
	"strings"

	"piperoom/evals"
	"piperoom/internal/config"
)

// agentProviders are the -provider values (internal/config's providers).
var agentProviders = []string{
	config.ModelProviderAnthropic, config.ModelProviderBedrock, config.ModelProviderVertex,
	config.ModelProviderOpenAI, config.ModelProviderGemini, config.ModelProviderBedrockConverse,
}

// judgeProviders are the -judge-provider values the evals judge can build
// today (evals.NewProviderJudge: the Claude providers). A judge on another
// provider needs the role client internal/agent keeps unexported.
var judgeProviders = []string{config.ModelProviderAnthropic, config.ModelProviderBedrock, config.ModelProviderVertex}

func normProvider(p string) string { return strings.ToLower(strings.TrimSpace(p)) }

func oneOf(v string, set []string) bool {
	for _, s := range set {
		if v == s {
			return true
		}
	}
	return false
}

// claudeProvider reports the providers whose model id comes from each
// request's Options.Model, as before V10.
func claudeProvider(p string) bool {
	return p == "" || p == config.ModelProviderAnthropic || p == config.ModelProviderBedrock || p == config.ModelProviderVertex
}

// withModelFlags applies -provider, -flavor and -model to cfg before the
// endpoint is built, as cmd/investigate does, so the server's start checks
// (supported list, probe, pricing) judge the model the run uses. With none
// of the new flags on a Claude provider cfg is returned unchanged: -model
// then reaches the agent through Options.Model only, exactly as before.
func withModelFlags(cfg config.Config, f flags) (config.Config, error) {
	if p := normProvider(f.provider); p != "" {
		if !oneOf(p, agentProviders) {
			return cfg, fmt.Errorf("-provider %q: want one of %s", f.provider, strings.Join(agentProviders, ", "))
		}
		cfg.ModelProvider = p
	}
	if jp := normProvider(f.judgeProvider); jp != "" && !oneOf(jp, judgeProviders) {
		if oneOf(jp, agentProviders) {
			return cfg, fmt.Errorf("-judge-provider %q: the evals judge runs on %s only for now; keep the judge on the reference's provider, or pass -judge terms or -judge-cache-only", f.judgeProvider, strings.Join(judgeProviders, ", "))
		}
		return cfg, fmt.Errorf("-judge-provider %q: want one of %s", f.judgeProvider, strings.Join(judgeProviders, ", "))
	}
	p := normProvider(cfg.ModelProvider)
	if f.flavor != "" {
		if p != config.ModelProviderOpenAI {
			return cfg, fmt.Errorf("-flavor is for provider openai only (the provider is %q)", orDefaultProvider(p))
		}
		cfg.ModelFlavor = strings.ToLower(strings.TrimSpace(f.flavor))
	}
	if f.model != "" && !claudeProvider(p) {
		cfg.Model = f.model
	}
	return cfg, nil
}

func orDefaultProvider(p string) string {
	if p == "" {
		return config.ModelProviderAnthropic
	}
	return p
}

// recordModel writes the provider, flavor and prompt hash kind into rep.
func recordModel(rep *evals.Report, cfg config.Config) {
	rep.Provider = orDefaultProvider(normProvider(cfg.ModelProvider))
	if rep.Provider == config.ModelProviderOpenAI {
		rep.Flavor = strings.ToLower(strings.TrimSpace(cfg.ModelFlavor))
		if rep.Flavor == "" {
			rep.Flavor = config.ModelProviderOpenAI
		}
	}
	rep.PromptHashKind = evals.PromptHashKind(rep.PromptHashes)
}

// gateInputs are -gate's reference and thresholds, loaded before any run.
type gateInputs struct {
	ref evals.Report
	cfg evals.GateConfig
}

// loadGate reads -gate-config and the -gate reference, and refuses a
// reference with fewer runs than the gate needs (a 1-run reference) before
// any model call.
func loadGate(f flags) (gateInputs, error) {
	c, err := evals.LoadGateConfig(f.gateConfig)
	if err != nil {
		return gateInputs{}, err
	}
	ref, err := evals.LoadReport(f.gate)
	if err != nil {
		return gateInputs{}, err
	}
	if ref.Runs < c.MinRuns {
		return gateInputs{}, fmt.Errorf("-gate %s: the reference has %d run(s) per scenario; the gate needs a %d-run reference", f.gate, ref.Runs, c.MinRuns)
	}
	if f.runs < c.MinRuns && len(f.args) == 0 {
		return gateInputs{}, fmt.Errorf("-gate: the candidate needs -runs %d or more (got %d)", c.MinRuns, f.runs)
	}
	return gateInputs{ref: ref, cfg: c}, nil
}

// runSavedGate is -gate REF.json CANDIDATE.json: no model calls.
func runSavedGate(f flags, stdout io.Writer) (int, error) {
	if len(f.args) != 1 {
		return 1, fmt.Errorf("-gate wants REF.json and one CANDIDATE.json, got %d argument(s)", len(f.args))
	}
	g, err := loadGate(f)
	if err != nil {
		return 1, err
	}
	cand, err := evals.LoadReport(f.args[0])
	if err != nil {
		return 1, err
	}
	return runGate(g.ref, cand, g.cfg, stdout)
}

// runGate prints the gate and returns its exit code (0, 4 or 5).
func runGate(ref, cand evals.Report, c evals.GateConfig, stdout io.Writer) (int, error) {
	g, err := evals.Gate(ref, cand, c)
	if err != nil {
		return 1, err
	}
	if err := g.WriteMarkdown(stdout, ref, cand); err != nil {
		return 1, err
	}
	return g.ExitCode(), nil
}

// runModelsTable is -models-table DIR.
func runModelsTable(f flags, stdout io.Writer) (int, error) {
	if len(f.args) > 0 {
		return 1, fmt.Errorf("unexpected arguments: %s", strings.Join(f.args, " "))
	}
	c, err := evals.LoadGateConfig(f.gateConfig)
	if err != nil {
		return 1, err
	}
	if err := evals.WriteModelsTable(stdout, f.modelsTable, c); err != nil {
		return 1, err
	}
	return 0, nil
}

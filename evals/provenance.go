package evals

import (
	"context"
	"os/exec"
	"runtime/debug"
	"strings"
	"time"
)

// Provenance is what produced a run. Every result carries it, so a number in
// a report can always be traced back to the code, the model and the prompt
// that made it, even after results from several reports are mixed.
type Provenance struct {
	// Commit is the source revision ("<sha>" or "<sha>-dirty"); "unknown"
	// when it could not be found.
	Commit string `json:"commit"`
	// Model is the configured model (agent.Options.Model; empty = the
	// agent's default).
	Model string `json:"model"`
	// ModelsUsed are the models the agent's requests named, in first-use
	// order, as the model tap saw them (the triage model too). Empty when
	// the run was not tapped.
	ModelsUsed []string `json:"models_used,omitempty"`
	// PromptHash is the hash of the system prompt and tool definitions the
	// model was sent (PromptHash in modeltap.go), or "untapped" when the
	// requests did not go through a tap (a scripted runner, Bedrock,
	// Vertex), or "none" when a tapped run made no model request.
	PromptHash string `json:"prompt_hash"`
	// PromptHashes is set only when one run saw more than one prompt hash.
	PromptHashes []string `json:"prompt_hashes,omitempty"`
	// Entry is how the room opened: "alert" or "watcher".
	Entry string `json:"entry"`
}

// Prompt-hash values that are not hashes.
const (
	promptUntapped = "untapped"
	promptNone     = "none"
)

// provenanceFor fills a run's provenance.
func provenanceFor(cfg Config, sc Scenario, roomID string) Provenance {
	p := Provenance{Commit: cfg.Commit, Model: cfg.Options.Model, Entry: sc.entry(), PromptHash: promptUntapped}
	if p.Commit == "" {
		p.Commit = "unknown"
	}
	if cfg.Tap == nil {
		return p
	}
	seen := cfg.Tap.Room(roomID)
	p.ModelsUsed = seen.Models
	switch len(seen.PromptHashes) {
	case 0:
		p.PromptHash = promptNone
	case 1:
		p.PromptHash = seen.PromptHashes[0]
	default:
		p.PromptHash = seen.PromptHashes[0]
		p.PromptHashes = seen.PromptHashes
	}
	return p
}

// SourceCommit returns the revision the running binary was built from: the
// VCS stamp of a go build, else (go run and go test do not stamp) what git
// says about the working directory, with "-dirty" when it has changes. It
// returns "unknown" when neither works.
func SourceCommit() string {
	if bi, ok := debug.ReadBuildInfo(); ok {
		var rev string
		var dirty bool
		for _, s := range bi.Settings {
			switch s.Key {
			case "vcs.revision":
				rev = s.Value
			case "vcs.modified":
				dirty = s.Value == "true"
			}
		}
		if rev != "" {
			if dirty {
				rev += "-dirty"
			}
			return rev
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	out, err := exec.CommandContext(ctx, "git", "rev-parse", "HEAD").Output()
	if err != nil {
		return "unknown"
	}
	rev := strings.TrimSpace(string(out))
	if st, err := exec.CommandContext(ctx, "git", "status", "--porcelain", "--untracked-files=no").Output(); err == nil && len(strings.TrimSpace(string(st))) > 0 {
		rev += "-dirty"
	}
	return rev
}

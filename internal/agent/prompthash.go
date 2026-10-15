package agent

import (
	"crypto/sha256"
	"encoding/hex"
)

// PromptHash returns the hex SHA-256 of the system prompt text only. It is
// not evals.PromptHash, which also hashes the tool definitions as sent and
// uses a different format ("sha256:" plus a short digest), so the two values
// never compare. The agent package must not import evals, hence the separate
// definition. cmd/investigate recordings store it as system_prompt_sha256.
func PromptHash() string {
	sum := sha256.Sum256([]byte(systemPrompt()))
	return hex.EncodeToString(sum[:])
}

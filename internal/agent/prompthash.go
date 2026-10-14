package agent

import (
	"crypto/sha256"
	"encoding/hex"
)

// PromptHash returns the hex SHA-256 of the fixed system prompt text. It is
// the one definition of "prompt hash": cmd/investigate recordings and eval
// reports record this same value, so runs compare by (commit, model, prompt
// hash). Tool definitions are not part of it; they are covered by the commit.
func PromptHash() string {
	sum := sha256.Sum256([]byte(systemPrompt()))
	return hex.EncodeToString(sum[:])
}

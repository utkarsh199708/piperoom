package agent

import (
	"crypto/sha256"
	"encoding/hex"
)

// PromptHash returns the hex SHA-256 of the fixed system prompt. Recordings of
// a run carry it so a score can be tied to the exact instructions the model
// had. Per-run text (alert, tool list) is not part of it.
func PromptHash() string {
	sum := sha256.Sum256([]byte(systemPrompt()))
	return hex.EncodeToString(sum[:])
}

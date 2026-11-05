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
// It is the CDC prompt's hash; PromptHashFor gives another kind's.
func PromptHash() string {
	return promptHash(systemPrompt())
}

// PromptHashFor is PromptHash for the system prompt of a room of pipeline
// kind ("" = cdc). An unknown kind is an error, as in RoomKind.
func PromptHashFor(kind string) (string, error) {
	k, err := RoomKind(kind)
	if err != nil {
		return "", err
	}
	return promptHash(systemPromptFor(k)), nil
}

// PromptHashForSource is PromptHashFor for a room whose source is of
// sourceKind ("" = postgres, or mysql): a Postgres source's hash is
// PromptHashFor(kind)'s (Q8g). Unknown kinds are errors.
func PromptHashForSource(kind, sourceKind string) (string, error) {
	k, err := RoomKind(kind)
	if err != nil {
		return "", err
	}
	sk, err := SourceKind(sourceKind)
	if err != nil {
		return "", err
	}
	return promptHash(systemPromptForSource(k, sk, RoomMode{Write: true})), nil
}

func promptHash(prompt string) string {
	sum := sha256.Sum256([]byte(prompt))
	return hex.EncodeToString(sum[:])
}

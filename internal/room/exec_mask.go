package room

import "piperoom/internal/redact"

// maskExecOutput is the policy for text an approved action produced (its
// result, its error text, or a stop reason built from either): invalid bytes
// are cleaned, then secrets are masked with the shared rules in
// internal/redact. It runs before the text is stored in the step row, emitted
// as action.executed (and so broadcast) or logged. Masking comes before
// capOutput, so a cap can never cut a secret in two and leave its unmasked
// first half behind. redact.Text is idempotent, so applying it again at a
// later sink (which the sinks do, as defence in depth) changes nothing.
func maskExecOutput(s string) string {
	out, _ := redact.Text(sanitizeAgentText(s))
	return out
}

// maskExecErr is maskExecOutput for an error about to be logged: an error
// wrapped around action output must not carry it into slog unmasked.
func maskExecErr(err error) string {
	if err == nil {
		return ""
	}
	return maskExecOutput(err.Error())
}

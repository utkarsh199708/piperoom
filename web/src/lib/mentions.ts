// Must match the server rule in internal/room/service_steer.go:
//   (?i)(^|\W)@agent\b
// "@agent" as a whole word, any case. "@agentless" and "mail@agent" do not match.
//
// Keep this regex without the `u` flag. Without `u`, \w, \W and \b are ASCII-only,
// which is what Go's RE2 does too, so both sides agree on text such as "@agenté".
// Adding `u` (or `v`) would make \w and \b Unicode-aware here and drift from the server.
// Go's (?i) also folds U+017F (long s) and U+212A (Kelvin sign) into \w, so the leading
// class excludes them explicitly; JS /i without `u` does not do that on its own.
const AGENT_MENTION = /(^|[^0-9A-Za-z_ſK])@agent\b/i

// mentionsAgent reports whether a comment addresses the agent. It must give the
// same answer as the server, because the server decides what wakes the agent.
export function mentionsAgent(text: string): boolean {
  return AGENT_MENTION.test(text)
}

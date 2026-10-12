// Must match the server rule in internal/room/service_steer.go:
//   (?i)(^|\W)@agent\b
// "@agent" as a whole word, any case. "@agentless" and "mail@agent" do not match.
//
// Keep this regex without the `u` flag. Without `u`, \w, \W and \b are ASCII-only,
// which is what Go's RE2 does too, so both sides agree on text such as "@agenté".
// Adding `u` (or `v`) would make \w and \b Unicode-aware here and drift from the server.
const AGENT_MENTION = /(^|\W)@agent\b/i

// mentionsAgent reports whether a comment addresses the agent. It must give the
// same answer as the server, because the server decides what wakes the agent.
export function mentionsAgent(text: string): boolean {
  return AGENT_MENTION.test(text)
}

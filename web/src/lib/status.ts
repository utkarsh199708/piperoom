// Server status words ("waiting_approval") are identifiers, not text for people.
const WORDS: Record<string, string> = {
  waiting_approval: 'waiting for approval',
  stopped_here: 'stopped here',
  not_seen: 'not seen',
}

/** "waiting_approval" → "waiting for approval"; an unknown word loses its underscores. */
export function humanStatus(state: string): string {
  return WORDS[state] ?? state.replace(/_/g, ' ')
}

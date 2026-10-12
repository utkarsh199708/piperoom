import { describe, expect, it } from 'vitest'
import { mentionsAgent } from './mentions'

// Cases mirror the server test expectations for internal/room/service_steer.go.
describe('mentionsAgent', () => {
  it.each([
    '@agent',
    '@Agent please',
    'hi @AGENT,',
    '(@agent)',
    'check this\n@agent',
    '@agent-ops', // "-" is not a word character, so the mention ends at "t"
    '@agenté', // ASCII \b: "é" is not a word character here, same as the Go regex
    '@agentK', // Kelvin sign after the word: trailing \b is ASCII, so it still matches
    '@agentſ', // long s after the word: same
  ])('matches %j', (text) => {
    expect(mentionsAgent(text)).toBe(true)
  })

  // Go's (?i) folds U+212A (Kelvin sign) and U+017F (long s) into \w, so they are not
  // "non-word" characters before "@agent" on the server.
  it.each([
    'mail@agent',
    '@agentless',
    '@agent_x',
    '',
    'agent',
    'hi @bot',
    'K@agent', // Kelvin sign + "@agent": Go does not match
    'ſ@agent', // long s + "@agent": Go does not match
  ])('does not match %j', (text) => {
    expect(mentionsAgent(text)).toBe(false)
  })

  it('gives the same answer on repeated calls (no sticky regex state)', () => {
    expect(mentionsAgent('@agent')).toBe(true)
    expect(mentionsAgent('@agent')).toBe(true)
  })
})

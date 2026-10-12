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
  ])('matches %j', (text) => {
    expect(mentionsAgent(text)).toBe(true)
  })

  it.each(['@agentless', 'mail@agent', '@agent_x', '', 'agent', 'hi @bot'])('does not match %j', (text) => {
    expect(mentionsAgent(text)).toBe(false)
  })

  it('gives the same answer on repeated calls (no sticky regex state)', () => {
    expect(mentionsAgent('@agent')).toBe(true)
    expect(mentionsAgent('@agent')).toBe(true)
  })
})

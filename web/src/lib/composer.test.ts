import { describe, expect, it } from 'vitest'
import { planComposer, quickSteers } from './composer'
import { emptyRoomState } from '../state/types'
import type { ProposalState, RoomState } from '../state/types'

describe('planComposer', () => {
  it.each([
    ['?why is task 0 failing', 'driver', 'ask', 'steer', 'Question: why is task 0 failing'],
    ['? why', 'suggester', 'ask', 'comment', '@agent why'],
    ['@priya look at this', 'driver', 'mention', 'comment', '@priya look at this'],
    ['@agent hi', 'suggester', 'mention', 'comment', '@agent hi'],
    ['/restart it', 'driver', 'steer', 'steer', 'restart it'],
    ['/restart it', 'suggester', 'suggest', 'steer', 'restart it'],
    ['  check lag', 'driver', 'steer', 'steer', 'check lag'],
    ['check lag', 'suggester', 'suggest', 'steer', 'check lag'],
  ] as const)('%s as %s', (raw, role, mode, send, text) => {
    expect(planComposer(raw, role, 'Arjun')).toMatchObject({ mode, send, text })
  })

  it('words the hint by role and driver', () => {
    expect(planComposer('x', 'suggester', 'Arjun').hint).toBe('Suggesting to Arjun · they can forward it')
    expect(planComposer('x', 'suggester', null).hint).toBe('Suggesting · queued until someone drives')
    expect(planComposer('x', 'driver', 'Arjun', '⌘').hint).toBe('Steering the agent · ⌘⏎ to send')
    expect(planComposer('?x', 'driver', 'Arjun').hint).toBe('Asking the agent · it answers in the stream')
    expect(planComposer('@a', 'driver', null).hint).toBe('Commenting · @agent also wakes the agent')
  })
})

const proposal = (over: Partial<ProposalState>): ProposalState =>
  ({ proposal_id: 'p', title: 't', steps: [{}, {}], status: 'open', executed: [], ...over }) as ProposalState

describe('quickSteers', () => {
  const base = (): RoomState => emptyRoomState('r1')
  it('suggests first reads with no steps', () => {
    expect(quickSteers(base())).toEqual([
      'Check the connector status',
      'Check consumer lag on the sink group',
      'Check the newest warehouse row',
    ])
  })
  it('suggests confirming a medium or high hypothesis with no open proposal', () => {
    const s = { ...base(), steps: [{ id: 's', title: 't', tool_calls: [], messages: [] }], hypothesis: { text: 'h', based_on_steps: [], confidence: 'high', confirmed: false } }
    expect(quickSteers(s)).toEqual(['Confirm the hypothesis with one more read', 'Propose a fix'])
    expect(quickSteers({ ...s, proposals: [proposal({})] })).toEqual([])
  })
  it('suggests a re-plan after a rejection and a verify after execution', () => {
    const steps = [{ id: 's', title: 't', tool_calls: [], messages: [] }]
    expect(quickSteers({ ...base(), steps, proposals: [proposal({ status: 'rejected' })] })).toEqual(['Re-plan after the rejection'])
    const done = proposal({ status: 'approved', executed: [{}, {}] as ProposalState['executed'] })
    expect(quickSteers({ ...base(), steps, proposals: [done] })).toEqual(['Verify the fix end to end'])
  })
})

import { describe, expect, it } from 'vitest'
import { applyEvent } from './reducer'
import { describeEvent } from './describe'
import { fromSnapshot } from './types'
import type { Event } from '../api/types'
import { fixprApproved, fixprCreated, fixprExecuted, fixprPreviewed, fixprSnapshot, prUpdated } from '../fixtures/fixpr'

// Q7: the log says which PR a step opened and how it moved; the web folds it per proposal and step.

const room = (...events: Event[]) => events.reduce(applyEvent, fromSnapshot(fixprSnapshot))

describe('Q7 fix PR fold', () => {
  it('keeps the preview diff and opens the PR from action.executed.pull_request', () => {
    const s = room(fixprCreated, fixprPreviewed, fixprApproved, fixprExecuted)
    const p = s.proposals[0]
    expect(p.preview?.steps[0].preview?.diff?.files).toHaveLength(2)
    expect(p.pull_requests?.[0]).toMatchObject({
      repo: 'connect-config',
      number: 12,
      state: 'open',
      head_changed: false,
      head_sha: 'c0ffee0123456789',
      updated_at: fixprExecuted.at,
    })
    expect(p.pull_requests?.[0].checks).toBeUndefined()
  })

  it('the newest pull_request.updated wins, merge and head_changed included', () => {
    const s = room(
      fixprCreated,
      fixprExecuted,
      prUpdated(5, { checks: { state: 'failure', total: 3, failed: ['lint'] } }),
      prUpdated(6, { state: 'merged', head_changed: true, merge_sha: 'abc', merged_at: '2026-10-22T11:00:00Z', checks: { state: 'success', total: 3 } }),
    )
    const pr = s.proposals[0].pull_requests?.[0]
    expect(pr).toMatchObject({ state: 'merged', head_changed: true, merge_sha: 'abc', merged_at: '2026-10-22T11:00:00Z' })
    expect(pr?.checks).toEqual({ state: 'success', total: 3 })
  })

  it('a proposal without a PR step folds as before', () => {
    const plain: Event = { ...fixprExecuted, payload: { proposal_id: 'p_pr', step_index: 0, ok: true, output: 'restarted' } }
    const s = room(fixprCreated, plain)
    expect(s.proposals[0]).not.toHaveProperty('pull_requests')
  })

  it('an update for an unknown proposal changes nothing', () => {
    const s = room(fixprCreated, { ...prUpdated(5, {}), payload: { ...(prUpdated(5, {}).payload as object), proposal_id: 'p_other' } })
    expect(s.proposals[0]).not.toHaveProperty('pull_requests')
  })

  it('describes the PR in the activity feed in words', () => {
    expect(describeEvent(fixprExecuted).text).toBe('Step 1 opened pull request #12 in connect-config')
    expect(describeEvent(prUpdated(5, { state: 'closed', checks: { state: 'none', total: 0 } })).text).toBe(
      'Pull request #12 in connect-config: closed without merge · no checks',
    )
  })
})

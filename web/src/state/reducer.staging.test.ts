import { describe, expect, it } from 'vitest'
import { applyEvent, applyModeRefresh } from './reducer'
import { describeEvent } from './describe'
import { fromSnapshot } from './types'
import {
  STAGING_HASH,
  stagedAllHealthy,
  stagingCreated,
  stagingProposal,
  stagingSnapshot,
  stagingStaged,
  stagingStarted,
} from '../fixtures/staging'
import type { Event } from '../api/types'

// X8: the snapshot says what can be staged and whether a stage is owed; the log says what ran.

const loaded = () => [stagingCreated].reduce(applyEvent, fromSnapshot(stagingSnapshot))

describe('X8 snapshot fields', () => {
  it('read mode.staging and seed stageable and staging_required when the proposal replays', () => {
    const s = loaded()
    expect(s.mode?.staging).toBe(true)
    expect(s.proposals[0].stageable).toEqual([0, 2])
    expect(s.proposals[0].staging_required).toBe(true)
    expect(s.proposals[0].stage_running).toBeUndefined()
    expect(s.proposals[0].staged).toBeUndefined()
  })
  it('a snapshot from before X8c leaves the proposal as it was', () => {
    const plain = { ...stagingProposal }
    delete plain.stageable
    delete plain.staging_required
    const old = { ...stagingSnapshot, mode: { write: true, solo: false, auto_approve: [] }, open_proposals: [plain] }
    const s = applyEvent(fromSnapshot(old), stagingCreated)
    expect(s.snapshot_staging).toBeUndefined()
    expect(s.proposals[0]).not.toHaveProperty('stageable')
    expect(s.proposals[0]).not.toHaveProperty('staging_required')
  })
  it('a mode refresh redoes both, and drops the requirement when the server no longer states it', () => {
    let s = loaded()
    const refreshed = { ...stagingSnapshot, open_proposals: [{ ...stagingProposal, stageable: [0], staging_required: false }] }
    s = applyModeRefresh(s, refreshed)
    expect(s.proposals[0].stageable).toEqual([0])
    expect(s.proposals[0].staging_required).toBeUndefined()
    s = applyModeRefresh(s, { ...stagingSnapshot, mode: { write: true, solo: false, auto_approve: [] }, open_proposals: [] })
    expect(s.snapshot_staging).toBeUndefined()
    expect(s.proposals[0].stageable).toEqual([0]) // a proposal the snapshot does not list keeps what it had
  })
})

describe('X8 activity lines', () => {
  it('say who started a stage and how it came out, without ids', () => {
    expect(describeEvent(stagingStarted).text).toBe('Arjun is trying the fix in staging')
    expect(describeEvent(stagingStaged).text).toBe('Staging result: not applied')
    expect(describeEvent({ ...stagingStaged, payload: stagedAllHealthy }).text).toBe('Staging result: healthy')
  })
})

describe('X8 events', () => {
  it('stage_started shows a running stage and names who started it', () => {
    const s = applyEvent(loaded(), stagingStarted)
    expect(s.proposals[0].stage_running).toMatchObject({
      by: { id: 'arjun' },
      expires_at: '2026-10-22T10:03:12Z',
      steps: [0, 2],
      params_hash: STAGING_HASH,
    })
  })
  it('stage_started fills stageable when the snapshot did not say', () => {
    const rest = { ...stagingProposal }
    delete rest.stageable
    delete rest.staging_required
    const s = applyEvent(applyEvent(fromSnapshot({ ...stagingSnapshot, open_proposals: [rest] }), stagingCreated), stagingStarted)
    expect(s.proposals[0].stageable).toEqual([0, 2])
  })
  it('staged ends the running stage and keeps the newest payload', () => {
    const s = [stagingStarted, stagingStaged].reduce(applyEvent, loaded())
    expect(s.proposals[0].stage_running).toBeUndefined()
    expect(s.proposals[0].staged?.steps).toHaveLength(2)
    const again: Event = { ...stagingStaged, seq: 4, payload: stagedAllHealthy }
    expect(applyEvent(s, again).proposals[0].staged?.steps[1].result?.applied).toBe(true)
  })
  it('a stage with an unhealthy step leaves "staging required" to the server', () => {
    const s = [stagingStarted, stagingStaged].reduce(applyEvent, loaded())
    expect(s.proposals[0].staging_required).toBe(true)
  })
  it('a healthy stage of this very fix lifts "staging required" and the server text', () => {
    let s = loaded()
    s = { ...s, proposals: [{ ...s.proposals[0], staging_message: 'step 0 needs a stage' }] }
    s = applyEvent(s, { ...stagingStaged, payload: stagedAllHealthy })
    expect(s.proposals[0].staging_required).toBeUndefined()
    expect(s.proposals[0].staging_message).toBeUndefined()
  })
  it('a healthy stage of an older params_hash lifts nothing and is kept as stale', () => {
    const s = applyEvent(loaded(), { ...stagingStaged, payload: { ...stagedAllHealthy, params_hash: 'old' } })
    expect(s.proposals[0].staging_required).toBe(true)
    expect(s.proposals[0].staged?.params_hash).toBe('old')
  })
  it('ignores events for an unknown proposal', () => {
    const s = [stagingStarted, stagingStaged].reduce(applyEvent, fromSnapshot(stagingSnapshot))
    expect(s.proposals).toHaveLength(0)
  })
})

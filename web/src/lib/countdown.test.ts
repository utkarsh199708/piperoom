import { describe, expect, it } from 'vitest'
import { STALE_COUNTDOWN_GRACE_MS, canCancelSchedule, countdownOf, formatRemaining, policyLabel } from './countdown'
import type { CurrentUser } from '../state/permissions'
import type { ProposalState, ScheduledState } from '../state/types'

const RUN_AT = '2026-10-19T10:01:00Z'
const runAt = Date.parse(RUN_AT)
const sched: ScheduledState = { run_at: RUN_AT, cancel_until: RUN_AT, policy: 'auto_approve' }

describe('countdownOf', () => {
  it('the grace is the server one: internal/room StaleCountdownGrace, 45 s', () => {
    expect(STALE_COUNTDOWN_GRACE_MS).toBe(45_000)
  })

  it.each([
    ['60 s ahead', -60_000, 'counting', 60],
    ['41.2 s ahead rounds up', -41_200, 'counting', 42],
    ['1 ms ahead', -1, 'counting', 1],
    ['at run_at', 0, 'due', 0],
    ['exactly the grace past', STALE_COUNTDOWN_GRACE_MS, 'due', 0],
    ['1 ms past the grace', STALE_COUNTDOWN_GRACE_MS + 1, 'stale', 0],
    ['an hour past', 3_600_000, 'stale', 0],
  ] as const)('%s', (_name, offset, phase, remaining) => {
    expect(countdownOf(sched, runAt + offset)).toEqual({ phase, remaining_s: remaining })
  })

  it('a run_at that does not parse is stale: nothing honest to count down to', () => {
    expect(countdownOf({ ...sched, run_at: 'soon' }, runAt).phase).toBe('stale')
  })
})

describe('formatRemaining and policyLabel', () => {
  it.each([
    [0, '0:00'],
    [9, '0:09'],
    [42, '0:42'],
    [60, '1:00'],
    [65, '1:05'],
    [720, '12:00'],
  ])('%i s reads %s', (s, text) => {
    expect(formatRemaining(s)).toBe(text)
  })

  it('names a plain policy and nothing else', () => {
    expect(policyLabel('auto_approve')).toBe('policy auto_approve')
    expect(policyLabel('setup_local')).toBe('policy setup_local')
    expect(policyLabel('<b>x</b>')).toBe('policy')
    expect(policyLabel('')).toBe('policy')
  })
})

describe('canCancelSchedule', () => {
  const p: Pick<ProposalState, 'status' | 'required_layers' | 'scheduled'> = {
    status: 'open',
    required_layers: ['kafka-connect', 'consumer'],
    scheduled: sched,
  }
  const owner: CurrentUser = { id: 'arjun', name: 'Arjun', layers: ['consumer'] }
  const other: CurrentUser = { id: 'lee', name: 'Lee', layers: ['warehouse'] }
  const driver = { id: 'lee' }

  it('an owner of any required layer, the driver and the solo caller may cancel', () => {
    expect(canCancelSchedule(p, owner, null)).toBe(true)
    expect(canCancelSchedule(p, other, driver)).toBe(true)
    expect(canCancelSchedule(p, other, null, true)).toBe(true)
  })

  it('nobody else, never a viewer, and not without a countdown or once decided', () => {
    expect(canCancelSchedule(p, other, { id: 'sam' })).toBe(false)
    expect(canCancelSchedule(p, { ...owner, role: 'viewer' }, null)).toBe(false)
    expect(canCancelSchedule(p, { ...other, role: 'viewer' }, driver, true)).toBe(false)
    expect(canCancelSchedule({ ...p, scheduled: undefined }, owner, null)).toBe(false)
    expect(canCancelSchedule({ ...p, status: 'rejected' }, owner, null)).toBe(false)
  })
})

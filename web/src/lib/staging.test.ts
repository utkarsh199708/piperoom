import { describe, expect, it } from 'vitest'
import type { ProposalStagedPayload, StageResult } from '../api/types'
import type { ProposalState } from '../state/types'
import {
  canTryStaging,
  formatStageDuration,
  stageErrorText,
  stageIsCurrent,
  stageLiftsRequirement,
  stageOutcome,
  stageRunning,
  stagedHealthy,
  stagedVerdict,
  stagingRequired,
} from './staging'

const res = (over: Partial<StageResult> = {}): StageResult => ({ applied: true, healthy: true, duration_ms: 61_000, ...over })
const staged = (steps: ProposalStagedPayload['steps'], hash = 'h1'): ProposalStagedPayload => ({
  proposal_id: 'p1',
  params_hash: hash,
  steps,
  at: '2026-10-22T10:00:00Z',
})
const proposal = (over: Partial<ProposalState> = {}): ProposalState => ({
  proposal_id: 'p1',
  title: 't',
  steps: [],
  risk: 'low',
  rollback: '',
  required_layers: ['kafka-connect'],
  approvals: { 'kafka-connect': 'pending' },
  decisions: {},
  params_hash: 'h1',
  status: 'open',
  executed: [],
  ...over,
})

// The same table as events.StageResult.Outcome (internal/events/events.go).
describe('stageOutcome', () => {
  it.each([
    ['applied and healthy', res(), 'healthy'],
    ['applied, unhealthy', res({ healthy: false }), 'failed'],
    ['pre-check refused, nothing written', res({ applied: false, healthy: false, dry_run: { ok: false, source: 'registry', text: 'incompatible' } }), 'failed'],
    ['pre-check refused but skipped', res({ applied: false, healthy: false, dry_run: { ok: false, source: 'registry', text: '', skipped: 'no subject' } }), 'not_applied'],
    ['pre-check accepted, nothing applied', res({ applied: false, healthy: false, dry_run: { ok: true, source: 'registry', text: 'ok' } }), 'not_applied'],
    ['nothing tried', res({ applied: false, healthy: false, detail: 'no staging declared' }), 'not_applied'],
  ])('%s -> %s', (_name, r, want) => {
    expect(stageOutcome(r)).toBe(want)
  })
})

describe('stagedVerdict', () => {
  it('is healthy only when every staged step is', () => {
    expect(stagedHealthy(staged([{ step_index: 0, result: res() }, { step_index: 2, result: res() }]))).toBe(true)
    expect(stagedHealthy(staged([{ step_index: 0, result: res() }, { step_index: 2, result: res({ healthy: false }) }]))).toBe(false)
    expect(stagedHealthy(staged([]))).toBe(false)
  })
  it('ranks a failure above a missing verdict above nothing tried', () => {
    const none = { step_index: 0, result: res({ applied: false, healthy: false }) }
    const noVerdict = { step_index: 1, error: 'staging unreachable' }
    const failed = { step_index: 2, result: res({ healthy: false }) }
    expect(stagedVerdict(staged([none]))).toBe('not_applied')
    expect(stagedVerdict(staged([none, noVerdict]))).toBe('no_verdict')
    expect(stagedVerdict(staged([none, noVerdict, failed]))).toBe('failed')
    expect(stagedVerdict(staged([{ step_index: 0, error: 'timeout', result: res() }]))).toBe('no_verdict')
  })
  it('does not throw on a payload without steps', () => {
    expect(stagedVerdict({ proposal_id: 'p1', params_hash: 'h', at: '' } as unknown as ProposalStagedPayload)).toBe('not_applied')
  })
})

describe('what counts for the fix on screen', () => {
  it('a stage of another params_hash is stale and lifts nothing', () => {
    const p = proposal({ staged: staged([{ step_index: 0, result: res() }], 'old'), stageable: [0] })
    expect(stageIsCurrent(p)).toBe(false)
    expect(stageLiftsRequirement(p, p.staged as ProposalStagedPayload)).toBe(false)
  })
  it('a healthy stage of every stageable step lifts the requirement; a partial one leaves it to the server', () => {
    const p = proposal({ stageable: [0, 2], staging_required: true })
    expect(stageLiftsRequirement(p, staged([{ step_index: 0, result: res() }, { step_index: 2, result: res() }]))).toBe(true)
    expect(stageLiftsRequirement(p, staged([{ step_index: 0, result: res() }, { step_index: 2, result: res({ healthy: false }) }]))).toBe(false)
  })
  it('only an open proposal can be held', () => {
    expect(stagingRequired(proposal({ staging_required: true }))).toBe(true)
    expect(stagingRequired(proposal({ staging_required: true, status: 'rejected' }))).toBe(false)
    expect(stagingRequired(proposal())).toBe(false)
  })
})

describe('stageRunning', () => {
  const running = { by: { kind: 'human' as const, id: 'a', name: 'A' }, started_at: '2026-10-22T10:00:00Z', expires_at: '2026-10-22T10:02:40Z', steps: [0], params_hash: 'h1' }
  it('runs until expires_at, then the stage is lost', () => {
    const p = proposal({ stage_running: running })
    expect(stageRunning(p, Date.parse('2026-10-22T10:01:00Z'))).toBe(true)
    expect(stageRunning(p, Date.parse('2026-10-22T10:02:41Z'))).toBe(false)
    expect(stageRunning(proposal(), 0)).toBe(false)
  })
})

describe('canTryStaging', () => {
  const on = { stagingOn: true, isDriver: true, viewer: false, roomOpen: true }
  const p = proposal({ stageable: [0] })
  it('is the driver’s, with staging on and a stageable step', () => {
    expect(canTryStaging(p, on)).toBe(true)
    expect(canTryStaging(p, { ...on, isDriver: false })).toBe(false)
    expect(canTryStaging(p, { ...on, viewer: true })).toBe(false)
    expect(canTryStaging(p, { ...on, stagingOn: false })).toBe(false)
    expect(canTryStaging(p, { ...on, roomOpen: false })).toBe(false)
    expect(canTryStaging(proposal({ stageable: [] }), on)).toBe(false)
    expect(canTryStaging(proposal(), on)).toBe(false)
    expect(canTryStaging(proposal({ stageable: [0], status: 'rejected' }), on)).toBe(false)
  })
})

describe('texts', () => {
  it('formats durations', () => {
    expect(formatStageDuration(420)).toBe('420 ms')
    expect(formatStageDuration(12_340)).toBe('12.3 s')
    expect(formatStageDuration(61_000)).toBe('1 min 1 s')
    expect(formatStageDuration(-1)).toBe('')
  })
  it('words the documented stage errors and falls back to the server text', () => {
    expect(stageErrorText({ code: 'stage_in_progress' })).toMatch(/already running/)
    expect(stageErrorText({ code: 'rate_limited' })).toMatch(/five times/)
    expect(stageErrorText({ code: 'weird', error: 'server says no' })).toBe('server says no')
    expect(stageErrorText({})).toMatch(/did not start/)
  })
})

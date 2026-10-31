import { describe, expect, it } from 'vitest'
import { roomStatus, statusInputFromSnapshot, statusInputFromState } from './roomStatus'
import type { RoomStatusInput, StatusProposal } from './roomStatus'
import { emptyRoomState } from '../state/types'
import type { LastFixSummary, RoomSnapshot } from '../api/types'

const now = Date.parse('2026-10-14T12:00:00Z')
const base: RoomStatusInput = {
  status: 'open',
  opened_at: '2026-10-14T11:30:00Z',
  driver: { lease_until: '2026-10-14T13:00:00Z' },
  agent: { state: 'idle' },
  proposals: [],
  steps: 0,
}
const prop = (over: Partial<StatusProposal> = {}): StatusProposal => ({
  status: 'open',
  required_layers: ['a', 'b'],
  approvals: { a: 'approved', b: 'pending' },
  steps: 2,
  executed: [],
  ...over,
})
const run = (over: Partial<RoomStatusInput>) => roomStatus({ ...base, ...over }, now)
const ok = (n: number) => Array.from({ length: n }, () => ({ ok: true }))

describe('roomStatus', () => {
  it.each([
    ['closed', { status: 'closed' }, 'Closed', 'muted'],
    ['resolved', { status: 'closed', resolution: 'fixed' }, 'Resolved', 'ok'],
    ['resolved', { status: 'closed', agent: { state: 'done' } }, 'Resolved · verified', 'ok'],
    ['agent-error', { agent: { state: 'error' } }, 'Agent error', 'fail'],
    ['log-full', { agent: { state: 'log_full' } }, 'Log full · agent stopped', 'fail'],
    ['queued', { agent: { state: 'queued' } }, 'Queued · waiting for an agent slot', 'agent'],
    ['executing', { proposals: [prop({ status: 'approved', executed: ok(1) })] }, 'Executing fix · 2 of 2', 'agent'],
    [
      'fix-failed',
      { proposals: [prop({ status: 'approved', executed: [{ ok: true }, { ok: false }] })] },
      'Fix failed · step 2',
      'fail',
    ],
    ['fixed', { proposals: [prop({ status: 'approved', executed: ok(2) })] }, 'Fix ran · verifying', 'ok'],
    ['needs-approval', { proposals: [prop()] }, 'Needs approval · 30m', 'warn'],
    ['no-driver', { driver: null }, 'No driver · 30m', 'warn'],
    ['no-driver', { driver: { lease_until: '2026-10-14T11:00:00Z' } }, 'No driver · 30m', 'warn'],
    ['investigating', { agent: { state: 'working' }, steps: 3 }, 'Investigating · step 3', 'agent'],
    ['investigating', { agent: { state: 'working' } }, 'Investigating', 'agent'],
    ['idle', {}, 'Idle · waiting for a steer', 'muted'],
  ] as const)('%s', (kind, over, label, tone) => {
    expect(run(over as Partial<RoomStatusInput>)).toMatchObject({ kind, label, tone })
  })

  it('precedence: closed beats error; executing beats waiting_approval; error beats approval', () => {
    expect(run({ status: 'closed', agent: { state: 'error' } }).kind).toBe('closed')
    const approved = prop({ status: 'approved', executed: ok(1) })
    expect(run({ agent: { state: 'waiting_approval' }, proposals: [approved] }).kind).toBe('executing')
    expect(run({ agent: { state: 'error' }, proposals: [prop()] }).kind).toBe('agent-error')
    expect(run({ driver: null, proposals: [prop()] }).kind).toBe('needs-approval')
  })

  it('verified only with agent done; fixed ends when the agent is done', () => {
    expect(run({ status: 'closed', resolution: 'x' }).label).toBe('Resolved')
    const done = run({ agent: { state: 'done' }, proposals: [prop({ status: 'approved', executed: ok(2) })] })
    expect(done.kind).not.toBe('fixed')
  })

  it('snapshot and state adapters agree', () => {
    const snap: RoomSnapshot = {
      id: 'r',
      title: 't',
      severity: 'sev2',
      pipeline_id: 'p',
      status: 'open',
      opened_at: base.opened_at,
      last_seq: 1,
      driver: { id: 'k', name: 'K' },
      agent_status: { state: 'waiting_approval' },
      hypothesis: null,
      open_proposals: [
        { proposal_id: 'p1', title: 'x', steps: [], risk: 'low', rollback: '', required_layers: ['a'], approvals: { a: 'pending' } },
      ],
      pending_suggestions: [],
      members: [],
    }
    const state = {
      ...emptyRoomState('r'),
      opened_at: base.opened_at,
      driver: { id: 'k', name: 'K' },
      agent_status: { state: 'waiting_approval' },
      proposals: [
        { proposal_id: 'p1', title: 'x', steps: [], risk: 'low', rollback: '', required_layers: ['a'], approvals: { a: 'pending' as const }, decisions: {}, params_hash: '', status: 'open' as const, executed: [] },
      ],
    }
    expect(roomStatus(statusInputFromSnapshot(snap), now)).toEqual(roomStatus(statusInputFromState(state), now))
    expect(roomStatus(statusInputFromSnapshot(snap), now).kind).toBe('needs-approval')
  })
})

describe('roomStatus after the verdict', () => {
  const ran = (verification?: StatusProposal['verification']) =>
    run({ agent: { state: 'working' }, proposals: [prop({ status: 'approved', executed: ok(2), verification })] })

  // A fix that ran and was verified NOT recovered must not keep a green "verifying" header.
  it.each([
    [undefined, 'fixed', 'Fix ran · verifying', 'ok'],
    [{ recovered: true, verdict: 'recovered: all good' }, 'fixed', 'Fix ran · recovered', 'ok'],
    [{ recovered: false, verdict: 'NOT recovered: a layer check did not pass.' }, 'fix-failed', 'Fix ran · not recovered', 'fail'],
    [{ recovered: false, verdict: 'not verified: a step outcome is unknown' }, 'fixed', 'Fix ran · not verified', 'warn'],
    [{ recovered: false, verdict: 'inconclusive: no source changes observed' }, 'fixed', 'Fix ran · inconclusive', 'warn'],
  ])('%j → %s %j', (verification, kind, label, tone) => {
    expect(ran(verification)).toMatchObject({ kind, label, tone })
  })
})


describe('roomStatus with a newer proposal after a finished fix', () => {
  const recovered = { recovered: true, verdict: 'recovered: all good' }
  const fixRan = prop({ status: 'approved', executed: ok(2), verification: recovered })

  it('shows needs-approval, not "Fix ran", when a newer proposal waits for approval', () => {
    const s = run({ agent: { state: 'working' }, proposals: [fixRan, prop()] })
    expect(s).toMatchObject({ kind: 'needs-approval', tone: 'warn' })
    expect(s.label).toMatch(/^Needs approval/)
  })

  it('shows needs-approval over a failed earlier fix too', () => {
    const failed = prop({ status: 'approved', steps: 3, executed: [{ ok: false }] })
    expect(run({ proposals: [failed, prop()] }).kind).toBe('needs-approval')
  })

  it('still reads "Fix ran · recovered" when it is the only proposal', () => {
    expect(run({ proposals: [fixRan] })).toMatchObject({ kind: 'fixed', label: 'Fix ran · recovered' })
  })

  it.each([
    ['rejected', prop({ status: 'rejected' })],
    ['needs no approval', prop({ approvals: { a: 'approved', b: 'approved' } })],
  ])('keeps the earlier fix result when the newer proposal is %s', (_n, newer) => {
    expect(run({ proposals: [fixRan, newer] })).toMatchObject({ kind: 'fixed', label: 'Fix ran · recovered' })
  })

  it('ignores an older open proposal; the fix is the newer one', () => {
    expect(run({ proposals: [prop(), fixRan] }).label).toBe('Fix ran · recovered')
  })
})

describe('statusInputFromSnapshot with last_fix (KAN-594)', () => {
  const snap = (over: Partial<RoomSnapshot> = {}): RoomSnapshot => ({
    id: 'r',
    title: 't',
    severity: 'sev2',
    pipeline_id: 'p',
    status: 'open',
    opened_at: base.opened_at,
    last_seq: 1,
    driver: { id: 'k', name: 'K', lease_until: '2026-10-14T13:00:00Z' },
    agent_status: { state: 'idle' },
    hypothesis: null,
    open_proposals: [],
    pending_suggestions: [],
    members: [],
    ...over,
  })
  const fix = (over: Partial<LastFixSummary> = {}): LastFixSummary => ({ proposal_id: 'p5', seq: 5, steps: 2, failed_step: 0, ...over })
  const open = (seq: number, approvals: Record<string, 'approved' | 'pending'>) => ({
    proposal_id: `p${seq}`,
    title: 'x',
    steps: [],
    risk: 'low',
    rollback: '',
    required_layers: Object.keys(approvals),
    approvals,
    seq,
  })
  const cell = (s: RoomSnapshot) => roomStatus(statusInputFromSnapshot(s), now)

  it.each([
    [undefined, 'fixed', 'Fix ran · verifying', 'ok'],
    [{ recovered: true, verdict: 'recovered: all good' }, 'fixed', 'Fix ran · recovered', 'ok'],
    [{ recovered: false, verdict: 'NOT recovered: a layer check did not pass.' }, 'fix-failed', 'Fix ran · not recovered', 'fail'],
    [{ recovered: false, verdict: 'not verified: a step outcome is unknown' }, 'fixed', 'Fix ran · not verified', 'warn'],
    [{ recovered: false, verdict: 'inconclusive: no source changes observed' }, 'fixed', 'Fix ran · inconclusive', 'warn'],
  ])('a finished fix with verdict %j reads %s %j', (verification, kind, label, tone) => {
    expect(cell(snap({ last_fix: fix({ verification }) }))).toMatchObject({ kind, label, tone })
  })

  it('names the failed step', () => {
    expect(cell(snap({ last_fix: fix({ failed_step: 2 }) }))).toMatchObject({ kind: 'fix-failed', label: 'Fix failed · step 2', step: 2 })
    expect(cell(snap({ last_fix: fix({ failed_step: 1 }) })).label).toBe('Fix failed · step 1')
  })

  it('reads as finished for a proposal with no steps', () => {
    expect(cell(snap({ last_fix: fix({ steps: 0 }) })).label).toBe('Fix ran · verifying')
  })

  it('gives way to a newer proposal that waits for approval', () => {
    const verification = { recovered: true, verdict: 'recovered: all good' }
    const s = cell(snap({ last_fix: fix({ verification }), open_proposals: [open(9, { a: 'pending' })] }))
    expect(s).toMatchObject({ kind: 'needs-approval', tone: 'warn' })
    expect(s.label).toMatch(/^Needs approval · /)
    const failed = cell(snap({ last_fix: fix({ failed_step: 1 }), open_proposals: [open(9, { a: 'pending' })] }))
    expect(failed.kind).toBe('needs-approval')
  })

  it('ignores an older proposal that still waits; the fix is the newer one', () => {
    const verification = { recovered: true, verdict: 'recovered: all good' }
    expect(cell(snap({ last_fix: fix({ verification }), open_proposals: [open(3, { a: 'pending' })] })).label).toBe('Fix ran · recovered')
  })

  it('does not call the room fixed while a newer proposal, every layer approved, runs', () => {
    const verification = { recovered: true, verdict: 'recovered: all good' }
    expect(cell(snap({ last_fix: fix({ verification }), open_proposals: [open(9, { a: 'approved' })] })).label).toBe('Idle · waiting for a steer')
  })

  it('reads as before without last_fix', () => {
    expect(cell(snap()).label).toBe('Idle · waiting for a steer')
    expect(cell(snap({ open_proposals: [open(3, { a: 'pending' })] })).kind).toBe('needs-approval')
  })

  it('agrees with the title row', () => {
    const verification = { recovered: false, verdict: 'NOT recovered: a layer check did not pass.' }
    const state = {
      ...emptyRoomState('r'),
      opened_at: base.opened_at,
      driver: { id: 'k', name: 'K', lease_until: '2026-10-14T13:00:00Z' },
      agent_status: { state: 'idle' },
      proposals: [
        {
          proposal_id: 'p5',
          title: 'x',
          steps: [
            { action: 'a', layer: 'a', params: {}, description: '' },
            { action: 'b', layer: 'a', params: {}, description: '' },
          ],
          risk: 'low',
          rollback: '',
          required_layers: ['a'],
          approvals: { a: 'approved' as const },
          decisions: {},
          params_hash: '',
          status: 'approved' as const,
          executed: [
            { proposal_id: 'p5', step_index: 0, ok: true, output: '' },
            { proposal_id: 'p5', step_index: 1, ok: true, output: '' },
          ],
          verification: { proposal_id: 'p5', checks: [], ...verification },
        },
      ],
    }
    expect(cell(snap({ last_fix: fix({ verification }) }))).toEqual(roomStatus(statusInputFromState(state), now))
  })
})

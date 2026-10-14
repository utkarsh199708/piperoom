import { describe, expect, it } from 'vitest'
import { roomStatus, statusInputFromSnapshot, statusInputFromState } from './roomStatus'
import type { RoomStatusInput, StatusProposal } from './roomStatus'
import { emptyRoomState } from '../state/types'
import type { RoomSnapshot } from '../api/types'

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

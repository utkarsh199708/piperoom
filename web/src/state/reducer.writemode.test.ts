import { describe, expect, it } from 'vitest'
import type { Event, RoomMode, RoomSnapshot } from '../api/types'
import { applyEvent, applyModeRefresh } from './reducer'
import { fromSnapshot } from './types'
import type { RoomState } from './types'
import { waitingForWriteMode } from '../lib/writeMode'

// P1a: with write mode off the last approval records as usual and no step runs. The room GET says
// so in `mode` (and, for a proposal already blocked, `open_proposals[].blocked`); the socket carries
// the events only, so the reducer has to work out `blocked` for approvals that arrive live.

const human = { kind: 'human' as const, id: 'arjun', name: 'Arjun' }
const agent = { kind: 'agent' as const, id: 'agent', name: 'Agent' }

function ev(seq: number, type: Event['type'], payload: unknown, actor: Event['actor'] = agent): Event {
  return { room_id: 'r1', seq, type, actor, payload, at: `2026-10-11T10:00:${String(seq).padStart(2, '0')}Z` }
}

const created = {
  proposal_id: 'p1',
  title: 'Restart the task',
  steps: [{ action: 'restart_connector_task', layer: 'kafka-connect', params: {}, description: '' }],
  risk: 'low',
  rollback: '',
  required_layers: ['schema-registry', 'kafka-connect'],
}

const modeOff: RoomMode = { write: false, solo: false, auto_approve: [] }

function snapshot(over: Partial<RoomSnapshot> = {}): RoomSnapshot {
  return {
    id: 'r1',
    title: 'T',
    severity: 'sev2',
    pipeline_id: 'orders',
    status: 'open',
    opened_at: '2026-10-11T10:00:00Z',
    last_seq: 5,
    driver: null,
    agent_status: { state: 'idle' },
    hypothesis: null,
    open_proposals: [],
    pending_suggestions: [],
    members: [],
    ...over,
  }
}

function approveAll(state: RoomState, from = 2): RoomState {
  let s = state
  s = applyEvent(s, ev(from, 'approval.given', { proposal_id: 'p1', layer: 'schema-registry' }, human))
  s = applyEvent(s, ev(from + 1, 'approval.given', { proposal_id: 'p1', layer: 'kafka-connect' }, human))
  return s
}

describe('fromSnapshot — write mode', () => {
  it('keeps mode and the blocked proposals of the room GET', () => {
    const s = fromSnapshot(
      snapshot({
        mode: modeOff,
        open_proposals: [
          {
            ...created,
            approvals: { 'schema-registry': 'approved', 'kafka-connect': 'approved' },
            blocked: 'write_mode_off',
          },
        ],
      }),
    )
    expect(s.mode).toEqual(modeOff)
    expect(s.snapshot_blocked).toEqual({ p1: 'write_mode_off' })
  })

  it('a snapshot without mode folds to the same state as before: no new keys', () => {
    const s = fromSnapshot(snapshot())
    expect('mode' in s).toBe(false)
    expect('snapshot_blocked' in s).toBe(false)
  })
})

describe('applyEvent — write mode off', () => {
  it('marks the proposal blocked when the last approval arrives over the socket', () => {
    const start = fromSnapshot(snapshot({ mode: modeOff }))
    let s = applyEvent(start, ev(1, 'proposal.created', created))
    expect(s.proposals[0].blocked).toBeUndefined()

    s = applyEvent(s, ev(2, 'approval.given', { proposal_id: 'p1', layer: 'schema-registry' }, human))
    expect(s.proposals[0].status).toBe('open')
    expect(s.proposals[0].blocked).toBeUndefined()

    s = applyEvent(s, ev(3, 'approval.given', { proposal_id: 'p1', layer: 'kafka-connect' }, human))
    expect(s.proposals[0].status).toBe('approved')
    expect(s.proposals[0].blocked).toBe('write_mode_off')
    expect(waitingForWriteMode(s.proposals[0])).toBe(true)
  })

  it('takes blocked from the snapshot when the replay approves a proposal it already blocked', () => {
    const start = fromSnapshot(
      snapshot({
        mode: modeOff,
        open_proposals: [
          {
            ...created,
            approvals: { 'schema-registry': 'approved', 'kafka-connect': 'approved' },
            blocked: 'write_mode_off',
          },
        ],
      }),
    )
    const s = approveAll(applyEvent(start, ev(1, 'proposal.created', created)))
    expect(waitingForWriteMode(s.proposals[0])).toBe(true)
  })

  it('an approved proposal is not waiting once a step ran', () => {
    const start = fromSnapshot(snapshot({ mode: modeOff }))
    let s = approveAll(applyEvent(start, ev(1, 'proposal.created', created)))
    s = applyEvent(s, ev(4, 'action.executed', { proposal_id: 'p1', step_index: 0, ok: true, output: 'done' }))
    expect(waitingForWriteMode(s.proposals[0])).toBe(false)
  })

  it('a rejection is never blocked', () => {
    const start = fromSnapshot(snapshot({ mode: modeOff }))
    let s = applyEvent(start, ev(1, 'proposal.created', created))
    s = applyEvent(s, ev(2, 'approval.rejected', { proposal_id: 'p1', layer: 'schema-registry', reason: 'no' }, human))
    expect(s.proposals[0].blocked).toBeUndefined()
    expect(waitingForWriteMode(s.proposals[0])).toBe(false)
  })
})

describe('applyEvent — write mode on or unknown', () => {
  it.each([
    ['mode.write true', snapshot({ mode: { write: true, solo: false, auto_approve: [] } })],
    ['no mode (a server from before P1a)', snapshot()],
  ])('never marks a proposal blocked with %s', (_name, snap) => {
    const s = approveAll(applyEvent(fromSnapshot(snap), ev(1, 'proposal.created', created)))
    expect(s.proposals[0].status).toBe('approved')
    expect(s.proposals[0].blocked).toBeUndefined()
    expect('blocked' in s.proposals[0]).toBe(false)
    expect(waitingForWriteMode(s.proposals[0])).toBe(false)
  })
})

describe('applyModeRefresh — the room GET again after a reconnect', () => {
  const on: RoomMode = { write: true, solo: false, auto_approve: [] }

  it('on to off: marks an approved proposal that ran nothing, and leaves an open one alone', () => {
    let s = applyEvent(fromSnapshot(snapshot({ mode: on })), ev(1, 'proposal.created', created))
    s = applyEvent(s, ev(2, 'proposal.created', { ...created, proposal_id: 'p2' }))
    s = approveAll(s, 3)
    expect(s.proposals[0].blocked).toBeUndefined()

    const next = applyModeRefresh(s, snapshot({ mode: modeOff }))
    expect(next.mode).toEqual(modeOff)
    expect(next.proposals[0].blocked).toBe('write_mode_off')
    expect(next.proposals[1].blocked).toBeUndefined() // p2 was never approved
    expect(waitingForWriteMode(next.proposals[0])).toBe(true)
  })

  it('off to on: drops the mark, the snapshot copy and the mode, and keeps the decision', () => {
    const start = fromSnapshot(
      snapshot({
        mode: modeOff,
        open_proposals: [
          { ...created, approvals: { 'schema-registry': 'approved', 'kafka-connect': 'approved' }, blocked: 'write_mode_off' },
        ],
      }),
    )
    const s = approveAll(applyEvent(start, ev(1, 'proposal.created', created)))
    expect(waitingForWriteMode(s.proposals[0])).toBe(true)

    const next = applyModeRefresh(s, snapshot({ mode: on }))
    expect(next.mode).toEqual(on)
    expect(next.snapshot_blocked).toBeUndefined()
    expect('blocked' in next.proposals[0]).toBe(false)
    expect(waitingForWriteMode(next.proposals[0])).toBe(false)
    expect(next.proposals[0].status).toBe('approved')
    expect(next.proposals[0].approvals).toEqual(s.proposals[0].approvals)
  })

  it('a snapshot without mode (a server from before P1a) reads as write mode on', () => {
    const start = fromSnapshot(snapshot({ mode: modeOff }))
    const s = approveAll(applyEvent(start, ev(1, 'proposal.created', created)))
    const next = applyModeRefresh(s, snapshot())
    expect('mode' in next).toBe(false)
    expect(waitingForWriteMode(next.proposals[0])).toBe(false)
  })

  it('takes the snapshot\'s own blocked marks, and never marks a proposal that ran', () => {
    let s = applyEvent(fromSnapshot(snapshot({ mode: on })), ev(1, 'proposal.created', created))
    s = approveAll(s)
    s = applyEvent(s, ev(5, 'action.executed', { proposal_id: 'p1', step_index: 0, ok: true, output: 'done' }))
    const next = applyModeRefresh(
      s,
      snapshot({
        mode: modeOff,
        open_proposals: [{ ...created, approvals: { 'schema-registry': 'approved', 'kafka-connect': 'approved' }, blocked: 'write_mode_off' }],
      }),
    )
    expect(next.snapshot_blocked).toEqual({ p1: 'write_mode_off' })
    expect(waitingForWriteMode(next.proposals[0])).toBe(false) // a step ran: the execution card tells the story
  })

  it('changes nothing else in the room', () => {
    let s = applyEvent(fromSnapshot(snapshot({ mode: on })), ev(1, 'proposal.created', created))
    s = approveAll(s)
    const next = applyModeRefresh(s, snapshot({ mode: modeOff }))
    expect(next.last_seq).toBe(s.last_seq)
    expect(next.title).toBe(s.title)
    expect(next.steps).toBe(s.steps)
    expect(next.activity).toBe(s.activity)
    expect(next.proposals[0].decisions).toBe(s.proposals[0].decisions)
  })
})

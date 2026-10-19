import { describe, expect, it } from 'vitest'
import { decisionFor } from './decision'
import { executionPhase } from './execution'
import { agentPill } from './agentPill'
import { roomStatus, statusInputFromState } from './roomStatus'
import { WAITING_FOR_WRITE_MODE, waitingForWriteMode, writeModeOff } from './writeMode'
import { emptyRoomState } from '../state/types'
import type { ProposalState, RoomState } from '../state/types'
import type { CurrentUser } from '../state/permissions'

const arjun: CurrentUser = { id: 'arjun', name: 'Arjun', layers: ['kafka-connect'] }
const step = (layer: string, action: string) => ({ action, layer, params: {}, description: '' })
function prop(over: Partial<ProposalState> = {}): ProposalState {
  return {
    proposal_id: 'p1',
    title: 'fix',
    steps: [step('schema-registry', 'a'), step('kafka-connect', 'b')],
    risk: 'low',
    rollback: '',
    required_layers: ['schema-registry', 'kafka-connect'],
    approvals: { 'schema-registry': 'approved', 'kafka-connect': 'approved' },
    decisions: {},
    params_hash: 'h',
    status: 'approved',
    executed: [],
    ...over,
  }
}
const blocked = (over: Partial<ProposalState> = {}) => prop({ blocked: 'write_mode_off', ...over })
const room = (proposals: ProposalState[], over: Partial<RoomState> = {}): RoomState => ({
  ...emptyRoomState('r'),
  opened_at: '2026-10-11T10:00:00Z',
  driver: { id: 'k', name: 'K' },
  proposals,
  ...over,
})
const ex = (i: number, ok = true) => ({ proposal_id: 'p1', step_index: i, ok, output: '' })

describe('writeModeOff', () => {
  it('is true only for an explicit write: false', () => {
    expect(writeModeOff({ write: false, solo: false, auto_approve: [] })).toBe(true)
    expect(writeModeOff({ write: true, solo: false, auto_approve: [] })).toBe(false)
    expect(writeModeOff(undefined)).toBe(false)
    expect(writeModeOff(null)).toBe(false)
  })
})

describe('waitingForWriteMode', () => {
  it('needs an approved proposal that carries blocked and has run nothing', () => {
    expect(waitingForWriteMode(blocked())).toBe(true)
    expect(waitingForWriteMode(prop())).toBe(false)
    expect(waitingForWriteMode(blocked({ status: 'open' }))).toBe(false)
    expect(waitingForWriteMode(blocked({ executed: [ex(0)] }))).toBe(false)
  })
})

describe('executionPhase with write mode off', () => {
  it('is blocked, not running', () => {
    expect(executionPhase(blocked())).toEqual({ phase: 'blocked', step: 0, of: 2 })
    expect(executionPhase(prop()).phase).toBe('running')
  })
  it('a step that ran wins over blocked', () => {
    expect(executionPhase(blocked({ executed: [ex(0), ex(1)] })).phase).toBe('done')
    expect(executionPhase(blocked({ executed: [ex(0, false)] })).phase).toBe('failed')
  })
})

describe('decisionFor with write mode off', () => {
  it('has no "executing" bar for a blocked proposal', () => {
    expect(decisionFor(room([blocked()]), arjun)).toBeNull()
    expect(decisionFor(room([prop()]), arjun)).toMatchObject({ kind: 'executing', step: 1, total: 2 })
  })
  it('a newer proposal that needs the person still shows', () => {
    const next = prop({
      proposal_id: 'p2',
      status: 'open',
      approvals: { 'schema-registry': 'approved', 'kafka-connect': 'pending' },
    })
    expect(decisionFor(room([blocked(), next]), arjun)).toMatchObject({ kind: 'needs-you', proposal: { proposal_id: 'p2' } })
  })
})

describe('room status and agent pill with write mode off', () => {
  it('say one neutral sentence', () => {
    const state = room([blocked()], { agent_status: { state: 'waiting_approval' } })
    const status = roomStatus(statusInputFromState(state))
    expect(status).toMatchObject({ kind: 'awaiting-write-mode', label: WAITING_FOR_WRITE_MODE, tone: 'muted', dot: 'idle' })
    expect(agentPill(state)).toMatchObject({ tone: 'muted', text: WAITING_FOR_WRITE_MODE })
  })

  it('never says executing or fixed for a proposal that cannot run', () => {
    const state = room([blocked()], { agent_status: { state: 'working' } })
    expect(roomStatus(statusInputFromState(state)).kind).not.toBe('executing')
    expect(agentPill(state).text).not.toMatch(/Executing/)
  })

  it('a proposal that ran keeps its own story', () => {
    const state = room([blocked({ executed: [ex(0)] })])
    expect(roomStatus(statusInputFromState(state)).kind).toBe('executing')
  })

  it('a newer proposal takes over from the blocked one', () => {
    const next = prop({
      proposal_id: 'p2',
      status: 'open',
      approvals: { 'schema-registry': 'approved', 'kafka-connect': 'pending' },
    })
    expect(roomStatus(statusInputFromState(room([blocked(), next]))).kind).toBe('needs-approval')
  })

  it('without blocked an approved proposal reads as before', () => {
    expect(roomStatus(statusInputFromState(room([prop()]))).kind).toBe('executing')
  })
})

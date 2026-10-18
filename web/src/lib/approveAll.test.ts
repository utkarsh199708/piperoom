import { describe, expect, it } from 'vitest'
import { approveAllFor, approveAllLabel, approveAllLayers, soloAllConfirmText } from './approveAll'
import type { RoomMode } from '../api/types'
import type { CurrentUser } from '../state/permissions'
import { emptyRoomState } from '../state/types'
import type { ProposalState, RoomState } from '../state/types'

const both: CurrentUser = { id: 'sam', name: 'Sam', layers: ['schema-registry', 'kafka-connect'] }
const one: CurrentUser = { id: 'arjun', name: 'Arjun', layers: ['kafka-connect', 'kafka'] }
const none: CurrentUser = { id: 'kabir', name: 'Kabir', layers: [] }
const solo: RoomMode = { write: true, solo: true, auto_approve: [] }
const notSolo: RoomMode = { write: true, solo: false, auto_approve: [] }

function proposal(over: Partial<ProposalState> = {}): ProposalState {
  return {
    proposal_id: 'p1',
    title: 'Fix',
    steps: [],
    risk: 'low',
    rollback: '',
    required_layers: ['schema-registry', 'kafka-connect'],
    approvals: { 'schema-registry': 'pending', 'kafka-connect': 'pending' },
    decisions: {},
    params_hash: 'h',
    status: 'open',
    executed: [],
    ...over,
  }
}
const room = (p: ProposalState, over: Partial<RoomState> = {}): RoomState => ({
  ...emptyRoomState('r1'),
  pipeline_id: 'orders',
  proposals: [p],
  ...over,
})

describe('approveAllLayers', () => {
  it.each<[string, ProposalState, CurrentUser, RoomMode | undefined, string[] | null]>([
    ['owns every layer, both pending', proposal(), both, undefined, ['schema-registry', 'kafka-connect']],
    ['owns every layer, mode says not solo', proposal(), both, notSolo, ['schema-registry', 'kafka-connect']],
    ['mode.solo with no /api/me layers', proposal(), none, solo, ['schema-registry', 'kafka-connect']],
    ['owns one of two layers', proposal(), one, undefined, null],
    ['owns one of two layers, mode not solo', proposal(), one, notSolo, null],
    ['owns no layer', proposal(), none, undefined, null],
    [
      'three layers, one approved by someone else: records the two left',
      proposal({
        required_layers: ['postgres', 'schema-registry', 'kafka-connect'],
        approvals: { postgres: 'approved', 'schema-registry': 'pending', 'kafka-connect': 'pending' },
      }),
      { ...both, layers: ['postgres', 'schema-registry', 'kafka-connect'] },
      undefined,
      ['schema-registry', 'kafka-connect'],
    ],
    [
      'an approved layer the caller does not own (the server checks every layer)',
      proposal({
        required_layers: ['postgres', 'schema-registry', 'kafka-connect'],
        approvals: { postgres: 'approved', 'schema-registry': 'pending', 'kafka-connect': 'pending' },
      }),
      both,
      undefined,
      null,
    ],
    [
      'one pending layer the row can decide: the row keeps it',
      proposal({ approvals: { 'schema-registry': 'approved', 'kafka-connect': 'pending' } }),
      both,
      undefined,
      null,
    ],
    [
      'a one-layer proposal the caller owns: the row keeps it',
      proposal({ required_layers: ['kafka-connect'], approvals: { 'kafka-connect': 'pending' } }),
      one,
      undefined,
      null,
    ],
    [
      'one pending layer the row cannot offer (mode.solo only)',
      proposal({ approvals: { 'schema-registry': 'approved', 'kafka-connect': 'pending' } }),
      none,
      solo,
      ['kafka-connect'],
    ],
    ['every layer approved', proposal({ approvals: { 'schema-registry': 'approved', 'kafka-connect': 'approved' }, status: 'approved' }), both, solo, null],
    ['rejected', proposal({ approvals: { 'schema-registry': 'rejected', 'kafka-connect': 'pending' }, status: 'rejected' }), both, solo, null],
    ['a rejection on an open row (a stale status) still blocks', proposal({ approvals: { 'schema-registry': 'rejected', 'kafka-connect': 'pending' } }), both, solo, null],
    ['a viewer, even with the layers and mode.solo', proposal(), { ...both, role: 'viewer' }, solo, null],
    ['no required layers', proposal({ required_layers: [], approvals: {} }), both, solo, null],
  ])('%s', (_name, p, user, mode, want) => {
    expect(approveAllLayers(p, user, mode)).toEqual(want)
  })

  it('a layer missing from approvals counts as pending', () => {
    expect(approveAllLayers(proposal({ approvals: {} }), both, undefined)).toEqual(['schema-registry', 'kafka-connect'])
  })
})

describe('approveAllFor', () => {
  it('reads mode.solo from the room', () => {
    const p = proposal()
    expect(approveAllFor(room(p, { mode: solo }), p, none)).toEqual(['schema-registry', 'kafka-connect'])
    expect(approveAllFor(room(p, { mode: notSolo }), p, none)).toBeNull()
  })

  it('a closed room offers nothing', () => {
    const p = proposal()
    expect(approveAllFor(room(p, { status: 'closed', mode: solo }), p, both)).toBeNull()
  })

  it('a proposal whose approve-all was refused goes back to the per-layer rows; others keep it', () => {
    const p = proposal()
    const q = proposal({ proposal_id: 'p2' })
    const state = room(p, { proposals: [p, q], approve_all_refused: { p1: 'layer "kafka-connect": no' } })
    expect(approveAllFor(state, p, both)).toBeNull()
    expect(approveAllFor(state, q, both)).toEqual(['schema-registry', 'kafka-connect'])
  })
})

describe('words', () => {
  it('the solo confirm is "<pipeline_id>/*"', () => {
    expect(soloAllConfirmText('orders')).toBe('orders/*')
  })

  it('never says run with write mode off', () => {
    expect(approveAllLabel(false)).toBe('Approve and run')
    expect(approveAllLabel(true)).toBe('Approve (will not run)')
  })
})

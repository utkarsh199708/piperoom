import { describe, expect, it } from 'vitest'
import type { ProposalState, RoomState } from './types'
import { emptyRoomState } from './types'
import {
  canAcceptHandoff,
  canApprove,
  canDecideLayer,
  canRequestControl,
  canSendSuggestionToAgent,
  canSteer,
  canSuggest,
  canTakeControl,
} from './permissions'
import type { CurrentUser } from './permissions'

const arjun: CurrentUser = { id: 'arjun', name: 'Arjun', layers: ['kafka-connect', 'kafka'] }
const priya: CurrentUser = { id: 'priya', name: 'Priya', layers: ['postgres', 'schema-registry'] }
const kabir: CurrentUser = { id: 'kabir', name: 'Kabir', layers: ['consumer'] }
const meera: CurrentUser = { id: 'meera', name: 'Meera', layers: ['warehouse'] }

function room(driverId: string | null, extra?: Partial<RoomState>): RoomState {
  return {
    ...emptyRoomState('r1'),
    driver: driverId ? { id: driverId, name: driverId } : null,
    ...extra,
  }
}

const proposal: ProposalState = {
  proposal_id: 'p1',
  title: 'fix',
  steps: [],
  risk: 'low',
  rollback: 'undo',
  required_layers: ['schema-registry', 'kafka-connect'],
  approvals: { 'schema-registry': 'pending', 'kafka-connect': 'pending' },
  status: 'open',
  decisions: {},
  params_hash: 'abc123abc123',
  executed: [],
}

describe('permissions', () => {
  it('only the driver can steer', () => {
    const s = room('arjun')
    expect(canSteer(s, arjun)).toBe(true)
    expect(canSteer(s, kabir)).toBe(false)
  })

  it('non-drivers can suggest but not when there is no driver', () => {
    expect(canSuggest(room('arjun'), kabir)).toBe(true)
    expect(canSuggest(room('arjun'), arjun)).toBe(false)
    expect(canSuggest(room(null), kabir)).toBe(false)
  })

  it('request control is gated to non-drivers who have not already asked', () => {
    expect(canRequestControl(room('arjun'), kabir)).toBe(true)
    expect(canRequestControl(room('arjun'), arjun)).toBe(false)
    const asked = room('arjun', { control_requests: [{ from: { kind: 'human', id: 'kabir', name: 'Kabir' } }] })
    expect(canRequestControl(asked, kabir)).toBe(false)
  })

  it('a pending layer can be decided only by its owner', () => {
    expect(canDecideLayer(proposal, priya, 'schema-registry')).toBe(true)
    expect(canDecideLayer(proposal, arjun, 'kafka-connect')).toBe(true)
    expect(canDecideLayer(proposal, arjun, 'schema-registry')).toBe(false)
    expect(canDecideLayer(proposal, meera, 'schema-registry')).toBe(false)
    expect(canDecideLayer(proposal, kabir, 'kafka-connect')).toBe(false)
  })

  it('canApprove requires ownership of a still-pending layer', () => {
    expect(canApprove(proposal, arjun, 'kafka-connect')).toBe(true)
    expect(canApprove(proposal, priya, 'schema-registry')).toBe(true)
    expect(canApprove(proposal, arjun, 'schema-registry')).toBe(false)
    expect(canApprove(proposal, meera, 'schema-registry')).toBe(false)
    const decided: ProposalState = { ...proposal, approvals: { 'schema-registry': 'approved', 'kafka-connect': 'pending' } }
    expect(canApprove(decided, priya, 'schema-registry')).toBe(false)
  })

  it('a closed or decided proposal can no longer be decided', () => {
    const rejected: ProposalState = { ...proposal, status: 'rejected' }
    expect(canDecideLayer(rejected, arjun, 'kafka-connect')).toBe(false)
    const approved: ProposalState = { ...proposal, status: 'approved' }
    expect(canDecideLayer(approved, arjun, 'kafka-connect')).toBe(false)
  })

  it('anyone can take control when there is no driver or the lease expired', () => {
    expect(canTakeControl(room(null), kabir)).toBe(true)
    const expired = room('arjun', {
      driver: { id: 'arjun', name: 'Arjun', lease_until: '2020-01-01T00:00:00Z' },
    })
    expect(canTakeControl(expired, kabir)).toBe(true)
    expect(canTakeControl(room('arjun'), kabir)).toBe(false)
    expect(canTakeControl(room('arjun'), arjun)).toBe(false)
  })

  it('only the driver can forward a suggestion', () => {
    const s = room('arjun')
    const sug = { suggestion_id: 'g1', from: { kind: 'human' as const, id: 'kabir', name: 'Kabir' }, text: 'x', status: 'pending' as const }
    expect(canSendSuggestionToAgent(s, arjun, sug)).toBe(true)
    expect(canSendSuggestionToAgent(s, kabir, sug)).toBe(false)
  })

  it('only the handoff recipient can accept a pending handoff', () => {
    const handoff = {
      handoff_id: 'h1',
      to: { kind: 'human' as const, id: 'kabir', name: 'Kabir' },
      summary: { what_broke: '', why: '', done: [], ruled_out: [], open: [], risks: [] },
      stay_as_watcher: false,
      status: 'pending' as const,
      from: { kind: 'human' as const, id: 'arjun', name: 'Arjun' }, created_at: '2026-10-14T10:05:00Z',
    }
    expect(canAcceptHandoff(kabir, handoff)).toBe(true)
    expect(canAcceptHandoff(arjun, handoff)).toBe(false)
  })
})

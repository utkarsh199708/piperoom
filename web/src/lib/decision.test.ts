import { describe, expect, it } from 'vitest'
import { decisionFor } from './decision'
import { emptyRoomState } from '../state/types'
import type { ProposalState, RoomState } from '../state/types'
import type { CurrentUser } from '../state/permissions'

const arjun: CurrentUser = { id: 'arjun', name: 'Arjun', layers: ['kafka-connect'] }
const viewer: CurrentUser = { ...arjun, role: 'viewer' }
const step = (layer: string, action: string) => ({ action, layer, params: {}, description: '' })
const prop = (over: Partial<ProposalState> = {}): ProposalState => ({
  proposal_id: 'p1',
  title: 'fix',
  steps: [step('schema-registry', 'a'), step('kafka-connect', 'b')],
  risk: 'low',
  rollback: '',
  required_layers: ['schema-registry', 'kafka-connect'],
  approvals: { 'schema-registry': 'approved', 'kafka-connect': 'pending' },
  decisions: {},
  params_hash: 'h',
  status: 'open',
  executed: [],
  ...over,
})
const room = (proposals: ProposalState[], over: Partial<RoomState> = {}): RoomState => ({
  ...emptyRoomState('r'),
  proposals,
  ...over,
})
const ex = (i: number, ok = true) => ({ proposal_id: 'p1', step_index: i, ok, output: '' })

describe('decisionFor', () => {
  it('needs-you: first proposal, first layer I can decide; completing only when the others approved', () => {
    expect(decisionFor(room([prop()]), arjun)).toMatchObject({ kind: 'needs-you', layer: 'kafka-connect', completing: true })
    const open = prop({
      required_layers: ['schema-registry', 'kafka-connect', 'consumer'],
      approvals: { 'schema-registry': 'approved', 'kafka-connect': 'pending', consumer: 'pending' },
    })
    expect(decisionFor(room([open]), arjun)).toMatchObject({ kind: 'needs-you', completing: false })
    const two = prop({ proposal_id: 'p2' })
    expect(decisionFor(room([two, prop()]), arjun)).toMatchObject({ proposal: { proposal_id: 'p2' } })
  })
  it('a viewer never gets needs-you; someone else pending is waiting', () => {
    expect(decisionFor(room([prop()]), viewer)).toMatchObject({ kind: 'waiting', pending: ['kafka-connect'], approved: 1, total: 2 })
    expect(decisionFor(room([prop()]), { ...arjun, layers: ['consumer'] })?.kind).toBe('waiting')
  })
  it('executing, done and failed come from the newest approved proposal', () => {
    const approved = prop({ status: 'approved', approvals: { 'schema-registry': 'approved', 'kafka-connect': 'approved' } })
    expect(decisionFor(room([{ ...approved, executed: [ex(0)] }]), arjun)).toMatchObject({
      kind: 'executing',
      step: 2,
      total: 2,
      action: 'b',
    })
    expect(decisionFor(room([{ ...approved, executed: [ex(0), ex(1)] }]), arjun)).toMatchObject({ kind: 'done', ok: true })
    expect(decisionFor(room([{ ...approved, executed: [ex(0), ex(1, false)] }]), arjun)).toMatchObject({
      kind: 'done',
      ok: false,
      failedStep: 2,
    })
  })
  it('rejected stays until a newer proposal exists', () => {
    const rejected = prop({
      status: 'rejected',
      approvals: { 'schema-registry': 'approved', 'kafka-connect': 'rejected' },
      decisions: {
        'kafka-connect': { outcome: 'rejected', reason: 'no', by: { kind: 'human', id: 'k', name: 'Kabir' }, params_hash: 'h' },
      },
    })
    expect(decisionFor(room([rejected]), arjun)).toMatchObject({ kind: 'rejected', by: 'Kabir', layer: 'kafka-connect' })
    expect(decisionFor(room([rejected, { ...rejected, proposal_id: 'p2', status: 'approved', executed: [ex(0), ex(1)] }]), arjun)?.kind).toBe('done')
  })
  it('a closed room has none', () => {
    expect(decisionFor(room([prop()], { status: 'closed' }), arjun)).toBeNull()
  })
})

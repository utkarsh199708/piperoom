import { describe, expect, it } from 'vitest'
import { agentPill } from './agentPill'
import { emptyRoomState } from '../state/types'
import type { ProposalState, RoomState } from '../state/types'

const room = (over: Partial<RoomState> = {}): RoomState => ({ ...emptyRoomState('r1'), driver: { id: 'a', name: 'A' }, ...over })

const proposal: ProposalState = {
  proposal_id: 'p1',
  title: 'fix',
  steps: [],
  risk: 'low',
  rollback: 'undo',
  required_layers: ['kafka-connect', 'schema-registry'],
  approvals: { 'kafka-connect': 'pending', 'schema-registry': 'approved' },
  decisions: {},
  params_hash: '',
  status: 'open',
  executed: [],
}
const withdrawn: ProposalState = { ...proposal, status: 'withdrawn', withdrawn: { reason: 'wrong connector' } }

describe('agentPill after a withdrawal (Q0)', () => {
  it('does not claim to wait for an approval that can no longer come', () => {
    expect(agentPill(room({ agent_status: { state: 'waiting_approval' }, proposals: [withdrawn] }))).toMatchObject({
      tone: 'muted',
      text: 'Proposal withdrawn · waiting for a steer',
    })
  })

  it('still waits while an older proposal is open', () => {
    const older = { ...proposal, proposal_id: 'p0' }
    expect(agentPill(room({ agent_status: { state: 'waiting_approval' }, proposals: [older, withdrawn] })).text).toBe(
      'Agent waiting · 1 of 2 approvals',
    )
  })

  it('a closed room says the agent stopped, whatever the proposals say', () => {
    expect(agentPill(room({ status: 'closed', agent_status: { state: 'waiting_approval' }, proposals: [withdrawn] })).text).toBe(
      'Agent stopped',
    )
  })
})

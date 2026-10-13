import { describe, expect, it } from 'vitest'
import { agentPill } from './agentPill'
import { emptyRoomState } from '../state/types'
import type { ProposalState, RoomState } from '../state/types'

const room = (over: Partial<RoomState> = {}): RoomState => ({ ...emptyRoomState('r1'), ...over })

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

describe('agentPill', () => {
  it('working shows the step count', () => {
    const steps = [1, 2, 3].map((i) => ({ id: `s${i}`, title: 't', tool_calls: [], messages: [] }))
    expect(agentPill(room({ agent_status: { state: 'working', detail: 'x' }, steps }))).toMatchObject({
      tone: 'agent',
      text: 'Agent working · step 3',
      title: 'x',
    })
  })
  it('working before the first step has no number', () => {
    expect(agentPill(room({ agent_status: { state: 'working' } })).text).toBe('Agent working')
  })
  it('waiting shows how many layers approved', () => {
    expect(agentPill(room({ agent_status: { state: 'waiting_approval' }, proposals: [proposal] }))).toMatchObject({
      tone: 'warn',
      text: 'Agent waiting · 1 of 2 approvals',
    })
  })
  it('idle with no driver says it waits for one', () => {
    expect(agentPill(room()).text).toBe('Agent idle · waiting for a driver')
    expect(agentPill(room({ driver: { id: 'a', name: 'A' } })).text).toBe('Agent idle')
  })
  it('done is the green resolved pill', () => {
    expect(agentPill(room({ agent_status: { state: 'done' } }))).toMatchObject({ tone: 'ok', text: 'resolved — verified' })
  })
  it('error and unknown states', () => {
    expect(agentPill(room({ agent_status: { state: 'error' } })).tone).toBe('fail')
    expect(agentPill(room({ agent_status: { state: 'paused', detail: 'brb' } })).text).toBe('Agent paused · brb')
  })
})

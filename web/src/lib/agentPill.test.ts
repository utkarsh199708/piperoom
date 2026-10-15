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
  it('an open shadow room says the agent works silently, not "idle · waiting for a driver"', () => {
    const p = agentPill(room({ shadow: true, agent_status: { state: 'idle' } }))
    expect(p).toMatchObject({ tone: 'agent', text: 'Shadow · agent works silently' })
    expect(agentPill(room({ shadow: true, status: 'closed' })).text).toBe('Agent stopped')
  })
  it('working shows the step count', () => {
    const steps = [1, 2, 3].map((i) => ({ id: `s${i}`, title: 't', tool_calls: [], messages: [] }))
    expect(agentPill(room({ agent_status: { state: 'working', detail: 'x' }, steps }))).toMatchObject({
      tone: 'agent',
      text: 'Agent working',
      step: 3,
      title: 'x',
    })
  })
  it('working before the first step has no number', () => {
    const p = agentPill(room({ agent_status: { state: 'working' } }))
    expect(p.text).toBe('Agent working')
    expect(p.step).toBeUndefined()
  })
  it('the live text never carries the step number, so a new step is not announced', () => {
    for (const n of [1, 2, 9]) {
      const steps = Array.from({ length: n }, (_, i) => ({ id: `s${i}`, title: 't', tool_calls: [], messages: [] }))
      expect(agentPill(room({ agent_status: { state: 'working' }, steps })).text).toBe('Agent working')
    }
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
  const step2 = [{ tool: 'a' }, { tool: 'b' }] as unknown as ProposalState['steps']
  const approved = (executed: ProposalState['executed']): ProposalState => ({
    ...proposal,
    status: 'approved',
    steps: step2,
    approvals: { 'kafka-connect': 'approved', 'schema-registry': 'approved' },
    executed,
  })
  const ex = (i: number, ok = true) => ({ proposal_id: 'p1', step_index: i, ok, output: '' })
  it('executed steps read as verifying, even if the agent still says waiting', () => {
    const r = room({ agent_status: { state: 'waiting_approval' }, proposals: [approved([ex(0), ex(1)])] })
    expect(agentPill(r)).toMatchObject({ tone: 'ok', text: 'Fix ran · verifying' })
  })
  it('a run in progress and a failed step', () => {
    const running = room({ agent_status: { state: 'waiting_approval' }, proposals: [approved([ex(0)])] })
    expect(agentPill(running)).toMatchObject({ tone: 'agent', text: 'Executing fix', step: 2 })
    const failed = room({ proposals: [approved([ex(0), ex(1, false)])] })
    expect(agentPill(failed)).toMatchObject({ tone: 'fail', text: 'Fix failed · step 2' })
  })
  it('a rejected proposal re-plans or waits for a steer', () => {
    const rejected = { ...proposal, status: 'rejected' as const }
    expect(agentPill(room({ agent_status: { state: 'working' }, proposals: [rejected] })).text).toBe(
      'Proposal rejected · re-planning',
    )
    expect(agentPill(room({ agent_status: { state: 'waiting_approval' }, proposals: [rejected] })).text).toBe(
      'Proposal rejected · waiting for a steer',
    )
  })
  it('a closed room says the agent stopped', () => {
    expect(agentPill(room({ status: 'closed', agent_status: { state: 'waiting_approval' } }))).toMatchObject({
      tone: 'muted',
      text: 'Agent stopped',
    })
  })
})

describe('agentPill queued and log_full', () => {
  it('say what is happening instead of the raw state', () => {
    expect(agentPill(room({ driver: { id: 'a', name: 'A' }, agent_status: { state: 'queued' } }))).toMatchObject({ tone: 'agent', text: 'Queued · waiting for an agent slot' })
    expect(agentPill(room({ driver: { id: 'a', name: 'A' }, agent_status: { state: 'log_full' } }))).toMatchObject({ tone: 'fail', text: 'Log full · agent stopped' })
  })
})

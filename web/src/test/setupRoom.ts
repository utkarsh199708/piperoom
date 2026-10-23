// Setup-room states for the Vitest cases (U1-12a). Built through the same snapshot + reducer
// as a live room, so a test that passes here passes against the server's documented shapes.
import type { Event, ProposalCreatedPayload, RoomSnapshot, SetupSnapshot } from '../api/types'
import { applyEvent } from '../state/reducer'
import { fromSnapshot } from '../state/types'
import type { RoomState } from '../state/types'
import { SETUP_ROOM_ID, planCreated, runnerAttached, setupCreated, setupRoomSnapshot } from '../fixtures/setup'

const arjun = { kind: 'human' as const, id: 'arjun', name: 'Arjun', verified: true }
const agent = { kind: 'agent' as const, id: 'agent', name: 'Agent' }
const system = { kind: 'system' as const, id: 'system', name: 'system' }

const at = (n: number) => `2026-10-22T10:0${n}:00Z`

/** A setup room folded from the snapshot and, when given, events. */
export function setupState(opts: { snapshot?: Partial<SetupSnapshot>; events?: Event[]; proposal?: ProposalCreatedPayload } = {}): RoomState {
  const snapshot: RoomSnapshot = setupRoomSnapshot(opts.snapshot ?? {})
  let state = fromSnapshot(snapshot)
  for (const event of opts.events ?? []) state = applyEvent(state, event)
  if (opts.proposal) {
    state = applyEvent(state, {
      room_id: SETUP_ROOM_ID,
      seq: state.last_seq + 1,
      type: 'proposal.created',
      actor: agent,
      at: at(4),
      payload: opts.proposal,
    })
  }
  return state
}

/** room.created + runner attached: the paired state (no plan yet). */
export const pairedEvents = (): Event[] => [setupCreated(), runnerAttached()]

/** room.created + runner + the plan: the planning state. */
export const planningEvents = (): Event[] => [...pairedEvents(), planCreated()]

export const setupProposal: ProposalCreatedPayload = {
  proposal_id: 'p_setup',
  title: 'Start the stack',
  steps: [
    { action: 'compose_up', layer: 'environment', params: { services: ['kafka', 'connect'] }, description: 'Start Kafka and Connect.' },
    { action: 'properties_write', layer: 'environment', params: { path: 'connect.properties' }, description: 'Write the worker properties.' },
  ],
  risk: 'medium',
  rollback: 'docker compose down',
  required_layers: ['environment'],
  params_hash: 'abc123',
}

/** approval.given for every required layer. */
export function approved(seq = 5): Event {
  return {
    room_id: SETUP_ROOM_ID,
    seq,
    type: 'approval.given',
    actor: arjun,
    at: at(5),
    payload: { proposal_id: 'p_setup', layer: 'environment', params_hash: 'abc123' },
  }
}

/** action.executed for step `index` of the proposal (and the plan). */
export function executed(index: number, seq: number): Event {
  return {
    room_id: SETUP_ROOM_ID,
    seq,
    type: 'action.executed',
    actor: system,
    at: at(5),
    payload: { proposal_id: 'p_setup', step_index: index, ok: true, output: 'ok' },
  }
}

/** The state between the plan and the first executed step, with an open proposal. */
export function awaitingState(): RoomState {
  return setupState({ events: [...planningEvents()], proposal: setupProposal })
}

/** A proposal that is approved and has run one step: "running step". */
export function runningState(): RoomState {
  const withPlan = setupState({ events: planningEvents(), proposal: setupProposal })
  const approvedState = applyEvent(withPlan, approved())
  const executedState = applyEvent(approvedState, executed(0, approvedState.last_seq + 1))
  // The second proposal step is now executing.
  const running = applyEvent(executedState, {
    room_id: SETUP_ROOM_ID,
    seq: executedState.last_seq + 1,
    type: 'agent.status',
    actor: agent,
    at: at(6),
    payload: { state: 'working', detail: 'running compose_up' },
  })
  return running
}

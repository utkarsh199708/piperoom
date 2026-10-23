// A setup room as the API documents it (docs/api.md "U-wave snapshot fields" and "U-wave
// events"): the room GET plus the log of a run from pairing to a blocked step. Tests pin the
// web's reading of these shapes; the fixture story uses the same values.
import type {
  CreateSetupResponse,
  Event,
  RoomSnapshot,
  SetupPlanPayload,
  SetupRequest,
  SetupSnapshot,
} from '../api/types'

export const SETUP_ROOM_ID = 'r_setup_orders'

const arjun = { kind: 'human' as const, id: 'arjun', name: 'Arjun', verified: true }
const runner = { kind: 'system' as const, id: 'runner', name: 'Runner' }
const agent = { kind: 'agent' as const, id: 'agent', name: 'Agent' }

export const setupRequest: SetupRequest = {
  pipeline_id: 'orders',
  tenant: 'acme',
  title: 'Watch orders end to end',
  source_kind: 'postgres',
  target: 'compose',
  topic_prefix: 'orders',
  tables: ['public.orders', 'public.order_items'],
  targets: [{ kind: 'postgres', address: 'db.acme:5432', class: 'production' }],
}

export const setupPlan: SetupPlanPayload = {
  plan_id: 'pl_1',
  phases: [
    {
      name: 'prerequisites',
      steps: [
        { index: 0, op: 'docker_info', target: 'local', tier: 'T0', layer: 'environment' },
        { index: 1, op: 'env_detect', target: 'local', tier: 'T0', layer: 'environment' },
      ],
    },
    {
      name: 'stack',
      steps: [
        { index: 2, op: 'compose_up', target: 'compose', tier: 'T2', layer: 'environment', verify: 'Connect answers on 8083' },
        { index: 3, op: 'properties_write', target: 'project', tier: 'T2', layer: 'environment' },
      ],
    },
  ],
}

/** The snapshot of a setup room; `over` is the state a test wants to pin. */
export function setupRoomSnapshot(over: Partial<SetupSnapshot> = {}, request: SetupRequest = setupRequest): RoomSnapshot {
  return {
    id: SETUP_ROOM_ID,
    title: request.title,
    severity: 'sev4',
    pipeline_id: request.pipeline_id,
    status: 'open',
    opened_at: '2026-10-22T10:00:00Z',
    last_seq: 0,
    driver: { id: 'arjun', name: 'Arjun', verified: true, lease_until: '2099-01-01T00:00:00Z' },
    agent_status: { state: 'idle' },
    hypothesis: null,
    open_proposals: [],
    pending_suggestions: [],
    members: [{ id: 'arjun', name: 'Arjun', verified: true, role: 'driver' }],
    mode: { write: true, solo: true, auto_approve: [] },
    setup: {
      mode: true,
      runner: null,
      plan_id: '',
      phase: '',
      step_index: 0,
      bounds: { attempts_per_step: 3, steps_per_plan: 12, wall_clock_s: 2700, token_budget: 2_000_000 },
      targets: [{ kind: 'postgres', address: 'db.acme:5432', class: 'production' }],
      provisional: { topic_prefix: request.topic_prefix, tables: request.tables, connectors: [] },
      verified: [],
      watched_by: '',
      ...over,
    },
  }
}

const at = (n: number) => `2026-10-22T10:0${Math.min(n, 9)}:00Z`

/** room.created: the creator is the driver, `alert` is the SetupRequest. */
export function setupCreated(seq = 1): Event {
  return {
    room_id: SETUP_ROOM_ID,
    seq,
    type: 'room.created',
    actor: arjun,
    at: at(0),
    payload: {
      title: setupRequest.title,
      severity: 'sev4',
      pipeline_id: setupRequest.pipeline_id,
      mode: 'setup',
      alert: setupRequest,
    },
  }
}

export function runnerAttached(seq = 2): Event {
  return {
    room_id: SETUP_ROOM_ID,
    seq,
    type: 'setup.runner_attached',
    actor: runner,
    at: at(1),
    payload: {
      runner_label: 'arjun-laptop',
      os: 'windows',
      arch: 'amd64',
      tools: { docker: '27.1.1', kubectl: '' },
      project_dir: 'C:\\work\\orders',
      catalogue_version: '2026-10-1',
      protocol_version: 1,
      protocol_minor: 0,
    },
  }
}

export function planCreated(seq = 3): Event {
  return {
    room_id: SETUP_ROOM_ID,
    seq,
    type: 'setup.plan',
    actor: agent,
    at: at(2),
    payload: setupPlan,
  }
}

export function stepExecuted(index: number, seq: number): Event {
  return {
    room_id: SETUP_ROOM_ID,
    seq,
    type: 'action.executed',
    actor: systemActor(),
    at: at(3),
    payload: { proposal_id: 'p_setup', step_index: index, ok: true, output: 'ok' },
  }
}

function systemActor() {
  return { kind: 'system' as const, id: 'executor', name: 'Executor' }
}

/** The pairing answer POST /api/setup returns; the room snapshot carries no runner yet. */
export function setupCreateResponse(): CreateSetupResponse {
  return {
    room: setupRoomSnapshot(),
    code: 'ABCD2345',
    expires_at: '2026-10-22T10:10:00Z',
    command: 'piperoom-setup --server http://127.0.0.1:8090 --code ABCD2345',
  }
}

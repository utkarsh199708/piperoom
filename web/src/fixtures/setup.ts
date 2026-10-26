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

// U1-12b: the events the setup controls answer with, so a test folds the same log the server
// would write. Names only for secrets: no helper takes a value, and none is ever written here.

export function targetRegistered(
  address = 'kafka.acme:9092',
  cls = 'shared',
  seq = 10,
  kind = 'kafka',
): Event {
  return {
    room_id: SETUP_ROOM_ID,
    seq,
    type: 'setup.target_registered',
    actor: arjun,
    at: at(5),
    payload: { kind, address, class: cls, by: 'arjun' },
  }
}

export function targetClassed(target = 'db.acme:5432', cls = 'production', seq = 11): Event {
  return {
    room_id: SETUP_ROOM_ID,
    seq,
    type: 'setup.target_classed',
    actor: arjun,
    at: at(5),
    payload: { target, class: cls, reason: 'person', by: 'person' },
  }
}

export function pipelineExtended(
  add: { add_connectors?: string[]; add_tables?: string[]; add_consumer_groups?: string[]; add_sink_tables?: string[] },
  seq = 12,
): Event {
  return {
    room_id: SETUP_ROOM_ID,
    seq,
    type: 'setup.pipeline_extended',
    actor: arjun,
    at: at(5),
    payload: { ...add, by: 'arjun' },
  }
}

export function maintenanceWindow(start = '2026-10-22T18:00:00Z', end = '2026-10-22T20:00:00Z', seq = 13): Event {
  return {
    room_id: SETUP_ROOM_ID,
    seq,
    type: 'setup.maintenance_window',
    actor: arjun,
    at: at(5),
    payload: { start, end, by: 'arjun' },
  }
}

/** setup.secret_set carries the name and the actor; the value is never in an event. */
export function secretSet(name = 'DB_PASSWORD', seq = 14): Event {
  return {
    room_id: SETUP_ROOM_ID,
    seq,
    type: 'setup.secret_set',
    actor: arjun,
    at: at(5),
    payload: { name, actor: 'arjun' },
  }
}

export function credentialsRegistered(
  names = ['piperoom_ro'],
  runnerLabel = 'arjun-laptop',
  seq = 15,
): Event {
  return {
    room_id: SETUP_ROOM_ID,
    seq,
    type: 'setup.credentials_registered',
    actor: runner,
    at: at(5),
    payload: { pipeline_id: 'orders', names, runner_label: runnerLabel },
  }
}

export function rungList(): { name: string; ok: boolean; note?: string }[] {
  return [
    { name: 'connector_running', ok: true },
    { name: 'tasks_running', ok: true },
    { name: 'canary_topic', ok: true },
    { name: 'snapshot_done', ok: true, note: 'SnapshotCompleted' },
    { name: 'canary_event', ok: true },
    { name: 'schema_subject', ok: true },
    { name: 'sink_row', ok: true },
    { name: 'person_change', ok: false, note: 'timed out; rungs 1 to 7 stay green' },
  ]
}

export function verified(rungs = rungList(), planId = setupPlan.plan_id, seq = 16): Event {
  return {
    room_id: SETUP_ROOM_ID,
    seq,
    type: 'setup.verified',
    actor: runner,
    at: at(6),
    payload: { plan_id: planId, nonce_hash: 'hash', rungs },
  }
}

export function awaitingChange(
  table = 'public.orders',
  exampleSQL = "UPDATE public.orders SET note = 'ping' WHERE id = 1",
  seq = 17,
): Event {
  return {
    room_id: SETUP_ROOM_ID,
    seq,
    type: 'setup.awaiting_change',
    actor: runner,
    at: at(6),
    payload: { table, example_sql: exampleSQL },
  }
}

export function pipelineRegistered(watchedBy = 'server', seq = 18): Event {
  return {
    room_id: SETUP_ROOM_ID,
    seq,
    type: 'pipeline.registered',
    actor: runner,
    at: at(7),
    payload: { pipeline_id: 'orders', watched_by: watchedBy, by: arjun, approved_by: [] },
  }
}

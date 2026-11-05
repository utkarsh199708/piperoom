// An X8 room as the API documents it (docs/api.md "X8 staging" and "X8 snapshot fields"): the room
// GET with `mode.staging` and a proposal that is stageable and needs a stage, then the events of a
// stage. Tests pin the web's reading of these shapes; nothing here is a story the app plays.
import type { Event, ProposalStagedPayload, ProposalSummary, RoomSnapshot } from '../api/types'

export const STAGING_HASH = '9f2c1e7ab04d58c3d6e1f0a7b92c4d835e6a1f0b7c3d9e2a48b5c6d7e8f90a1b'

const arjun = { kind: 'human' as const, id: 'arjun', name: 'Arjun', verified: true }
const agent = { kind: 'agent' as const, id: 'agent', name: 'Agent' }
const system = { kind: 'system' as const, id: 'system', name: 'system' }

export const stagingProposal: ProposalSummary = {
  proposal_id: 'p_stage',
  title: 'Raise the connector batch size',
  steps: [
    { action: 'connect_update_connector_config', layer: 'kafka-connect', params: { connector: 'orders-src' }, description: 'Raise max.batch.size.' },
    { action: 'connect_restart_task', layer: 'kafka-connect', params: { connector: 'orders-src', task: 0 }, description: 'Restart task 0.' },
    { action: 'connect_update_connector_config', layer: 'kafka-connect', params: { connector: 'orders-aux' }, description: 'Raise max.batch.size.' },
  ],
  risk: 'low',
  rollback: 'Put the old config back.',
  required_layers: ['kafka-connect'],
  approvals: { 'kafka-connect': 'pending' },
  params_hash: STAGING_HASH,
  stageable: [0, 2],
  staging_required: true,
}

export const stagingSnapshot: RoomSnapshot = {
  id: 'r_stage',
  title: 'orders-src lag growing',
  severity: 'sev2',
  pipeline_id: 'orders',
  status: 'open',
  opened_at: '2026-10-22T10:00:00Z',
  last_seq: 3,
  driver: { id: 'arjun', name: 'Arjun', verified: true, lease_until: '2099-01-01T00:00:00Z' },
  agent_status: { state: 'idle' },
  hypothesis: null,
  open_proposals: [stagingProposal],
  pending_suggestions: [],
  members: [{ id: 'arjun', name: 'Arjun', verified: true, role: 'driver' }],
  mode: { write: true, solo: false, auto_approve: [], staging: true },
}

const at = (n: number) => `2026-10-22T10:00:${String(n).padStart(2, '0')}Z`

export const stagingCreated: Event = {
  room_id: 'r_stage',
  seq: 1,
  type: 'proposal.created',
  actor: agent,
  at: at(1),
  payload: {
    proposal_id: stagingProposal.proposal_id,
    title: stagingProposal.title,
    steps: stagingProposal.steps,
    risk: 'low',
    rollback: stagingProposal.rollback,
    required_layers: ['kafka-connect'],
    params_hash: STAGING_HASH,
  },
}

export const stagingStarted: Event = {
  room_id: 'r_stage',
  seq: 2,
  type: 'proposal.stage_started',
  actor: arjun,
  at: at(2),
  payload: { proposal_id: stagingProposal.proposal_id, params_hash: STAGING_HASH, steps: [0, 2], expires_at: '2026-10-22T10:03:12Z' },
}

export const stagedPayload: ProposalStagedPayload = {
  proposal_id: stagingProposal.proposal_id,
  params_hash: STAGING_HASH,
  at: at(63),
  steps: [
    {
      step_index: 0,
      result: { target: 'connector orders-src', applied: true, healthy: true, detail: 'RUNNING, 1 of 1 tasks RUNNING for 60 s', duration_ms: 61_000 },
    },
    {
      step_index: 2,
      result: { applied: false, healthy: false, detail: 'connector not present in staging', duration_ms: 0 },
    },
  ],
}

export const stagingStaged: Event = {
  room_id: 'r_stage',
  seq: 3,
  type: 'proposal.staged',
  actor: system,
  at: at(63),
  payload: stagedPayload,
}

/** The same stage with every step healthy: what lifts "staging required". */
export const stagedAllHealthy: ProposalStagedPayload = {
  ...stagedPayload,
  steps: [
    stagedPayload.steps[0],
    { step_index: 2, result: { target: 'connector orders-aux', applied: true, healthy: true, detail: 'RUNNING, 1 of 1 tasks RUNNING for 60 s', duration_ms: 60_500 } },
  ],
}

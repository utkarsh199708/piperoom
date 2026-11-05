// A Q7 fix-PR room as the API documents it (docs/api.md "Q7 pull-request fix", design
// docs/design/fix-pr.md §5.3 and §6.3): a repo_open_pull_request proposal, its preview with the diff,
// the action.executed that opened the PR and the follower's pull_request.updated events. Tests pin the
// web's reading of these shapes; nothing here is a story the app plays.
import type { DiffPreview, Event, ProposalSummary, PullRequestUpdatedPayload, RoomSnapshot } from '../api/types'

export const FIXPR_HASH = '4b1c2d3e4f5a6b7c8d9e0f1a2b3c4d5e6f7a8b9c0d1e2f3a4b5c6d7e8f9a0b1c'

const agent = { kind: 'agent' as const, id: 'agent', name: 'Agent' }
const system = { kind: 'system' as const, id: 'system', name: 'system' }
const arjun = { kind: 'human' as const, id: 'arjun', name: 'Arjun', verified: true }

export const FOOTER =
  'Opened by Piperoom from room r_pr (proposal p_pr, step 0). Approved in Piperoom by the kafka-connect owners. ' +
  "This PR changes nothing until merged; this repository's own review rules apply."

export const fixprProposal: ProposalSummary = {
  proposal_id: 'p_pr',
  title: 'Add the Debezium Postgres plugin to the Connect image',
  steps: [
    {
      action: 'repo_open_pull_request',
      layer: 'kafka-connect',
      params: { repo: 'connect-config', base_sha: '1a2b3c4d5e6f', title: 'connect: add the postgres plugin' },
      description: 'Install the missing plugin in the image.',
    },
  ],
  risk: 'low',
  rollback: 'Close the pull request.',
  required_layers: ['kafka-connect'],
  approvals: { 'kafka-connect': 'pending' },
  params_hash: FIXPR_HASH,
}

export const fixprSnapshot: RoomSnapshot = {
  id: 'r_pr',
  title: 'connector plugin missing',
  severity: 'sev2',
  pipeline_id: 'orders',
  status: 'open',
  opened_at: '2026-10-22T10:00:00Z',
  last_seq: 2,
  driver: { id: 'arjun', name: 'Arjun', verified: true, lease_until: '2099-01-01T00:00:00Z' },
  agent_status: { state: 'idle' },
  hypothesis: null,
  open_proposals: [fixprProposal],
  pending_suggestions: [],
  members: [{ id: 'arjun', name: 'Arjun', verified: true, role: 'driver' }],
  mode: { write: true, solo: false, auto_approve: [] },
}

export const fixprDiff: DiffPreview = {
  repo: 'connect-config',
  repo_url: 'https://github.com/acme/connect-config',
  base: 'main',
  base_sha: '1a2b3c4d5e6f7a8b9c0d',
  branch: 'piperoom/r_pr-p_pr-0',
  layer: 'kafka-connect',
  files: [
    {
      path: 'docker/connect/Dockerfile',
      status: 'modified',
      unified:
        '--- a/docker/connect/Dockerfile\n+++ b/docker/connect/Dockerfile\n@@ -1,3 +1,4 @@\n FROM quay.io/debezium/connect:2.7\n-ENV LOG_LEVEL=INFO\n+ENV LOG_LEVEL=WARN\n+RUN curl -fsSL https://repo1.maven.org/plugin.tgz | tar xz\n COPY config /kafka/config\n',
      added: 2,
      removed: 1,
    },
    {
      path: 'connectors/orders.json',
      status: 'added',
      unified: '--- /dev/null\n+++ b/connectors/orders.json\n@@ -0,0 +1,2 @@\n+{"name": "orders"}\n+\n',
      added: 2,
      removed: 0,
    },
  ],
  hosts: ['repo1.maven.org'],
  warnings: ['a download (curl) in docker/connect/Dockerfile'],
}

const at = (n: number) => `2026-10-22T10:00:${String(n).padStart(2, '0')}Z`

export const fixprCreated: Event = {
  room_id: 'r_pr',
  seq: 1,
  type: 'proposal.created',
  actor: agent,
  at: at(1),
  payload: {
    proposal_id: 'p_pr',
    title: fixprProposal.title,
    steps: fixprProposal.steps,
    risk: 'low',
    rollback: fixprProposal.rollback,
    required_layers: ['kafka-connect'],
    params_hash: FIXPR_HASH,
  },
}

export const fixprPreviewed: Event = {
  room_id: 'r_pr',
  seq: 2,
  type: 'proposal.previewed',
  actor: system,
  at: at(2),
  payload: {
    proposal_id: 'p_pr',
    at: at(2),
    steps: [
      {
        step_index: 0,
        preview: {
          target: 'github.com/acme/connect-config @ main (repo connect-config, layer kafka-connect)',
          current: 'base 1a2b3c4 (the commit the agent read); base head now 9f8e7d6',
          expected:
            'a pull request from piperoom/r_pr-p_pr-0 into main: 2 file(s), +4 -1. Nothing changes in any system until a reviewer merges it. ' +
            'Piperoom will treat it as applied when the connector runs again. The body ends with: ' +
            FOOTER,
          reversible: true,
          rollback: 'close the pull request (nothing to undo until it merges); after a merge, revert it in the repository',
          dry_run: { ok: true, source: 'base branch check', text: 'no file of the change differs between base 1a2b3c4 and the base head 9f8e7d6' },
          diff: fixprDiff,
        },
      },
    ],
  },
}

export const fixprApproved: Event = {
  room_id: 'r_pr',
  seq: 3,
  type: 'approval.given',
  actor: arjun,
  at: at(3),
  payload: { proposal_id: 'p_pr', layer: 'kafka-connect', params_hash: FIXPR_HASH, grant: 'owner' },
}

export const fixprExecuted: Event = {
  room_id: 'r_pr',
  seq: 4,
  type: 'action.executed',
  actor: system,
  at: at(4),
  payload: {
    proposal_id: 'p_pr',
    step_index: 0,
    ok: true,
    output: 'opened pull request #12: https://github.com/acme/connect-config/pull/12',
    params_hash: FIXPR_HASH,
    pull_request: {
      repo: 'connect-config',
      number: 12,
      url: 'https://github.com/acme/connect-config/pull/12',
      branch: 'piperoom/r_pr-p_pr-0',
      head_sha: 'c0ffee0123456789',
    },
  },
}

export function prUpdated(seq: number, over: Partial<PullRequestUpdatedPayload>): Event {
  const payload: PullRequestUpdatedPayload = {
    proposal_id: 'p_pr',
    step_index: 0,
    repo: 'connect-config',
    number: 12,
    url: 'https://github.com/acme/connect-config/pull/12',
    branch: 'piperoom/r_pr-p_pr-0',
    head_sha: 'c0ffee0123456789',
    state: 'open',
    checks: { state: 'pending', total: 3 },
    ...over,
  }
  return { room_id: 'r_pr', seq, type: 'pull_request.updated', actor: system, at: at(seq), payload }
}

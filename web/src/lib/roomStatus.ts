// The one sentence every surface (header badge, list row, pill, ⌘K) shows about a room.
// Pure: it reads only what the reducer and the list snapshot already carry.
import type { RoomSnapshot } from '../api/types'
import type { RoomState } from '../state/types'
import type { Dot } from './rooms'
import { ageLabel } from './time'
import { verdictKind, verdictWords } from './execution'
import { WAITING_FOR_WRITE_MODE, waitingForWriteMode } from './writeMode'

/** Precedence is this order: the first kind that applies wins. */
export type StatusKind =
  | 'closed'
  | 'resolved'
  | 'agent-error'
  | 'log-full'
  | 'executing'
  // Its place in this list is not its precedence: it applies only when the blocked proposal is the newest.
  | 'awaiting-write-mode'
  | 'fix-failed'
  | 'fixed'
  | 'needs-approval'
  | 'no-driver'
  | 'queued'
  | 'investigating'
  | 'idle'
export type StatusTone = 'ok' | 'warn' | 'fail' | 'agent' | 'muted'

export interface RoomStatusInfo {
  kind: StatusKind
  label: string
  tone: StatusTone
  dot: Dot
  /** Step number the label mentions (executing: the running step, fix-failed: the failed one). */
  step?: number
}

export interface StatusProposal {
  status: string
  required_layers: string[]
  approvals: Record<string, string>
  /** Steps in the proposal; 0 when unknown (the list snapshot has none). */
  steps: number
  executed: { ok: boolean }[]
  /** The agent's verdict on the fix, once it ran (the list snapshot has none). */
  verification?: { recovered: boolean; verdict?: string }
  /** P1a: approved by every layer, held back because write mode is off (the list snapshot has none). */
  waitingForWriteMode?: boolean
}

export interface RoomStatusInput {
  status: string
  opened_at: string
  closed_at?: string
  resolution?: string
  driver: { lease_until?: string } | null
  agent: { state: string; detail?: string }
  proposals: StatusProposal[]
  /** Agent investigation steps so far (0 from the snapshot). */
  steps: number
}

const TONE_DOT: Record<StatusTone, Dot> = { ok: 'ok', warn: 'warn', fail: 'fail', agent: 'agent', muted: 'idle' }

function info(kind: StatusKind, label: string, tone: StatusTone, step?: number): RoomStatusInfo {
  return { kind, label, tone, dot: TONE_DOT[tone], ...(step !== undefined ? { step } : {}) }
}

function liveDriver(driver: RoomStatusInput['driver'], now: number): boolean {
  if (driver == null) return false
  const until = driver.lease_until
  return until == null || !(Date.parse(until) <= now)
}

/** The one sentence every surface shows. Precedence is the order of StatusKind. */
export function roomStatus(input: RoomStatusInput, now = Date.now()): RoomStatusInfo {
  const agent = input.agent.state
  if (input.status !== 'open') {
    // A closed room with a resolution (or a verified agent) was resolved; otherwise it was just closed.
    if (agent === 'done') return info('resolved', 'Resolved · verified', 'ok')
    if (input.resolution) return info('resolved', 'Resolved', 'ok')
    return info('closed', 'Closed', 'muted')
  }
  if (agent === 'error') return info('agent-error', 'Agent error', 'fail')
  if (agent === 'log_full') return info('log-full', 'Log full · agent stopped', 'fail')

  // The newest approved proposal decides executing / failed / fixed. These outrank the agent's own
  // status: the demo agent re-emits waiting_approval after every approval.
  const approved = [...input.proposals].reverse().find((p) => p.status === 'approved')
  // Nothing runs while write mode is off, so it is neither executing nor fixed. A newer proposal
  // takes over, like a finished fix.
  if (approved?.waitingForWriteMode && input.proposals[input.proposals.length - 1] === approved) {
    return info('awaiting-write-mode', WAITING_FOR_WRITE_MODE, 'muted')
  }
  if (approved && !approved.waitingForWriteMode) {
    const failed = approved.executed.findIndex((e) => !e.ok)
    if (failed >= 0) return info('fix-failed', `Fix failed · step ${failed + 1}`, 'fail', failed + 1)
    if (approved.steps > 0 && approved.executed.length < approved.steps) {
      const k = approved.executed.length + 1
      return info('executing', `Executing fix · ${k} of ${approved.steps}`, 'agent', k)
    }
    if (approved.executed.length > 0 && agent !== 'done') {
      // The verdict is in: a fix that did not recover the pipeline must not read as a green "verifying".
      const v = approved.verification ? verdictKind(approved.verification) : null
      if (v === 'not-recovered') return info('fix-failed', 'Fix ran · not recovered', 'fail')
      if (v === 'not-verified' || v === 'inconclusive') return info('fixed', `Fix ran · ${verdictWords(v)}`, 'warn')
      return info('fixed', v === 'recovered' ? 'Fix ran · recovered' : 'Fix ran · verifying', 'ok')
    }
  }

  const awaiting = input.proposals.some(
    (p) => p.status === 'open' && p.required_layers.some((l) => p.approvals[l] === 'pending'),
  )
  if (awaiting) return info('needs-approval', `Needs approval · ${ageLabel(input.opened_at, now) || 'now'}`, 'warn')
  if (!liveDriver(input.driver, now)) {
    return info('no-driver', `No driver · ${ageLabel(input.opened_at, now) || 'now'}`, 'warn')
  }
  if (agent === 'queued') return info('queued', 'Queued · waiting for an agent slot', 'agent')
  if (agent === 'working') {
    return info('investigating', input.steps > 0 ? `Investigating · step ${input.steps}` : 'Investigating', 'agent')
  }
  return info('idle', 'Idle · waiting for a steer', 'muted')
}

export function statusInputFromState(s: RoomState): RoomStatusInput {
  return {
    status: s.status,
    opened_at: s.opened_at,
    closed_at: s.closed_at,
    resolution: s.resolution,
    driver: s.driver,
    agent: s.agent_status,
    proposals: s.proposals.map((p) => ({
      status: p.status,
      required_layers: p.required_layers,
      approvals: p.approvals,
      steps: p.steps.length,
      executed: p.executed,
      verification: p.verification,
      ...(waitingForWriteMode(p) ? { waitingForWriteMode: true } : {}),
    })),
    steps: s.steps.length,
  }
}

/** The snapshot lists only open proposals and has no step count, so `steps` is 0 and nothing executed. */
export function statusInputFromSnapshot(r: RoomSnapshot): RoomStatusInput {
  return {
    status: r.status,
    opened_at: r.opened_at,
    closed_at: r.closed_at,
    resolution: r.resolution,
    driver: r.driver,
    // Test and fixture snapshots may omit it; a missing status reads as idle.
    agent: r.agent_status ?? { state: 'idle' },
    proposals: (r.open_proposals ?? []).map((p) => ({
      status: 'open',
      required_layers: p.required_layers,
      approvals: p.approvals,
      steps: 0,
      executed: [],
    })),
    steps: 0,
  }
}

// Staging (X8, docs/api.md "X8 staging"): the rules the proposal card and the approve buttons share.
// Pure. The server decides what staging_required means; the web only mirrors its two derived
// rules (Outcome, StepHealthy) so a stage that arrives over the socket reads the same everywhere.
import type { ProposalStagedPayload, StageOutcome, StageResult } from '../api/types'
import type { ProposalState } from '../state/types'

/** events.StageResult.Outcome, rule for rule. */
export function stageOutcome(r: StageResult): StageOutcome {
  if (r.applied && r.healthy) return 'healthy'
  if (r.applied) return 'failed'
  // Staging's own pre-check refused the change and nothing was written: the change would fail.
  if (r.dry_run && !r.dry_run.ok && !r.dry_run.skipped) return 'failed'
  return 'not_applied'
}

/** events.ProposalStagedPayload.StepHealthy: a verdict (no error) that is healthy. */
export function stepHealthy(staged: ProposalStagedPayload, stepIndex: number): boolean {
  const s = (staged.steps ?? []).find((x) => x.step_index === stepIndex)
  return !!s && !s.error && !!s.result && stageOutcome(s.result) === 'healthy'
}

/** The stage as a whole is healthy when at least one step was staged and every one of them is. */
export function stagedHealthy(staged: ProposalStagedPayload): boolean {
  const steps = staged.steps ?? []
  return steps.length > 0 && steps.every((s) => stepHealthy(staged, s.step_index))
}

export type StagedVerdict = StageOutcome | 'no_verdict'

/** One word for the whole stage: a failure outranks a missing verdict, which outranks "nothing tried". */
export function stagedVerdict(staged: ProposalStagedPayload): StagedVerdict {
  if (stagedHealthy(staged)) return 'healthy'
  const outcomes = (staged.steps ?? []).map((s) => (s.error || !s.result ? 'no_verdict' : stageOutcome(s.result)))
  if (outcomes.includes('failed')) return 'failed'
  if (outcomes.includes('no_verdict')) return 'no_verdict'
  return 'not_applied'
}

/** The stage on the card belongs to the fix on screen (same params_hash); otherwise it is shown as stale. */
export function stageIsCurrent(p: ProposalState): boolean {
  return !!p.staged && p.staged.params_hash === p.params_hash
}

/**
 * A stage is running while its proposal.stage_started has no proposal.staged and is before
 * expires_at; after it the stage is lost (the server stopped) and may be started again.
 */
export function stageRunning(p: ProposalState, now: number): boolean {
  const r = p.stage_running
  if (!r) return false
  const until = Date.parse(r.expires_at)
  return Number.isNaN(until) ? true : until > now
}

/** The approve buttons are off while the server says a healthy stage is missing. */
export function stagingRequired(p: ProposalState): boolean {
  return p.status === 'open' && p.staging_required === true
}

/**
 * Whether a stage that just arrived lifts the requirement without asking the server: the stage is
 * of this very fix and every stageable step (a superset of the required ones) is healthy. Any
 * other stage leaves the question to the server's snapshot, which the hook asks for after it.
 */
export function stageLiftsRequirement(p: ProposalState, staged: ProposalStagedPayload): boolean {
  if (staged.params_hash !== p.params_hash) return false
  const steps = p.stageable && p.stageable.length > 0 ? p.stageable : (staged.steps ?? []).map((s) => s.step_index)
  return steps.length > 0 && steps.every((i) => stepHealthy(staged, i))
}

/** "Try in staging" is the driver's, on an open room, where the snapshot says staging is on and a step can stage. */
export function canTryStaging(
  p: ProposalState,
  opts: { stagingOn: boolean; isDriver: boolean; viewer: boolean; roomOpen: boolean },
): boolean {
  return (
    opts.stagingOn &&
    opts.isDriver &&
    !opts.viewer &&
    opts.roomOpen &&
    p.status === 'open' &&
    (p.stageable?.length ?? 0) > 0
  )
}

/** Why approve is off, said the same way on the card, its buttons and the decision bar. */
export const STAGING_REQUIRED_SHORT = 'Staging required'
export const STAGING_REQUIRED_HINT = 'Approving is off until this fix has run healthy in staging.'

const STAGE_ERRORS: Record<string, string> = {
  stage_in_progress: 'A stage of this fix is already running.',
  rate_limited: 'This fix has been staged five times already.',
  no_staging: "This pipeline has no staging stack, or write mode is off.",
  not_stageable: 'None of this fix’s steps can be tried in staging.',
  not_driver: 'Only the driver can try a fix in staging.',
  room_closed: 'The room is closed.',
  already_decided: 'This fix has already been decided.',
}

/** The words for a failed "Try in staging": ours for the documented codes, the server's text otherwise. */
export function stageErrorText(attempt: { code?: string; error?: string }): string {
  return (attempt.code && STAGE_ERRORS[attempt.code]) || attempt.error || 'Piperoom did not start the stage. Try again.'
}

/** 61 s reads "1 min 1 s"; under a second, milliseconds. */
export function formatStageDuration(ms: number): string {
  if (!Number.isFinite(ms) || ms < 0) return ''
  if (ms < 1000) return `${Math.round(ms)} ms`
  const s = ms / 1000
  if (s < 60) return `${s.toFixed(1)} s`
  const whole = Math.round(s)
  return `${Math.floor(whole / 60)} min ${whole % 60} s`
}

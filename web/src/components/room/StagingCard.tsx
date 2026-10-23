// "Try in staging" on the proposal card (X8e; docs/api.md "X8 staging").
//
// Rules that do not move with the look:
//  - Everything staging says (`detail`, `leftover`, `target`, `error`, the server's 409 text) is
//    untrusted text: rendered as text nodes, bidi controls stripped, never as markup.
//  - The button is the driver's (canTryStaging); everyone in the room reads the result.
//  - A stage counts only for the fix it ran on: one with another params_hash is drawn as stale.
//  - "Staging required" is a fact the server states (snapshot, 409); the card only shows it and
//    the approve buttons go off through lib/staging.ts `stagingRequired`.
//  - A stage whose stage_started is past expires_at with no proposal.staged is lost (the server
//    stopped): the card says so and offers the button again.
import { useEffect, useState } from 'react'
import { CircleCheck, CircleX, CircleHelp, CircleSlash, FlaskConical, LoaderCircle, TriangleAlert } from 'lucide-react'
import type { StageOutcome, StageStepResult } from '../../api/types'
import type { ProposalState } from '../../state/types'
import type { StageAttempt } from '../../hooks/useRoom'
import { Badge } from '@/components/ui/badge'
import { Button } from '@/components/ui/button'
import { PersonName } from '@/components/VerifiedBadge'
import { cn } from '@/lib/utils'
import { stripBidi } from '../../lib/title'
import {
  STAGING_REQUIRED_HINT,
  formatStageDuration,
  stageErrorText,
  STAGING_REQUIRED_SHORT,
  stageIsCurrent,
  stageOutcome,
  stageRunning,
  stagedVerdict,
} from '../../lib/staging'
import type { StagedVerdict } from '../../lib/staging'
import { Clock } from './Clock'
import { CollapsibleText } from './CollapsibleText'

/** After a stage is accepted, how long the button waits for the stage_started event before it comes back. */
const ACCEPT_WAIT_MS = 8000

const VERDICT: Record<StagedVerdict, { label: string; variant: 'success' | 'destructive' | 'neutral' | 'warning'; sentence: string }> = {
  healthy: { label: 'Healthy', variant: 'success', sentence: 'The fix ran healthy in staging.' },
  failed: { label: 'Failed', variant: 'destructive', sentence: 'The fix failed in staging. Production was not touched.' },
  not_applied: { label: 'Not applied', variant: 'neutral', sentence: 'Staging could not try the fix (see why below).' },
  no_verdict: { label: 'No verdict', variant: 'warning', sentence: 'Staging gave no verdict for a step (see below).' },
}

const OUTCOME_ICON: Record<StageOutcome | 'no_verdict', typeof CircleCheck> = {
  healthy: CircleCheck,
  failed: CircleX,
  not_applied: CircleSlash,
  no_verdict: CircleHelp,
}

const text = (value: string | undefined) => (value ? stripBidi(value) : '')

function StepResult({ step, proposal }: { step: StageStepResult; proposal: ProposalState }) {
  const action = proposal.steps[step.step_index]?.action
  const r = step.result
  const outcome: StageOutcome | 'no_verdict' = step.error || !r ? 'no_verdict' : stageOutcome(r)
  const Icon = OUTCOME_ICON[outcome]
  const label = outcome === 'no_verdict' ? 'No verdict' : outcome === 'not_applied' ? 'Not applied' : outcome === 'healthy' ? 'Healthy' : 'Failed'
  const variant = outcome === 'healthy' ? 'success' : outcome === 'failed' ? 'destructive' : outcome === 'no_verdict' ? 'warning' : 'neutral'
  const dry = r?.dry_run
  return (
    <li className="flex flex-col gap-1" data-testid={`stage-step-${step.step_index}`} data-outcome={outcome}>
      <div className="flex flex-wrap items-center gap-2">
        <span className="text-xs text-subtle-foreground">Step {step.step_index + 1}</span>
        {action && <span className="font-mono text-[13px] font-medium">{text(action)}</span>}
        <Badge variant={variant}>
          <Icon className="size-3" aria-hidden="true" />
          {label}
        </Badge>
        {r && formatStageDuration(r.duration_ms) && (
          <span className="text-xs text-subtle-foreground">{formatStageDuration(r.duration_ms)}</span>
        )}
      </div>
      {r?.target && <span className="break-words font-mono text-xs text-muted-foreground">{text(r.target)}</span>}
      {step.error && (
        <p className="m-0 break-words text-xs text-warning-foreground">
          <span className="font-medium">No verdict: </span>
          {text(step.error)}
        </p>
      )}
      {r?.detail && <CollapsibleText text={text(r.detail)} className="font-mono text-xs text-muted-foreground" />}
      {dry && !dry.skipped && (
        <p className="m-0 break-words text-xs text-muted-foreground">
          <span className="font-medium">Staging’s own check {dry.ok ? 'accepted' : 'refused'} the change</span>
          {dry.source && <> · {text(dry.source)}</>}
          {dry.text && <>: {text(dry.text)}</>}
        </p>
      )}
      {r?.leftover && (
        <p className="m-0 flex items-start gap-1 break-words text-xs text-warning-foreground" data-testid={`stage-leftover-${step.step_index}`}>
          <TriangleAlert className="mt-px size-3 shrink-0" aria-hidden="true" />
          <span>
            <span className="font-medium">Left on staging: </span>
            {text(r.leftover)}
          </span>
        </p>
      )}
    </li>
  )
}

export function StagingCard({
  proposal,
  required,
  canTry,
  onStage,
  noteId,
}: {
  proposal: ProposalState
  /** Approve is off until a healthy stage exists (stagingRequired, on an open room). */
  required: boolean
  /** canTryStaging: the driver, on an open room, where staging is on and a step can be staged. */
  canTry: boolean
  /** Absent in hosts from before X8: no button. */
  onStage?: (proposalId: string) => Promise<StageAttempt>
  /** The id of the "staging required" notice: the disabled approve buttons point at it. */
  noteId: string
}) {
  const staged = proposal.staged
  const running = proposal.stage_running
  const [now, setNow] = useState(() => Date.now())
  const live = !!running
  useEffect(() => {
    if (!live) return
    setNow(Date.now())
    const t = window.setInterval(() => setNow(Date.now()), 1000)
    return () => window.clearInterval(t)
  }, [live])

  const [busy, setBusy] = useState(false)
  const [error, setError] = useState<string | null>(null)
  // The event that follows the 202 (stage_started) or a result (staged) ends the wait.
  useEffect(() => {
    setBusy(false)
  }, [running, staged])

  const isRunning = stageRunning(proposal, now)
  const lost = !!running && !isRunning
  const showButton = canTry && !!onStage && !isRunning && proposal.status === 'open'
  if (!required && !staged && !running && !showButton) return null

  const start = async () => {
    if (!onStage || busy) return
    setBusy(true)
    setError(null)
    const res = await onStage(proposal.proposal_id)
    if (!res.ok) {
      setBusy(false)
      setError(stageErrorText(res))
      return
    }
    window.setTimeout(() => setBusy(false), ACCEPT_WAIT_MS)
  }

  const current = stageIsCurrent(proposal)
  const verdict = staged ? stagedVerdict(staged) : null
  const stepNumbers = (running?.steps ?? []).map((i) => i + 1).join(', ')

  return (
    <section aria-label="Staging" className="flex flex-col gap-2 rounded-md border border-border bg-muted/40 px-3 py-2.5" data-testid="staging-card">
      <div className="flex flex-wrap items-center gap-2">
        <h4 className="m-0 flex items-center gap-1.5 text-xs font-medium uppercase tracking-wide text-subtle-foreground">
          <FlaskConical className="size-3.5" aria-hidden="true" />
          Staging
        </h4>
        {verdict && (
          <Badge variant={current ? VERDICT[verdict].variant : 'outline'} data-testid="stage-verdict">
            {VERDICT[verdict].label}
            {!current && ' · stale'}
          </Badge>
        )}
        {staged && (
          <span className="text-xs text-subtle-foreground">
            Tried <Clock iso={staged.at} />
          </span>
        )}
        {showButton && (
          <Button
            variant="outline"
            size="sm"
            className="ml-auto coarse:h-11 coarse:px-4"
            disabled={busy}
            aria-describedby={required ? noteId : undefined}
            onClick={() => void start()}
          >
            {busy && <LoaderCircle className="animate-spin" aria-hidden="true" />}
            {staged ? 'Try again in staging' : 'Try in staging'}
          </Button>
        )}
      </div>

      {required && (
        <p id={noteId} role="status" className="m-0 break-words text-xs font-medium text-warning-foreground" data-testid="staging-required">
          {STAGING_REQUIRED_SHORT}. {STAGING_REQUIRED_HINT}
          {!canTry && ' Ask the driver to try it in staging.'}
          {proposal.staging_message && <span className="block font-normal text-muted-foreground">{text(proposal.staging_message)}</span>}
        </p>
      )}

      {isRunning && running && (
        <p role="status" className="m-0 flex flex-wrap items-center gap-x-1.5 gap-y-0.5 text-xs text-muted-foreground" data-testid="stage-running">
          <LoaderCircle className="size-3.5 animate-spin" aria-hidden="true" />
          <span>Trying {stepNumbers ? `step ${stepNumbers} ` : 'the fix '}in staging…</span>
          <span className="inline-flex items-center gap-1">
            started by <PersonName person={running.by} /> <Clock iso={running.started_at} />
          </span>
        </p>
      )}
      {lost && running && (
        <p className="m-0 text-xs text-warning-foreground" data-testid="stage-lost">
          The stage started at <Clock iso={running.started_at} /> never reported back (the server may have restarted).
          {canTry ? ' Try again.' : ''}
        </p>
      )}

      {staged && verdict && (
        <div className={cn('flex flex-col gap-2', !current && 'opacity-70')} data-testid="stage-result" data-current={current}>
          <p className="m-0 text-xs text-muted-foreground">
            {current
              ? VERDICT[verdict].sentence
              : 'This stage ran on an earlier version of the fix and does not count for this one.'}
          </p>
          <ol className="m-0 flex list-none flex-col gap-2 p-0">
            {(staged.steps ?? []).map((s) => (
              <StepResult key={s.step_index} step={s} proposal={proposal} />
            ))}
          </ol>
        </div>
      )}

      {error && (
        <p role="alert" className="m-0 text-xs font-medium text-destructive-fg" data-testid="stage-error">
          {error}
        </p>
      )}
    </section>
  )
}

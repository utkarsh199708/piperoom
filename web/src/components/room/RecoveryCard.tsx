import { CircleCheck, CircleHelp, CircleX, LoaderCircle, TriangleAlert } from 'lucide-react'
import type { AgentVerificationPayload, RecoveryOutcome } from '../../api/types'
import type { ProposalState, RecoveryProgress as RecoveryProgressState } from '../../state/types'
import { cn } from '@/lib/utils'
import { asCount, checkingLabel, countWithUnit, gapLine, outcomeView, tookLabel } from '../../lib/recovery'
import type { RecoveryTone } from '../../lib/recovery'

const TONE_TEXT: Record<RecoveryTone, string> = {
  success: 'text-success-foreground',
  warning: 'text-warning-foreground',
  danger: 'text-destructive-fg',
  muted: 'text-muted-foreground',
}

// The word is the primary signal; the icon repeats it for people who do not see the colour.
const OUTCOME_ICON: Record<RecoveryOutcome, typeof CircleCheck> = {
  recovered: CircleCheck,
  partial: TriangleAlert,
  not_recovered: CircleX,
  unverifiable: CircleHelp,
}

/**
 * While the agent polls a recovery step's Verify (agent.recovery_check): "checking (2 of 8)" and the
 * count still missing. Progress only, never a verdict, so it says nothing about success. Renders
 * nothing for a proposal that is not a recovery, has no poll yet, or already has its verdict.
 */
export function RecoveryProgress({ proposal }: { proposal: ProposalState }) {
  const check = proposal.recovery?.check
  if (proposal.class !== 'recovery' || !check || proposal.verification) return null
  const gap = gapLine(check.before, check.after, check.unit)
  return (
    <div className="flex flex-wrap items-center gap-2 px-3.5 py-2.5 text-sm" role="status" aria-live="polite" data-testid="recovery-progress">
      <LoaderCircle className="size-4 shrink-0 text-agent-foreground motion-safe:animate-spin" role="img" aria-label="checking" />
      <span className="text-xs font-medium text-agent-foreground">{checkingLabel(check)}</span>
      {gap && <span className="text-xs tabular-nums text-muted-foreground">{gap}</span>}
    </div>
  )
}

/**
 * The counted verdict of a recovery proposal: the outcome word, "412 missing → 0 rows" and, when it
 * recovered, how long it took. `partial` and `not_recovered` say how much is still missing.
 * `unverifiable` has no counts to show (the source or sink could not count).
 */
export function RecoveryResult({
  verification,
  outcome,
  recovery,
}: {
  verification: AgentVerificationPayload
  outcome: RecoveryOutcome
  recovery?: RecoveryProgressState
}) {
  const view = outcomeView(outcome)
  const Icon = OUTCOME_ICON[outcome]
  const counted = outcome !== 'unverifiable'
  const gap = counted ? gapLine(verification.before, verification.after, verification.unit) : undefined
  const left = counted ? asCount(verification.after) : undefined
  const took = outcome === 'recovered' ? tookLabel(recovery?.ran_at, recovery?.verified_at) : ''
  return (
    <div className="flex flex-col gap-0.5" data-testid="recovery-result" data-outcome={outcome}>
      <p className={cn('m-0 inline-flex items-center gap-1.5 font-medium', TONE_TEXT[view.tone])}>
        <Icon className="size-4 shrink-0" aria-hidden="true" />
        {view.word}
      </p>
      {(gap || took) && (
        <p className="m-0 text-sm tabular-nums">
          {gap}
          {gap && took && ' · '}
          {took && `recovered in ${took}`}
          {(outcome === 'partial' || outcome === 'not_recovered') && left !== undefined && left > 0 && (
            <span className={cn('ml-1', TONE_TEXT[view.tone])}>· {countWithUnit(left, verification.unit)} still missing</span>
          )}
        </p>
      )}
      {outcome === 'unverifiable' && <p className="m-0 text-xs text-muted-foreground">The missing data could not be counted, so recovery is not claimed.</p>}
    </div>
  )
}

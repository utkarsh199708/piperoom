// The policy countdown at the head of a proposal card (P2c-3): "Runs in 0:42 · policy auto_approve
// · Cancel". Rules that do not move with the look:
//  - cancel_until is shown as "the policy may run from <time>", never as a deadline (#629): a
//    cancel after it still wins if it lands first.
//  - Past run_at plus STALE_COUNTDOWN_GRACE_MS the countdown is hidden (#652), the proposal waits
//    for a person, and Cancel stays for those allowed it, since the server still takes it.
//  - Cancel is a write: canCancelSchedule is its only gate (no button for a viewer).
//  - aria-live is polite in the last ANNOUNCE_LAST_S seconds only; a screen reader is not read a
//    number every second for a minute.
import { useEffect, useState } from 'react'
import { Timer } from 'lucide-react'
import { Button } from '@/components/ui/button'
import { ANNOUNCE_LAST_S, countdownOf, formatRemaining, policyLabel } from '@/lib/countdown'
import { clockTimeSeconds, utcLabel } from '@/lib/time'
import type { CancelResult } from '../../hooks/useRoom'
import type { ScheduledState } from '../../state/types'

export function PolicyCountdown({
  scheduled,
  onCancel,
}: {
  scheduled: ScheduledState
  /** Set only when this person may cancel (canCancelSchedule) and the host can. */
  onCancel?: () => Promise<CancelResult>
}) {
  const [now, setNow] = useState(() => Date.now())
  const { phase, remaining_s } = countdownOf(scheduled, now)
  const ticking = phase !== 'stale'
  useEffect(() => {
    if (!ticking) return
    const t = window.setInterval(() => setNow(Date.now()), 1000)
    return () => window.clearInterval(t)
  }, [ticking])

  const [busy, setBusy] = useState(false)
  const [note, setNote] = useState<string | null>(null)
  const cancel = async () => {
    if (!onCancel || busy) return
    setBusy(true)
    setNote(null)
    const res = await onCancel()
    // On success the countdown leaves when proposal.cancelled arrives; until then stay busy.
    if (res.ok) return
    setBusy(false)
    if (res.notScheduled) setNote('Nothing to cancel: the policy has started, or someone cancelled first.')
  }

  const policy = policyLabel(scheduled.policy)
  const from = (
    <time dateTime={scheduled.cancel_until} title={utcLabel(scheduled.cancel_until)} className="tabular-nums">
      {clockTimeSeconds(scheduled.cancel_until)}
    </time>
  )
  const announce = phase === 'counting' && remaining_s <= ANNOUNCE_LAST_S

  return (
    <div
      className="flex flex-col gap-0.5 border-b border-border bg-info-subtle px-3.5 py-1.5 text-info-foreground first:rounded-t-lg"
      data-testid="policy-countdown"
      data-phase={phase}
    >
      <div className="flex min-h-8 flex-wrap items-center gap-x-2 gap-y-0.5 text-sm font-medium">
        <Timer className="size-3.5 shrink-0" aria-hidden="true" />
        {phase === 'stale' ? (
          <span>The {policy} has not run this fix · it waits for approval</span>
        ) : (
          <>
            <span aria-live={announce ? 'polite' : 'off'} aria-atomic="true" data-testid="countdown-time" className="tabular-nums">
              {phase === 'counting' ? `Runs in ${formatRemaining(remaining_s)}` : 'Running any moment'}
            </span>
            <span aria-hidden="true">·</span>
            <span>{policy}</span>
          </>
        )}
        {onCancel && (
          <Button
            variant="outline"
            size="sm"
            className="ml-auto h-7 bg-card px-2 text-xs"
            onClick={() => void cancel()}
            disabled={busy}
          >
            {busy ? 'Cancelling…' : phase === 'stale' ? 'Cancel the policy' : 'Cancel'}
          </Button>
        )}
      </div>
      {phase !== 'stale' && (
        <p className="text-xs font-normal text-muted-foreground" data-testid="countdown-from">
          The policy may run from {from} unless someone cancels.
          {!onCancel && ' The driver or an owner of one of its layers can cancel.'}
        </p>
      )}
      {note && (
        <p className="text-xs font-normal text-muted-foreground" role="status">
          {note}
        </p>
      )}
    </div>
  )
}

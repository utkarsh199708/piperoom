import { useState } from 'react'
import { CircleCheck, CircleX } from 'lucide-react'
import type { AgentVerificationPayload, SinkCheck } from '../../api/types'
import type { RecoveryProgress } from '../../state/types'
import { Badge, layerVariant } from '@/components/ui/badge'
import { Button } from '@/components/ui/button'
import { Kbd } from '@/components/ui/kbd'
import { cn } from '@/lib/utils'
import { PersonName } from '@/components/VerifiedBadge'
import { clockTime } from '../../lib/time'
import { verdictKind, verdictWords } from '../../lib/execution'
import { recoveryOutcome } from '../../lib/recovery'
import { RecoveryResult } from './RecoveryCard'

export interface CloseNudge {
  /** The room is open, so closing it is still possible. */
  roomOpen: boolean
  /** The driver, as the server reports them. */
  driver?: { id: string; name?: string; verified?: boolean }
  isDriver: boolean
  /** Opens the close dialog with the resolution filled in. */
  onClose?: (initialText: string) => void
}

function CheckRow({ check }: { check: AgentVerificationPayload['checks'][number] }) {
  const [open, setOpen] = useState(false)
  const long = check.detail.length > 120 || check.detail.includes('\n')
  return (
    <li className="flex flex-wrap items-start gap-2" data-testid="verification-check">
      <Badge variant={layerVariant(check.layer)} mono>
        {check.layer}
      </Badge>
      <span className="font-mono text-xs">{check.name}</span>
      {check.ok ? (
        <CircleCheck className="size-4 text-success-foreground" role="img" aria-label="passed" />
      ) : (
        <CircleX className="size-4 text-destructive-fg" role="img" aria-label="failed" />
      )}
      <span className="basis-full">
        <span className={cn('block break-words font-mono text-xs text-muted-foreground', !open && 'line-clamp-2')}>{check.detail}</span>
        {long && (
          <button type="button" className="text-xs text-muted-foreground underline" aria-expanded={open} onClick={() => setOpen((o) => !o)}>
            {open ? 'Show less' : 'Show more'}
          </button>
        )}
      </span>
    </li>
  )
}

function sinkLine(s: SinkCheck): string {
  const parts = [s.table]
  if (s.newest) parts.push(`newest ${clockTime(s.newest)}`)
  if (s.baseline) parts.push(`after baseline ${clockTime(s.baseline)}`)
  if (s.passed && s.passed_by) parts.push(`${s.passed_by} ✓`)
  else if (!s.passed) parts.push('no new rows ✗')
  return parts.join(' · ')
}

// The agent's verdict after the last step ran: which layer checks it made, whether data flows
// again, and what the person can do next. It never claims more than the verdict says.
export function VerificationCard({
  verification,
  title,
  nudge,
  recovery,
}: {
  verification: AgentVerificationPayload
  /** The proposal's title, for the pre-filled resolution. */
  title: string
  nudge: CloseNudge
  /** X2: the recovery proposal's timing, for "recovered in 3 min". */
  recovery?: RecoveryProgress
}) {
  if (verification.repeat) {
    return (
      <p role="status" className="m-0 px-3.5 py-2 text-xs text-muted-foreground">
        Already verified · no checks re-run
      </p>
    )
  }
  const kind = verdictKind(verification)
  // X2: a recovery proposal carries a counted outcome, which says more than the boolean (partial is not "not recovered").
  const outcome = recoveryOutcome(verification)
  const ok = outcome ? outcome === 'recovered' && verification.recovered : kind === 'recovered'
  const flow = verification.data_flow
  return (
    <div role="status" className="flex flex-col gap-2 px-3.5 py-2.5 text-sm" data-testid="verification-card">
      <div className="flex flex-col gap-0.5">
        {outcome ? (
          <RecoveryResult verification={verification} outcome={outcome} recovery={recovery} />
        ) : (
          <p
            className={cn(
              'm-0 font-medium',
              ok ? 'text-success-foreground' : kind === 'not-recovered' ? 'text-destructive-fg' : 'text-warning-foreground',
            )}
          >
            {ok || kind === 'not-recovered' ? `Verified · ${verdictWords(kind)}` : kind === 'inconclusive' ? 'Inconclusive' : 'Not verified'}
          </p>
        )}
        <p className="m-0 max-w-[72ch] text-sm leading-5 text-muted-foreground">{verification.verdict}</p>
      </div>
      {verification.checks.length > 0 && (
        <ul className="m-0 flex list-none flex-col gap-1.5 p-0">
          {verification.checks.map((c, i) => (
            <CheckRow key={i} check={c} />
          ))}
        </ul>
      )}
      {flow &&
        (!flow.checked ? (
          <p className="m-0 text-xs text-muted-foreground">Data flow not verified: {flow.detail ?? 'no reading'}</p>
        ) : (
          <ul className="m-0 flex list-none flex-col gap-0.5 p-0">
            {(flow.sinks ?? []).map((s) => (
              <li key={s.table} className={cn('text-xs tabular-nums', flow.fresh && s.passed ? 'text-success-foreground' : 'text-muted-foreground')}>
                {sinkLine(s)}
              </li>
            ))}
          </ul>
        ))}
      {ok && nudge.roomOpen && (
        <div className="flex flex-wrap items-center gap-2 border-t border-border pt-2">
          <span className="font-medium">Pipeline recovered.</span>
          {nudge.isDriver && nudge.onClose ? (
            <Button size="sm" aria-keyshortcuts="C" onClick={() => nudge.onClose?.(`Fixed by "${title}"`)}>
              Close room
              <Kbd aria-hidden="true" className="max-sm:hidden coarse:hidden">
                C
              </Kbd>
            </Button>
          ) : nudge.driver ? (
            <span className="inline-flex items-center gap-1 text-xs text-muted-foreground">
              <PersonName person={{ ...nudge.driver, kind: 'human' }} /> can close the room
            </span>
          ) : (
            <span className="text-xs text-muted-foreground">Take control to close</span>
          )}
        </div>
      )}
    </div>
  )
}

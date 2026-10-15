import { useEffect, useState } from 'react'
import type { ProposalState } from '../../state/types'
import type { Topology } from '../../api/types'
import { Badge } from '@/components/ui/badge'
import { Clock } from './Clock'
import { StatusDot } from './StatusDot'

/** How long the "Previewing…" skeleton shows before it gives way to the muted "unavailable". */
export const PREVIEW_WAIT_MS = 20_000

function useElapsed(since: string | undefined, enabled: boolean): number {
  const start = since ? Date.parse(since) : NaN
  const [now, setNow] = useState(() => Date.now())
  useEffect(() => {
    if (!enabled || Number.isNaN(start)) return
    const left = PREVIEW_WAIT_MS - (Date.now() - start)
    if (left <= 0) return
    const t = window.setTimeout(() => setNow(Date.now()), left + 50)
    return () => window.clearTimeout(t)
  }, [enabled, start])
  return Number.isNaN(start) ? PREVIEW_WAIT_MS : now - start
}

// "This touches": what each step would do, read before anyone approves. Informational only: the
// text never changes the fingerprint, and a missing or failed preview never blocks an approval.
export function BlastRadius({ proposal, topology }: { proposal: ProposalState; topology: Topology | null }) {
  const preview = proposal.preview
  const waiting = !preview
  const elapsed = useElapsed(proposal.created_at, waiting && proposal.status === 'open')

  if (waiting) {
    if (proposal.status !== 'open') return null
    return elapsed < PREVIEW_WAIT_MS ? (
      <p className="m-0 animate-shimmer text-xs text-muted-foreground" role="status">
        Previewing what this will do…
      </p>
    ) : (
      <p className="m-0 text-xs text-muted-foreground">Preview unavailable</p>
    )
  }

  const nameOf = (id: string) => topology?.nodes.find((n) => n.id === id)?.name ?? id
  return (
    <section aria-label="What this touches" className="flex flex-col gap-2 rounded-md border border-border bg-muted/40 px-3 py-2.5">
      <div className="flex items-baseline gap-2">
        <h4 className="m-0 text-xs font-medium uppercase tracking-wide text-subtle-foreground">This touches</h4>
        <span className="ml-auto text-xs text-subtle-foreground">
          Previewed <Clock iso={preview.at} />
        </span>
      </div>
      {preview.steps.map((sp) => {
        const step = proposal.steps[sp.step_index]
        const pv = sp.preview
        return (
          <div key={sp.step_index} className="flex flex-col gap-1" data-testid={`preview-step-${sp.step_index}`}>
            {sp.error && (
              <p className="m-0 break-words text-xs text-warning-foreground">
                Step {sp.step_index + 1}
                {step ? ` (${step.action})` : ''}: Preview failed: {sp.error}
              </p>
            )}
            {pv && (
              <>
                <div className="flex flex-wrap items-center gap-2">
                  <span className="font-mono text-[13px] font-medium leading-[18px]">{pv.target}</span>
                  <Badge variant="outline" title={pv.reversible ? pv.rollback : undefined}>
                    {pv.reversible ? 'reversible' : 'not reversible'}
                  </Badge>
                </div>
                <p className="m-0 break-words text-[13px] leading-[18px]">
                  <span className="text-xs font-medium text-subtle-foreground">Current: </span>
                  {pv.current}
                  <span className="mx-1.5 text-subtle-foreground" aria-hidden="true">
                    →
                  </span>
                  <span className="text-xs font-medium text-subtle-foreground">Expected: </span>
                  {pv.expected}
                </p>
                <p className="m-0 break-words text-[13px] leading-[18px]">
                  <span className="text-xs font-medium text-subtle-foreground">Downstream: </span>
                  {pv.downstream && pv.downstream.length > 0 ? pv.downstream.map(nameOf).join(' → ') : 'downstream unknown'}
                </p>
                {pv.dry_run && <DryRun dry={pv.dry_run} />}
              </>
            )}
          </div>
        )
      })}
    </section>
  )
}

function DryRun({ dry }: { dry: NonNullable<NonNullable<ProposalState['preview']>['steps'][number]['preview']>['dry_run'] }) {
  if (!dry) return null
  if (dry.skipped) return <p className="m-0 text-xs text-muted-foreground">Skipped: {dry.skipped}</p>
  const multi = dry.text.includes('\n')
  return (
    <div className="flex flex-col gap-1 text-xs">
      <span className="inline-flex items-center gap-1.5">
        <StatusDot tone={dry.ok ? 'ok' : 'fail'} />
        <span className="font-medium">{dry.ok ? 'Would be accepted' : 'Would be rejected'}</span>
        <span className="text-muted-foreground">· {dry.source}</span>
      </span>
      {dry.text &&
        (multi ? (
          <pre className="m-0 max-h-32 overflow-auto whitespace-pre-wrap break-words rounded bg-muted px-2 py-1 font-mono text-xs">{dry.text}</pre>
        ) : (
          <span className="break-words text-muted-foreground">{dry.text}</span>
        ))}
    </div>
  )
}

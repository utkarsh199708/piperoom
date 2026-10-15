import type { RoomState } from '@/state/types'
import { Badge, layerVariant } from '@/components/ui/badge'
import { TRIAGE_FAILED_SHOW_MS, stepAnchor, triageReasonText, triageStepNumbers } from '@/lib/triage'
import { useNow } from './useNow'

// The triage pass's first look (F6) under the room title: the one-line diagnosis the
// on-call reads before the full investigation has a hypothesis. It is labelled
// "triage, unverified" and every text in it is model or system output, rendered as
// plain text only. Once a hypothesis exists it shrinks to one muted line.
export function TriageCard({ state }: { state: Pick<RoomState, 'triage' | 'hypothesis' | 'steps'> }) {
  const now = useNow(5_000)
  const t = state.triage
  if (!t) return null

  if (t.status !== 'ok' || !t.headline) {
    const at = Date.parse(t.at)
    if (Number.isFinite(at) && now - at > TRIAGE_FAILED_SHOW_MS) return null
    return (
      <p className="m-0 text-xs text-muted-foreground" data-testid="triage-failed">
        First look unavailable · {triageReasonText(t.reason)}
      </p>
    )
  }

  if (state.hypothesis) {
    return (
      <p className="m-0 truncate text-xs text-muted-foreground" title={t.headline} data-testid="triage-collapsed">
        First look: {t.headline}
      </p>
    )
  }

  const refs = triageStepNumbers(t, state.steps)
  return (
    <section
      aria-label="First look"
      className="flex flex-col gap-1.5 rounded-lg border border-agent/30 bg-card px-3 py-2.5 text-sm"
    >
      <div className="flex flex-wrap items-center gap-2 text-xs text-muted-foreground">
        <Badge variant="agent">First look</Badge>
        <span>triage, unverified</span>
        {t.confidence && (
          <>
            <span aria-hidden="true">·</span>
            <span>{t.confidence}</span>
          </>
        )}
        {t.suspected_layer && <Badge variant={layerVariant(t.suspected_layer)}>{t.suspected_layer}</Badge>}
      </div>
      <p className="m-0 break-words text-foreground" aria-live="polite">
        {t.headline}
      </p>
      {(refs.length > 0 || t.next_check) && (
        <div className="flex flex-wrap items-center gap-x-3 gap-y-1 text-xs text-muted-foreground">
          {refs.length > 0 && (
            <span className="inline-flex flex-wrap items-center gap-1">
              based on steps
              {refs.map((r) => (
                <a
                  key={r.id}
                  href={`#${stepAnchor(r.id)}`}
                  className="rounded-sm border border-border px-1 font-mono text-foreground hover:bg-accent focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring"
                >
                  {r.n}
                </a>
              ))}
            </span>
          )}
          {t.next_check && <span className="break-words">Next: {t.next_check}</span>}
        </div>
      )}
    </section>
  )
}

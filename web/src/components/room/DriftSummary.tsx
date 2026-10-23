import type { DriftFact } from '@/lib/drift'
import { driftLabel, driftPhrase, windowClock } from '@/lib/drift'
import { utcLabel } from '@/lib/time'
import { cn } from '@/lib/utils'
import { StatusDot } from './StatusDot'

// The header shows at most this many drift lines; the rest are counted.
const MAX_LINES = 3

/**
 * The first lines of a watcher-opened room (X1): what drift the watcher saw, where and over which window, e.g.
 * "count drift · public.orders · 2.1 % rows missing over 1 h · 10:00–11:00 UTC". Every string came from the
 * evidence and is rendered as text.
 */
export function DriftSummary({ facts }: { facts?: DriftFact[] }) {
  if (!facts || facts.length === 0) return null
  const shown = facts.slice(0, MAX_LINES)
  return (
    <ul aria-label="Drift the watcher saw" className="m-0 flex list-none flex-col gap-1 p-0">
      {shown.map((f, i) => {
        const where = f.table || f.topic
        const when = windowClock(f)
        return (
          <li
            key={i}
            data-drift={f.kind ?? 'unknown'}
            className="flex min-w-0 flex-wrap items-center gap-x-2 gap-y-0.5 text-sm tabular-nums"
          >
            <span className={cn('inline-flex items-center gap-1.5 font-medium', f.kind ? 'text-warning-foreground' : 'text-muted-foreground')}>
              <StatusDot tone={f.kind ? 'warn' : 'muted'} />
              {driftLabel(f)}
            </span>
            {where && <span className="min-w-0 max-w-full truncate font-mono text-muted-foreground">{where}</span>}
            <span className="min-w-0 break-words">{driftPhrase(f)}</span>
            {when && (
              <span className="text-xs text-subtle-foreground" title={`${utcLabel(f.from)} to ${utcLabel(f.to)}`}>
                {when}
              </span>
            )}
            {f.detail && (
              <span className="basis-full min-w-0 break-words text-xs text-muted-foreground">{f.detail}</span>
            )}
          </li>
        )
      })}
      {facts.length > shown.length && (
        <li className="text-xs text-subtle-foreground">+{facts.length - shown.length} more drift signals</li>
      )}
    </ul>
  )
}

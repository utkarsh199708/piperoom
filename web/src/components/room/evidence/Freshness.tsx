import { isFreshness } from '@/lib/evidenceShapes'
import type { ToolCallState } from '@/state/types'
import { Raw } from './Raw'

/** "09:58:14" (UTC) from an RFC 3339 time; the text itself when it does not parse. */
function clock(iso: string): string {
  const ms = Date.parse(iso)
  return Number.isNaN(ms) ? iso : new Date(ms).toISOString().slice(11, 19)
}

/** warehouse_freshness: one line, `newest row 09:58:14 · 48m ago`, or `no rows (empty table)`. */
export function Freshness({ t }: { t: ToolCallState }) {
  const d = t.data
  if (!isFreshness(d)) return <Raw t={t} />
  return (
    <p className="m-0 flex flex-wrap items-center gap-x-2 font-mono text-xs">
      <span className="font-medium">{d.table}</span>
      {d.newest === null ? (
        <span className="text-warning-foreground">no rows (empty table)</span>
      ) : (
        <span title={d.newest}>
          newest row {clock(d.newest)} UTC{d.stale_for ? ` · ${d.stale_for} ago` : ''}
        </span>
      )}
    </p>
  )
}

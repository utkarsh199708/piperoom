import { useState } from 'react'
import type { StepState } from '@/state/types'
import { changeLayer, diffText, latestChanges } from '@/lib/f8'
import type { ChangeRow } from '@/lib/f8'
import { timeAgo } from '@/lib/time'
import { Badge, layerVariant } from '@/components/ui/badge'

const utc = (iso: string) => `${iso.replace('T', ' ').replace(/\.\d+Z$|Z$/, '')} UTC`

function ChangeItem({
  row,
  now,
  selectedLayer,
  onSelectLayer,
}: {
  row: ChangeRow
  now: number
  selectedLayer?: string
  onSelectLayer?: (layer: string | null) => void
}) {
  const before = diffText(row.before)
  const after = diffText(row.after)
  const layer = changeLayer(row.kind)
  const badge = (
    <Badge variant={layerVariant(layer)} mono>
      {row.kind}
    </Badge>
  )
  return (
    <li className="flex flex-col gap-1 border-l-2 border-border pl-3">
      <div className="flex flex-wrap items-center gap-x-2 gap-y-1">
        {onSelectLayer && layer ? (
          // Row click filters the step stream to this change's layer (F7); click again to clear.
          // A change kind without a layer (changeLayer -> '') is a plain badge, never a dead filter.
          <button
            type="button"
            aria-pressed={selectedLayer === layer}
            aria-label={`Show only ${layer} steps (${row.kind})`}
            onClick={() => onSelectLayer(selectedLayer === layer ? null : layer)}
            className="rounded-md focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring aria-pressed:ring-2 aria-pressed:ring-ring"
          >
            {badge}
          </button>
        ) : (
          badge
        )}
        <span className="min-w-0 break-all font-mono text-xs">{row.object}</span>
        <time dateTime={row.at} title={utc(row.at)} className="ml-auto text-xs text-subtle-foreground">
          {timeAgo(row.at, now)}
        </time>
      </div>
      {(row.summary || row.actor) && (
        <p className="m-0 break-words text-xs text-muted-foreground">
          {row.actor && <span className="font-medium">{row.actor}: </span>}
          {row.summary}
        </p>
      )}
      {(before || after) && (
        <details className="text-xs">
          <summary className="w-fit cursor-pointer rounded-sm text-subtle-foreground hover:text-foreground focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring">
            Before → after
          </summary>
          {/* Plain text only: a value like "<b>x</b>" must show literally. */}
          <div className="mt-1.5 grid grid-cols-1 gap-2 sm:grid-cols-2">
            <pre aria-label="Before" className="m-0 max-h-48 overflow-auto whitespace-pre-wrap break-words rounded-md border border-border bg-muted px-2.5 py-2 font-mono text-xs">
              {before || '—'}
            </pre>
            <pre aria-label="After" className="m-0 max-h-48 overflow-auto whitespace-pre-wrap break-words rounded-md border border-border bg-muted px-2.5 py-2 font-mono text-xs">
              {after || '—'}
            </pre>
          </div>
        </details>
      )}
    </li>
  )
}

// The "What changed" lane: DDL, connector config and state changes and schema versions before
// the alert, newest first, from the agent's what_changed result. A rule marks when the alert
// fired, so the changes under it read as suspects. Under 768 px it starts as one line (the count).
export function ChangesLane({
  steps,
  alertAt,
  now = Date.now(),
  selectedLayer,
  onSelectLayer,
}: {
  steps: StepState[]
  alertAt: string
  now?: number
  /** The layer the step stream is filtered to, and the setter (F7's filter); without it rows are not clickable. */
  selectedLayer?: string
  onSelectLayer?: (layer: string | null) => void
}) {
  const { changes, seen } = latestChanges(steps)
  const [open, setOpen] = useState(() =>
    typeof window === 'undefined' || !window.matchMedia ? true : window.matchMedia('(min-width: 768px)').matches,
  )
  if (!seen) return null
  const alertMs = Date.parse(alertAt)
  const firstBefore = Number.isNaN(alertMs) ? -1 : changes.findIndex((c) => Date.parse(c.at) < alertMs)
  const rule = (
    <li key="alert-rule" data-testid="alert-rule" className="flex items-center gap-2 text-xs font-medium text-destructive-fg">
      <span className="h-px flex-1 bg-destructive-fg/40" aria-hidden="true" />
      alert fired {alertAt && utc(alertAt)}
      <span className="h-px flex-1 bg-destructive-fg/40" aria-hidden="true" />
    </li>
  )
  const items: React.ReactNode[] = changes.map((c, i) => [
    i === firstBefore ? rule : null,
    <ChangeItem key={`${c.kind}:${c.object}:${c.at}:${c.id}:${i}`} row={c} now={now} selectedLayer={selectedLayer} onSelectLayer={onSelectLayer} />,
  ])
  if (changes.length > 0 && firstBefore === -1 && !Number.isNaN(alertMs)) items.push(rule)
  return (
    <section aria-label="What changed" className="flex flex-col gap-2 rounded-lg border border-border bg-card px-3.5 py-3">
      <button
        type="button"
        aria-expanded={open}
        onClick={() => setOpen((o) => !o)}
        className="flex min-h-8 w-full items-center gap-2 rounded-sm text-left focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring"
      >
        <h2 className="m-0 text-sm font-medium">What changed</h2>
        <span className="text-xs tabular-nums text-subtle-foreground">
          {changes.length === 0 ? 'nothing recorded' : `${changes.length} ${changes.length === 1 ? 'change' : 'changes'}`}
        </span>
      </button>
      {open && changes.length > 0 && <ul className="m-0 flex list-none flex-col gap-3 p-0">{items}</ul>}
    </section>
  )
}

import { useId, useState } from 'react'
import type { ReactNode } from 'react'
import { ChevronDown } from 'lucide-react'
import type { Actor } from '@/api/types'
import type { HandoffState } from '@/state/types'
import { Badge } from '@/components/ui/badge'
import { PersonName } from '@/components/VerifiedBadge'
import { clockTime, utcLabel } from '@/lib/time'
import { neutralizeBadgeText } from '@/lib/verification'
import { cn } from '@/lib/utils'
import { HandoffChecks } from './HandoffChecks'

function SummaryList({ title, items }: { title: string; items: string[] }) {
  if (!items || items.length === 0) return null
  return (
    <div className="flex flex-col gap-1">
      <h4 className="text-xs font-medium text-muted-foreground">{title}</h4>
      <ul className="m-0 flex list-disc flex-col gap-0.5 pl-5">
        {items.map((it, i) => (
          <li key={i} className="break-words">
            {it}
          </li>
        ))}
      </ul>
    </div>
  )
}

/**
 * The six sections of a hand-off summary. Every string is agent-written (untrusted) and is rendered
 * as a React text node: no HTML, no Markdown, no links.
 */
export function HandoffSummary({ summary: s, className }: { summary: HandoffState['summary']; className?: string }) {
  return (
    <div className={cn('handoff__summary flex flex-col gap-3', className)}>
      {s.what_broke && (
        <div className="flex flex-col gap-1">
          <h4 className="text-xs font-medium text-muted-foreground">What broke</h4>
          <p className="m-0 break-words">{s.what_broke}</p>
        </div>
      )}
      {s.why && (
        <div className="flex flex-col gap-1">
          <h4 className="text-xs font-medium text-muted-foreground">Why</h4>
          <p className="m-0 break-words">{s.why}</p>
        </div>
      )}
      <SummaryList title="Done" items={s.done} />
      <SummaryList title="Ruled out" items={s.ruled_out} />
      <SummaryList title="Still open" items={s.open} />
      <SummaryList title="Risks" items={s.risks} />
      <HandoffChecks summary={s} />
    </div>
  )
}

function StatusBadge({ handoff, acceptedAt }: { handoff: HandoffState; acceptedAt?: string }) {
  if (handoff.status === 'accepted') {
    return (
      <Badge variant="success" title={acceptedAt ? utcLabel(acceptedAt) : undefined}>
        accepted{acceptedAt ? ` ${clockTime(acceptedAt)}` : ''}
      </Badge>
    )
  }
  if (handoff.status === 'voided') return <Badge variant="neutral">superseded</Badge>
  return <Badge variant="warning">waiting for {neutralizeBadgeText(handoff.to.name) || handoff.to.id}</Badge>
}

/**
 * One shift change (F2a): who handed the room to whom and when, its state, and the agent's summary.
 * Collapsible; `open` makes it controlled (the receiver's card), otherwise it starts at `defaultOpen`.
 */
export function HandoffCard({
  handoff,
  index,
  from,
  createdAt,
  acceptedAt,
  defaultOpen,
  open: openProp,
  onOpenChange,
  onFocusSelf,
  footer,
  id,
  className,
}: {
  handoff: HandoffState
  /** 0-based shift number: the header reads "Shift {index+1}". */
  index: number
  from: Actor
  createdAt: string
  acceptedAt?: string
  defaultOpen: boolean
  open?: boolean
  onOpenChange?: (open: boolean) => void
  /** The card itself took focus (a "Read summary" jump to `id`). */
  onFocusSelf?: () => void
  /** Buttons under the summary (the receiver's Not yet / Accept). */
  footer?: ReactNode
  id?: string
  className?: string
}) {
  const [openState, setOpenState] = useState(defaultOpen)
  const open = openProp ?? openState
  const bodyId = useId()
  const titleId = useId()
  const toggle = () => {
    const next = !open
    if (openProp === undefined) setOpenState(next)
    onOpenChange?.(next)
  }
  return (
    <section
      id={id}
      tabIndex={id ? -1 : undefined}
      aria-labelledby={titleId}
      data-handoff-status={handoff.status}
      onFocus={onFocusSelf ? (e) => e.target === e.currentTarget && onFocusSelf() : undefined}
      className={cn('rounded-lg border border-border bg-card text-card-foreground focus:outline-none focus-visible:ring-2 focus-visible:ring-ring', className)}
    >
      <div className="flex min-h-11 items-center gap-2 px-3 py-2">
        <button
          type="button"
          aria-expanded={open}
          aria-controls={bodyId}
          aria-label={`Shift ${index + 1} summary`}
          onClick={toggle}
          className="flex size-7 shrink-0 items-center justify-center rounded-md text-muted-foreground hover:bg-accent focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring coarse:size-11"
        >
          <ChevronDown aria-hidden="true" className={cn('size-4 transition-transform motion-reduce:transition-none', !open && '-rotate-90')} />
        </button>
        <h3
          id={titleId}
          className="m-0 flex min-w-0 flex-1 flex-wrap items-center gap-x-1.5 gap-y-1 text-[13px] font-medium leading-[18px]"
        >
          <span>Shift {index + 1}</span>
          <span aria-hidden="true" className="text-subtle-foreground">·</span>
          <span className="inline-flex min-w-0 items-center gap-1">
            from <PersonName person={from} />
          </span>
          <span className="inline-flex min-w-0 items-center gap-1">
            to <PersonName person={handoff.to} />
          </span>
          <span aria-hidden="true" className="text-subtle-foreground">·</span>
          <time dateTime={createdAt} title={utcLabel(createdAt)} className="tabular-nums">
            {clockTime(createdAt)}
          </time>
        </h3>
        <StatusBadge handoff={handoff} acceptedAt={acceptedAt} />
      </div>
      <div id={bodyId} hidden={!open} className="flex flex-col gap-3 border-t border-border px-4 py-3">
        <HandoffSummary summary={handoff.summary} className="max-w-[72ch] text-sm leading-5" />
        {footer && <div className="flex flex-wrap justify-end gap-2">{footer}</div>}
      </div>
    </section>
  )
}

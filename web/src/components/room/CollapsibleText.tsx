import { useId, useState } from 'react'
import { cn } from '@/lib/utils'
import { AgentText } from './AgentText'

/** Prose longer than this (or with more lines) starts collapsed. */
const COLLAPSE_CHARS = 280
const COLLAPSE_LINES = 4

function isLongText(text: string): boolean {
  return text.length > COLLAPSE_CHARS || text.split('\n').length > COLLAPSE_LINES
}

// Long agent prose folds to three lines so a room that ran for hours stays scannable.
// Tool steps are not wrapped in this: they stay visible. Short text renders as is.
export function CollapsibleText({ text, className }: { text: string; className?: string }) {
  const [open, setOpen] = useState(false)
  const id = useId()
  if (!isLongText(text)) return <span className={cn('[overflow-wrap:anywhere]', className)}><AgentText text={text} /></span>
  return (
    // items-start sizes a child to its content, so without max-w-full one long line (a trace, a URL)
    // makes the text wider than its column and it overflows a phone screen instead of wrapping.
    <span className={cn('flex w-full min-w-0 max-w-full flex-col items-start gap-0.5', className)}>
      <span id={id} className={cn('block min-w-0 max-w-full whitespace-pre-line break-words [overflow-wrap:anywhere]', !open && 'line-clamp-3')}>
        <AgentText text={text} />
      </span>
      <button
        type="button"
        aria-expanded={open}
        aria-controls={id}
        onClick={() => setOpen((o) => !o)}
        className="rounded-sm text-xs text-agent-foreground underline-offset-2 hover:underline focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring"
      >
        {open ? 'Show less' : 'Show more'}
      </button>
    </span>
  )
}

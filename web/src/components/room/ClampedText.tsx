import { useId, useState } from 'react'
import { cn } from '@/lib/utils'

/** Text longer than this (or with more lines than LINES) starts folded. */
const CHARS = 280
const LINES = 4

function isLong(text: string): boolean {
  return text.length > CHARS || text.split('\n').length > LINES
}

// Agent messages, activity lines and executor output can be any length (QA9: a 4 kB message
// filled the whole rail). Long text starts folded to a few lines with a "Show more" toggle.
// A length rule, not a measurement, so the result is the same in every layout and in tests.
export function ClampedText({
  text,
  className,
  clampClassName = 'max-h-[4.5rem]',
}: {
  text: string
  className?: string
  /** The folded height. */
  clampClassName?: string
}) {
  const [open, setOpen] = useState(false)
  const id = useId()
  if (!isLong(text)) {
    return <span className={cn('whitespace-pre-wrap [overflow-wrap:anywhere]', className)}>{text}</span>
  }
  return (
    <span className={cn('flex min-w-0 flex-col items-start gap-0.5', className)}>
      <span
        id={id}
        data-clamped={!open || undefined}
        className={cn('block whitespace-pre-wrap [overflow-wrap:anywhere]', !open && cn('overflow-hidden', clampClassName))}
      >
        {text}
      </span>
      <button
        type="button"
        aria-expanded={open}
        aria-controls={id}
        onClick={() => setOpen((o) => !o)}
        className="rounded-sm text-xs font-medium text-primary-fg hover:underline focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring"
      >
        {open ? 'Show less' : 'Show more'}
      </button>
    </span>
  )
}

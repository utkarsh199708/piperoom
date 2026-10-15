import { cn } from '@/lib/utils'
import type { ToolCallState } from '@/state/types'

/** The tool summary as plain text: the view for a tool with no renderer, an error, or data that does not fit its renderer. */
export function Raw({ t }: { t: ToolCallState }) {
  if (t.summary == null) return null
  return (
    <pre
      className={cn(
        'm-0 whitespace-pre-wrap break-words rounded-md border bg-muted px-2.5 py-2 font-mono text-xs leading-[17px]',
        t.is_error ? 'border-destructive/50 text-destructive-fg' : 'border-border text-muted-foreground',
      )}
    >
      {t.summary}
    </pre>
  )
}

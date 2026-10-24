// Shared bits of the setup controls (U1-12b): one place for the field styles, the error and the
// success line, so every control reads and announces the same way.
import type { ReactNode } from 'react'

export const FIELD_LABEL = 'text-xs font-medium text-muted-foreground'
export const FIELD_ERROR = 'text-xs text-destructive-fg'
export const SELECT =
  'h-8 w-full rounded-md border border-input-border bg-background px-2 text-sm text-foreground focus-visible:border-ring focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring/30 disabled:cursor-not-allowed disabled:opacity-50'

/** A server or form error, announced once. */
export function SetupAlert({ children }: { children: ReactNode }) {
  return (
    <div
      role="alert"
      className="rounded-md border border-border bg-destructive-subtle px-3 py-2 text-xs text-destructive-fg"
    >
      {children}
    </div>
  )
}

/** A quiet confirmation: copying, sending, asking the runner. */
export function SetupStatus({ children, id }: { children: ReactNode; id?: string }) {
  return (
    <p id={id} role="status" aria-live="polite" className="m-0 min-h-4 text-xs text-success-foreground">
      {children}
    </p>
  )
}

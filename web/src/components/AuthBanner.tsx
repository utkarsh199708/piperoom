import type { ReactNode } from 'react'
import { TriangleAlert } from 'lucide-react'
import { Button } from '@/components/ui/button'
import { problemText } from '../lib/auth'
import type { AuthProblem } from '../lib/auth'

// The app's alert bar (the Stale-409 look): a tinted strip with an icon, one sentence and, when
// the person can do something about it, a quiet button. role="alert": these are not caused by a click.
export function ErrorBar({ children, action }: { children: ReactNode; action?: ReactNode }) {
  return (
    <div
      className="flex flex-wrap items-center gap-2 border-b border-border bg-destructive-subtle px-4 py-2 text-sm font-medium text-destructive-fg"
      role="alert"
    >
      <TriangleAlert className="size-3.5 shrink-0" aria-hidden="true" />
      <span className="min-w-0 flex-1">{children}</span>
      {action}
    </div>
  )
}

// Clear message for 403 proxy_identity, 421 bad_host and 403 viewer. These are
// not things the person can fix by clicking, so the banner says what happened
// and who can fix it (docs/api.md, Errors).
export function AuthBanner({ problem, onDismiss }: { problem: AuthProblem; onDismiss: () => void }) {
  return (
    <div data-kind={problem.kind}>
      <ErrorBar
        action={
          <Button variant="ghost" size="sm" className="h-7" onClick={onDismiss}>
            Dismiss
          </Button>
        }
      >
        {problemText(problem)}
      </ErrorBar>
    </div>
  )
}

import { RefreshCw, TriangleAlert } from 'lucide-react'
import { Button } from '@/components/ui/button'

// Shown after a 409 params_mismatch on approve or reject (auth PR 4): the params_hash
// the UI sent is not the proposal's, so the screen is out of date. The room is
// already reloading; nothing is retried, the person looks again and decides again.
// (docs/design/ui-v2 Stale-409: an inline alert bar, plus a toast that offers Reload.)
export const STALE_TEXT = 'This fix changed since you loaded it. Reload to see the new version.'

export function StaleBanner({ onReload, onDismiss }: { onReload: () => void; onDismiss: () => void }) {
  return (
    <div
      className="flex flex-wrap items-center gap-2 border-y border-border bg-destructive-subtle px-3.5 py-2 text-sm font-medium text-destructive-fg"
      role="alert"
    >
      <TriangleAlert className="size-3.5 shrink-0" aria-hidden="true" />
      <span className="min-w-0">{STALE_TEXT}</span>
      <span className="ml-auto flex items-center gap-2">
        <Button variant="outline" size="sm" className="h-7 text-foreground" onClick={onReload}>
          <RefreshCw className="size-3" aria-hidden="true" />
          Reload
        </Button>
        <Button variant="ghost" size="sm" className="h-7" onClick={onDismiss}>
          Dismiss
        </Button>
      </span>
    </div>
  )
}

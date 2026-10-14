import { Button } from '@/components/ui/button'
import { OFFLINE_TEXT } from '@/lib/errors'

/** Under a composer whose send never reached the server: the words are kept, the person can retry. */
export function OfflineStrip({ onRetry, disabled }: { onRetry: () => void; disabled?: boolean }) {
  return (
    <div role="alert" className="flex items-center gap-2 text-xs text-warning-foreground">
      <span className="min-w-0 flex-1">{OFFLINE_TEXT}</span>
      <Button type="button" variant="ghost" size="sm" className="h-7" disabled={disabled} onClick={onRetry}>
        Retry
      </Button>
    </div>
  )
}

import { Progress } from '@/components/ui/progress'

// Shown while the log up to the snapshot's last_seq is still being replayed over the WebSocket,
// so a half-built timeline is not mistaken for the whole room.
export function ReplayProgress({ done, total }: { done: number; total: number }) {
  const pct = Math.min(100, Math.round((done / total) * 100))
  return (
    <div
      role="status"
      className="flex flex-col gap-1.5 border-b border-border bg-card px-4 py-2 text-xs text-muted-foreground sm:px-6"
    >
      <span>
        Loading history: event {Math.min(done, total)} of {total}
      </span>
      <Progress value={pct} aria-label="Replay progress" className="h-1" />
    </div>
  )
}

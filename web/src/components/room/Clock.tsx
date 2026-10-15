import { clockTime, utcLabel } from '@/lib/time'
import { cn } from '@/lib/utils'

/** A wall-clock time in the browser's zone; hover shows the UTC time the Kafka and Connect logs use. */
export function Clock({ iso, className }: { iso: string; className?: string }) {
  return (
    <time dateTime={iso} title={utcLabel(iso)} className={cn('tabular-nums', className)}>
      {clockTime(iso)}
    </time>
  )
}

import { Badge } from '../ui/badge'
import type { RowStatus } from '../../lib/integrations'
import { cn } from '../../lib/utils'

const PILL: Record<RowStatus, { text: string; variant: 'success' | 'info' | 'warning' | 'destructive' | 'neutral'; dot: string }> = {
  not_started: { text: 'Not started', variant: 'neutral', dot: 'bg-subtle-foreground' },
  testing: { text: 'Testing', variant: 'info', dot: 'bg-info' },
  connected: { text: 'Connected', variant: 'success', dot: 'bg-success' },
  attention: { text: 'Needs attention', variant: 'destructive', dot: 'bg-destructive' },
  disabled: { text: 'Disabled', variant: 'neutral', dot: 'bg-subtle-foreground' },
  untested: { text: 'Not tested yet', variant: 'warning', dot: 'bg-warning' },
}

/** The status of one checklist row or card, as a dotted pill. Colour is never the only signal: the text says it. */
export function StatusPill({ status, readOnlyVerified = false }: { status: RowStatus; readOnlyVerified?: boolean }) {
  const p = PILL[status]
  const text = status === 'connected' && readOnlyVerified ? 'Connected · verified read-only' : p.text
  return (
    <Badge variant={p.variant} className="rounded-full px-2" data-status={status}>
      <span className={cn('size-1.5 rounded-full', p.dot)} aria-hidden="true" />
      {text}
    </Badge>
  )
}

/** "Managed by environment": the category is set by environment variables and is read-only here. */
export function EnvPill() {
  return (
    <Badge variant="outline" className="rounded-full px-2" title="Set by environment variables; change them on the server">
      Managed by environment
    </Badge>
  )
}

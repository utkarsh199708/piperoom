import { useId, useState } from 'react'
import { ChevronDown, ChevronRight, CircleCheck, CircleMinus, CircleX, TriangleAlert } from 'lucide-react'
import type { Check, CheckStatus } from '../../api/integrations'
import { countFailed } from '../../lib/integrations'
import { cn } from '../../lib/utils'
import { Button } from '../ui/button'
import { CopyButton } from './CopyButton'

const ICON: Record<CheckStatus, { Icon: typeof CircleCheck; className: string; label: string }> = {
  pass: { Icon: CircleCheck, className: 'text-success-foreground', label: 'Passed' },
  warn: { Icon: TriangleAlert, className: 'text-warning-foreground', label: 'Warning' },
  fail: { Icon: CircleX, className: 'text-destructive-fg', label: 'Failed' },
  skipped: { Icon: CircleMinus, className: 'text-subtle-foreground', label: 'Skipped' },
}

function CheckRow({ check }: { check: Check }) {
  // A failing line opens with its fix in view; a warning keeps it one click away.
  const [open, setOpen] = useState(check.status === 'fail')
  const { Icon, className, label } = ICON[check.status]
  const hasMore = check.detail !== '' || check.fix !== ''
  // useId: check ids (reach, login) repeat between lists shown at once in the drawer.
  const panelId = `${useId()}-check-more`
  return (
    <li className="flex flex-col gap-2 border-t border-border py-2.5 first:border-t-0" data-check={check.id} data-status={check.status}>
      <div className="flex items-start gap-2.5">
        <Icon className={cn('mt-0.5 size-4 shrink-0', className)} aria-hidden="true" />
        <span className="min-w-0 flex-1 text-sm">
          <span className="sr-only">{label}: </span>
          {check.title}
        </span>
        {hasMore && (
          <Button
            variant="ghost"
            size="sm"
            className="h-7 px-2"
            aria-expanded={open}
            aria-controls={panelId}
            onClick={() => setOpen((o) => !o)}
          >
            {open ? <ChevronDown aria-hidden="true" /> : <ChevronRight aria-hidden="true" />}
            {check.fix ? 'Fix' : 'Details'}
            <span className="sr-only"> for {check.title}</span>
          </Button>
        )}
      </div>
      {hasMore && open && (
        <div id={panelId} className="ml-6 flex flex-col gap-2 rounded-md border border-border bg-sidebar p-3">
          {check.detail && <p className="m-0 text-sm text-muted-foreground">{check.detail}</p>}
          {check.fix && (
            <>
              <span className="text-xs font-medium text-muted-foreground">Fix</span>
              <pre className="m-0 whitespace-pre-wrap break-words font-mono text-xs text-foreground">{check.fix}</pre>
              <div>
                <CopyButton text={check.fix} label={`Copy fix for ${check.title}`} />
              </div>
            </>
          )}
        </div>
      )}
    </li>
  )
}

/**
 * The connection test as a checklist: one line per check in server order, an icon and
 * hidden text for its status, and an expandable Fix with a copy button.
 * `stale`: the form changed since this test ran.
 */
export function CheckList({ checks, stale = false, title = 'Connection test' }: { checks: Check[]; stale?: boolean; title?: string }) {
  const failed = countFailed(checks)
  const warned = checks.filter((c) => c.status === 'warn').length
  let summary = 'All checks passed'
  if (checks.length === 0) summary = 'No checks for this card'
  else if (failed > 0) summary = `${failed} failed`
  else if (warned > 0) summary = `${warned} ${warned === 1 ? 'warning' : 'warnings'}, saved as follow-ups`
  return (
    <section aria-label={title} className={cn('flex flex-col', stale && 'opacity-60')}>
      <div className="flex flex-wrap items-center justify-between gap-2 pb-1.5">
        <h3 className="m-0 text-sm font-semibold">{title}</h3>
        <span className="text-xs text-muted-foreground" role="status">
          {stale ? 'Values changed since this test' : summary}
        </span>
      </div>
      {checks.length > 0 && <ul className="m-0 list-none p-0">{checks.map((c) => <CheckRow key={c.id} check={c} />)}</ul>}
    </section>
  )
}

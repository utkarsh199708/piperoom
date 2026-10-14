import { useState } from 'react'
import { ChevronDown, ChevronRight } from 'lucide-react'
import { integrationsApi } from '../../api/integrations'
import type { AuditAction, AuditEntry } from '../../api/integrations'
import { integrationErrorText } from '../../lib/integrationErrors'
import { Button } from '../ui/button'

const ACTION_TEXT: Record<AuditAction, string> = {
  created: 'connected',
  updated: 'updated',
  tested: 'tested',
  enabled: 'enabled',
  disabled: 'disabled',
  deleted: 'deleted',
  secret_rotated: 'rotated the secret',
}

function when(at: string): string {
  const d = new Date(at)
  return Number.isNaN(d.getTime()) ? at : d.toLocaleString()
}

/** The change log of one integration (who, which field names, when), loaded on first open. */
export function AuditList({ integrationId, api = integrationsApi }: { integrationId: string; api?: Pick<typeof integrationsApi, 'audit'> }) {
  const [open, setOpen] = useState(false)
  const [entries, setEntries] = useState<AuditEntry[] | null>(null)
  const [error, setError] = useState<string | null>(null)
  const panelId = `audit-${integrationId}`

  const toggle = () => {
    const next = !open
    setOpen(next)
    if (next && entries === null) {
      setError(null)
      api.audit(integrationId).then(setEntries, (e: unknown) => setError(`Could not load the change log: ${integrationErrorText(e)}`))
    }
  }

  return (
    <section className="flex flex-col gap-2" aria-label="Change log">
      <Button variant="ghost" size="sm" className="w-fit px-1" aria-expanded={open} aria-controls={panelId} onClick={toggle}>
        {open ? <ChevronDown aria-hidden="true" /> : <ChevronRight aria-hidden="true" />}
        Change log
      </Button>
      {open && (
        <div id={panelId}>
          {error && (
            <p role="alert" className="m-0 text-sm text-destructive-fg">
              {error}
            </p>
          )}
          {!error && entries === null && <p className="m-0 text-sm text-muted-foreground">Loading…</p>}
          {entries !== null && entries.length === 0 && <p className="m-0 text-sm text-muted-foreground">No changes yet.</p>}
          {entries !== null && entries.length > 0 && (
            <ol className="m-0 flex list-none flex-col gap-1.5 p-0 text-sm">
              {entries.map((e) => (
                <li key={e.seq} className="flex flex-wrap gap-x-2 text-muted-foreground">
                  <span className="font-medium text-foreground">{e.actor}</span>
                  <span>{ACTION_TEXT[e.action] ?? e.action}</span>
                  {e.changed.length > 0 && <span className="font-mono text-xs">({e.changed.join(', ')})</span>}
                  <time dateTime={e.at} className="ml-auto text-xs text-subtle-foreground">
                    {when(e.at)}
                  </time>
                </li>
              ))}
            </ol>
          )}
        </div>
      )}
    </section>
  )
}

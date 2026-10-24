import { AlertTriangle } from 'lucide-react'
import type { ModelPanel, ModelRole } from '@/api/types'
import { Badge } from '@/components/ui/badge'
import { priceText, providerLabel, roleLabel, roles, STATUS_TEXT } from '@/lib/models'

// The read-only Model panel (V8; docs/design/model-providers.md section 11): which model serves each role.
// It shows the base URL host only and never a key: the API sends neither, and `key_set` is a yes or no.
export function ModelPanelView({ panel }: { panel: ModelPanel }) {
  const list = roles(panel)
  if (list.length === 0) {
    return <p className="m-0 text-muted-foreground">No model is configured yet.</p>
  }
  return (
    <ul aria-label="Models by role" className="m-0 flex list-none flex-col gap-2 p-0">
      {list.map((r) => (
        <RoleRow key={r.role} role={r} />
      ))}
    </ul>
  )
}

function statusVariant(status: ModelRole['status']): 'success' | 'warning' | 'accent' | 'neutral' {
  if (status === 'supported') return 'success'
  if (status === 'experimental' || status === 'unverified') return 'warning'
  if (status === 'reference') return 'accent'
  return 'neutral'
}

function RoleRow({ role: r }: { role: ModelRole }) {
  return (
    <li className="flex min-w-0 flex-col gap-2 rounded-md border border-border bg-card p-3" data-testid={`model-role-${r.role}`}>
      <div className="flex flex-wrap items-center justify-between gap-x-3 gap-y-1">
        <h3 className="m-0 text-xs font-medium uppercase tracking-wide text-subtle-foreground">{roleLabel(r.role)}</h3>
        <Badge variant={statusVariant(r.status)}>{STATUS_TEXT[r.status] ?? r.status}</Badge>
      </div>
      <p className="m-0 break-all font-mono text-[13px] font-medium text-foreground">{r.model}</p>
      <dl className="m-0 grid grid-cols-[auto_minmax(0,1fr)] gap-x-3 gap-y-1 text-xs">
        <dt className="text-subtle-foreground">Provider</dt>
        <dd className="m-0 break-words">{providerLabel(r)}</dd>
        {r.base_url_host && (
          <>
            <dt className="text-subtle-foreground">Host</dt>
            <dd className="m-0 break-all font-mono">{r.base_url_host}</dd>
          </>
        )}
        {r.region && (
          <>
            <dt className="text-subtle-foreground">Region</dt>
            <dd className="m-0 break-words font-mono">{r.region}</dd>
          </>
        )}
        <dt className="text-subtle-foreground">Prompt cache</dt>
        <dd className="m-0">{r.prompt_cache ? 'Yes' : 'No'}</dd>
        <dt className="text-subtle-foreground">Price</dt>
        <dd className="m-0">{priceText(r.price)}</dd>
        <dt className="text-subtle-foreground">Key</dt>
        <dd className="m-0">{r.key_set ? 'Configured' : 'Not set'}</dd>
      </dl>
      {r.status === 'unverified' && (
        <p className="m-0 text-xs text-warning-foreground">
          This model has not passed the eval gate against the Claude reference, so answers may be worse than the
          supported list.
        </p>
      )}
      {r.sunset && (
        <p role="note" className="m-0 flex items-start gap-1.5 text-xs text-warning-foreground">
          <AlertTriangle className="mt-px size-3.5 shrink-0" aria-hidden="true" />
          <span>Sunset warning: this model is retired on {r.sunset}. Pick another before then.</span>
        </p>
      )}
    </li>
  )
}

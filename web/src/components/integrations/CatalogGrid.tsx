import { CATEGORIES } from '../../api/integrations'
import type { CatalogKind, Integration } from '../../api/integrations'
import { CATEGORY_LABEL, integrationForKind, verifiedReadOnly, viewStatus } from '../../lib/integrations'
import { Button } from '../ui/button'
import { StatusPill } from './StatusPill'

/**
 * Every card of the catalog, grouped by category in spec order. A category that holds one
 * row (all but alerts and secret managers) offers its other kinds only once that row is
 * removed, since the server answers 409 duplicate.
 */
export function CatalogGrid({
  catalog,
  list,
  admin,
  onOpen,
}: {
  catalog: CatalogKind[]
  list: Integration[]
  admin: boolean
  onOpen: (kind: CatalogKind, existing?: Integration) => void
}) {
  return (
    <div className="flex flex-col gap-6">
      {CATEGORIES.map((category) => {
        const kinds = catalog.filter((k) => k.category === category)
        if (kinds.length === 0) return null
        const headingId = `catalog-${category}`
        return (
          <section key={category} aria-labelledby={headingId} className="flex flex-col gap-3">
            <h3 id={headingId} className="m-0 text-sm font-semibold text-muted-foreground">
              {CATEGORY_LABEL[category]}
            </h3>
            <ul className="m-0 grid list-none grid-cols-[repeat(auto-fill,minmax(240px,1fr))] gap-3 p-0">
              {kinds.map((k) => {
                const existing = integrationForKind(k, list)
                const mine = existing?.kind === k.id
                const taken = existing !== undefined && !mine
                let action = 'Connect'
                if (!admin) action = 'Details'
                else if (mine) action = existing.source === 'env' ? 'View' : 'Manage'
                return (
                  <li key={k.id} className="flex flex-col gap-2 rounded-lg border border-border bg-card p-4" data-kind={k.id}>
                    <div className="flex items-start gap-3">
                      <span
                        aria-hidden="true"
                        className="flex size-9 shrink-0 items-center justify-center rounded-lg border border-input bg-secondary font-mono text-xs font-semibold uppercase"
                      >
                        {(k.logo || k.category).slice(0, 2)}
                      </span>
                      <div className="min-w-0 flex-1">
                        <h4 className="m-0 text-sm font-semibold">{k.title}</h4>
                        <p className="m-0 text-xs text-muted-foreground">{k.summary}</p>
                      </div>
                    </div>
                    <div className="mt-auto flex flex-wrap items-center gap-2 pt-1">
                      {mine && <StatusPill status={viewStatus(existing)} readOnlyVerified={verifiedReadOnly(existing)} />}
                      {taken && (
                        <span className="text-xs text-muted-foreground">
                          {CATEGORY_LABEL[category]} uses {existing.title}
                        </span>
                      )}
                      {!taken && (
                        <Button
                          size="sm"
                          variant={action === 'Connect' ? 'default' : 'secondary'}
                          className="ml-auto"
                          aria-label={`${action} ${k.title}`}
                          onClick={() => onOpen(k, mine ? existing : undefined)}
                        >
                          {action}
                        </Button>
                      )}
                    </div>
                  </li>
                )
              })}
            </ul>
          </section>
        )
      })}
    </div>
  )
}

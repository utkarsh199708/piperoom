import { Workflow } from 'lucide-react'
import type { AdminPipeline } from '@/api/types'
import { Badge, layerLabel, layerVariant } from '@/components/ui/badge'
import { Card } from '@/components/ui/card'
import { lines, principalLabel } from '@/lib/settings'

function None() {
  return (
    <span className="text-subtle-foreground">
      <span aria-hidden="true">–</span>
      <span className="sr-only">none</span>
    </span>
  )
}

function Names({ items }: { items: readonly string[] }) {
  if (items.length === 0) return <None />
  return (
    <ul className="m-0 flex list-none flex-col gap-0.5 p-0">
      {items.map((s) => (
        <li key={s} className="break-all font-mono text-xs">
          {s}
        </li>
      ))}
    </ul>
  )
}

/** Every pipeline of the owners file, read only: one card each, so nothing scrolls sideways. */
export function PipelinesTab({ pipelines }: { pipelines: readonly AdminPipeline[] }) {
  if (pipelines.length === 0) {
    return (
      <Card className="flex flex-col items-center gap-2 px-6 py-12 text-center">
        <Workflow className="size-6 text-subtle-foreground" aria-hidden="true" />
        <p role="status" className="m-0 text-muted-foreground">
          No pipelines are configured. Add them to the owners file.
        </p>
      </Card>
    )
  }
  return (
    <ul className="m-0 flex list-none flex-col gap-3 p-0" aria-label="Pipelines">
      {pipelines.map((p) => (
        <li key={p.id}>
          <Card className="overflow-hidden" role="group" aria-labelledby={`pipeline-${p.id}`}>
            <div className="flex flex-wrap items-start gap-x-3 gap-y-2 border-b border-border px-4 py-3">
              <div className="flex min-w-0 flex-1 flex-col gap-0.5">
                <h3 id={`pipeline-${p.id}`} className="m-0 text-sm font-semibold">
                  {p.title || p.id}
                </h3>
                <span className="font-mono text-xs text-subtle-foreground">{p.id}</span>
              </div>
              <div className="flex flex-wrap items-center gap-1.5">
                {p.tenant && <Badge variant="outline">tenant {p.tenant}</Badge>}
                <Badge variant={p.connections === 'own' ? 'accent' : 'neutral'}>
                  {p.connections === 'own' ? 'own connections' : 'shared connections'}
                </Badge>
                {p.shadow ? <Badge variant="info">shadow mode</Badge> : <Badge variant="success">live</Badge>}
              </div>
            </div>
            <div className="grid gap-x-8 gap-y-4 px-4 py-3 md:grid-cols-2">
              <section aria-label={`Data flow of ${p.id}`} className="flex min-w-0 flex-col gap-2">
                <h4 className="m-0 text-xs font-medium uppercase tracking-wide text-subtle-foreground">Data flow</h4>
                <dl className="m-0 grid grid-cols-[8.5rem_minmax(0,1fr)] gap-x-3 gap-y-1.5">
                  <dt className="text-xs text-muted-foreground">Source tables</dt>
                  <dd className="m-0">
                    <Names items={lines(p.source_tables)} />
                  </dd>
                  <dt className="text-xs text-muted-foreground">Connectors</dt>
                  <dd className="m-0">
                    <Names items={lines(p.connectors)} />
                  </dd>
                  <dt className="text-xs text-muted-foreground">Consumer groups</dt>
                  <dd className="m-0">
                    <Names items={lines(p.consumer_groups)} />
                  </dd>
                  <dt className="text-xs text-muted-foreground">Sink tables</dt>
                  <dd className="m-0">
                    <Names items={lines(p.sink_tables)} />
                  </dd>
                </dl>
              </section>
              <section aria-label={`Owners of ${p.id}`} className="flex min-w-0 flex-col gap-2">
                <h4 className="m-0 text-xs font-medium uppercase tracking-wide text-subtle-foreground">Layer owners</h4>
                {lines(p.owners).length === 0 ? (
                  <None />
                ) : (
                  <ul className="m-0 grid list-none grid-cols-[auto_minmax(0,1fr)] items-center gap-x-3 gap-y-1.5 p-0">
                    {lines(p.owners).map((o) => (
                      <li key={o.layer} className="contents">
                        <Badge variant={layerVariant(o.layer)} className="justify-self-start">
                          {layerLabel(o.layer)}
                        </Badge>
                        <span className="truncate text-sm">{lines(o.principals).map(principalLabel).join(', ')}</span>
                      </li>
                    ))}
                  </ul>
                )}
              </section>
            </div>
          </Card>
        </li>
      ))}
    </ul>
  )
}

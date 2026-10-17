import type { ReactNode } from 'react'
import type { AdminPipeline } from '@/api/types'
import { Badge, layerVariant } from '@/components/ui/badge'
import { lines } from '@/lib/settings'

function Mono({ items }: { items: readonly string[] }) {
  if (items.length === 0) {
    return (
      <span className="text-subtle-foreground">
        <span aria-hidden="true">–</span>
        <span className="sr-only">none</span>
      </span>
    )
  }
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

function Th({ children }: { children: ReactNode }) {
  return (
    <th scope="col" className="py-2 pr-4 align-bottom font-medium">
      {children}
    </th>
  )
}

/** Every pipeline of owners.yaml, read only. */
export function PipelinesTab({ pipelines }: { pipelines: readonly AdminPipeline[] }) {
  if (pipelines.length === 0) {
    return (
      <p role="status" className="m-0 text-muted-foreground">
        No pipelines are configured. Add them to the owners file.
      </p>
    )
  }
  return (
    <div className="overflow-x-auto">
      <table className="w-full min-w-[1100px] border-collapse text-left">
        <caption className="sr-only">Pipelines</caption>
        <thead>
          <tr className="border-b border-border text-xs text-subtle-foreground">
            <Th>Pipeline</Th>
            <Th>Tenant</Th>
            <Th>Connections</Th>
            <Th>Connectors</Th>
            <Th>Consumer groups</Th>
            <Th>Source tables</Th>
            <Th>Sink tables</Th>
            <Th>Owners</Th>
            <Th>Shadow</Th>
          </tr>
        </thead>
        <tbody>
          {pipelines.map((p) => (
            <tr key={p.id} className="border-b border-border align-top">
              <th scope="row" className="py-2 pr-4 text-left font-normal">
                <div className="font-mono text-xs font-medium">{p.id}</div>
                {p.title && <div className="text-xs text-muted-foreground">{p.title}</div>}
              </th>
              <td className="py-2 pr-4">{p.tenant || <span className="text-subtle-foreground">–</span>}</td>
              <td className="py-2 pr-4">
                <Badge variant={p.connections === 'own' ? 'accent' : 'neutral'}>{p.connections}</Badge>
              </td>
              <td className="py-2 pr-4"><Mono items={lines(p.connectors)} /></td>
              <td className="py-2 pr-4"><Mono items={lines(p.consumer_groups)} /></td>
              <td className="py-2 pr-4"><Mono items={lines(p.source_tables)} /></td>
              <td className="py-2 pr-4"><Mono items={lines(p.sink_tables)} /></td>
              <td className="py-2 pr-4">
                {lines(p.owners).length === 0 ? (
                  <span className="text-subtle-foreground">
                    <span aria-hidden="true">–</span>
                    <span className="sr-only">none</span>
                  </span>
                ) : (
                  <ul className="m-0 flex list-none flex-col gap-1 p-0">
                    {lines(p.owners).map((o) => (
                      <li key={o.layer} className="flex flex-wrap items-center gap-1.5">
                        <Badge variant={layerVariant(o.layer)}>{o.layer}</Badge>
                        <span className="font-mono text-xs">{lines(o.principals).join(', ')}</span>
                      </li>
                    ))}
                  </ul>
                )}
              </td>
              <td className="py-2">
                <Badge variant={p.shadow ? 'info' : 'outline'}>{p.shadow ? 'shadow' : 'off'}</Badge>
              </td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  )
}

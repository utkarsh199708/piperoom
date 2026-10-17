import { BellRing } from 'lucide-react'
import type { PageState } from '@/state/types'
import { Badge, layerVariant } from '@/components/ui/badge'
import { ownerNames, pagedBy } from '@/lib/pages'
import { plainText } from '@/lib/title'
import { Clock } from './Clock'

// The owners the room has asked to look, in order: "Agent paged Kabir (consumer): <reason>". A room that
// opened with no driver is easy to miss; this is the record that someone was told. The reason is
// agent or member text, so it is a plain text node.
export function PagesList({ pages, names }: { pages: PageState[]; names?: Record<string, string> }) {
  if (pages.length === 0) return null
  return (
    <section aria-label="Owners paged" className="flex flex-col gap-2 pt-2">
      <h3 className="m-0 text-sm font-medium">Owners paged</h3>
      <ul className="m-0 flex list-none flex-col gap-1.5 p-0">
        {pages.map((p) => (
          <li key={p.seq} className="flex items-start gap-2">
            <BellRing className="mt-0.5 size-3.5 shrink-0 text-subtle-foreground" aria-hidden="true" />
            <div className="min-w-0 flex-1 break-words">
              <span className="font-medium">{pagedBy(p)}</span> paged{' '}
              {p.owners.length > 0 ? (
                <span className="font-medium">{ownerNames(p.owners, names)}</span>
              ) : (
                <span className="text-muted-foreground">nobody (no owner is listed)</span>
              )}{' '}
              <Badge variant={layerVariant(p.layer)} mono>
                {plainText(p.layer, 40)}
              </Badge>
              {p.reason && <span className="text-muted-foreground">: {plainText(p.reason, 300)}</span>}
            </div>
            <Clock iso={p.at} className="shrink-0 text-xs text-subtle-foreground" />
          </li>
        ))}
      </ul>
    </section>
  )
}

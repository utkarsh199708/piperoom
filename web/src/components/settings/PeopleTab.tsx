import { Users } from 'lucide-react'
import type { AdminPerson } from '@/api/types'
import { Badge, layerLabel, layerVariant } from '@/components/ui/badge'
import { Card } from '@/components/ui/card'
import { lines } from '@/lib/settings'

// The room list's table styles, so every table in the app reads the same.
const TH = 'h-9 whitespace-nowrap px-4 text-left text-xs font-medium text-subtle-foreground'
const TD = 'border-t border-border px-4 py-2.5 align-middle'

function initials(name: string, id: string): string {
  const words = (name || id).trim().split(/\s+/).filter(Boolean)
  const s = words.length > 1 ? words[0][0] + words[1][0] : (words[0] ?? '?').slice(0, 2)
  return s.toUpperCase()
}

/** Every person of the users file, read only. */
export function PeopleTab({ people }: { people: readonly AdminPerson[] }) {
  if (people.length === 0) {
    return (
      <Card className="flex flex-col items-center gap-2 px-6 py-12 text-center">
        <Users className="size-6 text-subtle-foreground" aria-hidden="true" />
        <p role="status" className="m-0 text-muted-foreground">
          No people are configured. Add them to the users file.
        </p>
      </Card>
    )
  }
  return (
    <Card className="overflow-x-auto">
      <table className="w-full min-w-[560px] border-collapse text-sm">
        <caption className="sr-only">People</caption>
        <thead>
          <tr>
            <th scope="col" className={TH}>
              Person
            </th>
            <th scope="col" className={TH}>
              Owns layers
            </th>
            <th scope="col" className={TH}>
              Role
            </th>
          </tr>
        </thead>
        <tbody>
          {people.map((p) => (
            <tr key={p.id}>
              <th scope="row" className={`${TD} text-left font-normal`}>
                <span className="flex items-center gap-2.5">
                  <span
                    className="grid size-7 shrink-0 place-items-center rounded-full bg-muted text-[11px] font-semibold text-muted-foreground"
                    aria-hidden="true"
                  >
                    {initials(p.name, p.id)}
                  </span>
                  <span className="flex min-w-0 flex-col">
                    <span className="font-medium">{p.name || p.id}</span>
                    <span className="font-mono text-xs text-subtle-foreground">{p.id}</span>
                  </span>
                </span>
              </th>
              <td className={TD}>
                {lines(p.layers).length === 0 ? (
                  <span className="text-subtle-foreground">
                    <span aria-hidden="true">–</span>
                    <span className="sr-only">none</span>
                  </span>
                ) : (
                  <span className="flex flex-wrap gap-1">
                    {lines(p.layers).map((l) => (
                      <Badge key={l} variant={layerVariant(l)}>
                        {layerLabel(l)}
                      </Badge>
                    ))}
                  </span>
                )}
              </td>
              <td className={TD}>
                {p.admin ? <Badge variant="accent">admin</Badge> : <span className="text-muted-foreground">member</span>}
              </td>
            </tr>
          ))}
        </tbody>
      </table>
    </Card>
  )
}

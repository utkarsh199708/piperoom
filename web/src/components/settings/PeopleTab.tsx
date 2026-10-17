import type { AdminPerson } from '@/api/types'
import { Badge, layerVariant } from '@/components/ui/badge'
import { lines } from '@/lib/settings'

/** Every person of users.yaml, read only. */
export function PeopleTab({ people }: { people: readonly AdminPerson[] }) {
  if (people.length === 0) {
    return (
      <p role="status" className="m-0 text-muted-foreground">
        No people are configured. Add them to the users file.
      </p>
    )
  }
  return (
    <div className="overflow-x-auto">
      <table className="w-full min-w-[520px] border-collapse text-left">
        <caption className="sr-only">People</caption>
        <thead>
          <tr className="border-b border-border text-xs text-subtle-foreground">
            <th scope="col" className="py-2 pr-4 font-medium">Id</th>
            <th scope="col" className="py-2 pr-4 font-medium">Name</th>
            <th scope="col" className="py-2 pr-4 font-medium">Layers</th>
            <th scope="col" className="py-2 font-medium">Role</th>
          </tr>
        </thead>
        <tbody>
          {people.map((p) => (
            <tr key={p.id} className="border-b border-border align-top">
              <th scope="row" className="py-2 pr-4 text-left font-mono text-xs font-normal">
                {p.id}
              </th>
              <td className="py-2 pr-4">{p.name}</td>
              <td className="py-2 pr-4">
                {lines(p.layers).length === 0 ? (
                  <span className="text-subtle-foreground">
                    <span aria-hidden="true">–</span>
                    <span className="sr-only">none</span>
                  </span>
                ) : (
                  <span className="flex flex-wrap gap-1">
                    {lines(p.layers).map((l) => (
                      <Badge key={l} variant={layerVariant(l)}>
                        {l}
                      </Badge>
                    ))}
                  </span>
                )}
              </td>
              <td className="py-2">{p.admin ? <Badge variant="accent">admin</Badge> : <span className="text-subtle-foreground">member</span>}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  )
}

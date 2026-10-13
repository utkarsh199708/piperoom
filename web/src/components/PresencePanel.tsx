import type { PresenceMember } from '../api/types'
import type { MemberState } from '../state/types'
import { cn } from '@/lib/utils'
import { Badge } from '@/components/ui/badge'
import { PersonAvatar } from './room/PersonAvatar'

export function PresencePanel({
  members,
  presence,
  selfId,
  selfIsViewer = false,
}: {
  members: MemberState[]
  presence: PresenceMember[]
  /** The signed-in person: that row says "you". */
  selfId?: string
  /** The signed-in person is a viewer: that row says "viewer". */
  selfIsViewer?: boolean
}) {
  const rows = members.map((m) => {
    const p = presence.find((x) => x.id === m.id)
    return { id: m.id, name: m.name, role: m.role, state: p?.state ?? 'viewing' }
  })
  for (const p of presence) {
    if (!rows.some((r) => r.id === p.id)) {
      rows.push({ id: p.id, name: p.name, role: 'watching', state: p.state })
    }
  }

  return (
    <section aria-labelledby="presence-heading" className="flex flex-col gap-1.5 px-4 py-3.5">
      <h2 id="presence-heading" className="m-0 flex items-center gap-1.5 text-sm font-medium">
        Who&rsquo;s here
        <span className="font-mono text-[11px] font-medium text-subtle-foreground">{rows.length}</span>
      </h2>
      <ul className="m-0 flex list-none flex-col gap-0.5 p-0">
        {rows.map((r) => {
          const driver = r.role === 'driver'
          const away = r.state === 'away'
          return (
            <li key={r.id} className={cn('flex min-h-8 items-center gap-2', away && 'opacity-70')}>
              <PersonAvatar actor={{ id: r.id, name: r.name }} driver={driver} className={driver ? 'mx-0.5' : undefined} />
              <span className="min-w-0 truncate font-medium">{r.name}</span>
              {r.id === selfId && <span className="text-xs text-muted-foreground">you</span>}
              {driver && <Badge variant="accent">Driver</Badge>}
              {r.id === selfId && selfIsViewer && <Badge variant="outline">viewer</Badge>}
              <span className={cn('ml-auto shrink-0 text-xs', away ? 'text-muted-foreground' : 'text-subtle-foreground')}>
                {r.state}
              </span>
            </li>
          )
        })}
      </ul>
    </section>
  )
}

import type { Person, PresenceMember } from '../api/types'
import type { MemberState } from '../state/types'
import { cn } from '@/lib/utils'
import { Badge, layerVariant } from '@/components/ui/badge'
import { ageLabel } from '../lib/time'
import { useNow } from './room/useNow'
import { PersonAvatar } from './room/PersonAvatar'

export function PresencePanel({
  members,
  presence,
  selfId,
  selfName,
  selfIsViewer = false,
  people,
  closed = false,
}: {
  members: MemberState[]
  presence: PresenceMember[]
  /** The signed-in person: that row says "you". */
  selfId?: string
  /** With selfId: the signed-in person is listed (as "you") even before presence reports them. */
  selfName?: string
  /** The signed-in person is a viewer: that row says "viewer". */
  selfIsViewer?: boolean
  /** GET /api/people (members only): the layers each person may approve. Viewers pass none. */
  people?: Person[]
  /** The room is closed: nobody is driving, so no Driver chip. */
  closed?: boolean
}) {
  const now = useNow()
  type Row = { id: string; name: string; role: string; state: PresenceMember['state'] | 'absent'; since?: string }
  const rows: Row[] = members.map((m) => {
    const p = presence.find((x) => x.id === m.id)
    // In the log but not connected: "not here". You are always here.
    return { id: m.id, name: m.name, role: m.role, state: p?.state ?? (m.id === selfId ? 'viewing' : 'absent'), since: p?.since }
  })
  for (const p of presence) {
    if (!rows.some((r) => r.id === p.id)) {
      rows.push({ id: p.id, name: p.name, role: 'watching', state: p.state, since: p.since })
    }
  }

  if (selfId && selfName && !rows.some((r) => r.id === selfId)) {
    rows.push({ id: selfId, name: selfName, role: 'watching', state: 'viewing' })
  }
  const layersOf = (id: string) => people?.find((x) => x.id === id)?.layers ?? []

  return (
    <section aria-labelledby="presence-heading" className="flex flex-col gap-1.5 px-4 py-3.5">
      <h2 id="presence-heading" className="m-0 flex items-center gap-1.5 text-sm font-medium">
        Who&rsquo;s here
        <span className="font-mono text-[11px] font-medium text-subtle-foreground">{rows.length}</span>
      </h2>
      <ul className="m-0 flex list-none flex-col gap-0.5 p-0">
        {rows.map((r) => {
          const driver = r.role === 'driver' && !closed
          const away = r.state === 'away' || r.state === 'absent'
          const layers = layersOf(r.id)
          const label =
            r.state === 'viewing'
              ? 'here'
              : r.state === 'away'
                ? r.since
                  ? `away ${ageLabel(r.since, now)}`
                  : 'away'
                : 'not here'
          return (
            <li key={r.id} className={cn('flex min-h-8 items-center gap-2', away && 'opacity-70')}>
              <PersonAvatar actor={{ id: r.id, name: r.name }} driver={driver} className={driver ? 'mx-0.5' : undefined} />
              {/* Chips wrap inside this group: a person with several layers must not push past the rail. */}
              <span className="flex min-w-0 flex-1 flex-wrap items-center gap-x-2 gap-y-1">
                <span className="min-w-0 truncate font-medium">{r.name}</span>
                {r.id === selfId && <span className="text-xs text-muted-foreground">you</span>}
                {driver && <Badge variant="accent">Driver</Badge>}
                {r.id === selfId && selfIsViewer && <Badge variant="outline">viewer</Badge>}
                {layers.map((l) => (
                  <Badge key={l} variant={layerVariant(l)} mono aria-label={`owns ${l}`}>
                    {l}
                  </Badge>
                ))}
              </span>
              <span className={cn('ml-auto shrink-0 self-start pt-1.5 text-xs', away ? 'text-muted-foreground' : 'text-subtle-foreground')}>
                {label}
              </span>
            </li>
          )
        })}
      </ul>
    </section>
  )
}

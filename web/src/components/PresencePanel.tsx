import { PersonName } from '@/components/VerifiedBadge'
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
  selfVerified,
  people,
}: {
  members: MemberState[]
  presence: PresenceMember[]
  /** The signed-in person: that row says "you". */
  selfId?: string
  /** With selfId: the signed-in person is listed (as "you") even before presence reports them. */
  selfName?: string
  /** The signed-in person is a viewer: that row says "viewer". */
  selfIsViewer?: boolean
  /** `/api/me.verified` for the signed-in person (server-set). */
  selfVerified?: boolean
  /** GET /api/people (members only): the layers each person may approve. Viewers pass none. */
  people?: Person[]
}) {
  const now = useNow()
  type Row = { id: string; name: string; role: string; state: PresenceMember['state'] | 'absent'; since?: string; verified?: unknown[] }
  const rows: Row[] = members.map((m) => {
    const p = presence.find((x) => x.id === m.id)
    // Both reports are server-set; the badge shows verified only if every report that exists agrees.
    // In the log but not connected: "not here". You are always here.
    return {
      id: m.id,
      name: m.name,
      role: m.role,
      state: p?.state ?? (m.id === selfId ? 'viewing' : 'absent'),
      since: p?.since,
      verified: [m.verified, p?.verified],
    }
  })
  for (const p of presence) {
    if (!rows.some((r) => r.id === p.id)) {
      rows.push({ id: p.id, name: p.name, role: 'watching', state: p.state, since: p.since, verified: [p.verified] })
    }
  }

  if (selfId && selfName && !rows.some((r) => r.id === selfId)) {
    rows.push({ id: selfId, name: selfName, role: 'watching', state: 'viewing', verified: [selfVerified] })
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
          const driver = r.role === 'driver'
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
              <PersonName person={{ id: r.id, name: r.name }} verified={r.verified} nameClassName="font-medium" />
              {r.id === selfId && <span className="text-xs text-muted-foreground">you</span>}
              {driver && <Badge variant="accent">Driver</Badge>}
              {r.id === selfId && selfIsViewer && <Badge variant="outline">viewer</Badge>}
              {layers.slice(0, 3).map((l) => (
                <Badge key={l} variant={layerVariant(l)} mono aria-label={`owns ${l}`}>
                  {l}
                </Badge>
              ))}
              {layers.length > 3 && (
                <span className="text-xs text-subtle-foreground" title={layers.slice(3).join(', ')}>
                  +{layers.length - 3}
                </span>
              )}
              <span className={cn('ml-auto shrink-0 text-xs', away ? 'text-muted-foreground' : 'text-subtle-foreground')}>
                {label}
              </span>
            </li>
          )
        })}
      </ul>
    </section>
  )
}

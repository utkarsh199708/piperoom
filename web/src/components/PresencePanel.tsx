import type { PresenceMember } from '../api/types'
import type { MemberState } from '../state/types'
import { Chip, Panel } from './ui'
import { Avatar, Icon } from './ui-p4-temp'

export function PresencePanel({
  members,
  presence,
  selfId,
}: {
  members: MemberState[]
  presence: PresenceMember[]
  /** The current user gets a quiet "you" next to the name. */
  selfId?: string
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
    <Panel
      className="presence"
      title={
        <>
          Who's here <span className="panel__count">{rows.length}</span>
        </>
      }
    >
      {rows.length === 0 ? (
        <p className="empty">Only you are here.</p>
      ) : (
        <ul className="presence__list who">
          {rows.map((r) => {
            const driver = r.role === 'driver'
            const away = r.state === 'away'
            return (
              <li key={r.id} className={`presence__row who__row${away ? ' who__row--away' : ''}`}>
                <span className="who__av">
                  <Avatar actor={{ kind: 'human', id: r.id, name: r.name }} size="md" driver={driver} />
                  <span className={`who__dot${away ? ' who__dot--away' : ''}`} aria-hidden="true" />
                </span>
                <span className="presence__name who__name">
                  {r.name}
                  {r.id === selfId && <span className="who__me">you</span>}
                  {driver && (
                    <Chip tone="info">
                      <Icon name="wheel" size={12} />
                      driver
                    </Chip>
                  )}
                  {(r.role === 'viewer' || r.role === 'auditor') && <Chip tone="muted">{r.role}</Chip>}
                </span>
                <span className={`presence__state ${away ? 'presence__state--away' : ''}`}>{r.state}</span>
              </li>
            )
          })}
        </ul>
      )}
    </Panel>
  )
}

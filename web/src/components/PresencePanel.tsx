import type { PresenceMember } from '../api/types'
import type { MemberState } from '../state/types'
import { Avatar, Chip, Panel } from './ui'

export function PresencePanel({
  members,
  presence,
}: {
  members: MemberState[]
  presence: PresenceMember[]
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
      title="Who's here"
      right={<span className="muted">{rows.length}</span>}
    >
      <ul className="presence__list">
        {rows.map((r) => (
          <li key={r.id} className="presence__row">
            <Avatar actor={{ kind: 'human', id: r.id, name: r.name }} size="sm" />
            <span className="presence__name">
              {r.name}
              {r.role === 'driver' && <Chip tone="info">driver</Chip>}
            </span>
            <span className={`presence__state ${r.state === 'away' ? 'presence__state--away' : ''}`}>
              {r.state}
            </span>
          </li>
        ))}
      </ul>
    </Panel>
  )
}

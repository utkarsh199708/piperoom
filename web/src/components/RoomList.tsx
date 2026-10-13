import { useState } from 'react'
import { Link, useNavigate } from 'react-router-dom'
import type { FormEvent } from 'react'
import type { RoomSnapshot } from '../api/types'
import { api, isFixtures } from '../api/client'
import { useCurrentUser } from '../context/user'
import { isViewer } from '../state/permissions'
import type { CurrentUser } from '../state/permissions'
import { addFixtureRoom, emptySnapshot } from '../fixtures/player'
import { useRoomList } from '../hooks/useRoomList'
import { ageLabel } from '../lib/time'
import { needsYou, pendingLayersForUser, roomLiveBadge, roomStats, sortRoomsForList } from '../lib/rooms'
import { errorText } from '../lib/errors'
import { Chip } from './ui'
// TEMP until P1: Avatar, Empty, Icon and Skeleton come from './ui' once it lands.
import { Avatar, Empty, Icon, Skeleton } from './p2Standins'
import { severityTone } from '../lib/tones'
import { UserSwitcher } from './UserSwitcher'
import '../styles/home.css'

const SEVERITIES = ['sev1', 'sev2', 'sev3', 'sev4']
const MAX_PEOPLE = 4

export function RoomList() {
  const navigate = useNavigate()
  const { user } = useCurrentUser()
  const viewer = isViewer(user) // a viewer cannot create rooms (POST /api/rooms is 403 viewer)
  const { rooms, loading, error, reload } = useRoomList()
  const [title, setTitle] = useState('')
  const [severity, setSeverity] = useState('sev2')
  const [pipelineId, setPipelineId] = useState('orders')
  const [busy, setBusy] = useState(false)
  const [createError, setCreateError] = useState<string | null>(null)

  // Open rooms first, then newest opened first (lib/rooms.ts); the two sections
  // below share the same order within their group.
  const sorted = sortRoomsForList(rooms)
  const openRooms = sorted.filter((r) => r.status === 'open')
  const closedRooms = sorted.filter((r) => r.status !== 'open')
  const stats = roomStats(sorted, user)
  const firstLoad = loading && rooms.length === 0

  const onSubmit = async (e: FormEvent) => {
    e.preventDefault()
    if (!title.trim()) return
    setBusy(true)
    setCreateError(null)
    try {
      if (isFixtures()) {
        const snapshot = emptySnapshot(
          `r_${Date.now().toString(36)}`,
          title.trim(),
          severity,
          pipelineId.trim() || 'orders',
        )
        addFixtureRoom(snapshot)
        navigate(`/rooms/${snapshot.id}`)
        return
      }
      const room = await api.createRoom({
        title: title.trim(),
        severity,
        pipeline_id: pipelineId.trim() || 'orders',
      })
      navigate(`/rooms/${room.id}`)
    } catch (err) {
      setCreateError(errorText(err))
    } finally {
      setBusy(false)
    }
  }

  const retry = () => {
    if (reload) void reload()
    else window.location.reload()
  }

  return (
    <div className="shell">
      <header className="topbar">
        <span className="brand">
          <Icon name="pipe" size={20} />
          Piperoom
        </span>
        <div className="topbar__right">
          {isFixtures() && <Chip tone="info">fixture mode</Chip>}
          <UserSwitcher />
        </div>
      </header>

      <main className="home">
        <div className="home__head">
          <h1>Rooms</h1>
          {rooms.length > 0 && (
            <span className="home__sub">
              {stats.open} open · {stats.closed} closed
            </span>
          )}
        </div>

        {error && (
          <div className="banner banner--error home__error" role="alert">
            <span>{error}</span>
            <button type="button" className="btn btn--ghost btn--small" onClick={retry}>
              Retry
            </button>
          </div>
        )}

        {!viewer && (
          <>
            <form className="new-room" aria-label="New room" onSubmit={onSubmit}>
              <div className="field">
                <label htmlFor="new-title">Title</label>
                <input
                  id="new-title"
                  value={title}
                  onChange={(e) => setTitle(e.target.value)}
                  placeholder="Warehouse orders stale"
                  required
                />
              </div>
              <div className="field">
                <label htmlFor="new-severity">Severity</label>
                <select id="new-severity" value={severity} onChange={(e) => setSeverity(e.target.value)}>
                  {SEVERITIES.map((s) => (
                    <option key={s} value={s}>
                      {s}
                    </option>
                  ))}
                </select>
              </div>
              <div className="field">
                <label htmlFor="new-pipeline">Pipeline</label>
                <input
                  id="new-pipeline"
                  value={pipelineId}
                  onChange={(e) => setPipelineId(e.target.value)}
                  placeholder="orders"
                />
              </div>
              <button type="submit" className="btn btn--primary" disabled={busy || !title.trim()}>
                Open room
              </button>
            </form>
            {createError && <p className="error-text new-room__error">{createError}</p>}
          </>
        )}

        {rooms.length > 0 && <StatStrip rooms={sorted} user={user} viewer={viewer} />}

        <section className="group" aria-busy={loading}>
          <div className="group__head">
            <h2>Open</h2>
            <span className="group__count">{openRooms.length}</span>
          </div>
          {firstLoad && (
            <div className="room-skeletons" role="status">
              <span className="home__sr">Loading rooms…</span>
              {[0, 1, 2].map((i) => (
                <Skeleton key={i} h={62} className="room-skeletons__row" />
              ))}
            </div>
          )}
          {!loading && !error && openRooms.length === 0 && (
            <Empty icon="pipe" title="No open rooms">
              {viewer ? 'Rooms open from alerts.' : 'Rooms open from alerts or from the form above.'}
            </Empty>
          )}
          {openRooms.length > 0 && (
            <ul className="room-cards">
              {openRooms.map((r) => (
                <RoomCard key={r.id} room={r} user={user} />
              ))}
            </ul>
          )}
        </section>

        {closedRooms.length > 0 && (
          <section className="group">
            <div className="group__head">
              <h2>Closed</h2>
              <span className="group__count">{closedRooms.length}</span>
            </div>
            <ul className="room-cards">
              {closedRooms.map((r) => (
                <RoomCard key={r.id} room={r} user={user} />
              ))}
            </ul>
          </section>
        )}
      </main>
    </div>
  )
}

function StatStrip({ rooms, user, viewer }: { rooms: RoomSnapshot[]; user: CurrentUser; viewer: boolean }) {
  const stats = roomStats(rooms, user)
  const firstNoDriver = stats.noDriver[0]
  const layers = Array.from(new Set(stats.needsYou.flatMap((r) => pendingLayersForUser(r, user))))
  return (
    <div className={`stats${viewer ? ' stats--3' : ''}`} role="group" aria-label="Summary">
      <div className="stat">
        <div className="stat__l">Open rooms</div>
        <div className="stat__v">{stats.open}</div>
      </div>
      <div className={`stat${stats.noDriver.length > 0 ? ' stat--warn' : ''}`}>
        <div className="stat__l">No driver</div>
        <div className="stat__v">
          {stats.noDriver.length}
          {firstNoDriver && (
            <small>
              {firstNoDriver.severity} · {ageLabel(firstNoDriver.opened_at)}
            </small>
          )}
        </div>
      </div>
      {!viewer && (
        <div className={`stat${stats.needsYou.length > 0 ? ' stat--accent' : ''}`}>
          <div className="stat__l">Needs you</div>
          <div className="stat__v">
            {stats.needsYou.length}
            {layers.length > 0 && <small>{layers.join(', ')}</small>}
          </div>
        </div>
      )}
      <div className="stat stat--agent">
        <div className="stat__l">Agent working</div>
        <div className="stat__v">
          {stats.working.length > 0 && <span className="stat__dot" aria-hidden="true" />}
          {stats.working.length}
        </div>
      </div>
    </div>
  )
}

function RoomCard({ room, user }: { room: RoomSnapshot; user: CurrentUser }) {
  const open = room.status === 'open'
  const mine = needsYou(room, user)
  const live = roomLiveBadge(room)
  const people = peopleFor(room)
  const shown = people.slice(0, MAX_PEOPLE)
  const classes = [
    'room-card',
    `room-card--${room.severity}`,
    mine ? 'room-card--mine' : '',
    open ? '' : 'room-card--closed',
  ]
    .filter(Boolean)
    .join(' ')

  return (
    <li>
      <Link to={`/rooms/${room.id}`} className={classes}>
        <span className="room-card__main">
          <span className="room-card__line">
            <span className="room-card__title">{room.title}</span>
            {mine && (
              <span className="room-card__needs">
                <Icon name="alert" size={12} />
                {mine.reason === 'approval' ? `Needs you · ${mine.layers[0]}` : 'No driver'}
              </span>
            )}
          </span>
          <span className="room-card__meta muted">
            {room.pipeline_id} · opened {ageLabel(room.opened_at)} ·{' '}
            {room.driver ? (
              `driver ${room.driver.name}`
            ) : (
              <span className={open ? 'room-card__nodriver' : undefined}>no driver</span>
            )}
            {!open && room.resolution ? ` · ${room.resolution}` : ''}
          </span>
        </span>
        <span className="room-card__side">
          <Chip tone={severityTone(room.severity)}>{room.severity}</Chip>
          <span className={`room-card__live room-card__live--${live.kind}`}>
            {live.kind === 'done' ? (
              <Icon name="check" size={12} />
            ) : live.kind === 'error' ? (
              <Icon name="alert" size={12} />
            ) : (
              <span className={`room-card__dot${live.kind === 'idle' ? ' room-card__dot--hollow' : ''}`} aria-hidden="true" />
            )}
            {live.label}
          </span>
          {shown.length > 0 && (
            <span className="room-card__people" role="group" aria-label="People in the room">
              {shown.map((p) => (
                <Avatar key={p.id} actor={{ kind: 'human', id: p.id, name: p.name }} size="sm" driver={p.driver} />
              ))}
              {people.length > shown.length && (
                <span className="room-card__more">+{people.length - shown.length}</span>
              )}
            </span>
          )}
          <span className="room-card__age">{ageLabel(room.opened_at)}</span>
          <Icon name="chevron-right" className="room-card__chev" />
        </span>
      </Link>
    </li>
  )
}

/** Room members, the driver first (and ringed), without duplicates. */
function peopleFor(room: RoomSnapshot): { id: string; name: string; driver: boolean }[] {
  const out: { id: string; name: string; driver: boolean }[] = []
  if (room.driver) out.push({ id: room.driver.id, name: room.driver.name, driver: true })
  for (const m of room.members) {
    if (!out.some((p) => p.id === m.id)) out.push({ id: m.id, name: m.name, driver: false })
  }
  return out
}

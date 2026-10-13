import { useState } from 'react'
import { Link, useNavigate } from 'react-router-dom'
import type { FormEvent } from 'react'
import type { RoomSnapshot } from '../api/types'
import { api, isFixtures } from '../api/client'
import { useCurrentUser } from '../context/user'
import { isViewer } from '../state/permissions'
import { addFixtureRoom, emptySnapshot } from '../fixtures/player'
import { useRoomList } from '../hooks/useRoomList'
import { ageLabel } from '../lib/time'
import { sortRoomsForList } from '../lib/rooms'
import { errorText } from '../lib/errors'
import { Chip, Empty } from './ui'
import { severityTone } from '../lib/tones'
import { UserSwitcher } from './UserSwitcher'

const SEVERITIES = ['sev1', 'sev2', 'sev3', 'sev4']

export function RoomList() {
  const navigate = useNavigate()
  const { user } = useCurrentUser()
  const viewer = isViewer(user) // a viewer cannot create rooms (POST /api/rooms is 403 viewer)
  const { rooms, loading, error } = useRoomList()
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

  return (
    <div className="shell">
      <header className="topbar">
        <h1 className="brand">Piperoom</h1>
        <div className="topbar__right">
          {isFixtures() && <Chip tone="info">fixture mode</Chip>}
          <UserSwitcher />
        </div>
      </header>

      <main className="roomlist">
        {!viewer && (
        <section className="panel">
          <div className="panel__body">
            <form className="new-room" onSubmit={onSubmit}>
              <h2 className="panel__title">New room</h2>
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
            {createError && <p className="error-text">{createError}</p>}
          </div>
        </section>
        )}

        <section className="panel roomlist__section">
          <div className="panel__head">
            <h2 className="panel__title">Open rooms</h2>
            <span className="panel__right muted">{openRooms.length}</span>
          </div>
          <div className="panel__body">
            {loading && <Empty>Loading rooms…</Empty>}
            {error && <p className="error-text">{error}</p>}
            {!loading && !error && openRooms.length === 0 && <Empty>No open rooms.</Empty>}
            <ul className="room-cards">
              {openRooms.map((r) => (
                <RoomCard key={r.id} room={r} />
              ))}
            </ul>
          </div>
        </section>

        {closedRooms.length > 0 && (
          <section className="panel roomlist__section">
            <div className="panel__head">
              <h2 className="panel__title">Closed</h2>
              <span className="panel__right muted">{closedRooms.length}</span>
            </div>
            <div className="panel__body">
              <ul className="room-cards">
                {closedRooms.map((r) => (
                  <RoomCard key={r.id} room={r} />
                ))}
              </ul>
            </div>
          </section>
        )}
      </main>
    </div>
  )
}

function RoomCard({ room }: { room: RoomSnapshot }) {
  return (
    <li>
      <Link to={`/rooms/${room.id}`} className="room-card">
        <span className="room-card__main">
          <span className="room-card__title">{room.title}</span>
          <span className="room-card__meta muted">
            {room.pipeline_id} · opened {ageLabel(room.opened_at)} ·{' '}
            {room.driver ? `driver ${room.driver.name}` : 'no driver'}
          </span>
        </span>
        <span className="room-card__side">
          <Chip tone={severityTone(room.severity)}>{room.severity}</Chip>
          <Chip tone={room.agent_status.state === 'error' ? 'fail' : 'info'}>
            {room.agent_status.state}
            {room.agent_status.detail ? ` · ${room.agent_status.detail}` : ''}
          </Chip>
        </span>
      </Link>
    </li>
  )
}

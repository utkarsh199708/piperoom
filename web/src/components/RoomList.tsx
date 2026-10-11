import { useState } from 'react'
import { useNavigate } from 'react-router-dom'
import type { FormEvent } from 'react'
import type { RoomSnapshot } from '../api/types'
import { api, isFixtures } from '../api/client'
import { addFixtureRoom, emptySnapshot } from '../fixtures/player'
import { useRoomList } from '../hooks/useRoomList'
import { useCurrentUser } from '../context/user'
import { ageLabel } from '../lib/time'
import { Chip, Empty } from './ui'
import { severityTone } from '../lib/tones'
import { UserSwitcher } from './UserSwitcher'

const SEVERITIES = ['sev1', 'sev2', 'sev3', 'sev4']

export function RoomList() {
  const navigate = useNavigate()
  const { user } = useCurrentUser()
  const { rooms, loading, error } = useRoomList()
  const [title, setTitle] = useState('')
  const [severity, setSeverity] = useState('sev2')
  const [pipelineId, setPipelineId] = useState('orders')
  const [busy, setBusy] = useState(false)

  const openRooms = rooms.filter((r) => r.status === 'open')
  const closedRooms = rooms.filter((r) => r.status !== 'open')

  const onSubmit = async (e: FormEvent) => {
    e.preventDefault()
    if (!title.trim()) return
    setBusy(true)
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
      const room = await api.createRoom(user.id, {
        title: title.trim(),
        severity,
        pipeline_id: pipelineId.trim() || 'orders',
      })
      navigate(`/rooms/${room.id}`)
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
          </div>
        </section>

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
                <RoomCard key={r.id} room={r} onOpen={() => navigate(`/rooms/${r.id}`)} />
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
                  <RoomCard key={r.id} room={r} onOpen={() => navigate(`/rooms/${r.id}`)} />
                ))}
              </ul>
            </div>
          </section>
        )}
      </main>
    </div>
  )
}

function RoomCard({ room, onOpen }: { room: RoomSnapshot; onOpen: () => void }) {
  return (
    <li>
      <button type="button" className="room-card" onClick={onOpen}>
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
      </button>
    </li>
  )
}

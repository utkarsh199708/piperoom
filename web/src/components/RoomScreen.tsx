import { useMemo, useState } from 'react'
import { Link, useParams } from 'react-router-dom'
import { useRoom } from '../hooks/useRoom'
import { useTopology } from '../hooks/useTopology'
import { useCurrentUser } from '../context/user'
import { canHandoff } from '../state/permissions'
import { ageLabel } from '../lib/time'
import { Avatar, Chip } from './ui'
import { severityTone } from '../lib/tones'
import { UserSwitcher } from './UserSwitcher'
import { PipelineMap } from './PipelineMap'
import { Timeline } from './Timeline'
import { SteerBox } from './SteerBox'
import { ProposalsPanel } from './ProposalsPanel'
import { PresencePanel } from './PresencePanel'
import { ActivityFeed } from './ActivityFeed'
import { TracePanel } from './TracePanel'
import { HandoffAcceptBanner, HandoffDialog } from './HandoffDialog'

export function RoomScreen() {
  const { roomId = '' } = useParams()
  const { user } = useCurrentUser()
  const { state, presence, conn, error, actions } = useRoom(roomId)
  const { topology } = useTopology(state?.pipeline_id ?? '')
  const [traceOpen, setTraceOpen] = useState(false)
  const [handoffOpen, setHandoffOpen] = useState(false)
  const [copied, setCopied] = useState(false)

  const hasPendingApproval = useMemo(() => {
    if (!state) return false
    return state.proposals.some((p) =>
      (p.required_layers ?? []).some(
        (l) => p.approvals[l] === 'pending' && user.layers.includes(l),
      ),
    )
  }, [state, user.layers])

  const headerMembers = useMemo(() => {
    const ids = new Set<string>()
    const rows: { id: string; name: string }[] = []
    for (const p of presence) {
      if (!ids.has(p.id)) {
        ids.add(p.id)
        rows.push({ id: p.id, name: p.name })
      }
    }
    if (state) {
      for (const m of state.members) {
        if (!ids.has(m.id)) {
          ids.add(m.id)
          rows.push({ id: m.id, name: m.name })
        }
      }
    }
    return rows
  }, [presence, state])

  const invite = () => {
    const url = window.location.href
    if (!navigator.clipboard) return
    void navigator.clipboard.writeText(url).then(
      () => {
        setCopied(true)
        window.setTimeout(() => setCopied(false), 1500)
      },
      () => undefined,
    )
  }

  if (!state) {
    return (
      <div className="shell">
        <header className="topbar">
          <Link to="/" className="topbar__back">
            ← Rooms
          </Link>
          <UserSwitcher />
        </header>
        <main className="room">
          <p className="empty">
            {conn === 'error' ? `Could not load room: ${error ?? 'unknown error'}` : 'Loading room…'}
          </p>
        </main>
      </div>
    )
  }

  return (
    <div className="shell">
      <header className="topbar">
        <Link to="/" className="topbar__back" aria-label="Back to rooms">
          ← Rooms
        </Link>
        <div className="topbar__room">
          <h1 className="topbar__title">{state.title}</h1>
          <Chip tone={severityTone(state.severity)}>{state.severity}</Chip>
          <span className="muted topbar__id mono">#{state.id}</span>
          <span className="muted topbar__age">open {ageLabel(state.opened_at)}</span>
        </div>
        <div className="topbar__right">
          {conn === 'reconnecting' && <Chip tone="warn">reconnecting…</Chip>}
          {conn === 'connecting' && <Chip tone="info">connecting…</Chip>}
          <div className="presence-avatars" aria-label="People in the room">
            {headerMembers.slice(0, 5).map((m) => (
              <Avatar key={m.id} actor={{ kind: 'human', id: m.id, name: m.name }} size="sm" />
            ))}
            {headerMembers.length > 5 && (
              <span className="muted">+{headerMembers.length - 5}</span>
            )}
          </div>
          {canHandoff(state, user) && (
            <button type="button" className="btn btn--ghost" onClick={() => setHandoffOpen(true)}>
              Hand off
            </button>
          )}
          <button type="button" className="btn btn--ghost" onClick={invite} aria-label="Invite (copy link)">
            {copied ? 'Copied' : 'Invite'}
          </button>
          <UserSwitcher />
        </div>
      </header>

      {error && <div className="banner banner--error">{error}</div>}
      <HandoffAcceptBanner state={state} user={user} actions={actions} />

      <main className={`room-grid ${hasPendingApproval ? 'room-grid--approve-first' : ''}`}>
        <div className="room-col room-col--left">
          <PipelineMap topology={topology} onTrace={() => setTraceOpen(true)} />
        </div>

        <div className="room-col room-col--mid">
          {state.hypothesis && (
            <section className="panel hypothesis">
              <header className="panel__head">
                <h2 className="panel__title">Current best guess</h2>
                <Chip tone={state.hypothesis.confirmed ? 'ok' : 'info'}>
                  {state.hypothesis.confidence}
                </Chip>
              </header>
              <div className="panel__body">
                <p className="hypothesis__text">{state.hypothesis.text}</p>
                {(state.hypothesis.based_on_steps ?? []).length > 0 && (
                  <p className="muted hypothesis__based">
                    based on {state.hypothesis.based_on_steps.join(', ')}
                  </p>
                )}
              </div>
            </section>
          )}

          <section className="panel timeline-panel">
            <header className="panel__head">
              <h2 className="panel__title">Timeline</h2>
              <Chip tone="info">{state.agent_status.state}</Chip>
            </header>
            <div className="panel__body">
              <Timeline
                steps={state.steps}
                messages={state.messages}
                comments={state.comments}
                onComment={actions.comment}
              />
            </div>
          </section>

          <SteerBox state={state} user={user} actions={actions} />
        </div>

        <div className="room-col room-col--right">
          <ProposalsPanel state={state} user={user} actions={actions} />
          <PresencePanel members={state.members} presence={presence} />
          <ActivityFeed activity={state.activity} />
        </div>
      </main>

      {traceOpen && <TracePanel trace={state.trace} actions={actions} onClose={() => setTraceOpen(false)} />}
      {handoffOpen && (
        <HandoffDialog state={state} user={user} actions={actions} onClose={() => setHandoffOpen(false)} />
      )}
    </div>
  )
}

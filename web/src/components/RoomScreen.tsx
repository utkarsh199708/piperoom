import { useEffect, useMemo, useRef, useState } from 'react'
import { Link, useParams } from 'react-router-dom'
import { useRoom } from '../hooks/useRoom'
import { useTopology } from '../hooks/useTopology'
import { useCurrentUser } from '../context/user'
import { canHandoff, isViewer } from '../state/permissions'
import { ageLabel } from '../lib/time'
import { investigationSummary } from '../lib/investigation'
import { Chip, Panel } from './ui'
import { Avatar, Icon, Skeleton, StatusPill } from './ui-p4-temp'
import { severityTone } from '../lib/tones'
import { UserSwitcher } from './UserSwitcher'
import { PipelineMap } from './PipelineMap'
import { Timeline } from './Timeline'
import { SteerBox } from './SteerBox'
import { ProposalsPanel } from './ProposalsPanel'
import { PresencePanel } from './PresencePanel'
import { ActivityFeed } from './ActivityFeed'
import { SituationCards } from './SituationCards'
import { TracePanel } from './TracePanel'
import { HandoffAcceptBanner, HandoffDialog } from './HandoffDialog'
import { NoDriverBanner } from './NoDriverBanner'
import { StaleBanner } from './StaleBanner'
import '../styles/p4-temp.css'
import '../styles/room.css'

/** True once `active` has been true for `ms` without a break. */
function useAfter(active: boolean, ms: number): boolean {
  const [late, setLate] = useState(false)
  useEffect(() => {
    if (!active) return
    const t = window.setTimeout(() => setLate(true), ms)
    return () => {
      window.clearTimeout(t)
      setLate(false)
    }
  }, [active, ms])
  return active && late
}

function BackLink({ label }: { label?: string }) {
  return (
    <Link to="/" className="topbar__back" aria-label={label}>
      <Icon name="back" size={16} />
      Rooms
    </Link>
  )
}

/** Shown while the room loads, and when it cannot be loaded. */
function RoomPlaceholder({ failed, message }: { failed: boolean; message: string }) {
  return (
    <main className="room-wrap">
      {failed ? (
        <div className="room-error" role="alert">
          <Icon name="alert" size={20} />
          <p>{message}</p>
          <Link to="/" className="btn">
            ← Rooms
          </Link>
        </div>
      ) : (
        <div aria-busy="true">
          <section className="panel room-hero room-hero--loading" aria-hidden="true">
            <div className="room-skel__lane">
              {[0, 1, 2, 3, 4].map((n) => (
                <Skeleton key={n} h={96} className="room-skel__node" />
              ))}
            </div>
          </section>
          <section className="panel room-skel__steps" aria-hidden="true">
            <Skeleton h={18} w="40%" />
            <Skeleton h={54} />
            <Skeleton h={18} w="30%" />
            <Skeleton h={54} />
          </section>
          <p className="empty room-loading">{message}</p>
        </div>
      )}
    </main>
  )
}

export function RoomScreen() {
  const { roomId = '' } = useParams()
  const { user } = useCurrentUser()
  const { state, presence, conn, error, actions, stale, dismissStale, reload } = useRoom(roomId)
  // One topology refetch per executed action, plus the hook's 30 s refresh
  // while the room is open (stops once closed).
  const executedCount = useMemo(
    () => (state ? state.proposals.reduce((n, p) => n + p.executed.length, 0) : 0),
    [state],
  )
  const { topology, updatedAt } = useTopology(state?.pipeline_id ?? '', {
    refreshKey: executedCount,
    active: state?.status === 'open',
  })
  const [traceOpen, setTraceOpen] = useState(false)
  const [handoffOpen, setHandoffOpen] = useState(false)
  const [copied, setCopied] = useState(false)
  const handoffBtn = useRef<HTMLButtonElement>(null)
  const paused = useAfter(conn === 'reconnecting', 10_000)

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
      <div className="shell room-ui">
        <header className="topbar">
          <BackLink />
          <span className="topbar__spacer" />
          <UserSwitcher />
        </header>
        {stale && <StaleBanner onReload={reload} onDismiss={dismissStale} />}
        <RoomPlaceholder
          failed={conn === 'error'}
          message={conn === 'error' ? `Could not load room: ${error ?? 'unknown error'}` : 'Loading room…'}
        />
      </div>
    )
  }

  const viewer = isViewer(user)

  return (
    <div className="shell room-ui">
      <header className="topbar topbar--room">
        <BackLink label="Back to rooms" />
        <div className="topbar__room">
          <h1 className="topbar__title" title={state.title}>
            {state.title}
          </h1>
          <Chip tone={severityTone(state.severity)}>{state.severity}</Chip>
          {state.alert && (
            <Chip tone="info" title="This room was opened by an alert webhook">
              {state.alert.source ? `alert · ${state.alert.source}` : 'alert'}
            </Chip>
          )}
          <span className="topbar__meta">
            <span className="mono topbar__id">#{state.id}</span>
            <span aria-hidden="true">·</span>
            <span className="topbar__age">open {ageLabel(state.opened_at)}</span>
          </span>
          <StatusPill state={state.agent_status.state} detail={state.agent_status.detail} />
        </div>
        <div className="topbar__right">
          {conn === 'reconnecting' && <Chip tone="warn">reconnecting…</Chip>}
          {conn === 'connecting' && <Chip tone="info">connecting…</Chip>}
          <div className="presence-avatars stack" aria-label="People in the room">
            {headerMembers.slice(0, 5).map((m) => (
              <Avatar
                key={m.id}
                actor={{ kind: 'human', id: m.id, name: m.name }}
                size="sm"
                driver={state.driver?.id === m.id}
              />
            ))}
            {headerMembers.length > 5 && <span className="more">+{headerMembers.length - 5}</span>}
          </div>
          {canHandoff(state, user) && (
            <button
              type="button"
              className="btn btn--ghost"
              ref={handoffBtn}
              onClick={() => setHandoffOpen(true)}
            >
              Hand off
            </button>
          )}
          <button type="button" className="btn btn--ghost" onClick={invite} aria-label="Invite (copy link)">
            {copied ? 'Copied' : 'Invite'}
          </button>
          <UserSwitcher />
        </div>
      </header>

      {paused && (
        <div className="banner banner--paused" aria-live="polite">
          <span>Live updates paused — retrying</span>
        </div>
      )}
      {stale && <StaleBanner onReload={reload} onDismiss={dismissStale} />}
      {error && <div className="banner banner--error">{error}</div>}
      {state.status === 'closed' && state.resolution && (
        <div className="banner banner--resolved" role="status">
          <span>Resolved</span>
          <span className="muted">{state.resolution}</span>
        </div>
      )}
      <NoDriverBanner state={state} user={user} actions={actions} />
      <HandoffAcceptBanner state={state} user={user} actions={actions} />

      <main className="room-wrap">
        <div className="room-hero">
          <PipelineMap
            topology={topology}
            updatedAt={updatedAt}
            onTrace={viewer ? undefined : () => setTraceOpen(true)}
          />
        </div>

        <SituationCards state={state} user={user} actions={actions} />

        <div className={`room-grid ${hasPendingApproval ? 'room-grid--approve-first' : ''}`}>
          <div className="room-col room-col--main">
            <div className="room-proposals" id="proposals">
              <ProposalsPanel state={state} user={user} actions={actions} />
            </div>

            <Panel
              className="timeline-panel investigation"
              title={
                <>
                  Investigation{' '}
                  <span className="panel__count">{investigationSummary(state.steps)}</span>
                </>
              }
            >
              <Timeline
                steps={state.steps}
                messages={state.messages}
                comments={state.comments}
                onComment={actions.comment}
                readOnly={viewer}
                agentState={state.agent_status.state}
                agentDetail={state.agent_status.detail}
              />
            </Panel>

            <SteerBox state={state} user={user} actions={actions} />
          </div>

          <div className="room-col room-col--side">
            <PresencePanel members={state.members} presence={presence} selfId={user.id} />
            <ActivityFeed activity={state.activity} />
          </div>
        </div>
      </main>

      {traceOpen && !viewer && <TracePanel trace={state.trace} actions={actions} onClose={() => setTraceOpen(false)} />}
      {handoffOpen && (
        <HandoffDialog
          state={state}
          user={user}
          actions={actions}
          onClose={() => {
            setHandoffOpen(false)
            // Some browsers do not focus a button on click, so the dialog cannot rely on
            // remembering it. Put focus back on the opener by hand.
            handoffBtn.current?.focus()
          }}
        />
      )}
    </div>
  )
}

import { useMemo, useState } from 'react'
import { useParams, useSearchParams } from 'react-router-dom'
import { isFixtures } from '../api/client'
import { useRoom } from '../hooks/useRoom'
import { useTopology } from '../hooks/useTopology'
import { useCurrentUser } from '../context/user'
import { canClose, canDecideLayer, canHandoff, canRequestControl, canTakeControl, isViewer } from '../state/permissions'
import { ageLabel } from '../lib/time'
import { cn } from '@/lib/utils'
import { PipelineMap } from './PipelineMap'
import { Timeline } from './Timeline'
import { SteerBox } from './SteerBox'
import { ProposalsPanel } from './ProposalsPanel'
import { PresencePanel } from './PresencePanel'
import { ActivityFeed } from './ActivityFeed'
import { TracePanel } from './TracePanel'
import { HandoffAcceptBanner, HandoffDialog } from './HandoffDialog'
import { CloseRoomDialog } from './CloseRoomDialog'
import { NoDriverBanner } from './NoDriverBanner'
import { StaleBanner } from './StaleBanner'
import { useBreadcrumbTail, useRegisterRoomCommands } from './shell/context'
import { HypothesisCard } from './room/HypothesisCard'
import { PROPOSAL_ANCHOR, ProposalsSummary } from './room/ProposalsSummary'
import { RoomError } from './room/RoomError'
import { RoomHeader } from './room/RoomHeader'
import { RoomSkeleton } from './room/RoomSkeleton'
import { useRoomShortcuts } from './room/useRoomShortcuts'

// The room screen (docs/design/ui-v2: Room-Investigating, -NoDriver, -Viewer, -Loading).
// One column for the investigation (header, pipeline lane, hypothesis, fix, step stream,
// composer) and a 300px rail from 1280px up (proposals summary, who is here, activity);
// below that the rail stacks under the column. All state and rules live in useRoom and
// state/permissions; this file only places things.
export function RoomScreen() {
  const { roomId = '' } = useParams()
  const [searchParams] = useSearchParams()
  const { user } = useCurrentUser()
  const { state: liveState, presence, conn, liveDown, error, actions, stale, dismissStale, reload } = useRoom(roomId)
  // Nothing from the log has been applied yet and the socket is not open: the room is the snapshot only.
  // Its proposals stand in until the replay rebuilds them, so the room never claims "no proposals"
  // (QA9: live updates down showed "Proposals 0" for a room waiting on approval).
  const snapshotOnly = !!liveState && liveState.last_seq === 0 && conn !== 'live'
  const state = useMemo(
    () =>
      liveState && snapshotOnly && liveState.proposals.length === 0 && liveState.snapshot_proposals.length > 0
        ? { ...liveState, proposals: liveState.snapshot_proposals }
        : liveState,
    [liveState, snapshotOnly],
  )
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
  const [closeOpen, setCloseOpen] = useState(false)
  const [copied, setCopied] = useState(false)

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
    // You are here too, even before the server has reported you.
    if (!ids.has(user.id)) rows.push({ id: user.id, name: user.name })
    return rows
  }, [presence, state, user.id, user.name])

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

  useBreadcrumbTail(state?.pipeline_id) // shell breadcrumb: "Rooms › <pipeline>"

  const takeControl = state && canTakeControl(state, user) ? () => actions.grantControl(user.id) : undefined
  const requestControl =
    state && !takeControl && canRequestControl(state, user) ? actions.requestControl : undefined
  const openHandoff = state && canHandoff(state, user) ? () => setHandoffOpen(true) : undefined
  const openTrace = state && !isViewer(user) ? () => setTraceOpen(true) : undefined
  const openClose = state && canClose(state, user) ? () => setCloseOpen(true) : undefined

  // The command menu (⌘K) offers what this room offers right now. Same permission
  // checks and callbacks as the buttons below; a viewer gets none of the writes.
  useRegisterRoomCommands(
    state
      ? {
          title: state.title,
          takeControl,
          requestControl,
          handoff: openHandoff,
          trace: openTrace,
          closeRoom: openClose,
          invite,
        }
      : null,
  )
  // The keys printed on the buttons (H, C, T, Shift+T).
  useRoomShortcuts({
    handoff: openHandoff,
    close: openClose,
    trace: openTrace,
    control: takeControl ?? requestControl,
  })

  // `?loading=1` previews the skeleton in fixture mode, where the room loads at once.
  const previewLoading = isFixtures() && searchParams.get('loading') === '1'
  if (!state || previewLoading) {
    const failed = !previewLoading && (conn === 'error' || conn === 'not_found')
    return (
      <>
        {stale && <StaleBanner onReload={reload} onDismiss={dismissStale} />}
        {failed ? (
          <RoomError
            message={error ?? (conn === 'not_found' ? 'this room does not exist' : 'unknown error')}
            onRetry={reload}
          />
        ) : (
          <RoomSkeleton pipelineId={state?.pipeline_id} />
        )}
      </>
    )
  }

  const viewer = isViewer(user)
  const stalledFor = state.status === 'open' && state.opened_at ? ageLabel(state.opened_at) : undefined
  const showProposals = state.proposals.length > 0 || user.solo === true
  // On a phone the fix waiting on this person comes before the map and the hypothesis (QA9).
  const needsMe = state.proposals.some(
    (p) => p.status === 'open' && p.required_layers.some((l) => canDecideLayer(p, user, l)),
  )
  // Empty lists while the log has not arrived say so instead of "nothing happened yet".
  const history: 'loading' | 'down' | undefined = snapshotOnly ? (liveDown ? 'down' : 'loading') : undefined

  return (
    <div className="flex min-h-[calc(100dvh-3rem)] flex-col text-sm">
      {stale && <StaleBanner onReload={reload} onDismiss={dismissStale} />}
      {liveDown && (
        <div
          role="status"
          data-testid="live-down"
          className="flex flex-wrap items-center gap-x-2.5 border-b border-border bg-warning-subtle px-4 py-2 font-medium text-warning-foreground sm:px-6"
        >
          <span>Live updates down — showing last snapshot</span>
          <span className="font-normal text-muted-foreground">Reconnecting… what you see may be out of date.</span>
        </div>
      )}
      {error && (
        <div
          role="alert"
          className="border-b border-border bg-destructive-subtle px-4 py-2 font-medium text-destructive-fg sm:px-6"
        >
          {error}
        </div>
      )}
      {state.status === 'closed' && state.resolution && (
        <div
          role="status"
          className="flex min-h-10 flex-wrap items-center gap-x-2.5 border-b border-border bg-success-subtle px-4 py-1.5 font-medium text-success-foreground sm:px-6"
        >
          <span>Resolved</span>
          <span className="font-normal text-muted-foreground">{state.resolution}</span>
        </div>
      )}
      <NoDriverBanner state={state} user={user} actions={actions} />
      <HandoffAcceptBanner state={state} user={user} actions={actions} />

      <div className="flex min-w-0 flex-1 flex-col xl:flex-row">
        <div className="flex min-w-0 flex-1 flex-col">
          <RoomHeader
            state={state}
            user={user}
            members={headerMembers}
            conn={conn}
            copied={copied}
            onHandoff={() => setHandoffOpen(true)}
            onCloseRoom={() => setCloseOpen(true)}
            onInvite={invite}
          />

          <div className="flex flex-col gap-4 px-4 pb-4 pt-4 sm:px-6">
            <PipelineMap
              topology={topology}
              updatedAt={updatedAt}
              pipelineId={state.pipeline_id}
              stalledFor={stalledFor}
              onTrace={viewer ? undefined : () => setTraceOpen(true)}
            />

            {state.hypothesis && <HypothesisCard hypothesis={state.hypothesis} steps={state.steps} />}

            {showProposals && (
              <div
                id={PROPOSAL_ANCHOR}
                tabIndex={-1}
                className={cn('scroll-mt-4 focus:outline-none', needsMe && 'max-[599px]:order-first')}
              >
                <ProposalsPanel state={state} user={user} actions={actions} stale={stale} />
              </div>
            )}

            <section aria-labelledby="investigation-heading" className="flex flex-col gap-2">
              <div className="flex items-center gap-2 pt-1">
                <h2 id="investigation-heading" className="m-0 text-sm font-medium">
                  Investigation
                </h2>
                {state.steps.length > 0 && (
                  <span className="text-xs tabular-nums text-subtle-foreground">
                    {state.steps.length} {state.steps.length === 1 ? 'step' : 'steps'}
                  </span>
                )}
              </div>
              <Timeline
                steps={state.steps}
                messages={state.messages}
                comments={state.comments}
                onComment={actions.comment}
                readOnly={viewer}
                working={state.agent_status.state === 'working'}
                onTakeControl={takeControl}
                history={history}
              />
            </section>
          </div>

          <SteerBox state={state} user={user} actions={actions} />
        </div>

        <aside
          aria-label="Room details"
          className="flex w-full shrink-0 flex-col border-t border-border xl:w-[300px] xl:border-l xl:border-t-0"
        >
          <ProposalsSummary proposals={state.proposals} />
          <div className="h-px shrink-0 bg-border" />
          <PresencePanel
            members={state.members}
            presence={presence}
            selfId={user.id}
            selfName={user.name}
            selfIsViewer={viewer}
          />
          <div className="h-px shrink-0 bg-border" />
          <ActivityFeed activity={state.activity} history={history} />
        </aside>
      </div>

      {traceOpen && !viewer && <TracePanel trace={state.trace} actions={actions} onClose={() => setTraceOpen(false)} />}
      {handoffOpen && (
        <HandoffDialog state={state} user={user} actions={actions} onClose={() => setHandoffOpen(false)} />
      )}
      {/* Gone once room.closed arrives: the "Resolved" strip above takes over. */}
      {closeOpen && state.status === 'open' && (
        <CloseRoomDialog actions={actions} onClose={() => setCloseOpen(false)} />
      )}
    </div>
  )
}

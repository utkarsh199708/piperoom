import { useMemo, useState } from 'react'
import { useParams, useSearchParams } from 'react-router-dom'
import { isFixtures } from '../api/client'
import { useRoom } from '../hooks/useRoom'
import { useTopology } from '../hooks/useTopology'
import { useCurrentUser } from '../context/user'
import { canClose, canHandoff, canRequestControl, canTakeControl, isViewer } from '../state/permissions'
import { ageLabel } from '../lib/time'
import { PipelineMap } from './PipelineMap'
import { Timeline } from './Timeline'
import { SteerBox } from './SteerBox'
import { ProposalsPanel } from './ProposalsPanel'
import { PresencePanel } from './PresencePanel'
import { allVerified } from '@/lib/verification'
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
  const [closeOpen, setCloseOpen] = useState(false)
  const [copied, setCopied] = useState(false)

  const headerMembers = useMemo(() => {
    const ids = new Set<string>()
    const rows: { id: string; name: string; verified: boolean }[] = []
    // Verified only when every server report on the person agrees (presence, event log, /api/me).
    const reports = (id: string) => [
      presence.find((p) => p.id === id)?.verified,
      state?.members.find((m) => m.id === id)?.verified,
      id === user.id ? user.verified : undefined,
    ]
    for (const p of presence) {
      if (!ids.has(p.id)) {
        ids.add(p.id)
        rows.push({ id: p.id, name: p.name, verified: allVerified(...reports(p.id)) })
      }
    }
    if (state) {
      for (const m of state.members) {
        if (!ids.has(m.id)) {
          ids.add(m.id)
          rows.push({ id: m.id, name: m.name, verified: allVerified(...reports(m.id)) })
        }
      }
    }
    // You are here too, even before the server has reported you.
    if (!ids.has(user.id)) rows.push({ id: user.id, name: user.name, verified: allVerified(...reports(user.id)) })
    return rows
  }, [presence, state, user.id, user.name, user.verified])

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

  return (
    <div className="flex min-h-[calc(100dvh-3rem)] flex-col text-sm">
      {stale && <StaleBanner onReload={reload} onDismiss={dismissStale} />}
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
              <div id={PROPOSAL_ANCHOR} tabIndex={-1} className="scroll-mt-4 focus:outline-none">
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
            selfVerified={user.verified}
          />
          <div className="h-px shrink-0 bg-border" />
          <ActivityFeed activity={state.activity} />
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

import { useContext, useMemo, useRef, useState } from 'react'
import { ArrowDown } from 'lucide-react'
import { useParams, useSearchParams } from 'react-router-dom'
import { toast } from 'sonner'
import { exportRoom, isFixtures } from '../api/client'
import { errorText } from '../lib/errors'
import { useRoom } from '../hooks/useRoom'
import { useTopology } from '../hooks/useTopology'
import { useJumpToLatest } from '../hooks/useJumpToLatest'
import { useRoomAttention } from '../hooks/useRoomAttention'
import { useCurrentUser } from '../context/user'
import { canAcceptHandoff, canClose, canHandoff, canRequestControl, canTakeControl, isViewer } from '../state/permissions'
import { ageLabel } from '../lib/time'
import { needsYou } from '../lib/rooms'
import { handoffInFlight } from '../lib/handoff'
import { useNow } from './room/useNow'
import { PipelineMap } from './PipelineMap'
import { Timeline } from './Timeline'
import { SteerBox } from './SteerBox'
import { ProposalsPanel } from './ProposalsPanel'
import { PresencePanel } from './PresencePanel'
import { ActivityFeed } from './ActivityFeed'
import { TracePanel } from './TracePanel'
import { HANDOFF_READ_ID, HandoffAcceptBanner, HandoffDialog } from './HandoffDialog'
import { CloseRoomDialog } from './CloseRoomDialog'
import { NoDriverBanner } from './NoDriverBanner'
import { StaleBanner } from './StaleBanner'
import { DecisionHandleContext, RoomListContext, useBreadcrumbTail, useRegisterRoomCommands } from './shell/context'
import type { DecisionHandle } from './shell/context'
import { DecisionBar } from './room/DecisionBar'
import { HypothesisCard } from './room/HypothesisCard'
import { PROPOSAL_ANCHOR, ProposalsSummary } from './room/ProposalsSummary'
import { RoomError } from './room/RoomError'
import { RoomHeader } from './room/RoomHeader'
import { RoomSkeleton } from './room/RoomSkeleton'
import { ReplayProgress } from './room/ReplayProgress'
import { SuggestionsPanel } from './room/SuggestionsPanel'
import { NotifyToggle } from './room/NotifyToggle'
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
  const handleRef = useRef<DecisionHandle | null>(null)
  const decisionRegistry = useMemo(
    () => ({ get: () => handleRef.current, set: (h: DecisionHandle | null) => void (handleRef.current = h) }),
    [],
  )
  const roomList = useContext(RoomListContext)
  const elsewhereNeedsMe = useMemo(
    () => (roomList?.rooms ?? []).some((r) => r.id !== roomId && needsYou(r, user)),
    [roomList, roomId, user],
  )
  const { state, presence, conn, error, actions, stale, dismissStale, reload } = useRoom(roomId)
  // R10: the first replay is done once the log has been folded up to the snapshot's last_seq.
  const replayTarget = state?.replay_target ?? 0
  const replaying =
    replayTarget > 0 && (state?.last_seq ?? 0) < replayTarget && conn !== 'error' && conn !== 'not_found'
  const { permission, enable: enableNotify } = useRoomAttention(
    state,
    user,
    replaying,
    {
      approve: () => handleRef.current?.approve(),
      view: () => handleRef.current?.scrollTo(),
      readHandoff: () => document.getElementById(HANDOFF_READ_ID)?.click(),
      steer: () => document.querySelector<HTMLTextAreaElement>('textarea[aria-label="Steer the agent"]')?.focus(),
    },
    elsewhereNeedsMe,
  )
  const { sentinelRef, unseen, jump } = useJumpToLatest(state?.activity ?? [], replaying)
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
  const [exporting, setExporting] = useState(false)
  const now = useNow(5_000) // a request past expires_at stops counting as in flight

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

  const exportTranscript = () => {
    if (!roomId || exporting) return
    setExporting(true)
    exportRoom(roomId)
      .then(({ text }) => {
        const url = URL.createObjectURL(new Blob([text], { type: 'application/json' }))
        const a = document.createElement('a')
        a.href = url
        a.download = `piperoom-${roomId}.json`
        document.body.appendChild(a)
        a.click()
        a.remove()
        URL.revokeObjectURL(url)
      })
      .catch((e: unknown) => toast.error(`Could not export the room: ${errorText(e)}`))
      .finally(() => setExporting(false))
  }

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
          readHandoff: state.handoffs.some((h) => canAcceptHandoff(user, h))
            ? () => document.getElementById(HANDOFF_READ_ID)?.click()
            : undefined,
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
    <DecisionHandleContext.Provider value={decisionRegistry}>
    <div className="flex min-h-full flex-col text-sm">
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
      {replaying && <ReplayProgress done={state.last_seq} total={replayTarget} />}
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
            onExport={viewer ? undefined : exportTranscript}
            exporting={exporting}
            handoffBusy={state ? handoffInFlight(state, now) !== null : false}
          />
          <DecisionBar state={state} user={user} />

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
                <ProposalsPanel state={state} user={user} actions={actions} stale={stale} ribbon={false} />
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
                agent={state.agent_status}
              />
            </section>
            <div ref={sentinelRef} aria-hidden="true" />
          </div>

          {unseen > 0 && (
            <button
              type="button"
              onClick={jump}
              className="fixed bottom-36 right-4 z-20 flex min-h-11 items-center gap-1.5 rounded-full border border-border bg-card px-4 text-sm font-medium shadow-md hover:bg-accent focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring sm:right-6 xl:right-[324px]"
            >
              <ArrowDown aria-hidden="true" className="size-4" />
              Jump to latest
              <span className="rounded-full bg-primary px-1.5 font-mono text-xs tabular-nums text-primary-foreground">
                {unseen >= 20 ? '20+' : unseen}
              </span>
            </button>
          )}

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
          <SuggestionsPanel suggestions={state.suggestions} />
          <NotifyToggle permission={permission} onEnable={() => void enableNotify()} />
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
    </DecisionHandleContext.Provider>
  )
}

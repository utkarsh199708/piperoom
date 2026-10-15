import { useCallback, useContext, useEffect, useMemo, useRef, useState } from 'react'
import { ArrowDown } from 'lucide-react'
import { Button } from '@/components/ui/button'
import { useParams, useSearchParams } from 'react-router-dom'
import { toast } from 'sonner'
import { exportRoom, isFixtures } from '../api/client'
import { downloadText, exportErrorText } from '../lib/postmortem'
import { useRoom } from '../hooks/useRoom'
import { useTopology } from '../hooks/useTopology'
import { useMediaQuery } from '../hooks/useMediaQuery'
import { rememberRoomTitle } from '../lib/signinNotice'
import { useJumpToLatest } from '../hooks/useJumpToLatest'
import { useRoomAttention } from '../hooks/useRoomAttention'
import { useCatchUp } from '../hooks/useCatchUp'
import { CatchUpStrip } from './room/CatchUpStrip'
import { useCurrentUser } from '../context/user'
import { canAcceptHandoff, canClose, canHandoff, canRequestControl, canTakeControl, isViewer } from '../state/permissions'
import { ageLabel } from '../lib/time'
import { needsYou } from '../lib/rooms'
import { handoffInFlight } from '../lib/handoff'
import { useNow } from './room/useNow'
import { Tabs, TabsContent, TabsList, TabsTrigger } from '@/components/ui/tabs'
import { PipelineMap } from './PipelineMap'
import { Timeline } from './Timeline'
import { SteerBox } from './SteerBox'
import { ProposalsPanel } from './ProposalsPanel'
import { PresencePanel } from './PresencePanel'
import { allVerified } from '@/lib/verification'
import { ActivityFeed } from './ActivityFeed'
import { TracePanel } from './TracePanel'
import { HANDOFF_READ_ID, HandoffAcceptBanner, HandoffDialog } from './HandoffDialog'
import { CloseRoomDialog } from './CloseRoomDialog'
import { NoDriverBanner } from './NoDriverBanner'
import { ChangesLane } from './room/ChangesLane'
import { LabelDialog } from './room/LabelDialog'
import { SimilarCard } from './room/SimilarCard'
import type { ChecklistWiring, CitedStep } from './Timeline'
import { StaleBanner } from './StaleBanner'
import { Badge } from '@/components/ui/badge'
import { ShadowPanel } from './shadow/ShadowPanel'
import { DecisionHandleContext, RoomListContext, useBreadcrumbTail, useRegisterRoomCommands } from './shell/context'
import type { DecisionHandle } from './shell/context'
import { DecisionBar } from './room/DecisionBar'
import { HypothesisCard } from './room/HypothesisCard'
import { usePeople } from '../hooks/usePeople'
import { PROPOSAL_ANCHOR, ProposalsSummary } from './room/ProposalsSummary'
import { RoomError } from './room/RoomError'
import { ClosedRoomActions, RoomHeader } from './room/RoomHeader'
import { PinnedHandoffs } from './room/PinnedHandoffs'
import { PostmortemDialog } from './room/PostmortemDialog'
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
  // Below 1280px the rail becomes tabs; the chosen one is remembered per room for this visit.
  const wide = useMediaQuery('(min-width: 1280px)')
  const phone = !useMediaQuery('(min-width: 768px)')
  const tabKey = `piperoom.tab.${roomId}`
  const [tab, setTabState] = useState<RoomTab>(() => readTab(tabKey))
  const setTab = useCallback(
    (next: RoomTab) => {
      setTabState(next)
      try {
        sessionStorage.setItem(tabKey, next)
      } catch {
        // storage blocked: the tab lasts until reload
      }
    },
    [tabKey],
  )
  const narrowRef = useRef(!wide)
  narrowRef.current = !wide
  const decisionRegistry = useMemo(
    () => ({
      // "View steps" must land on a visible card: on the tabbed layout the Fix tab opens first.
      get: (): DecisionHandle | null => {
        const h = handleRef.current
        if (!h) return null
        // Reject opens a reason field in the card: in a hidden Fix panel it could not take focus,
        // and the card would then refuse every later Approve/Reject while "rejecting".
        const onFixTab = (run: () => void) => () => {
          if (narrowRef.current) {
            setTab('fix')
            window.requestAnimationFrame(run)
          } else run()
        }
        return { ...h, reject: onFixTab(() => h.reject()), scrollTo: onFixTab(() => h.scrollTo()) }
      },
      set: (h: DecisionHandle | null) => void (handleRef.current = h),
    }),
    [setTab],
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
      view: () => decisionRegistry.get()?.scrollTo(),
      readHandoff: focusHandoff,
      steer: () => document.querySelector<HTMLTextAreaElement>('textarea[aria-label="Steer the agent"]')?.focus(),
    },
    elsewhereNeedsMe,
  )
  const catchUp = useCatchUp(state, conn, user)
  const { sentinelRef, unseen, jump } = useJumpToLatest(state?.activity ?? [], replaying)
  // One topology refetch per executed action, plus the hook's 30 s refresh
  // while the room is open (stops once closed).
  const executedCount = useMemo(
    // A verdict also re-polls at once, so the lane shows the healed node without waiting 30 s.
    () => (state ? state.proposals.reduce((n, p) => n + p.executed.length + (p.verification ? 1 : 0), 0) : 0),
    [state],
  )
  const { topology, updatedAt, notFound, retry: retryTopology } = useTopology(state?.pipeline_id ?? '', {
    refreshKey: executedCount,
    active: state?.status === 'open',
  })
  // The sign-in screen names this room if the session ends while it is open.
  useEffect(() => {
    rememberRoomTitle(state?.title)
    return () => rememberRoomTitle(undefined)
  }, [state?.title])
  const [layerFilter, setLayerFilter] = useState<string | null>(null)
  const [citedStep, setCitedStep] = useState<CitedStep | null>(null)
  const citeNonce = useRef(0)
  // A citation chip: on the tabbed layout the steps live on the Investigation tab, so it opens first.
  // A layer filter hides the steps outside it, so it is cleared: the chip must land on its row.
  const citeStep = useCallback(
    (id: string) => {
      if (narrowRef.current) setTab('investigation')
      setLayerFilter(null)
      citeNonce.current += 1
      setCitedStep({ id, nonce: citeNonce.current })
    },
    [setTab],
  )
  const [traceOpen, setTraceOpen] = useState(false)
  const [handoffOpen, setHandoffOpen] = useState(false)
  const [closeOpen, setCloseOpen] = useState(false)
  const [closeText, setCloseText] = useState('')
  const [labelOpen, setLabelOpen] = useState(false)
  const [postmortemOpen, setPostmortemOpen] = useState(false)
  // "Ask the agent" on a checklist item fills the composer; n makes the same text count again.
  const [prefill, setPrefill] = useState<{ text: string; n: number } | undefined>()
  const [copied, setCopied] = useState(false)
  const copiedTimer = useRef<number | undefined>(undefined)
  // Stop the "Copied" reset when the screen goes away, so it cannot fire after unmount.
  useEffect(() => () => window.clearTimeout(copiedTimer.current), [])
  const [exporting, setExporting] = useState(false)
  // One GET /api/people per room mount: layers for the presence rows and the pending layers. A viewer gets none.
  const peopleState = usePeople()
  const peopleList = peopleState.status === 'ready' ? peopleState.people : undefined
  const now = useNow(5_000) // a request past expires_at stops counting as in flight

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

  const exportTranscript = () => {
    if (!roomId || exporting) return
    setExporting(true)
    exportRoom(roomId)
      .then(({ text }) => downloadText(text, `piperoom-${roomId}.json`, 'application/json'))
      // Only approvers, backups and auditors may export (not the driver): a 403 names the role, never the raw text.
      .catch((e: unknown) => toast.error(exportErrorText(e, state?.pipeline_id ?? '')))
      .finally(() => setExporting(false))
  }

  const invite = () => {
    const url = window.location.href
    if (!navigator.clipboard) return
    void navigator.clipboard.writeText(url).then(
      () => {
        setCopied(true)
        window.clearTimeout(copiedTimer.current)
        copiedTimer.current = window.setTimeout(() => setCopied(false), 1500)
      },
      () => undefined,
    )
  }

  const openPostmortem = () => setPostmortemOpen(true)

  useBreadcrumbTail(state?.pipeline_id) // shell breadcrumb: "Rooms › <pipeline>"

  const takeControl = state && canTakeControl(state, user) ? () => actions.grantControl(user.id) : undefined
  const requestControl =
    state && !takeControl && canRequestControl(state, user) ? actions.requestControl : undefined
  const openHandoff = state && canHandoff(state, user) ? () => setHandoffOpen(true) : undefined
  const openTrace = state && !isViewer(user) ? () => setTraceOpen(true) : undefined
  const canCloseHere = !!state && canClose(state, user)
  const openClose = canCloseHere
    ? () => {
        setCloseText('')
        setCloseOpen(true)
      }
    : undefined
  // The verified-fix nudge opens the same dialog with the resolution filled in.
  const closeWith = (text: string) => {
    setCloseText(text)
    setCloseOpen(true)
  }

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
          exportJson: isViewer(user) ? undefined : exportTranscript,
          postmortem: isViewer(user) ? undefined : openPostmortem,
          readHandoff: state.handoffs.some((h) => canAcceptHandoff(user, h)) ? focusHandoff : undefined,
        }
      : null,
  )
  // The keys printed on the buttons (H, C, T, Shift+T).
  useRoomShortcuts({
    handoff: openHandoff,
    close: openClose,
    trace: openTrace,
    control: takeControl ?? requestControl,
    clearFilter: layerFilter ? () => setLayerFilter(null) : undefined,
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
  const roomOpen = state.status === 'open'
  const checklist: ChecklistWiring = {
    done: state.checklist_done,
    access: { canTick: roomOpen && !viewer, canAsk: roomOpen && !viewer },
    onTick: actions.checklist,
    onAsk: (text) => setPrefill((p) => ({ text, n: (p?.n ?? 0) + 1 })),
  }

  const lane = (
    <PipelineMap
      topology={topology}
      updatedAt={updatedAt}
        notFound={notFound}
        onRetry={retryTopology}
      pipelineId={state.pipeline_id}
      stalledFor={stalledFor}
      onTrace={viewer ? undefined : () => setTraceOpen(true)}
      selectedLayer={layerFilter ?? undefined}
      onSelectLayer={setLayerFilter}
    />
  )
  const pinned = <PinnedHandoffs key={state.id} roomId={state.id} handoffs={state.handoffs} />
  const hypothesis = state.hypothesis ? (
    <HypothesisCard
      hypothesis={state.hypothesis}
      steps={state.steps}
      history={state.hypothesis_history}
      selfId={user.id}
      canDecide={!viewer && state.status === 'open'}
      onVerdict={actions.hypothesisVerdict}
      pipelineId={state.pipeline_id}
      insight={state.insight}
      onCite={citeStep}
    />
  ) : null
  const proposalsCard = showProposals ? (
    <div id={PROPOSAL_ANCHOR} tabIndex={-1} className="scroll-mt-4 focus:outline-none">
      <ProposalsPanel state={state} user={user} actions={actions} stale={stale} ribbon={false} people={peopleList} presence={presence} topology={topology} onCloseRoom={canCloseHere ? closeWith : undefined} />
    </div>
  ) : null
  const investigation = (
    <>
      <ChangesLane
        steps={state.steps}
        alertAt={state.opened_at}
        selectedLayer={layerFilter ?? undefined}
        onSelectLayer={setLayerFilter}
      />
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
        checklist={checklist}
        filterLayer={layerFilter ?? undefined}
        onClearFilter={() => setLayerFilter(null)}
        shadow={state.shadow}
        citedStep={citedStep}
      />
    </section>
    </>
  )
  const similar = <SimilarCard steps={state.steps} />
  const people = (
    <PresencePanel
      members={state.members}
      presence={presence}
      selfId={user.id}
      selfName={user.name}
      selfIsViewer={viewer}
      selfVerified={user.verified}
      people={peopleList}
    />
  )
  const suggestions = <SuggestionsPanel suggestions={state.suggestions} />
  const openProposals = state.proposals.filter((p) => p.status === 'open').length

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
          {state.labelled && <span className="font-normal text-muted-foreground">· labelled</span>}
          <span className="ml-auto flex flex-wrap items-center gap-2 py-0.5">
            {/* A shadow room takes the F8 label only after the blind label (409 not_labelled). */}
            {!state.labelled && !viewer && (!state.shadow || state.shadow_labelled) && (
              <Button size="sm" variant="outline" onClick={() => setLabelOpen(true)}>
                Add label
              </Button>
            )}
            <ClosedRoomActions
              onExport={viewer ? undefined : exportTranscript}
              onPostmortem={openPostmortem}
              exporting={exporting}
            />
          </span>
        </div>
      )}
      {state.shadow && (
        <div
          role="status"
          className="flex flex-wrap items-center gap-x-2 border-b border-border bg-agent-subtle px-4 py-2 text-agent-foreground sm:px-6"
        >
          <Badge variant="agent">Shadow</Badge>
          {state.status === 'open' ? (
            <span>
              The agent is investigating silently and proposes nothing. Work the incident as usual; its answer is
              shown after you close the room and record your own.
            </span>
          ) : (
            <span>Shadow room: the agent ran silently and proposed nothing.</span>
          )}
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
            onPostmortem={openPostmortem}
            exporting={exporting}
            handoffBusy={state ? handoffInFlight(state, now) !== null : false}
            onCite={citeStep}
          />
          <CatchUpStrip strip={catchUp.strip} onDismiss={catchUp.dismiss} />
          <DecisionBar state={state} user={user} placement={phone ? 'sheet' : 'top'} />

          {wide ? (
            <div className="flex flex-col gap-4 px-4 pb-4 pt-4 sm:px-6">
              {lane}
              {pinned}
              {hypothesis}
              {proposalsCard}
              {investigation}
              <div ref={sentinelRef} aria-hidden="true" />
            </div>
          ) : (
            <Tabs value={tab} onValueChange={(v) => setTab(v as RoomTab)} className="flex min-w-0 flex-col">
              <TabsList
                aria-label="Room sections"
                className="sticky top-[var(--decision-h,0px)] z-10 flex w-full overflow-x-auto bg-background px-2 sm:px-4"
              >
                <TabsTrigger value="investigation" className="coarse:h-11">Investigation</TabsTrigger>
                <TabsTrigger value="fix" className="coarse:h-11">
                  Fix
                  {openProposals > 0 && <span className="font-mono text-[11px] text-subtle-foreground">{openProposals}</span>}
                </TabsTrigger>
                <TabsTrigger value="people" className="coarse:h-11">
                  People
                  <span className="font-mono text-[11px] text-subtle-foreground">{headerMembers.length}</span>
                </TabsTrigger>
                <TabsTrigger value="activity" className="coarse:h-11">Activity</TabsTrigger>
              </TabsList>
              {/* Every panel stays mounted (hidden) so half-written comments survive a tab change. */}
              <TabsContent value="investigation" forceMount hidden={tab !== 'investigation'} className="flex flex-col gap-4 px-4 pb-4 pt-4 sm:px-6">
                {lane}
                {pinned}
                {hypothesis}
                {similar}
                {investigation}
                <div ref={sentinelRef} aria-hidden="true" />
              </TabsContent>
              <TabsContent value="fix" forceMount hidden={tab !== 'fix'} className="flex flex-col gap-4 px-4 pb-4 pt-4 sm:px-6">
                {proposalsCard}
                <ProposalsSummary proposals={state.proposals} shadow={state.shadow} />
              </TabsContent>
              <TabsContent value="people" forceMount hidden={tab !== 'people'}>
                {people}
              </TabsContent>
              <TabsContent value="activity" forceMount hidden={tab !== 'activity'}>
                {suggestions}
                <NotifyToggle permission={permission} onEnable={() => void enableNotify()} />
                <div className="h-px shrink-0 bg-border" />
                <ActivityFeed activity={state.activity} openedAt={state.opened_at} />
              </TabsContent>
            </Tabs>
          )}

          {unseen > 0 && (
            <button
              type="button"
              onClick={jump}
              className="fixed bottom-[calc(9rem+var(--sheet-h,0px))] right-4 z-20 flex min-h-11 items-center gap-1.5 rounded-full border border-border bg-card px-4 text-sm font-medium shadow-md hover:bg-accent focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring sm:right-6 xl:right-[324px]"
            >
              <ArrowDown aria-hidden="true" className="size-4" />
              Jump to latest
              <span className="rounded-full bg-primary px-1.5 font-mono text-xs tabular-nums text-primary-foreground">
                {unseen >= 20 ? '20+' : unseen}
              </span>
            </button>
          )}

          {state.shadow && state.status === 'closed' && <ShadowPanel state={state} user={user} />}
          {!state.shadow && <SteerBox state={state} user={user} actions={actions} prefill={prefill} conn={conn} />}
        </div>

        {wide && (
          <aside aria-label="Room details" className="flex w-full shrink-0 flex-col border-t border-border xl:w-[300px] xl:border-l xl:border-t-0">
            {similar}
            <ProposalsSummary proposals={state.proposals} shadow={state.shadow} />
            <div className="h-px shrink-0 bg-border" />
            {people}
            <div className="h-px shrink-0 bg-border" />
            {suggestions}
            <NotifyToggle permission={permission} onEnable={() => void enableNotify()} />
            <div className="h-px shrink-0 bg-border" />
            <ActivityFeed activity={state.activity} openedAt={state.opened_at} />
          </aside>
        )}
      </div>

      {traceOpen && !viewer && <TracePanel trace={state.trace} actions={actions} onClose={() => setTraceOpen(false)} />}
      {handoffOpen && (
        <HandoffDialog state={state} user={user} actions={actions} onClose={() => setHandoffOpen(false)} />
      )}
      {/* Gone once room.closed arrives: the "Resolved" strip above takes over. */}
      {closeOpen && state.status === 'open' && (
        <CloseRoomDialog state={state} actions={actions} initialText={closeText} onClose={() => setCloseOpen(false)} />
      )}
      {postmortemOpen && <PostmortemDialog state={state} onClose={() => setPostmortemOpen(false)} />}
      {labelOpen && state.status === 'closed' && !state.labelled && (
        <LabelDialog state={state} onSubmit={actions.label} onClose={() => setLabelOpen(false)} />
      )}
    </div>
    </DecisionHandleContext.Provider>
  )
}

// "Read summary" (toast, ⌘K): the pending card to me takes focus, which opens it.
function focusHandoff() {
  const el = document.getElementById(HANDOFF_READ_ID)
  el?.scrollIntoView?.({ block: 'nearest' })
  el?.focus()
}

type RoomTab = 'investigation' | 'fix' | 'people' | 'activity'
const TABS: RoomTab[] = ['investigation', 'fix', 'people', 'activity']

function readTab(key: string): RoomTab {
  try {
    const v = sessionStorage.getItem(key)
    return TABS.find((t) => t === v) ?? 'investigation'
  } catch {
    return 'investigation'
  }
}

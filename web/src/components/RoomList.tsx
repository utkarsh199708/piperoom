import { useCallback, useContext, useEffect, useMemo, useRef, useState } from 'react'
import { plainText } from '@/lib/title'
import type { FormEvent, KeyboardEvent as ReactKeyboardEvent, ReactNode } from 'react'
import { Link, useNavigate, useSearchParams } from 'react-router-dom'
import { AlertTriangle, ChevronDown, Plus, X } from 'lucide-react'
import { toast } from 'sonner'
import type { RoomSnapshot } from '../api/types'
import { api, isFixtures } from '../api/client'
import { useCurrentUser } from '../context/user'
import { isViewer } from '../state/permissions'
import type { CurrentUser } from '../state/permissions'
import { addFixtureRoom, emptySnapshot, fixtureRooms } from '../fixtures/player'
import { useRoomList } from '../hooks/useRoomList'
import { usePipelines } from '../hooks/usePipelines'
import { ageLabel, durationLabel, updatedAgoLabel, utcLabel } from '../lib/time'
import {
  ROOM_TABS,
  agentCell,
  canTakeControlOf,
  layersNeedingYou,
  needsYou,
  pipelineIds,
  roomStats,
  roomStatusCell,
  roomsForPipeline,
  roomsForTab,
  sortRoomsForList,
} from '../lib/rooms'
import type { Dot, RoomTab } from '../lib/rooms'
import { errorText } from '../lib/errors'
import { triageLine } from '../lib/triage'
import { useNow } from './room/useNow'
import { TitleFlagChip } from './room/TitleFlagChip'
import { initials, personColor } from '../lib/people'
import { cn } from '../lib/utils'
import { modKeyLabel } from '../lib/envLabel'
import { Avatar, AvatarFallback } from './ui/avatar'
import { Badge, layerVariant } from './ui/badge'
import { Button } from './ui/button'
import { Card } from './ui/card'
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from './ui/dialog'
import {
  DropdownMenu,
  DropdownMenuContent,
  DropdownMenuRadioGroup,
  DropdownMenuRadioItem,
  DropdownMenuTrigger,
} from './ui/dropdown-menu'
import { Input } from './ui/input'
import { Kbd } from './ui/kbd'
import { Skeleton } from './ui/skeleton'
import { Tabs, TabsContent, TabsList, TabsTrigger } from './ui/tabs'
import { TopBarSlot } from './shell/TopBarSlot'
import { RoomListContext, useInShell } from './shell/context'
import { isEditableTarget } from './shell/keys'

const SEVERITIES = ['sev1', 'sev2', 'sev3', 'sev4']

const TAB_LABEL: Record<RoomTab, string> = {
  open: 'Open',
  mine: 'Mine',
  resolved: 'Resolved',
  all: 'All',
}

const DOT_CLASS: Record<Dot, string> = {
  ok: 'bg-success',
  warn: 'bg-warning',
  fail: 'bg-destructive',
  info: 'bg-info',
  agent: 'bg-agent',
  idle: 'bg-subtle-foreground',
}

function StatusDot({ dot }: { dot: Dot }) {
  return <span className={cn('size-1.5 shrink-0 rounded-full', DOT_CLASS[dot])} aria-hidden="true" />
}

function severityVariant(sev: string) {
  return sev === 'sev1' || sev === 'sev2' || sev === 'sev3' || sev === 'sev4' ? sev : 'neutral'
}

// The rooms home (docs/design/ui-v2 Main.dc.html, Rooms-Empty.dc.html). The page title and
// the controls live in the shell's top bar; this component draws the stat strip and the table.
export function RoomList() {
  const navigate = useNavigate()
  const inShell = useInShell()
  const { user } = useCurrentUser()
  const viewer = isViewer(user) // a viewer cannot create rooms or take control (the server answers 403 viewer)
  // In the shell the list is the shell's copy (the sidebar shows it too); on its own it loads its own.
  const shared = useContext(RoomListContext)
  const own = useRoomList({ enabled: shared === null })
  // Pushed over GET /api/rooms/ws (F6); `updatedAt` is set only while the list falls back to polling.
  const { rooms, loading, error, reload, updatedAt } = shared ?? own
  const [searchParams, setSearchParams] = useSearchParams()
  const pipeline = searchParams.get('pipeline') || null
  const [tab, setTab] = useState<RoomTab>('open')
  // The keyboard row is a room id, not a position: a refresh can insert or reorder rows.
  const [activeId, setActiveId] = useState<string | null>(null)
  const [newOpen, setNewOpen] = useState(false)
  const [taking, setTaking] = useState<string | null>(null)
  const [loaded, setLoaded] = useState(false)
  const rowLinks = useRef(new Map<string, HTMLAnchorElement>())

  // The skeleton is only for the first load; a later update keeps the table on screen.
  useEffect(() => {
    if (!loading) setLoaded(true)
  }, [loading])

  const sorted = useMemo(() => sortRoomsForList(rooms), [rooms])
  const scoped = useMemo(() => roomsForPipeline(sorted, pipeline), [sorted, pipeline])
  const visible = useMemo(() => roomsForTab(scoped, tab, user), [scoped, tab, user])
  const active = activeId === null ? -1 : visible.findIndex((r) => r.id === activeId)
  // The row left the list (a refresh, a filter): nothing is selected any more.
  useEffect(() => {
    if (activeId !== null && active === -1) setActiveId(null)
  }, [activeId, active])
  const counts = useMemo(
    () => ({
      open: roomsForTab(scoped, 'open', user).length,
      mine: roomsForTab(scoped, 'mine', user).length,
      resolved: roomsForTab(scoped, 'resolved', user).length,
    }),
    [scoped, user],
  )
  const stats = useMemo(() => roomStats(scoped, user), [scoped, user])
  const pipelines = useMemo(() => pipelineIds(sorted), [sorted])

  const setPipeline = useCallback(
    (id: string | null) => {
      setSearchParams(
        (prev) => {
          const next = new URLSearchParams(prev)
          if (id) next.set('pipeline', id)
          else next.delete('pipeline')
          return next
        },
        { replace: true },
      )
      setActiveId(null)
    },
    [setSearchParams],
  )

  const takeControl = useCallback(
    async (room: RoomSnapshot) => {
      setTaking(room.id)
      try {
        if (isFixtures()) {
          const i = fixtureRooms.findIndex((r) => r.id === room.id)
          const lease = new Date(Date.now() + 30 * 60 * 1000).toISOString()
          if (i >= 0) fixtureRooms[i] = { ...room, driver: { id: user.id, name: user.name, lease_until: lease } }
        } else {
          await api.grantControl(room.id, user.id)
        }
        toast.success(`You now drive "${room.title}"`)
        await reload()
      } catch (err) {
        toast.error(errorText(err))
      } finally {
        setTaking(null)
      }
    },
    [reload, user.id, user.name],
  )

  const focusRow = (index: number) => {
    const room = visible[index]
    if (!room) return
    setActiveId(room.id)
    rowLinks.current.get(room.id)?.focus()
  }

  // J/K move, Enter opens, T takes control, C opens the new-room dialog. Never while typing, never
  // with a modifier, never while a dialog (the new-room form, ⌘K, the phone drawer) is open.
  useEffect(() => {
    const onKey = (e: KeyboardEvent) => {
      if (e.isComposing || e.ctrlKey || e.metaKey || e.altKey) return
      if (isEditableTarget(e.target) || document.querySelector('[role="dialog"], [role="menu"]')) return
      const key = e.key.toLowerCase()
      if (key === 'j' || key === 'k') {
        if (visible.length === 0) return
        e.preventDefault()
        const next = key === 'j' ? Math.min(active + 1, visible.length - 1) : Math.max(active - 1, 0)
        focusRow(next)
      } else if (key === 'enter') {
        // On a link or button, Enter already does its own thing.
        if ((e.target as Element | null)?.closest?.('a, button, [role="tab"], [role="menuitem"]')) return
        const room = visible[active]
        if (room) navigate(`/rooms/${room.id}`)
      } else if (e.repeat) {
        return
      } else if (key === 't') {
        const room = visible[active]
        // One take at a time: a second T while the first is in flight does nothing.
        if (room && taking === null && canTakeControlOf(room, user)) void takeControl(room)
      } else if (key === 'c' && !viewer) {
        e.preventDefault()
        setNewOpen(true)
      }
    }
    window.addEventListener('keydown', onKey)
    return () => window.removeEventListener('keydown', onKey)
  })

  const changeTab = (next: string) => {
    setTab(next as RoomTab)
    setActiveId(null)
  }

  const controls = (
    <>
      <LivePill updatedAt={updatedAt ?? null} error={error !== null} />
      {!viewer && (
        <Button
          size="sm"
          aria-label="New room"
          aria-keyshortcuts="C"
          className="max-sm:w-8 max-sm:px-0"
          onClick={() => setNewOpen(true)}
        >
          <Plus aria-hidden="true" />
          <span className="max-sm:hidden" aria-hidden="true">
            New room
          </span>
          <Kbd className="max-sm:hidden" aria-hidden="true">
            C
          </Kbd>
        </Button>
      )}
    </>
  )

  const firstLoad = loading && !loaded
  const failed = error !== null && rooms.length === 0

  return (
    <div className="flex flex-col gap-4 px-3 py-5 md:px-6">
      {inShell ? (
        <TopBarSlot>{controls}</TopBarSlot>
      ) : (
        <div className="flex flex-wrap items-center gap-3">
          <h1 className="text-xl font-semibold tracking-tight">Rooms</h1>
          {controls}
        </div>
      )}

      {firstLoad ? (
        <ListSkeleton />
      ) : failed ? (
        <ErrorCard message={error} onRetry={() => void reload()} />
      ) : (
        <>
          {error && (
            <p
              role="alert"
              className="flex items-center gap-2 rounded-md border border-border bg-warning-subtle px-3 py-2 text-sm text-warning-foreground"
            >
              <AlertTriangle className="size-4 shrink-0" aria-hidden="true" />
              Could not refresh the list: {error}
              <Button variant="ghost" size="sm" className="ml-auto h-7" onClick={() => void reload()}>
                Retry
              </Button>
            </p>
          )}

          <StatStrip stats={stats} />

          <Card className="flex min-h-0 flex-col">
            <Tabs value={tab} onValueChange={changeTab} className="flex flex-col">
              <div className="flex flex-wrap items-center gap-2 px-3">
                <TabsList className="border-b-0">
                  {ROOM_TABS.map((t) => (
                    <TabsTrigger key={t} value={t}>
                      {TAB_LABEL[t]}
                      {t !== 'all' && (
                        <span className="font-mono text-[11px] text-subtle-foreground">{counts[t]}</span>
                      )}
                    </TabsTrigger>
                  ))}
                </TabsList>
                <div className="ml-auto flex items-center gap-1.5 py-1">
                  {pipeline && (
                    <Button
                      variant="ghost"
                      size="sm"
                      className="h-7 px-2"
                      aria-label={`Clear pipeline filter ${pipeline}`}
                      onClick={() => setPipeline(null)}
                    >
                      <X className="!size-3" aria-hidden="true" />
                      Clear
                    </Button>
                  )}
                  <PipelineFilter pipelines={pipelines} value={pipeline} onChange={setPipeline} />
                </div>
              </div>
              <div className="h-px bg-border" />
              <TabsContent value={tab} className="min-w-0">
                {visible.length === 0 ? (
                  <EmptyPanel
                    tab={tab}
                    pipeline={pipeline}
                    viewer={viewer}
                    onNew={() => setNewOpen(true)}
                    onResolved={() => changeTab('resolved')}
                    onClearFilter={() => setPipeline(null)}
                  />
                ) : (
                  <RoomTable
                    rooms={visible}
                    user={user}
                    active={active}
                    taking={taking}
                    linkRefs={rowLinks.current}
                    onActive={setActiveId}
                    onOpen={(id) => navigate(`/rooms/${id}`)}
                    onTake={(r) => void takeControl(r)}
                    showResolution={tab === 'resolved'}
                  />
                )}
              </TabsContent>
            </Tabs>
            <div className="h-px bg-border" />
            <div className="flex flex-wrap items-center gap-3 px-3 py-2 text-xs text-subtle-foreground">
              <span>
                {visible.length} {visible.length === 1 ? 'room' : 'rooms'}
              </span>
              <span className="ml-auto hidden items-center gap-2.5 sm:inline-flex" aria-hidden="true">
                <Hint keys={['J', 'K']} label="move" />
                <Hint keys={['⏎']} label="open" />
                {!viewer && <Hint keys={['T']} label="take control" />}
              </span>
            </div>
          </Card>
        </>
      )}

      {!viewer && (
        <NewRoomDialog open={newOpen} onOpenChange={setNewOpen} defaultPipeline={pipeline} />
      )}
    </div>
  )
}

function Hint({ keys, label }: { keys: string[]; label: string }) {
  return (
    <span className="inline-flex items-center gap-1">
      {keys.map((k) => (
        <Kbd key={k}>{k}</Kbd>
      ))}
      {label}
    </span>
  )
}

// "live · updated 5s ago". It ticks on its own so the table does not re-render every second.
// On a phone only the dot shows (the words stay for screen readers).
function LivePill({ updatedAt, error }: { updatedAt: number | null; error: boolean }) {
  const [now, setNow] = useState(() => Date.now())
  useEffect(() => {
    const t = window.setInterval(() => setNow(Date.now()), 1000)
    return () => window.clearInterval(t)
  }, [])
  const ago = updatedAgoLabel(updatedAt, now)
  const text = error ? 'offline · retrying' : ago ? `live · ${ago}` : 'live'
  return (
    <span className="inline-flex h-6 shrink-0 items-center gap-1.5 whitespace-nowrap rounded-full border border-border bg-card px-2 text-xs font-medium text-muted-foreground max-sm:w-6 max-sm:justify-center max-sm:px-0">
      <span className={cn('size-1.5 rounded-full', error ? 'bg-warning' : 'bg-success')} aria-hidden="true" />
      <span className="max-sm:sr-only">{text}</span>
    </span>
  )
}

function Stat({
  label,
  dot,
  value,
  caption,
  valueClass,
  className,
}: {
  label: string
  dot?: Dot
  value: number
  caption: string
  valueClass?: string
  className?: string
}) {
  return (
    <div className={cn('flex min-w-0 flex-col gap-1 border-border p-4', className)}>
      <span className="inline-flex items-center gap-1.5 text-xs text-muted-foreground">
        {dot && <StatusDot dot={dot} />}
        {label}
      </span>
      <span className={cn('text-xl font-semibold leading-6 tracking-tight tabular-nums', valueClass)}>{value}</span>
      <span className="truncate text-xs text-subtle-foreground">{caption}</span>
    </div>
  )
}

function StatStrip({ stats }: { stats: ReturnType<typeof roomStats> }) {
  // Two columns on a phone, four from md. The borders follow the grid.
  const cell = 'max-md:[&:nth-child(even)]:border-l max-md:[&:nth-child(n+3)]:border-t md:[&:nth-child(n+2)]:border-l'
  return (
    <Card className="grid grid-cols-2 md:grid-cols-4" aria-label="Summary" role="group">
      <Stat
        className={cell}
        label="Open"
        value={stats.open}
        caption={stats.open === 0 ? 'nothing firing' : `${stats.openWithDriver} with a driver`}
      />
      <Stat
        className={cell}
        label="Needs you"
        dot="warn"
        value={stats.needsYou}
        valueClass={stats.needsYou > 0 ? 'text-warning-foreground' : undefined}
        caption={stats.needsYou > 0 ? `${stats.needsYouLayers.join(', ')} approval` : 'no pending approvals'}
      />
      <Stat
        className={cell}
        label="Agent working"
        dot="agent"
        value={stats.agentWorking}
        valueClass={stats.agentWorking > 0 ? 'text-agent-foreground' : undefined}
        caption={stats.agentCaption}
      />
      <Stat
        className={cell}
        label="Resolved today"
        value={stats.resolvedToday}
        caption={stats.medianMinutes === null ? 'none yet' : `median ${stats.medianMinutes} min`}
      />
    </Card>
  )
}

function PipelineFilter({
  pipelines,
  value,
  onChange,
}: {
  pipelines: string[]
  value: string | null
  onChange: (id: string | null) => void
}) {
  // A filter that came from the URL may name a pipeline no room mentions; keep it in the menu.
  const list = value && !pipelines.includes(value) ? [value, ...pipelines] : pipelines
  return (
    <DropdownMenu>
      <DropdownMenuTrigger asChild>
        <Button variant="ghost" size="sm" className="h-7 px-2">
          {value ? (
            <>
              Pipeline: <span className="font-mono text-foreground">{value}</span>
            </>
          ) : (
            'Pipeline'
          )}
          <ChevronDown className="!size-3" aria-hidden="true" />
        </Button>
      </DropdownMenuTrigger>
      <DropdownMenuContent align="end" className="min-w-[10rem]">
        <DropdownMenuRadioGroup value={value ?? ''} onValueChange={(v) => onChange(v || null)}>
          <DropdownMenuRadioItem value="">All pipelines</DropdownMenuRadioItem>
          {list.map((p) => (
            <DropdownMenuRadioItem key={p} value={p} className="font-mono">
              {p}
            </DropdownMenuRadioItem>
          ))}
        </DropdownMenuRadioGroup>
      </DropdownMenuContent>
    </DropdownMenu>
  )
}

const TH = 'h-9 whitespace-nowrap px-3 text-left text-xs font-medium text-subtle-foreground'

function RoomTable({
  rooms,
  user,
  active,
  taking,
  linkRefs,
  onActive,
  onOpen,
  onTake,
  showResolution,
}: {
  showResolution?: boolean
  rooms: RoomSnapshot[]
  user: CurrentUser
  active: number
  taking: string | null
  linkRefs: Map<string, HTMLAnchorElement>
  onActive: (id: string) => void
  onOpen: (id: string) => void
  onTake: (room: RoomSnapshot) => void
}) {
  return (
    <div className="overflow-x-auto">
      <table className="w-full border-collapse text-sm" aria-label="Rooms">
        <thead>
          <tr className="border-b border-border">
            <th scope="col" className={cn(TH, 'w-[38%]')}>
              Incident
            </th>
            <th scope="col" className={TH}>
              Severity
            </th>
            <th scope="col" className={cn(TH, 'max-md:hidden')}>
              Status
            </th>
            <th scope="col" className={cn(TH, 'max-md:hidden')}>
              Pipeline
            </th>
            <th scope="col" className={TH}>
              Driver
            </th>
            <th scope="col" className={cn(TH, 'max-lg:hidden')}>
              Agent
            </th>
            <th scope="col" className={cn(TH, 'text-right max-md:hidden')}>
              Opened
            </th>
          </tr>
        </thead>
        <tbody>
          {rooms.map((room, i) => (
            <RoomRow
              key={room.id}
              room={room}
              user={user}
              active={i === active}
              taking={taking === room.id}
              linkRef={(el) => {
                if (el) linkRefs.set(room.id, el)
                else linkRefs.delete(room.id)
              }}
              onActivate={() => onActive(room.id)}
              onOpen={() => onOpen(room.id)}
              onTake={() => onTake(room)}
              showResolution={showResolution}
            />
          ))}
        </tbody>
      </table>
    </div>
  )
}

const TD = 'h-11 whitespace-nowrap border-b border-border px-3 group-last:border-b-0'

function RoomRow({
  room,
  user,
  active,
  taking,
  linkRef,
  onActivate,
  onOpen,
  onTake,
  showResolution,
}: {
  showResolution?: boolean
  room: RoomSnapshot
  user: CurrentUser
  active: boolean
  taking: boolean
  linkRef: (el: HTMLAnchorElement | null) => void
  onActivate: () => void
  onOpen: () => void
  onTake: () => void
}) {
  const tinted = needsYou(room, user) // only for layers this person owns
  const resolved = room.status !== 'open'
  const status = roomStatusCell(room)
  // A closed room reads "Resolved · 6 min": how long it was open, not a stale live state.
  const duration = resolved && room.closed_at ? durationLabel(room.opened_at, room.closed_at) : ''
  const agent = agentCell(room)
  const layers = layersNeedingYou(room, user)
  const canTake = canTakeControlOf(room, user)
  return (
    <tr
      data-room-id={room.id}
      data-active={active || undefined}
      className={cn(
        'group cursor-pointer transition-colors',
        tinted ? 'bg-warning-subtle' : 'hover:bg-accent/60',
        active && 'bg-accent',
        'focus-within:bg-accent',
      )}
      onClick={(e) => {
        // The title link, the Take control button and text selection keep their own behaviour.
        if ((e.target as Element).closest('a, button')) return
        if (window.getSelection()?.toString()) return
        onOpen()
      }}
    >
      <td className={cn(TD, 'max-w-0')}>
        {/* Wide: the title and its badge on one line. Phone: the title, then the badge and the pipeline (its column is hidden). */}
        <div className="flex min-w-0 items-center gap-x-2 max-md:flex-wrap max-md:py-1">
          <Link
            ref={linkRef}
            to={`/rooms/${room.id}`}
            onFocus={onActivate}
            className="min-w-0 truncate rounded-sm font-medium text-foreground focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring max-md:basis-full"
          >
            {plainText(room.title, 300)}
          </Link>
          <TitleFlagChip flags={room.title_flags} />
          {tinted && (
            <Badge variant="warning" title={`Waiting on your ${layers.join(', ')} approval`}>
              Needs you
            </Badge>
          )}
          <span className="truncate font-mono text-xs text-muted-foreground md:hidden">{room.pipeline_id}</span>
          {room.status === 'open' && !room.shadow && (room.insight?.time_to_first_hypothesis_s ?? -1) >= 0 && (
            <span className="whitespace-nowrap font-mono text-xs tabular-nums text-muted-foreground">
              · insight {room.insight?.time_to_first_hypothesis_s} s
            </span>
          )}
        </div>
        <TriageRowLine room={room} />
        {showResolution && resolved && room.resolution && (
          <p className="m-0 line-clamp-1 whitespace-normal text-xs text-muted-foreground">{room.resolution.slice(0, 100)}</p>
        )}
      </td>
      <td className={TD}>
        <Badge variant={severityVariant(room.severity)} className="uppercase">
          {room.severity}
        </Badge>
      </td>
      <td className={cn(TD, 'max-md:hidden')}>
        <span className="inline-flex items-center gap-1.5">
          <StatusDot dot={status.dot} />
          {resolved && duration ? `Resolved · ${duration}` : status.label}
        </span>
      </td>
      <td className={cn(TD, 'font-mono text-muted-foreground max-md:hidden')}>{room.pipeline_id}</td>
      <td className={TD}>
        {canTake ? (
          <Button
            variant="outline"
            className="h-[26px] gap-1.5 px-2 text-xs coarse:h-11 coarse:px-4"
            disabled={taking}
            title={room.driver ? `${room.driver.name}'s lease has expired` : undefined}
            onClick={onTake}
          >
            Take control
            <Kbd className="h-4 min-w-4 max-sm:hidden" aria-hidden="true">
              T
            </Kbd>
          </Button>
        ) : room.driver ? (
          <span className="inline-flex items-center gap-1.5">
            <Avatar className="size-5">
              <AvatarFallback
                className="text-[9px]"
                style={{ backgroundColor: personColor(room.driver.id) }}
                aria-hidden="true"
              >
                {initials(room.driver.name)}
              </AvatarFallback>
            </Avatar>
            {room.driver.name}
            <span className="sr-only">, driver</span>
          </span>
        ) : (
          <span className="text-muted-foreground">{room.status === 'open' ? 'No driver' : '—'}</span>
        )}
      </td>
      <td className={cn(TD, 'max-lg:hidden')}>
        <span className={cn('inline-flex items-center gap-1.5', agent.agent ? 'text-agent-foreground' : 'text-muted-foreground')}>
          <StatusDot dot={agent.dot} />
          {agent.label}
        </span>
      </td>
      <td className={cn(TD, 'text-right tabular-nums text-muted-foreground max-md:hidden')}>
        {resolved && room.closed_at ? (
          <span title={utcLabel(room.closed_at)}>
            closed {ageLabel(room.closed_at)} ago
          </span>
        ) : (
          <span title={utcLabel(room.opened_at)}>
            <span className="sr-only">opened </span>
            {ageLabel(room.opened_at)}
            <span className="sr-only"> ago</span>
          </span>
        )}
      </td>
    </tr>
  )
}

// The row's second line (F6): the first look's headline, "triage, unverified". A young open
// room with no first look yet shows a shimmer; a failed or old-enough-to-miss one shows nothing.
function TriageRowLine({ room }: { room: RoomSnapshot }) {
  const now = useNow(10_000)
  const line = triageLine(room, now)
  if (line.kind === 'none') return null
  if (line.kind === 'pending') {
    return (
      <span role="status" aria-label="First look in progress" className="mt-0.5 flex items-center gap-1" data-testid="triage-pending">
        {[0, 1, 2].map((i) => (
          <span
            key={i}
            aria-hidden="true"
            className="size-1 animate-pulse rounded-full bg-agent/60"
            style={{ animationDelay: `${i * 150}ms` }}
          />
        ))}
      </span>
    )
  }
  return (
    <div className="mt-0.5 flex min-w-0 items-center gap-1.5" data-testid="triage-line">
      <Badge variant="agent" className="shrink-0" title="triage, unverified">
        first look
      </Badge>
      {line.layer && (
        <Badge variant={layerVariant(line.layer)} className="shrink-0">
          {line.layer}
        </Badge>
      )}
      <span className="min-w-0 truncate text-xs text-muted-foreground" title={line.headline}>
        {line.headline}
      </span>
    </div>
  )
}

function EmptyPanel({
  tab,
  pipeline,
  viewer,
  onNew,
  onResolved,
  onClearFilter,
}: {
  tab: RoomTab
  pipeline: string | null
  viewer: boolean
  onNew: () => void
  onResolved: () => void
  onClearFilter: () => void
}) {
  let title: string
  let body: ReactNode
  let actions: ReactNode = null
  if (pipeline) {
    title = tab === 'mine' ? `Nothing for you in ${pipeline}` : `No ${TAB_LABEL[tab].toLowerCase()} rooms in ${pipeline}`
    body = 'The pipeline filter is on. Clear it to see every pipeline.'
    actions = (
      <Button variant="outline" onClick={onClearFilter}>
        Clear pipeline filter
      </Button>
    )
  } else if (tab === 'open') {
    title = 'No open rooms'
    body = viewer
      ? 'Alerts land here. A room opens the moment a rule fires.'
      : 'Alerts land here. A room opens the moment a rule fires. You can also open one by hand.'
    actions = (
      <>
        {!viewer && (
          <Button onClick={onNew}>
            New room
            <Kbd aria-hidden="true">C</Kbd>
          </Button>
        )}
        <Button variant="ghost" onClick={onResolved}>
          See resolved rooms
        </Button>
      </>
    )
  } else if (tab === 'mine') {
    title = 'Nothing needs you right now'
    body = 'Rooms you drive, or where one of your layers has to approve, show up here.'
  } else if (tab === 'resolved') {
    title = 'No resolved rooms yet'
    body = 'A room moves here when it is closed.'
  } else {
    title = 'No rooms yet'
    body = 'Alerts land here.'
  }
  return (
    <div className="flex flex-col items-center justify-center gap-3 px-6 py-12 text-center">
      {tab === 'open' && !pipeline && (
        <div className="flex items-center opacity-70" aria-hidden="true">
          <Ghost node />
          <Ghost />
          <Ghost node />
          <Ghost />
          <Ghost node />
          <Ghost className="max-sm:hidden" />
          <Ghost node className="max-sm:hidden" />
          <Ghost className="max-sm:hidden" />
          <Ghost node className="max-sm:hidden" />
        </div>
      )}
      <p className="mt-2 text-base font-medium">{title}</p>
      <p className="max-w-[52ch] text-muted-foreground">{body}</p>
      {actions && <div className="mt-1 flex flex-wrap justify-center gap-2">{actions}</div>}
    </div>
  )
}

// A dashed pipeline node, or the dashed edge between two.
function Ghost({ node, className }: { node?: boolean; className?: string }) {
  return node ? (
    <span className={cn('h-10 w-16 rounded-md border border-dashed border-input sm:w-[120px]', className)} />
  ) : (
    <span className={cn('w-7 border-t-2 border-dashed border-input', className)} />
  )
}

function ListSkeleton() {
  return (
    <div role="status" aria-label="Loading rooms" className="flex flex-col gap-4">
      <span className="sr-only">Loading rooms…</span>
      <Card className="grid grid-cols-2 md:grid-cols-4">
        {[0, 1, 2, 3].map((i) => (
          <div key={i} className="flex flex-col gap-2 p-4">
            <Skeleton className="h-3 w-16" />
            <Skeleton className="h-6 w-8" />
            <Skeleton className="h-3 w-24" />
          </div>
        ))}
      </Card>
      <Card className="flex flex-col">
        <div className="flex gap-3 px-3 py-2.5">
          <Skeleton className="h-5 w-16" />
          <Skeleton className="h-5 w-16" />
          <Skeleton className="h-5 w-16" />
        </div>
        <div className="h-px bg-border" />
        {[0, 1, 2, 3, 4].map((i) => (
          <div key={i} className="flex h-11 items-center gap-4 border-b border-border px-3 last:border-b-0">
            <Skeleton className="h-4 w-1/3" />
            <Skeleton className="h-5 w-12" />
            <Skeleton className="h-4 w-20 max-md:hidden" />
            <Skeleton className="ml-auto h-4 w-10" />
          </div>
        ))}
      </Card>
    </div>
  )
}

function ErrorCard({ message, onRetry }: { message: string | null; onRetry: () => void }) {
  return (
    <Card role="alert" className="flex flex-col items-center gap-3 px-6 py-12 text-center">
      <AlertTriangle className="size-5 text-destructive-fg" aria-hidden="true" />
      <p className="text-base font-medium">Could not load rooms</p>
      <p className="max-w-[52ch] text-muted-foreground">{message}</p>
      <Button variant="outline" onClick={onRetry}>
        Retry
      </Button>
    </Card>
  )
}

const FIELD_LABEL = 'text-xs font-medium text-muted-foreground'

// The form lives inside the dialog content, which Radix unmounts on close: every open starts
// fresh (empty title, no old error) and the pipeline defaults to the filter on screen right
// now, else the first pipeline.
function NewRoomDialog({
  open,
  onOpenChange,
  defaultPipeline,
}: {
  open: boolean
  onOpenChange: (open: boolean) => void
  defaultPipeline: string | null
}) {
  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContent>
        <NewRoomForm onClose={() => onOpenChange(false)} defaultPipeline={defaultPipeline} />
      </DialogContent>
    </Dialog>
  )
}

function NewRoomForm({ onClose, defaultPipeline }: { onClose: () => void; defaultPipeline: string | null }) {
  const loaded = usePipelines()
  const mod = modKeyLabel()
  const pipelines = loaded.status === 'ready' ? loaded.data : []
  const navigate = useNavigate()
  const [title, setTitle] = useState('')
  const [severity, setSeverity] = useState('sev2')
  const [pipelineId, setPipelineId] = useState(defaultPipeline ?? '')
  const [busy, setBusy] = useState(false)
  const [createError, setCreateError] = useState<string | null>(null)
  // The choice must be one the server lists; until the person picks, the first (or the filter on screen).
  const chosen = pipelines.some((p) => p.id === pipelineId) ? pipelineId : (pipelines[0]?.id ?? '')

  const onSubmit = async (e: FormEvent) => {
    e.preventDefault()
    if (!title.trim() || !chosen) return
    setBusy(true)
    setCreateError(null)
    try {
      const pipeline = chosen
      let id: string
      if (isFixtures()) {
        const snapshot = emptySnapshot(`r_${Date.now().toString(36)}`, title.trim(), severity, pipeline)
        addFixtureRoom(snapshot)
        id = snapshot.id
      } else {
        id = (await api.createRoom({ title: title.trim(), severity, pipeline_id: pipeline })).id
      }
      onClose()
      navigate(`/rooms/${id}`)
    } catch (err) {
      setCreateError(errorText(err))
    } finally {
      setBusy(false)
    }
  }

  // Same shortcut as the close and hand-off dialogs.
  const onKeyDown = (e: ReactKeyboardEvent<HTMLFormElement>) => {
    if (e.key === 'Enter' && (e.metaKey || e.ctrlKey) && !e.nativeEvent.isComposing) {
      e.preventDefault()
      e.currentTarget.requestSubmit()
    }
  }

  return (
        <form className="grid gap-4" onSubmit={onSubmit} onKeyDown={onKeyDown}>
          <DialogHeader>
            <DialogTitle>New room</DialogTitle>
            <DialogDescription>Open a room by hand. Alerts open one on their own.</DialogDescription>
          </DialogHeader>
          <div className="grid gap-1.5">
            <label htmlFor="new-title" className={FIELD_LABEL}>
              Title
            </label>
            <Input
              id="new-title"
              value={title}
              onChange={(e) => setTitle(e.target.value)}
              placeholder="What is wrong?"
              required
              autoFocus
            />
          </div>
          <div className="grid grid-cols-2 gap-3">
            <div className="grid gap-1.5">
              <label htmlFor="new-severity" className={FIELD_LABEL}>
                Severity
              </label>
              <select
                id="new-severity"
                value={severity}
                onChange={(e) => setSeverity(e.target.value)}
                className="h-8 w-full rounded-md border border-input-border bg-background px-2 text-sm uppercase text-foreground focus-visible:border-ring focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring/30"
              >
                {SEVERITIES.map((s) => (
                  <option key={s} value={s}>
                    {s}
                  </option>
                ))}
              </select>
            </div>
            <div className="grid gap-1.5">
              <label htmlFor="new-pipeline" className={FIELD_LABEL}>
                Pipeline
              </label>
              <select
                id="new-pipeline"
                value={chosen}
                onChange={(e) => setPipelineId(e.target.value)}
                disabled={pipelines.length === 0}
                className="h-8 w-full rounded-md border border-input-border bg-background px-2 font-mono text-sm text-foreground focus-visible:border-ring focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring/30 disabled:opacity-60"
              >
                {pipelines.length === 0 && <option value="">{loaded.status === 'loading' ? 'Loading…' : 'None'}</option>}
                {pipelines.map((p) => (
                  <option key={p.id} value={p.id}>
                    {p.title && p.title !== p.id ? `${p.id} · ${p.title}` : p.id}
                  </option>
                ))}
              </select>
            </div>
          </div>
          {loaded.status === 'error' && (
            <div role="alert" className="flex items-center gap-2 text-sm text-destructive-fg">
              <span>Could not load the pipelines: {loaded.message}</span>
              <Button type="button" variant="secondary" onClick={loaded.reload}>
                Try again
              </Button>
            </div>
          )}
          {loaded.status === 'ready' && pipelines.length === 0 && (
            <p role="status" className="m-0 text-sm text-muted-foreground">
              This server has no pipelines configured, so a room cannot be opened by hand. Add one to owners.yaml.
            </p>
          )}
          {createError && (
            <p role="alert" className="text-sm text-destructive-fg">
              {createError}
            </p>
          )}
          <DialogFooter>
            <Button type="button" variant="ghost" onClick={onClose}>
              Cancel
            </Button>
            <Button
              type="submit"
              disabled={busy || !title.trim() || !chosen}
              aria-keyshortcuts={`${mod === '⌘' ? 'Meta' : 'Control'}+Enter`}
            >
              Open room
              <Kbd aria-hidden="true">{mod}⏎</Kbd>
            </Button>
          </DialogFooter>
        </form>
  )
}

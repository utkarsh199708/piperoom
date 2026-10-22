import { useMemo } from 'react'
import type { ReactNode } from 'react'
import { Link, useNavigate, useSearchParams } from 'react-router-dom'
import { AlertTriangle, ArrowDown, ArrowUp, ArrowUpDown, ChevronDown, X } from 'lucide-react'
import type { FleetHealth, FleetPipeline } from '../../api/types'
import { useCurrentUser } from '../../context/user'
import { useFleet } from '../../hooks/useFleet'
import { useMediaQuery } from '../../hooks/useMediaQuery'
import {
  DEFAULT_DIR,
  FLEET_HEALTH_LABEL,
  FLEET_SORT_KEYS,
  facetValues,
  filterFleet,
  laneHref,
  sortFleet,
  verdictLabel,
} from '../../lib/fleet'
import type { FleetFilter, FleetSortKey, SortDir } from '../../lib/fleet'
import { etaBadge } from '../../lib/trends'
import { ageLabel, updatedAgoLabel, utcLabel } from '../../lib/time'
import { cn } from '../../lib/utils'
import { isViewer } from '../../state/permissions'
import { useNow } from '../room/useNow'
import { FirstRunPanel } from '../FirstRunPanel'
import { TopBarSlot } from '../shell/TopBarSlot'
import { useInShell } from '../shell/context'
import { Badge } from '../ui/badge'
import { Button } from '../ui/button'
import { Card } from '../ui/card'
import {
  DropdownMenu,
  DropdownMenuContent,
  DropdownMenuRadioGroup,
  DropdownMenuRadioItem,
  DropdownMenuTrigger,
} from '../ui/dropdown-menu'
import { Skeleton } from '../ui/skeleton'
import { Tabs, TabsContent, TabsList, TabsTrigger } from '../ui/tabs'
import { FleetChanges } from './FleetChanges'

const HEALTH_BADGE: Record<FleetHealth, 'destructive' | 'warning' | 'neutral' | 'success'> = {
  fail: 'destructive',
  warn: 'warning',
  unknown: 'neutral',
  ok: 'success',
}

const HEALTH_DOT: Record<FleetHealth, string> = {
  fail: 'bg-destructive',
  warn: 'bg-warning',
  unknown: 'bg-subtle-foreground',
  ok: 'bg-success',
}

const SORT_LABEL: Record<FleetSortKey, string> = {
  health: 'health',
  eta: 'forecast ETA',
  name: 'pipeline',
  changes: 'changes in 24 h',
}

const FILTERS: { field: keyof FleetFilter; label: string }[] = [
  { field: 'tenant', label: 'Tenant' },
  { field: 'team', label: 'Team' },
  { field: 'kind', label: 'Kind' },
]

// Signals and ETA badges shown in a cell before "+N"; the rest stay in the hover and the accessible name.
const SHOWN_SIGNALS = 3
const SHOWN_ETAS = 2

// The fleet (X5b): one row per pipeline the caller can see, from GET /api/fleet, and what changed this week from
// GET /api/fleet/changes. It is the landing page; the room list stays at "/".
export function FleetScreen() {
  const inShell = useInShell()
  const { user } = useCurrentUser()
  const fleet = useFleet()
  const [params, setParams] = useSearchParams()
  const tab = params.get('tab') === 'changes' ? 'changes' : 'pipelines'
  const sortParam = params.get('sort') as FleetSortKey | null
  const sort: FleetSortKey = sortParam && FLEET_SORT_KEYS.includes(sortParam) ? sortParam : 'health'
  const dir: SortDir = params.get('dir') === 'desc' ? 'desc' : params.get('dir') === 'asc' ? 'asc' : DEFAULT_DIR[sort]
  const filter: FleetFilter = {
    tenant: params.get('tenant') ?? '',
    team: params.get('team') ?? '',
    kind: params.get('kind') ?? '',
  }
  const filtered = Boolean(filter.tenant || filter.team || filter.kind)

  const set = (changes: Record<string, string | null>) =>
    setParams(
      (prev) => {
        const next = new URLSearchParams(prev)
        for (const [k, v] of Object.entries(changes)) {
          if (v) next.set(k, v)
          else next.delete(k)
        }
        return next
      },
      { replace: true },
    )

  const all = useMemo(() => fleet.rows ?? [], [fleet.rows])
  const rows = useMemo(
    () => sortFleet(filterFleet(all, filter), sort, dir),
    // filter is rebuilt each render from the URL; its three strings are the real inputs
    // eslint-disable-next-line react-hooks/exhaustive-deps
    [all, filter.tenant, filter.team, filter.kind, sort, dir],
  )

  const onSort = (key: FleetSortKey) => {
    const nextDir: SortDir = key === sort ? (dir === 'asc' ? 'desc' : 'asc') : DEFAULT_DIR[key]
    set({ sort: key, dir: nextDir })
  }

  const live = (
    <span className="inline-flex h-6 shrink-0 items-center gap-1.5 whitespace-nowrap rounded-full border border-border bg-card px-2 text-xs font-medium text-muted-foreground">
      <span className={cn('size-1.5 rounded-full', fleet.error ? 'bg-warning' : 'bg-success')} aria-hidden="true" />
      <FleetUpdated updatedAt={fleet.updatedAt} error={fleet.error !== null} />
    </span>
  )

  let body: ReactNode
  if (fleet.rows === null && fleet.error === null) {
    body = <FleetSkeleton />
  } else if (fleet.rows === null) {
    body = <FleetError message={fleet.error ?? ''} onRetry={fleet.reload} />
  } else if (all.length === 0) {
    body = (
      <div className="flex flex-col gap-3">
        <p className="m-0 text-sm text-muted-foreground">No pipelines yet: owners.yaml lists none you can see.</p>
        {!isViewer(user) && <FirstRunPanel userId={user.id} />}
      </div>
    )
  } else {
    body = (
      <>
        {fleet.error && (
          <p
            role="alert"
            className="flex items-center gap-2 rounded-md border border-border bg-warning-subtle px-3 py-2 text-sm text-warning-foreground"
          >
            <AlertTriangle className="size-4 shrink-0" aria-hidden="true" />
            Could not refresh the fleet: {fleet.error}
            <Button variant="ghost" size="sm" className="ml-auto h-7" onClick={fleet.reload}>
              Retry
            </Button>
          </p>
        )}
        <div className="flex flex-wrap items-center gap-1.5" role="group" aria-label="Filters">
          {FILTERS.map(({ field, label }) => (
            <FacetFilter
              key={field}
              label={label}
              values={field === 'kind' ? ['cdc', 'batch'] : facetValues(all, field)}
              value={filter[field]}
              onChange={(v) => set({ [field]: v || null })}
            />
          ))}
          {filtered && (
            <Button
              variant="ghost"
              size="sm"
              className="h-7 px-2"
              onClick={() => set({ tenant: null, team: null, kind: null })}
            >
              <X className="size-3!" aria-hidden="true" />
              Clear filters
            </Button>
          )}
          <span className="ml-auto text-xs text-subtle-foreground" aria-live="polite">
            {rows.length} of {all.length} {all.length === 1 ? 'pipeline' : 'pipelines'}
          </span>
        </div>
        {rows.length === 0 ? (
          <Card className="px-6 py-10 text-center text-sm text-muted-foreground">No pipeline matches these filters.</Card>
        ) : (
          <FleetRows rows={rows} sort={sort} dir={dir} onSort={onSort} />
        )}
      </>
    )
  }

  return (
    <div className="flex min-w-0 flex-col gap-4 px-3 py-5 md:px-6">
      {inShell ? (
        <TopBarSlot>{live}</TopBarSlot>
      ) : (
        <div className="flex flex-wrap items-center gap-3">
          <h1 className="text-xl font-semibold tracking-tight">Fleet</h1>
          {live}
        </div>
      )}
      <Tabs value={tab} onValueChange={(v) => set({ tab: v === 'changes' ? 'changes' : null })} className="flex flex-col gap-4">
        <TabsList className="self-start">
          <TabsTrigger value="pipelines">
            Pipelines
            {fleet.rows && <span className="font-mono text-[11px] text-subtle-foreground">{all.length}</span>}
          </TabsTrigger>
          <TabsTrigger value="changes">What changed this week</TabsTrigger>
        </TabsList>
        <TabsContent value="pipelines" className="flex min-w-0 flex-col gap-3">
          {body}
        </TabsContent>
        <TabsContent value="changes" className="min-w-0">
          <FleetChanges enabled={tab === 'changes'} />
        </TabsContent>
      </Tabs>
    </div>
  )
}

function FleetUpdated({ updatedAt, error }: { updatedAt: number | null; error: boolean }) {
  const now = useNow(5_000)
  const ago = updatedAgoLabel(updatedAt, now)
  return <span>{error ? 'offline · retrying' : ago ? `live · ${ago}` : 'live'}</span>
}

function FacetFilter({
  label,
  values,
  value,
  onChange,
}: {
  label: string
  values: string[]
  value: string
  onChange: (v: string) => void
}) {
  // A value that came from the URL may match no row; keep it in the menu so it can be seen and cleared.
  const list = value && !values.includes(value) ? [value, ...values] : values
  if (list.length === 0) return null
  return (
    <DropdownMenu>
      <DropdownMenuTrigger asChild>
        <Button variant="outline" size="sm" className="h-7 px-2" aria-label={value ? `${label}: ${value}` : `Filter by ${label.toLowerCase()}`}>
          {label}
          {value && <span className="max-w-[12rem] truncate font-mono text-foreground">{value}</span>}
          <ChevronDown className="size-3!" aria-hidden="true" />
        </Button>
      </DropdownMenuTrigger>
      <DropdownMenuContent align="start" className="min-w-[10rem]">
        <DropdownMenuRadioGroup value={value} onValueChange={onChange}>
          <DropdownMenuRadioItem value="">Any {label.toLowerCase()}</DropdownMenuRadioItem>
          {list.map((v) => (
            <DropdownMenuRadioItem key={v} value={v} className="font-mono">
              {v}
            </DropdownMenuRadioItem>
          ))}
        </DropdownMenuRadioGroup>
      </DropdownMenuContent>
    </DropdownMenu>
  )
}

function FleetRows({
  rows,
  sort,
  dir,
  onSort,
}: {
  rows: FleetPipeline[]
  sort: FleetSortKey
  dir: SortDir
  onSort: (key: FleetSortKey) => void
}) {
  const wide = useMediaQuery('(min-width: 768px)')
  const now = useNow(60_000)
  if (!wide) {
    return (
      <div className="flex flex-col gap-2">
        <div className="flex flex-wrap gap-1.5" role="group" aria-label="Sort">
          {FLEET_SORT_KEYS.map((k) => (
            <SortButton key={k} sortKey={k} sort={sort} dir={dir} onSort={onSort} pill />
          ))}
        </div>
        <ul className="m-0 flex list-none flex-col gap-2 p-0" aria-label="Pipelines">
          {rows.map((r) => (
            <FleetCard key={r.pipeline_id} row={r} now={now} />
          ))}
        </ul>
      </div>
    )
  }
  const th = 'h-9 whitespace-nowrap px-3 text-left text-xs font-medium text-subtle-foreground'
  const ariaSort = (k: FleetSortKey) => (k === sort ? (dir === 'asc' ? 'ascending' : 'descending') : undefined)
  return (
    <Card className="overflow-x-auto">
      <table className="w-full border-collapse text-sm" aria-label="Pipelines">
        <thead>
          <tr className="border-b border-border">
            <th scope="col" className={cn(th, 'w-[26%]')} aria-sort={ariaSort('name')}>
              <SortButton sortKey="name" sort={sort} dir={dir} onSort={onSort} />
            </th>
            <th scope="col" className={th} aria-sort={ariaSort('health')}>
              <SortButton sortKey="health" sort={sort} dir={dir} onSort={onSort} />
            </th>
            <th scope="col" className={th}>
              Worst node
            </th>
            <th scope="col" className={cn(th, 'max-lg:hidden')}>
              Signals
            </th>
            <th scope="col" className={th} aria-sort={ariaSort('eta')}>
              <SortButton sortKey="eta" sort={sort} dir={dir} onSort={onSort} />
            </th>
            <th scope="col" className={cn(th, 'text-right')}>
              Rooms
            </th>
            <th scope="col" className={cn(th, 'text-right')} aria-sort={ariaSort('changes')}>
              <SortButton sortKey="changes" sort={sort} dir={dir} onSort={onSort} />
            </th>
            <th scope="col" className={cn(th, 'max-xl:hidden')}>
              Last verdict
            </th>
          </tr>
        </thead>
        <tbody>
          {rows.map((r) => (
            <FleetRow key={r.pipeline_id} row={r} now={now} />
          ))}
        </tbody>
      </table>
    </Card>
  )
}

const COLUMN_LABEL: Record<FleetSortKey, string> = {
  health: 'Health',
  eta: 'Forecast',
  name: 'Pipeline',
  changes: 'Changes 24 h',
}

function SortButton({
  sortKey,
  sort,
  dir,
  onSort,
  pill,
}: {
  sortKey: FleetSortKey
  sort: FleetSortKey
  dir: SortDir
  onSort: (key: FleetSortKey) => void
  pill?: boolean
}) {
  const active = sortKey === sort
  const Icon = !active ? ArrowUpDown : dir === 'asc' ? ArrowUp : ArrowDown
  return (
    <button
      type="button"
      onClick={() => onSort(sortKey)}
      aria-label={`Sort by ${SORT_LABEL[sortKey]}`}
      aria-pressed={pill ? active : undefined}
      className={cn(
        'inline-flex items-center gap-1 rounded-sm font-medium hover:text-foreground focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring',
        active && 'text-foreground',
        pill && 'h-7 rounded-md border border-border bg-card px-2 text-xs',
      )}
    >
      {COLUMN_LABEL[sortKey]}
      <Icon className="size-3" aria-hidden="true" />
    </button>
  )
}

function HealthBadge({ health }: { health: FleetHealth }) {
  return (
    <Badge variant={HEALTH_BADGE[health]} data-health={health}>
      <span className={cn('size-1.5 rounded-full', HEALTH_DOT[health])} aria-hidden="true" />
      {FLEET_HEALTH_LABEL[health]}
    </Badge>
  )
}

function WorstNode({ row }: { row: FleetPipeline }) {
  if (!row.worst_node) return <span className="text-muted-foreground">—</span>
  return (
    <Link
      to={laneHref(row.pipeline_id, row.worst_node)}
      className="block max-w-[16rem] truncate rounded-sm font-mono text-xs text-foreground hover:underline focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring"
      aria-label={`Worst node ${row.worst_node} on the ${row.pipeline_id} lane`}
      title={row.worst_node}
    >
      {row.worst_node}
    </Link>
  )
}

function Signals({ signals }: { signals: string[] }) {
  if (signals.length === 0) return <span className="text-muted-foreground">none</span>
  const rest = signals.length - SHOWN_SIGNALS
  return (
    <span className="inline-flex flex-wrap items-center gap-1" aria-label={`Signals: ${signals.join(', ')}`} title={signals.join(', ')}>
      {signals.slice(0, SHOWN_SIGNALS).map((s) => (
        <Badge key={s} variant="outline" mono aria-hidden="true">
          {s}
        </Badge>
      ))}
      {rest > 0 && (
        <span className="text-xs text-muted-foreground" aria-hidden="true">
          +{rest}
        </span>
      )}
    </span>
  )
}

function Etas({ row, now }: { row: FleetPipeline; now: number }) {
  const badges = row.forecasts.map((f) => etaBadge(f, now)).filter((b) => b !== null)
  if (badges.length === 0) return <span className="text-muted-foreground">—</span>
  const rest = badges.length - SHOWN_ETAS
  return (
    <span className="inline-flex flex-wrap items-center gap-1">
      {badges.slice(0, SHOWN_ETAS).map((b, i) => (
        <Badge key={i} variant={b.due ? 'destructive' : 'warning'} role="img" aria-label={b.long} title={b.long}>
          {b.short}
        </Badge>
      ))}
      {rest > 0 && (
        <span className="text-xs text-muted-foreground" title={badges.slice(SHOWN_ETAS).map((b) => b.long).join('\n')}>
          +{rest}
        </span>
      )}
    </span>
  )
}

function OpenRooms({ row }: { row: FleetPipeline }) {
  if (row.open_rooms === 0) return <span className="tabular-nums text-muted-foreground">0</span>
  return (
    <Link
      to={`/?pipeline=${encodeURIComponent(row.pipeline_id)}`}
      className="rounded-sm tabular-nums text-foreground hover:underline focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring"
      aria-label={`${row.open_rooms} open ${row.open_rooms === 1 ? 'room' : 'rooms'} on ${row.pipeline_id}`}
    >
      {row.open_rooms} open
    </Link>
  )
}

function Verdict({ row }: { row: FleetPipeline }) {
  const v = row.last_verdict
  const label = verdictLabel(v)
  if (!v || !label) return <span className="text-muted-foreground">—</span>
  return (
    <Link
      to={`/rooms/${encodeURIComponent(v.room_id)}`}
      className="rounded-sm text-foreground hover:underline focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring"
      title={v.at ? utcLabel(v.at) : undefined}
    >
      {label}
      {v.at && <span className="text-muted-foreground"> · {ageLabel(v.at)} ago</span>}
    </Link>
  )
}

function PipelineName({ row }: { row: FleetPipeline }) {
  const meta = [row.kind, row.tenant, row.team].filter(Boolean).join(' · ')
  return (
    <div className="flex min-w-0 flex-col">
      <Link
        to={laneHref(row.pipeline_id)}
        className="min-w-0 truncate rounded-sm font-medium text-foreground focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring"
      >
        {row.title || row.pipeline_id}
      </Link>
      <span className="truncate font-mono text-xs text-muted-foreground">
        {row.title && row.title !== row.pipeline_id ? `${row.pipeline_id} · ` : ''}
        {meta}
      </span>
    </div>
  )
}

const TD = 'whitespace-nowrap border-b border-border px-3 py-2 align-middle group-last:border-b-0'

function FleetRow({ row, now }: { row: FleetPipeline; now: number }) {
  const navigate = useNavigate()
  return (
    <tr
      data-pipeline-id={row.pipeline_id}
      className="group cursor-pointer transition-colors hover:bg-accent/60 focus-within:bg-accent"
      onClick={(e) => {
        // Links keep their own target; selecting text is not a click on the row.
        if ((e.target as Element).closest('a, button')) return
        if (window.getSelection()?.toString()) return
        navigate(laneHref(row.pipeline_id))
      }}
    >
      <td className={cn(TD, 'max-w-0')}>
        <PipelineName row={row} />
      </td>
      <td className={TD}>
        <HealthBadge health={row.health} />
      </td>
      <td className={TD}>
        <WorstNode row={row} />
      </td>
      <td className={cn(TD, 'max-lg:hidden')}>
        <Signals signals={row.signals_firing} />
      </td>
      <td className={TD}>
        <Etas row={row} now={now} />
      </td>
      <td className={cn(TD, 'text-right')}>
        <OpenRooms row={row} />
      </td>
      <td className={cn(TD, 'text-right tabular-nums')}>{row.changes_24h}</td>
      <td className={cn(TD, 'max-xl:hidden')}>
        <Verdict row={row} />
      </td>
    </tr>
  )
}

function FleetCard({ row, now }: { row: FleetPipeline; now: number }) {
  return (
    <li data-pipeline-id={row.pipeline_id}>
      <Card className="flex min-w-0 flex-col gap-2 p-3 text-sm">
        <div className="flex min-w-0 items-start gap-2">
          <PipelineName row={row} />
          <span className="ml-auto shrink-0">
            <HealthBadge health={row.health} />
          </span>
        </div>
        <dl className="m-0 grid grid-cols-[auto_minmax(0,1fr)] gap-x-3 gap-y-1 text-xs">
          <dt className="text-muted-foreground">Worst node</dt>
          <dd className="m-0 min-w-0">
            <WorstNode row={row} />
          </dd>
          <dt className="text-muted-foreground">Signals</dt>
          <dd className="m-0 min-w-0">
            <Signals signals={row.signals_firing} />
          </dd>
          <dt className="text-muted-foreground">Forecast</dt>
          <dd className="m-0 min-w-0">
            <Etas row={row} now={now} />
          </dd>
          <dt className="text-muted-foreground">Rooms</dt>
          <dd className="m-0">
            <OpenRooms row={row} />
          </dd>
          <dt className="text-muted-foreground">Changes 24 h</dt>
          <dd className="m-0 tabular-nums">{row.changes_24h}</dd>
          <dt className="text-muted-foreground">Last verdict</dt>
          <dd className="m-0 min-w-0">
            <Verdict row={row} />
          </dd>
        </dl>
      </Card>
    </li>
  )
}

function FleetSkeleton() {
  return (
    <Card role="status" aria-label="Loading the fleet" className="flex flex-col">
      {[0, 1, 2, 3, 4].map((i) => (
        <div key={i} className="flex h-12 items-center gap-4 border-b border-border px-3 last:border-b-0">
          <Skeleton className="h-4 w-1/4" />
          <Skeleton className="h-5 w-16" />
          <Skeleton className="h-4 w-32 max-md:hidden" />
          <Skeleton className="ml-auto h-4 w-10" />
        </div>
      ))}
    </Card>
  )
}

function FleetError({ message, onRetry }: { message: string; onRetry: () => void }) {
  return (
    <Card role="alert" className="flex flex-col items-center gap-3 px-6 py-12 text-center">
      <AlertTriangle className="size-5 text-destructive-fg" aria-hidden="true" />
      <p className="text-base font-medium">Could not load the fleet</p>
      <p className="max-w-[52ch] text-muted-foreground">{message}</p>
      <div className="flex flex-wrap justify-center gap-2">
        <Button variant="outline" onClick={onRetry}>
          Retry
        </Button>
        <Button variant="ghost" asChild>
          <Link to="/">Go to rooms</Link>
        </Button>
      </div>
    </Card>
  )
}

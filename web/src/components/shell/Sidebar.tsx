import { Activity, Inbox, Plug, Search, Workflow } from 'lucide-react'
import type { ReactNode } from 'react'
import { Link, useLocation } from 'react-router-dom'
import { Badge } from '@/components/ui/badge'
import { Button } from '@/components/ui/button'
import { Kbd } from '@/components/ui/kbd'
import { useTopology } from '@/hooks/useTopology'
import { envLabel, modKeyLabel } from '@/lib/envLabel'
import { cn } from '@/lib/utils'
import { useCurrentUser } from '@/context/user'
import { AccountMenu } from './AccountMenu'
import { BrandMark } from './BrandMark'
import { HEALTH_LABEL, pipelineHealth } from './pipelines'
import type { Health, PipelineSummary } from './pipelines'

// A row is `rowClass`; only a row that goes somewhere also gets `linkClass` (hover and focus).
const rowClass =
  'flex h-8 items-center gap-2 rounded-md px-2 text-sm font-medium text-muted-foreground [&_svg]:size-4 [&_svg]:shrink-0'
const linkClass =
  'transition-colors hover:bg-accent hover:text-foreground focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring'
const itemClass = cn(rowClass, linkClass)
const activeClass = 'bg-accent text-foreground'

function Count({ children }: { children: ReactNode }) {
  return <span className="ml-auto font-mono text-[11px] font-medium text-subtle-foreground">{children}</span>
}

const DOT: Record<Health, string> = {
  ok: 'bg-success',
  warn: 'bg-warning',
  fail: 'bg-destructive',
  unknown: 'bg-subtle-foreground',
}

// A pipeline row. Its dot is the worst node health from the topology the room screen
// already uses (useTopology), so the sidebar and the pipeline map agree.
// A link to the room list filtered to this pipeline (/?pipeline=<id>); it is the current
// row while that filter is on screen.
// Polling is kept small on purpose, there is one topology call per pipeline:
//  - a pipeline that only closed rooms mention is not fetched at all (unknown dot);
//  - the pipeline of the room on screen is not polled here, the room screen already polls
//    it; this row refetches once per navigation instead.
function PipelineItem({
  pipeline,
  quiet,
  navKey,
  current,
  onNavigate,
}: {
  pipeline: PipelineSummary
  /** The open room is on this pipeline: do not run a second 30 s poll. */
  quiet: boolean
  navKey: number
  /** The room list is filtered to this pipeline right now. */
  current: boolean
  onNavigate?: () => void
}) {
  const { topology } = useTopology(pipeline.open > 0 ? pipeline.id : '', {
    active: !quiet,
    refreshKey: navKey,
  })
  const health = pipelineHealth(topology)
  return (
    <li>
      <Link
        to={`/?pipeline=${encodeURIComponent(pipeline.id)}`}
        className={cn(itemClass, current && activeClass)}
        aria-current={current ? 'page' : undefined}
        onClick={onNavigate}
      >
        <span className={cn('size-1.5 shrink-0 rounded-full', DOT[health])} aria-hidden="true" />
        <span className="truncate">{pipeline.id}</span>
        <span className="sr-only">, {HEALTH_LABEL[health]}</span>
        {pipeline.open > 0 && <Count>{pipeline.open} open</Count>}
      </Link>
    </li>
  )
}

// Pipelines and Audit log have no screen yet; they are listed (the design has them) but off.
function SoonItem({ icon, label }: { icon: ReactNode; label: string }) {
  return (
    <li>
      <span
        aria-disabled="true"
        title="Not available yet"
        className={cn(rowClass, 'cursor-not-allowed opacity-60')}
      >
        {icon}
        {label}
        <span className="sr-only">, not available yet</span>
      </span>
    </li>
  )
}

export function SidebarContent({
  openRooms,
  pipelines,
  currentPipelineId,
  navKey,
  onSearch,
  onNavigate,
}: {
  openRooms: number
  pipelines: PipelineSummary[]
  /** The pipeline of the room on screen, if any. */
  currentPipelineId?: string
  /** Changes on every navigation: the quiet row refetches. */
  navKey: number
  onSearch: () => void
  /** Called after a link is followed, so the phone drawer can close. */
  onNavigate?: () => void
}) {
  const env = envLabel()
  // The room list's pipeline filter, if the list is on screen.
  const { pathname, search } = useLocation()
  const filter = pathname === '/' ? new URLSearchParams(search).get('pipeline') : null
  const onList = pathname === '/' && !filter // the unfiltered list is the current page of the Rooms link
  const onIntegrations = pathname === '/integrations'
  const admin = useCurrentUser().user.admin === true
  return (
    <div className="flex h-full min-h-0 flex-col bg-sidebar text-foreground">
      <div className="flex h-12 shrink-0 items-center gap-2 border-b border-sidebar-border px-4">
        <BrandMark />
        <span className="text-sm font-semibold">Piperoom</span>
        {env && (
          <Badge variant="outline" mono className="ml-auto h-[18px] text-[11px]">
            {env}
          </Badge>
        )}
      </div>

      <div className="px-2 pt-3">
        <Button
          variant="outline"
          className="w-full justify-start bg-background font-normal text-subtle-foreground hover:text-foreground"
          onClick={onSearch}
          aria-label="Search"
        >
          <Search className="!size-3.5" aria-hidden="true" />
          Search
          <span className="ml-auto inline-flex gap-0.5" aria-hidden="true">
            <Kbd>{modKeyLabel()}</Kbd>
            <Kbd>K</Kbd>
          </span>
        </Button>
      </div>

      <nav aria-label="Main" className="px-2 pt-3">
        <ul className="flex flex-col gap-0.5">
          <li>
            <Link
              to="/"
              className={cn(itemClass, onList && activeClass)}
              aria-current={onList ? 'page' : undefined}
              onClick={onNavigate}
            >
              <Inbox aria-hidden="true" />
              Rooms
              <Count>{openRooms}</Count>
            </Link>
          </li>
          <SoonItem icon={<Workflow aria-hidden="true" />} label="Pipelines" />
          <li>
            <Link
              to="/integrations"
              className={cn(itemClass, onIntegrations && activeClass)}
              aria-current={onIntegrations ? 'page' : undefined}
              onClick={onNavigate}
            >
              <Plug aria-hidden="true" />
              Integrations
              {/* Everyone may look; only admins change anything (docs/api.md, Who is admin). */}
              {!admin && <span className="sr-only">, read-only</span>}
            </Link>
          </li>
          <SoonItem icon={<Activity aria-hidden="true" />} label="Audit log" />
        </ul>
      </nav>

      {pipelines.length > 0 && (
        <>
          <div className="px-4 pb-1.5 pt-5 text-xs font-medium text-subtle-foreground" id="sidebar-pipelines">
            Pipelines
          </div>
          <ul aria-labelledby="sidebar-pipelines" className="flex min-h-0 flex-col gap-0.5 overflow-y-auto px-2">
            {pipelines.map((p) => (
              <PipelineItem
                key={p.id}
                pipeline={p}
                quiet={p.id === currentPipelineId}
                navKey={navKey}
                current={p.id === filter}
                onNavigate={onNavigate}
              />
            ))}
          </ul>
        </>
      )}

      <div className="mt-auto shrink-0">
        <AccountMenu onNavigate={onNavigate} />
      </div>
    </div>
  )
}

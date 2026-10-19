import { useEffect, useLayoutEffect, useRef, useState } from 'react'
import { ChevronsLeftRight, ChevronsRightLeft, Route } from 'lucide-react'
import type { Topology, TopologyNode } from '../api/types'
import { updatedAgoLabel } from '../lib/time'
import { cn } from '@/lib/utils'
import { Badge, layerVariant } from '@/components/ui/badge'
import { Button } from '@/components/ui/button'
import { Kbd } from '@/components/ui/kbd'
import { Skeleton } from '@/components/ui/skeleton'
import { useLaneOverflow } from '../hooks/useLaneOverflow'
import { hasEmptyDlq, laneStorageKey, visibleNodes, worstNode } from '../lib/lane'
import { StatusDot } from './room/StatusDot'
import type { DotTone } from './room/StatusDot'

const KIND_LABEL: Record<TopologyNode['kind'], string> = {
  source_table: 'Source',
  connector: 'Connector',
  topic: 'Topic',
  dlq_topic: 'DLQ topic',
  consumer_group: 'Consumer',
  sink_table: 'Sink',
  dag: 'DAG',
  task: 'Task',
  model: 'Model',
}

// A batch node's run state is one of its facts (internal/topology/batch.go); the lane shows it first, under a
// readable label, so the state is what a compact node shows next to its health.
const RUN_STATE: Partial<Record<TopologyNode['kind'], { key: string; label: string }>> = {
  dag: { key: 'last_run_state', label: 'last run' },
  task: { key: 'state', label: 'state' },
  model: { key: 'last_run_status', label: 'last run' },
}

/**
 * The facts a node shows, lead fact first. CDC nodes keep the server's order untouched; a batch node leads with its
 * run state, and a DAG's tasks_skipped is drawn as its own note instead of a fact.
 */
function displayFacts(node: TopologyNode): [string, string][] {
  const entries = node.facts ? Object.entries(node.facts) : []
  const run = RUN_STATE[node.kind]
  if (!run) return entries
  const rest = entries.filter(([k]) => k !== run.key && !(node.kind === 'dag' && k === 'tasks_skipped'))
  const state = node.facts?.[run.key]
  return state === undefined ? rest : [[run.label, state], ...rest]
}

/** Tasks of the DAG's latest run the server did not draw (their id is not an Airflow id); 0 when none. */
function skippedTasks(node: TopologyNode): number {
  if (node.kind !== 'dag') return 0
  const n = Number.parseInt(node.facts?.tasks_skipped ?? '', 10)
  return Number.isFinite(n) && n > 0 ? n : 0
}

function SkippedNote({ count }: { count: number }) {
  if (count === 0) return null
  return (
    <span
      data-skipped=""
      title="Tasks of the latest run whose id is not a valid Airflow id are not drawn"
      className="whitespace-nowrap rounded-sm border border-dashed border-border px-1 text-[11px] leading-4 text-muted-foreground"
    >
      {count === 1 ? '1 task not drawn' : `${count} tasks not drawn`}
    </span>
  )
}

const HEALTH: Record<TopologyNode['health'], { word: string; tone: DotTone }> = {
  ok: { word: 'healthy', tone: 'ok' },
  warn: { word: 'degraded', tone: 'warn' },
  fail: { word: 'failed', tone: 'fail' },
  unknown: { word: 'unknown', tone: 'muted' },
}

// The edge line sits 38px down (the middle of a minimum-height node) however tall the nodes grow, so the
// caption on a stalled edge stays level with the kind label.
// An edge is drawn between each two neighbours. It is "stalled" when the node it
// leaves has failed: nothing flows past a failed node.
export const EDGE =
  'relative h-0 w-11 shrink-0 self-start border-t-2 border-dashed border-input ' +
  "after:absolute after:-right-px after:-top-[5px] after:border-y-4 after:border-r-0 after:border-l-[6px] after:border-solid after:border-y-transparent after:border-l-input after:content-['']"
export const EDGE_STALLED =
  'relative h-0 w-16 shrink-0 self-start border-t-2 border-solid border-destructive ' +
  "after:absolute after:-right-px after:-top-[5px] after:border-y-4 after:border-r-0 after:border-l-[6px] after:border-solid after:border-y-transparent after:border-l-destructive after:content-['']"

// The line sits at the middle of the node: 27px of a 56px compact strip, 38px of a 92px card.
const edgeTop = (compact: boolean) => (compact ? 'mt-[27px]' : 'mt-[38px]')

function Node({
  node,
  compact,
  worst,
  selected,
  onSelect,
  healed = false,
}: {
  /** The node just went from fail/warn to ok: it flashes once. */
  healed?: boolean
  node: TopologyNode
  compact: boolean
  worst: boolean
  selected: boolean
  onSelect?: () => void
}) {
  const health = HEALTH[node.health] ?? HEALTH.unknown
  const facts = displayFacts(node)
  const skipped = skippedTasks(node)
  const shared = cn(
    'rounded-lg border bg-card text-left focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring',
    // Status is never a card border, except a failed node.
    node.health === 'fail' ? 'border-destructive' : 'border-border',
    node.health === 'unknown' && 'border-dashed',
    selected && 'ring-2 ring-ring',
    healed && 'motion-safe:animate-heal',
  )
  if (compact) {
    return (
      <button
        type="button"
        data-node=""
        data-health={node.health}
        data-worst={worst ? 'true' : undefined}
        aria-pressed={selected}
        onClick={onSelect}
        title={`owner: ${node.owner}`}
        className={cn(shared, 'flex h-14 min-w-[168px] flex-1 basis-0 flex-col justify-center gap-0.5 px-3 py-1.5')}
      >
        <span className="flex items-center gap-2 text-xs">
          <span className="whitespace-nowrap text-subtle-foreground">{KIND_LABEL[node.kind] ?? node.kind}</span>
          <span aria-hidden="true" className="text-subtle-foreground">·</span>
          <Badge variant={layerVariant(node.layer)} mono>
            {node.layer}
          </Badge>
          <SkippedNote count={skipped} />
        </span>
        <span className="flex min-w-0 items-center gap-1.5 text-xs">
          <span className="min-w-0 truncate font-mono text-sm font-medium" title={node.name}>
            {node.name}
          </span>
          <StatusDot tone={health.tone} />
          <span className={cn('whitespace-nowrap', node.health === 'fail' ? 'text-destructive-fg' : 'text-muted-foreground')}>
            {health.word}
          </span>
          {facts.length > 0 && (
            <span className="ml-auto whitespace-nowrap font-mono text-muted-foreground">
              <span className="text-subtle-foreground">{facts[0][0]}</span> {facts[0][1]}
            </span>
          )}
        </span>
      </button>
    )
  }
  return (
    <button
      type="button"
      data-node=""
      data-health={node.health}
      data-worst={worst ? 'true' : undefined}
      aria-pressed={selected}
      onClick={onSelect}
      title={`owner: ${node.owner}`}
      className={cn(shared, 'flex min-h-[92px] min-w-[200px] flex-1 basis-0 flex-col gap-1.5 px-3 py-2.5')}
    >
      <div className="flex items-center gap-2">
        <span className="whitespace-nowrap text-xs text-subtle-foreground">{KIND_LABEL[node.kind] ?? node.kind}</span>
        <SkippedNote count={skipped} />
        <Badge variant={layerVariant(node.layer)} mono className="ml-auto">
          {node.layer}
        </Badge>
      </div>
      <div className="truncate font-mono text-sm font-medium" title={node.name}>
        {node.name}
      </div>
      <div className="mt-auto flex flex-col gap-1">
      <div
        className={cn(
          'flex flex-wrap items-center gap-x-1.5 text-xs',
          node.health === 'fail' ? 'text-destructive-fg' : 'text-muted-foreground',
        )}
      >
        <StatusDot tone={health.tone} />
        <span>{health.word}</span>
        {facts.length > 0 && (
          <span className="ml-auto font-mono">
            <span className="text-subtle-foreground">{facts[0][0]}</span> {facts[0][1]}
          </span>
        )}
      </div>
      {facts.length > 1 && (
        <dl className="m-0 flex flex-wrap gap-x-2 font-mono text-xs text-muted-foreground">
          {facts.slice(1).map(([k, v]) => (
            <div key={k} className="flex gap-1">
              <dt className="text-subtle-foreground">{k}</dt>
              <dd className="m-0">{v}</dd>
            </div>
          ))}
        </dl>
      )}
      </div>
    </button>
  )
}

function SkeletonNode({ compact }: { compact: boolean }) {
  return (
    <div className={cn('flex flex-1 basis-0 flex-col gap-2 rounded-lg border border-border bg-card px-3', compact ? 'h-14 min-w-[168px] justify-center py-1.5' : 'min-h-[92px] min-w-[200px] py-2.5')}>
      <Skeleton className="h-3 w-3/5" />
      <Skeleton className="h-3.5 w-[85%]" />
      {!compact && <Skeleton className="mt-auto h-3 w-[45%]" />}
    </div>
  )
}
// Set VITE_RUNBOOK_URL where the runbook is served; without it the link is not shown.
const RUNBOOK_URL = (import.meta.env.VITE_RUNBOOK_URL as string | undefined) || ''

function NotConfigured({ id, onRetry }: { id: string; onRetry?: () => void }) {
  return (
    <div className="flex flex-col gap-2 rounded-lg border border-dashed border-border bg-card px-4 py-3">
      <h2 className="m-0 text-sm font-medium">Pipeline &quot;{id}&quot; is not configured</h2>
      <p className="m-0 text-muted-foreground">
        Alerts for it open a room, but there is no topology to show and the agent cannot propose a fix here. Add the
        pipeline to owners.yaml (see docs/runbook.md) and restart the server.
      </p>
      <div className="flex items-center gap-2">
        {onRetry && (
          <Button variant="secondary" size="sm" onClick={onRetry}>
            Try again
          </Button>
        )}
        {RUNBOOK_URL && (
          <Button variant="ghost" size="sm" asChild>
            <a href={RUNBOOK_URL}>Open runbook</a>
          </Button>
        )}
      </div>
    </div>
  )
}

export function PipelineMap({
  topology,
  updatedAt = null,
  onTrace,
  pipelineId,
  stalledFor,
  selectedLayer,
  onSelectLayer,
  notFound = false,
  onRetry,
}: {
  topology: Topology | null
  /** The server answered 404 for this pipeline: it is not in owners.yaml. */
  notFound?: boolean
  onRetry?: () => void
  updatedAt?: number | null
  /** Absent for a viewer: tracing a record is a write (POST .../trace). */
  onTrace?: () => void
  /** Shown next to the heading; defaults to the topology's own id. */
  pipelineId?: string
  /** How long nothing has flowed, e.g. "48m": the caption on the edge after a failed node. */
  stalledFor?: string
  /** The layer the step stream is filtered to; its node shows as pressed. */
  selectedLayer?: string
  /** Click a node to filter the stream to its layer; click it again to clear. */
  onSelectLayer?: (layer: string | null) => void
}) {
  // Re-render once a second so "updated Ns ago" stays fresh between fetches.
  const [now, setNow] = useState(() => Date.now())
  useEffect(() => {
    const timer = window.setInterval(() => setNow(Date.now()), 1000)
    return () => window.clearInterval(timer)
  }, [])

  const id = pipelineId ?? topology?.pipeline_id ?? ''
  const [expanded, setExpanded] = useState(() => readExpanded(id))
  const [expandedFor, setExpandedFor] = useState(id)
  if (expandedFor !== id) {
    setExpandedFor(id)
    setExpanded(readExpanded(id))
  }
  const compact = !expanded
  const toggle = () => {
    const next = !expanded
    setExpanded(next)
    try {
      sessionStorage.setItem(laneStorageKey(id), next ? '1' : '0')
    } catch {
      // storage blocked: the choice lasts until reload
    }
  }

  const nodes = topology ? visibleNodes(topology.nodes) : []
  const dlqHidden = topology ? hasEmptyDlq(topology.nodes) : false
  const worst = worstNode(nodes)
  const worstId = worst && worst.health !== 'ok' ? worst.id : null

  // Nodes that went from fail/warn to ok since the last render flash once (animate-heal, 600 ms). The ref is
  // read while rendering and written after it, so a re-render for another reason does not replay the flash
  // beyond the next tick (the clock re-renders every second, longer than the animation).
  const prevHealth = useRef<Map<string, string>>(new Map())
  const healedIds = new Set(
    nodes.filter((n) => n.health === 'ok' && ['fail', 'warn'].includes(prevHealth.current.get(n.id) ?? '')).map((n) => n.id),
  )
  useEffect(() => {
    prevHealth.current = new Map(nodes.map((n) => [n.id, n.health]))
  })

  const scroller = useRef<HTMLDivElement>(null)
  const overflow = useLaneOverflow(scroller, [nodes.length, compact, !!topology])

  // Bring the worst node to the middle of the lane, on first paint and when it changes. Only the lane
  // scrolls: scrollIntoView would move the page too.
  useLayoutEffect(() => {
    const el = scroller.current
    if (!el || !worstId) return
    const node = el.querySelector<HTMLElement>('[data-worst="true"]')
    if (!node) return
    const reduced = typeof window.matchMedia === 'function' && window.matchMedia('(prefers-reduced-motion: reduce)').matches
    const left = node.offsetLeft - (el.clientWidth - node.offsetWidth) / 2
    if (typeof el.scrollTo === 'function') el.scrollTo({ left, behavior: reduced ? 'auto' : 'smooth' })
    else el.scrollLeft = left
  }, [worstId, compact])

  const by = (dir: 1 | -1) => {
    const el = scroller.current
    if (el && typeof el.scrollBy === 'function') el.scrollBy({ left: dir * 0.8 * el.clientWidth, behavior: 'smooth' })
  }

  if (notFound) {
    return (
      <section aria-label="Pipeline" className="flex min-w-0 flex-col gap-2.5">
        <NotConfigured id={id} onRetry={onRetry} />
      </section>
    )
  }

  return (
    <section aria-labelledby="pipeline-heading" className="flex min-w-0 flex-col gap-2.5">
      <div className="flex flex-wrap items-center gap-2">
        <h2 id="pipeline-heading" className="text-sm font-medium">
          Pipeline
        </h2>
        {id && <span className="font-mono text-muted-foreground">{id}</span>}
        {updatedAt != null ? (
          <span className="ml-2 text-xs text-subtle-foreground">{updatedAgoLabel(updatedAt, now)}</span>
        ) : (
          !topology && <span className="ml-2 text-xs text-subtle-foreground">Loading topology…</span>
        )}
        {dlqHidden && (
          <span className="rounded-sm border border-border px-1.5 text-xs leading-5 text-muted-foreground">DLQ empty</span>
        )}
        {topology?.partial && (
          <span
            data-partial=""
            title="Some nodes are left out: a reader this pipeline needs (orchestrator or transform) is not configured or not supported. The map is incomplete, not healthy."
            className="rounded-sm border border-dashed border-border px-1.5 text-xs leading-5 text-muted-foreground"
          >
            partial
          </span>
        )}
        <div className="ml-auto flex items-center gap-1">
          <Button variant="ghost" className="h-7 px-2.5" aria-pressed={expanded} onClick={toggle}>
            {expanded ? (
              <ChevronsRightLeft aria-hidden="true" className="!size-3.5" />
            ) : (
              <ChevronsLeftRight aria-hidden="true" className="!size-3.5" />
            )}
            {expanded ? 'Compact' : 'Expand'}
          </Button>
          {onTrace && (
            <Button variant="ghost" className="h-7 px-2.5" onClick={onTrace} aria-keyshortcuts="Shift+T">
              <Route aria-hidden="true" className="!size-3.5" />
              Trace a record
              <Kbd aria-hidden="true">⇧T</Kbd>
            </Button>
          )}
        </div>
      </div>

      {/* Nodes do not all fit a phone: the lane scrolls sideways, the page does not. */}
      <div className="relative">
        <div
          ref={scroller}
          role="group"
          aria-label="Pipeline, source to sink"
          tabIndex={0}
          className="flex items-stretch overflow-x-auto rounded-md pb-1 pt-5 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring"
        >
          {!topology ? (
            <>
              {[0, 1, 2, 3, 4].map((i) => (
                <div key={i} className="contents">
                  {i > 0 && <div aria-hidden="true" className={cn(EDGE, edgeTop(compact))} />}
                  <SkeletonNode compact={compact} />
                </div>
              ))}
            </>
          ) : (
            nodes.map((n, i) => {
              const prev = i > 0 ? nodes[i - 1] : null
              const stalled = prev?.health === 'fail'
              return (
                <div key={n.id} className="contents">
                  {prev &&
                    (stalled ? (
                      <div className={cn(EDGE_STALLED, edgeTop(compact))}>
                        <span className="absolute left-1/2 top-[-24px] -translate-x-1/2 whitespace-nowrap rounded-sm border border-border bg-background px-1.5 font-mono text-[11px] font-medium leading-4 text-destructive-fg">
                          {stalledFor ? `no flow · ${stalledFor}` : 'no flow'}
                        </span>
                      </div>
                    ) : (
                      <div aria-hidden="true" className={cn(EDGE, edgeTop(compact))} />
                    ))}
                  <Node
                    node={n}
                    compact={compact}
                    worst={n.id === worstId}
                    healed={healedIds.has(n.id)}
                    selected={selectedLayer === n.layer}
                    onSelect={onSelectLayer ? () => onSelectLayer(selectedLayer === n.layer ? null : n.layer) : undefined}
                  />
                </div>
              )
            })
          )}
        </div>
        {overflow.left && (
          <>
            <div aria-hidden="true" className="pointer-events-none absolute inset-y-0 left-0 w-8 bg-gradient-to-r from-background to-transparent" />
            {overflow.moreLeft > 0 && (
              <button
                type="button"
                aria-label={`Show ${overflow.moreLeft} more ${overflow.moreLeft === 1 ? 'node' : 'nodes'}`}
                onClick={() => by(-1)}
                className="absolute left-1 top-0 z-10 h-5 rounded-full border border-border bg-card px-2 font-mono text-[11px] leading-4 shadow-sm hover:bg-accent"
              >
                ← +{overflow.moreLeft}
              </button>
            )}
          </>
        )}
        {overflow.right && (
          <>
            <div aria-hidden="true" className="pointer-events-none absolute inset-y-0 right-0 w-8 bg-gradient-to-l from-background to-transparent" />
            {overflow.moreRight > 0 && (
              <button
                type="button"
                aria-label={`Show ${overflow.moreRight} more ${overflow.moreRight === 1 ? 'node' : 'nodes'}`}
                onClick={() => by(1)}
                className="absolute right-1 top-0 z-10 h-5 rounded-full border border-border bg-card px-2 font-mono text-[11px] leading-4 shadow-sm hover:bg-accent"
              >
                +{overflow.moreRight} →
              </button>
            )}
          </>
        )}
      </div>
    </section>
  )
}

function readExpanded(id: string): boolean {
  try {
    return sessionStorage.getItem(laneStorageKey(id)) === '1'
  } catch {
    return false
  }
}

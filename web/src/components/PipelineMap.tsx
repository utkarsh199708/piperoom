import { useEffect, useState } from 'react'
import { Route } from 'lucide-react'
import type { Topology, TopologyNode } from '../api/types'
import { updatedAgoLabel } from '../lib/time'
import { cn } from '@/lib/utils'
import { Badge, layerVariant } from '@/components/ui/badge'
import { Button } from '@/components/ui/button'
import { Kbd } from '@/components/ui/kbd'
import { Skeleton } from '@/components/ui/skeleton'
import { StatusDot } from './room/StatusDot'
import type { DotTone } from './room/StatusDot'

const KIND_LABEL: Record<TopologyNode['kind'], string> = {
  source_table: 'Source',
  connector: 'Connector',
  topic: 'Topic',
  dlq_topic: 'DLQ topic',
  consumer_group: 'Consumer',
  sink_table: 'Sink',
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
const EDGE =
  'relative mt-[38px] h-0 w-11 shrink-0 self-start border-t-2 border-dashed border-input ' +
  "after:absolute after:-right-px after:-top-[5px] after:border-y-4 after:border-r-0 after:border-l-[6px] after:border-solid after:border-y-transparent after:border-l-input after:content-['']"
const EDGE_STALLED =
  'relative mt-[38px] h-0 w-16 shrink-0 self-start border-t-2 border-solid border-destructive ' +
  "after:absolute after:-right-px after:-top-[5px] after:border-y-4 after:border-r-0 after:border-l-[6px] after:border-solid after:border-y-transparent after:border-l-destructive after:content-['']"

function Node({ node }: { node: TopologyNode }) {
  const health = HEALTH[node.health] ?? HEALTH.unknown
  const facts = node.facts ? Object.entries(node.facts) : []
  return (
    <div
      data-health={node.health}
      title={`owner: ${node.owner}`}
      className={cn(
        'flex min-h-[92px] min-w-[200px] flex-1 basis-0 flex-col gap-1.5 rounded-lg border bg-card px-3 py-2.5',
        // Status is never a card border, except a failed node.
        node.health === 'fail' ? 'border-destructive' : 'border-border',
        node.health === 'unknown' && 'border-dashed',
      )}
    >
      <div className="flex items-center gap-2">
        <span className="whitespace-nowrap text-xs text-subtle-foreground">{KIND_LABEL[node.kind] ?? node.kind}</span>
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
    </div>
  )
}

function SkeletonNode() {
  return (
    <div className="flex min-h-[92px] min-w-[200px] flex-1 basis-0 flex-col gap-2 rounded-lg border border-border bg-card px-3 py-2.5">
      <Skeleton className="h-3 w-3/5" />
      <Skeleton className="h-3.5 w-[85%]" />
      <Skeleton className="mt-auto h-3 w-[45%]" />
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
}) {
  // Re-render once a second so "updated Ns ago" stays fresh between fetches.
  const [now, setNow] = useState(() => Date.now())
  useEffect(() => {
    const timer = window.setInterval(() => setNow(Date.now()), 1000)
    return () => window.clearInterval(timer)
  }, [])

  const id = pipelineId ?? topology?.pipeline_id ?? ''

  return (
    <section aria-labelledby="pipeline-heading" className="flex min-w-0 flex-col gap-2.5">
      <div className="flex flex-wrap items-center gap-2">
        <h2 id="pipeline-heading" className="text-sm font-medium">
          Pipeline
        </h2>
        {id && <span className="font-mono text-muted-foreground">{id}</span>}
        {notFound ? null : updatedAt != null ? (
          <span className="ml-2 text-xs text-subtle-foreground">{updatedAgoLabel(updatedAt, now)}</span>
        ) : (
          !topology && <span className="ml-2 text-xs text-subtle-foreground">Loading topology…</span>
        )}
        {onTrace && (
          <Button
            variant="ghost"
            className="ml-auto h-7 px-2.5"
            onClick={onTrace}
            aria-keyshortcuts="Shift+T"
          >
            <Route aria-hidden="true" className="!size-3.5" />
            Trace one record
            <Kbd aria-hidden="true">⇧T</Kbd>
          </Button>
        )}
      </div>

      {notFound ? (
        <NotConfigured id={id} onRetry={onRetry} />
      ) : (
      <>
      {/* Five 200px nodes do not fit a phone: the lane scrolls sideways, the page does not. */}
      <div
        role="group"
        aria-label="Pipeline, source to sink"
        tabIndex={0}
        className="flex items-stretch overflow-x-auto rounded-md pb-1 pt-5 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring"
      >
        {!topology ? (
          <>
            {[0, 1, 2, 3, 4].map((i) => (
              <div key={i} className="contents">
                {i > 0 && <div aria-hidden="true" className={EDGE} />}
                <SkeletonNode />
              </div>
            ))}
          </>
        ) : (
          topology.nodes.map((n, i) => {
            const prev = i > 0 ? topology.nodes[i - 1] : null
            const stalled = prev?.health === 'fail'
            return (
              <div key={n.id} className="contents">
                {prev &&
                  (stalled ? (
                    <div className={EDGE_STALLED}>
                      <span className="absolute left-1/2 top-[-24px] -translate-x-1/2 whitespace-nowrap rounded-sm border border-border bg-background px-1.5 font-mono text-[11px] font-medium leading-4 text-destructive-fg">
                        {stalledFor ? `no flow · ${stalledFor}` : 'no flow'}
                      </span>
                    </div>
                  ) : (
                    <div aria-hidden="true" className={EDGE} />
                  ))}
                <Node node={n} />
              </div>
            )
          })
        )}
      </div>
      </>
      )}
    </section>
  )
}

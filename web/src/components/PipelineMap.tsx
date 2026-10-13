import { Fragment, useEffect, useState } from 'react'
import type { Topology, TopologyNode } from '../api/types'
import { updatedAgoLabel } from '../lib/time'
import { Panel } from './ui'
// TEMP until P1: Avatar, Kicker, LayerTag and Skeleton come from './ui' once it lands.
import { Avatar, Kicker, LayerTag, Skeleton } from './p2Standins'
import '../styles/flow.css'

const HEALTH_WORD: Record<TopologyNode['health'], string> = {
  ok: 'healthy',
  warn: 'degraded',
  fail: 'failed',
  unknown: 'unknown',
}

function kindLabel(kind: string): string {
  return kind.replace(/_/g, ' ')
}

interface Lane {
  nodes: TopologyNode[]
  /** The nodes form one chain, so edges are drawn. Branching edges fall back to a plain list. */
  linear: boolean
  /** Index i is true when a real edge joins nodes[i] to nodes[i + 1]. */
  joined: boolean[]
}

/** Nodes in `edges` order; `nodes` order when there are no edges; a plain list if edges branch. */
function buildLane(topology: Topology): Lane {
  const byId = new Map(topology.nodes.map((n) => [n.id, n]))
  const edges = topology.edges.filter((e) => byId.has(e.from) && byId.has(e.to))
  const plain = (linear: boolean): Lane => ({
    nodes: topology.nodes,
    linear,
    joined: topology.nodes.map(() => false),
  })
  if (edges.length === 0) return plain(true)

  const next = new Map<string, string>()
  const hasIncoming = new Set<string>()
  for (const e of edges) {
    if (next.has(e.from) || hasIncoming.has(e.to)) return plain(false) // branches or merges
    next.set(e.from, e.to)
    hasIncoming.add(e.to)
  }
  const starts = topology.nodes.filter((n) => next.has(n.id) && !hasIncoming.has(n.id))
  if (starts.length !== 1) return plain(false) // several chains, or a cycle

  const ordered: TopologyNode[] = []
  const joined: boolean[] = []
  const seen = new Set<string>()
  let cur: string | undefined = starts[0].id
  while (cur !== undefined && !seen.has(cur)) {
    seen.add(cur)
    ordered.push(byId.get(cur)!)
    const to = next.get(cur)
    joined.push(to !== undefined)
    cur = to
  }
  // Nodes that no edge touches keep their place after the chain.
  for (const n of topology.nodes) {
    if (!seen.has(n.id)) {
      ordered.push(n)
      joined.push(false)
    }
  }
  return { nodes: ordered, linear: true, joined }
}

/** "no flow · 48m" when the node after the break has a `newest_record` age, else "stalled". */
function stalledCaption(downstream: TopologyNode | undefined): string {
  const age = downstream?.facts?.newest_record
  if (!age) return 'stalled'
  return `no flow · ${age.replace(/\s*ago$/i, '')}`
}

export function PipelineMap({
  topology,
  updatedAt = null,
  onTrace,
}: {
  topology: Topology | null
  updatedAt?: number | null
  /** Absent for a viewer: tracing a record is a write (POST .../trace). */
  onTrace?: () => void
}) {
  // Re-render once a second so "updated Ns ago" stays fresh between fetches.
  const [now, setNow] = useState(() => Date.now())
  useEffect(() => {
    const timer = window.setInterval(() => setNow(Date.now()), 1000)
    return () => window.clearInterval(timer)
  }, [])

  return (
    <Panel
      className="pipeline"
      title={
        <>
          Pipeline
          {topology && <span className="pipeline__id"> · {topology.pipeline_id}</span>}
        </>
      }
      right={
        <>
          {updatedAt != null && (
            <span className="muted pipeline__updated">{updatedAgoLabel(updatedAt, now)}</span>
          )}
          {onTrace && (
            <button type="button" className="btn btn--ghost btn--small" onClick={onTrace}>
              Trace one record
            </button>
          )}
        </>
      }
    >
      {!topology ? <LaneSkeleton /> : <LaneBody topology={topology} />}
    </Panel>
  )
}

function LaneSkeleton() {
  return (
    <div className="flow" aria-busy="true">
      <div className="flow__lane" aria-hidden="true">
        {[0, 1, 2, 3, 4].map((i) => (
          <Fragment key={i}>
            {i > 0 && (
              <div className="edge edge--flow">
                <div className="edge__line" />
              </div>
            )}
            <div className="node-card node-card--skeleton">
              <Skeleton h={12} w="40%" />
              <Skeleton h={16} w="75%" />
              <Skeleton h={12} w="55%" />
              <Skeleton h={12} w="35%" />
            </div>
          </Fragment>
        ))}
      </div>
      <p className="empty flow__status">Loading topology…</p>
    </div>
  )
}

function LaneBody({ topology }: { topology: Topology }) {
  if (topology.nodes.length === 0) {
    return (
      <div className="flow">
        <div className="flow__lane flow__lane--list">
          <div className="node-card node-card--unknown node-card--empty">
            <div className="node-card__name">
              No topology for <span className="mono">{topology.pipeline_id}</span>
            </div>
          </div>
        </div>
      </div>
    )
  }

  const lane = buildLane(topology)
  return (
    <div className="flow">
      <div className={`flow__lane${lane.linear ? '' : ' flow__lane--list'}`}>
        {lane.nodes.map((n, i) => (
          <Fragment key={n.id}>
            <NodeCard node={n} />
            {lane.linear && i < lane.nodes.length - 1 && (
              <Edge
                upstream={n}
                downstream={lane.nodes[i + 1]}
                joined={lane.joined[i]}
              />
            )}
          </Fragment>
        ))}
      </div>
    </div>
  )
}

function NodeCard({ node: n }: { node: TopologyNode }) {
  const facts = Object.entries(n.facts ?? {}).slice(0, 2)
  const flagged = n.health === 'fail' || n.health === 'warn'
  return (
    <article
      className={`node-card node-card--${n.health}`}
      tabIndex={0}
      aria-label={`${n.name}, ${kindLabel(n.kind)}, ${n.layer}, ${HEALTH_WORD[n.health]}`}
    >
      <div className="node-card__top">
        <Kicker>{kindLabel(n.kind)}</Kicker>
        <LayerTag layer={n.layer} />
      </div>
      <div className="node-card__name" title={n.name}>
        {n.name}
      </div>
      {facts.length > 0 && (
        <dl className="node-card__facts">
          {facts.map(([k, v], i) => (
            <Fragment key={k}>
              <dt>{k}</dt>
              <dd className={i === 0 && flagged ? `is-${n.health}` : undefined}>{v}</dd>
            </Fragment>
          ))}
        </dl>
      )}
      <div className="node-card__foot">
        <span className="node-card__owner">
          <Avatar actor={{ kind: 'human', id: n.owner, name: n.owner }} size="xs" />
          {n.owner}
        </span>
        <span className="node-card__health">
          <span className="dot" aria-hidden="true" />
          {HEALTH_WORD[n.health]}
        </span>
      </div>
    </article>
  )
}

/** The state comes from the upstream node: a failed source means nothing flows past it. */
function Edge({
  upstream,
  downstream,
  joined,
}: {
  upstream: TopologyNode
  downstream: TopologyNode
  joined: boolean
}) {
  if (!joined) return <div className="edge edge--gap" aria-hidden="true" />
  if (upstream.health === 'fail') {
    return (
      <div className="edge edge--stalled">
        <span className="edge__cap">{stalledCaption(downstream)}</span>
        <div className="edge__line" aria-hidden="true" />
      </div>
    )
  }
  return (
    <div className="edge edge--flow" aria-hidden="true">
      <div className="edge__line" />
    </div>
  )
}

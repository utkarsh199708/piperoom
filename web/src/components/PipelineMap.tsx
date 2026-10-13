import { useEffect, useState } from 'react'
import type { Topology } from '../api/types'
import { updatedAgoLabel } from '../lib/time'
import { Panel } from './ui'

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
      title="Pipeline"
      right={
        <>
          {updatedAt != null && (
            <span className="muted pipeline__updated">{updatedAgoLabel(updatedAt, now)}</span>
          )}
          {onTrace && (
            <button type="button" className="btn btn--ghost" onClick={onTrace}>
              Trace one record
            </button>
          )}
        </>
      }
    >
      {!topology ? (
        <p className="empty">Loading topology…</p>
      ) : (
        <div className="pipeline__nodes">
          {topology.nodes.map((n) => (
            <div key={n.id} className={`node-card node-card--${n.health}`}>
              <div className="node-card__head">
                <span className={`node-dot node-dot--${n.health}`} aria-hidden="true" />
                <span className="node-card__name">{n.name}</span>
              </div>
              <div className="node-card__meta muted">
                <span>{n.kind}</span>
                <span>· {n.layer}</span>
                <span>· {n.owner}</span>
              </div>
              {n.facts && Object.keys(n.facts).length > 0 && (
                <dl className="node-card__facts">
                  {Object.entries(n.facts).map(([k, v]) => (
                    <div key={k} className="node-card__fact">
                      <dt>{k}</dt>
                      <dd>{v}</dd>
                    </div>
                  ))}
                </dl>
              )}
            </div>
          ))}
        </div>
      )}
    </Panel>
  )
}

import type { Topology } from '../api/types'
import { Panel } from './ui'

export function PipelineMap({
  topology,
  onTrace,
}: {
  topology: Topology | null
  onTrace: () => void
}) {
  return (
    <Panel
      className="pipeline"
      title="Pipeline"
      right={
        <button type="button" className="btn btn--ghost" onClick={onTrace}>
          Trace one record
        </button>
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

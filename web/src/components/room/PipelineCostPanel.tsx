import { useAgentStats } from '@/hooks/useAgentStats'
import { infraFacts, modelCostText } from '@/lib/insight'
import { utcLabel } from '@/lib/time'

/**
 * What this pipeline costs (X6), from GET /api/pipelines/{pid}/agent-stats: the model side (median cost of a
 * closed room) next to the infrastructure hints of the last 7 days of watch samples. Infrastructure is in units
 * only: a GB-day or a record has no price here, the customer's platform sets it. Nothing renders while the
 * stats are unavailable (fixtures, 404, network), like the track record on the hypothesis card.
 */
export function PipelineCostPanel({ pipelineId }: { pipelineId: string }) {
  const stats = useAgentStats(pipelineId)
  if (!stats) return null
  const infra = stats.infra ?? null
  return (
    <section aria-labelledby="pipeline-cost-heading" className="flex flex-col gap-2.5 px-4 py-3.5" data-testid="pipeline-cost">
      <h2 id="pipeline-cost-heading" className="m-0 text-sm font-medium">
        Cost on <span className="font-mono">{stats.pipeline_id}</span>
      </h2>
      <div role="group" aria-labelledby="pipeline-cost-model" className="flex flex-col gap-0.5">
        <h3 id="pipeline-cost-model" className="m-0 text-xs font-medium text-muted-foreground">
          Model
        </h3>
        <p className="m-0 break-words text-sm tabular-nums" data-testid="pipeline-cost-model">
          {modelCostText(stats)}
        </p>
      </div>
      <div role="group" aria-labelledby="pipeline-cost-infra" className="flex flex-col gap-0.5">
        <h3 id="pipeline-cost-infra" className="m-0 text-xs font-medium text-muted-foreground">
          Infrastructure · last 7 d
        </h3>
        {infra ? (
          <>
            <dl className="m-0 grid grid-cols-[auto_1fr] gap-x-3 gap-y-0.5 text-sm tabular-nums">
              {infraFacts(infra).map((f) => (
                <div key={f.label} className="contents">
                  <dt className="text-muted-foreground" title={f.hint}>
                    {f.label}
                  </dt>
                  <dd className="m-0 min-w-0 break-words">
                    {f.value ?? <span className="text-subtle-foreground">no sample</span>}
                  </dd>
                </div>
              ))}
            </dl>
            <p className="m-0 text-xs text-subtle-foreground" title={`${utcLabel(infra.from)} to ${utcLabel(infra.to)}`}>
              {infra.samples} {infra.samples === 1 ? 'sample' : 'samples'} · units only, priced by your platform
            </p>
          </>
        ) : (
          <p role="status" className="m-0 text-sm text-subtle-foreground">
            not enough history
          </p>
        )}
      </div>
    </section>
  )
}

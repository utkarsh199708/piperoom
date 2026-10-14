import type { AgentToolResultPayload, ShadowEvent, ShadowLabelledPayload, ShadowResponse } from '@/api/types'
import { formatSeconds, STATUS_LABEL } from '@/lib/shadow'
import { Badge } from '@/components/ui/badge'
import { Button } from '@/components/ui/button'
import { Card } from '@/components/ui/card'

function secondsFrom(openedAt: string | undefined, at: string | null): number {
  if (!openedAt || !at) return -1
  const d = (Date.parse(at) - Date.parse(openedAt)) / 1000
  return Number.isFinite(d) && d >= 0 ? Math.round(d) : -1
}

function timelineLine(e: ShadowEvent): string | null {
  const p = e.payload as Record<string, unknown>
  switch (e.type) {
    case 'agent.step.started':
      return `Step: ${String(p.title ?? '')}`
    case 'agent.tool_call':
      return `Called ${String(p.tool ?? '')}`
    case 'agent.tool_result': {
      const r = e.payload as AgentToolResultPayload
      return `${r.tool}${r.is_error ? ' failed' : ''}: ${r.summary}`
    }
    case 'agent.status':
      return `Agent ${String(p.state ?? '')}${p.detail ? ` (${String(p.detail)})` : ''}`
    default:
      return null // messages and hypotheses are shown above or are not needed here
  }
}

// What the agent concluded, next to what the human wrote down. Only rendered once the
// server has released the run (after the blind label).
export function RunReveal({
  data,
  openedAt,
  humanMttrS,
  showData,
  onToggleData,
}: {
  data: ShadowResponse
  openedAt?: string
  humanMttrS: number
  showData: boolean
  onToggleData: () => void
}) {
  const { run, events } = data
  const label = data.room?.labelled?.payload as ShadowLabelledPayload | undefined
  const hyp = run.final_hypothesis
  const toFinal = secondsFrom(openedAt, run.final_hypothesis_at)
  const lines = events.map((e) => ({ e, text: timelineLine(e) })).filter((x) => x.text !== null)

  return (
    <div className="flex flex-col gap-4">
      <Card className="flex flex-col gap-2 px-3.5 py-3" role="region" aria-labelledby="shadow-answer-heading">
        <div className="flex flex-wrap items-center gap-2">
          <h2 id="shadow-answer-heading" className="m-0 text-sm font-medium">
            Agent’s conclusion
          </h2>
          <Badge variant={run.status === 'hypothesised' ? 'agent' : 'warning'}>{STATUS_LABEL[run.status]}</Badge>
          {hyp && <Badge variant="agent">{hyp.confidence} confidence</Badge>}
        </div>
        {hyp ? (
          <p className="m-0 max-w-[72ch] text-base">{hyp.text}</p>
        ) : (
          <p className="m-0 text-sm text-muted-foreground">The agent did not reach a hypothesis ({STATUS_LABEL[run.status]}).</p>
        )}
        <dl className="m-0 grid grid-cols-2 gap-x-6 gap-y-1 text-sm sm:grid-cols-4">
          <Stat label="Agent, final answer" value={formatSeconds(toFinal)} />
          <Stat label="Human, to resolve" value={formatSeconds(humanMttrS)} />
          <Stat label="Tool calls" value={String(run.tool_calls)} />
          <Stat label="Tokens" value={run.tokens > 0 ? run.tokens.toLocaleString() : 'not counted'} />
        </dl>
        {run.finished_after_close && (
          <p className="m-0 text-xs text-subtle-foreground">The agent finished after the room was closed.</p>
        )}
      </Card>

      {label && (
        <Card className="flex flex-col gap-1 px-3.5 py-3" role="region" aria-labelledby="shadow-label-heading">
          <h2 id="shadow-label-heading" className="m-0 text-sm font-medium">
            Your blind label
          </h2>
          <p className="m-0 text-sm">
            Layer <Badge variant="outline">{label.actual_layer}</Badge>
          </p>
          <ul className="m-0 flex list-disc flex-col gap-0.5 pl-5 text-sm">
            {label.root_cause_terms.map((t, i) => (
              <li key={`${i}-${t}`}>{t}</li>
            ))}
          </ul>
        </Card>
      )}

      <section aria-labelledby="shadow-run-heading" className="flex flex-col gap-2">
        <div className="flex items-center gap-2">
          <h2 id="shadow-run-heading" className="m-0 text-sm font-medium">
            What the agent did
          </h2>
          <Button type="button" variant="ghost" size="sm" className="ml-auto" aria-pressed={showData} onClick={onToggleData}>
            {showData ? 'Hide raw tool data' : 'Show raw tool data'}
          </Button>
        </div>
        {lines.length === 0 ? (
          <p className="m-0 text-sm text-muted-foreground">No steps were recorded.</p>
        ) : (
          <ol className="m-0 flex list-none flex-col gap-1 p-0 text-sm">
            {lines.map(({ e, text }) => (
              <li key={e.seq} className="flex flex-col gap-1">
                <span className="font-mono text-[12.5px]">{text}</span>
                {showData && e.type === 'agent.tool_result' && (e.payload as AgentToolResultPayload).data !== undefined && (
                  <pre className="m-0 max-h-60 overflow-auto rounded-md border border-border bg-muted p-2 font-mono text-xs">
                    {JSON.stringify((e.payload as AgentToolResultPayload).data, null, 2)}
                  </pre>
                )}
              </li>
            ))}
          </ol>
        )}
      </section>
    </div>
  )
}

function Stat({ label, value }: { label: string; value: string }) {
  return (
    <div className="flex flex-col">
      <dt className="text-xs text-subtle-foreground">{label}</dt>
      <dd className="m-0 font-medium tabular-nums">{value}</dd>
    </div>
  )
}

import { Check } from 'lucide-react'
import type { AgentStats } from '@/api/types'
import type { HypothesisState, InsightState, StepState } from '@/state/types'
import { useAgentStats } from '@/hooks/useAgentStats'
import { recordText } from '@/lib/insight'
import { Badge } from '@/components/ui/badge'
import { Card } from '@/components/ui/card'
import { PersonAvatar } from './PersonAvatar'

const AGENT = { kind: 'agent', id: 'agent', name: 'Agent' } as const

// "based on steps 1, 3, 4": step ids become the numbers the timeline shows.
function basedOn(ids: string[], steps: StepState[]): string {
  const nums = ids.map((id) => {
    const i = steps.findIndex((s) => s.id === id)
    return i >= 0 ? String(i + 1) : id
  })
  return `based on ${nums.length === 1 ? 'step' : 'steps'} ${nums.join(', ')}`
}

function recordTitle(stats: AgentStats): string {
  const rows = stats.by_confidence.map((b) => `${b.confidence} ${b.correct}/${b.runs}`)
  if (stats.median_time_to_first_hypothesis_s >= 0) {
    rows.push(`median first insight ${stats.median_time_to_first_hypothesis_s} s`)
  }
  return rows.join(' · ')
}

export function HypothesisCard({
  hypothesis,
  steps,
  pipelineId = '',
  insight,
}: {
  hypothesis: HypothesisState
  steps: StepState[]
  pipelineId?: string
  insight?: InsightState
}) {
  const stats = useAgentStats(pipelineId)
  const record = stats ? recordText(stats, hypothesis.confidence) : null
  const noRecord = stats !== null && stats.graded === 0
  const based = hypothesis.based_on_steps ?? []
  const firstS = insight?.time_to_first_hypothesis_s ?? -1
  return (
    <Card className="flex flex-col gap-2 px-3.5 py-3" role="region" aria-labelledby="hypothesis-heading">
      <div className="flex flex-wrap items-center gap-2">
        <PersonAvatar actor={AGENT} size={20} />
        <h2 id="hypothesis-heading" className="m-0 text-sm font-medium">
          Hypothesis
        </h2>
        <Badge variant="agent" title={noRecord ? 'no labelled rooms yet' : undefined}>
          {hypothesis.confidence} confidence
        </Badge>
        {record && stats && (
          <Badge variant="outline" title={recordTitle(stats)}>
            {record}
          </Badge>
        )}
        {hypothesis.confirmed && (
          <Badge variant="success">
            <Check className="size-3" aria-hidden="true" />
            confirmed
          </Badge>
        )}
        {(based.length > 0 || firstS >= 0) && (
          <span className="ml-auto flex flex-wrap gap-x-2 text-xs text-subtle-foreground">
            {firstS >= 0 && <span>first hypothesis in {firstS} s</span>}
            {based.length > 0 && <span>{basedOn(based, steps)}</span>}
          </span>
        )}
      </div>
      <p className="m-0 max-w-[72ch] text-base">{hypothesis.text}</p>
    </Card>
  )
}

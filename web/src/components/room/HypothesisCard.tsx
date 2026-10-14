import { Check } from 'lucide-react'
import type { HypothesisState, StepState } from '@/state/types'
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

export function HypothesisCard({ hypothesis, steps }: { hypothesis: HypothesisState; steps: StepState[] }) {
  const based = hypothesis.based_on_steps ?? []
  return (
    <Card className="flex flex-col gap-2 px-3.5 py-3" role="region" aria-labelledby="hypothesis-heading">
      <div className="flex flex-wrap items-center gap-2">
        <PersonAvatar actor={AGENT} size={20} />
        <h2 id="hypothesis-heading" className="m-0 text-sm font-medium">
          Hypothesis
        </h2>
        <Badge variant="agent">{hypothesis.confidence} confidence</Badge>
        {hypothesis.confirmed && (
          <Badge variant="success">
            <Check className="size-3" aria-hidden="true" />
            confirmed
          </Badge>
        )}
        {based.length > 0 && <span className="ml-auto text-xs text-subtle-foreground">{basedOn(based, steps)}</span>}
      </div>
      <p className="m-0 max-w-[72ch] text-base">{hypothesis.text}</p>
    </Card>
  )
}

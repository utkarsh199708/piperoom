import { Check, TriangleAlert } from 'lucide-react'
import type { HandoffSummary } from '@/api/types'
import { Badge } from '@/components/ui/badge'

/**
 * What the hand-off still fails after the agent's one correction pass (F3). Shown to the
 * recipient before Accept; never a block. A summary without the field (older event) shows
 * nothing; `violations: []` shows the green row. `detail` is agent text: a React text node.
 */
export function HandoffChecks({ summary }: { summary: Pick<HandoffSummary, 'violations'> }) {
  const vs = summary.violations
  if (!vs) return null
  if (vs.length === 0) {
    return (
      <p className="m-0 flex items-center gap-1.5 text-xs text-success-foreground" data-testid="handoff-checks">
        <Check className="size-3.5 shrink-0" aria-hidden="true" />
        Checked against the room log · no issues
      </p>
    )
  }
  return (
    <div className="flex flex-col gap-1" data-testid="handoff-checks">
      <h4 className="text-xs font-medium text-muted-foreground">Checks</h4>
      <ul className="m-0 flex list-none flex-col gap-1.5 p-0">
        {vs.map((v, i) => (
          <li key={i} className="flex items-start gap-2">
            <TriangleAlert className="mt-0.5 size-3.5 shrink-0 text-warning-foreground" aria-hidden="true" />
            <Badge mono title={v.field}>
              {v.rule}
            </Badge>
            <span className="min-w-0 break-words text-sm">{v.detail}</span>
          </li>
        ))}
      </ul>
    </div>
  )
}

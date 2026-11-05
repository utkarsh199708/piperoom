import { Check, TriangleAlert } from 'lucide-react'
import type { HandoffSummary } from '@/api/types'

// The server's rule names (internal/agent/handoff_check.go) in words. An unknown rule reads with spaces.
const RULE_LABEL: Record<string, string> = {
  empty_field: 'Section left empty',
  list_too_long: 'List too long',
  string_too_long: 'Text too long',
  unknown_reference: 'Cites something not in the room',
  unknown_identifier: 'Name not found in the room log',
  unknown_number: 'Number not found in the room log',
  open_proposal_missing: 'Open fix not mentioned',
  pending_layer_missing: 'Pending approval not mentioned',
  failed_action_missing: 'Failed fix step not mentioned',
  interrupted_run_missing: 'Interrupted fix not mentioned',
}

// Summary fields as the card titles them; "open[2]" is the third item of "Still open".
const FIELD_LABEL: Record<string, string> = {
  what_broke: 'What broke',
  why: 'Why',
  done: 'Done',
  ruled_out: 'Ruled out',
  open: 'Still open',
  risks: 'Risks',
}

function ruleLabel(rule: string): string {
  const words = RULE_LABEL[rule] ?? rule.replace(/_/g, ' ')
  return words.charAt(0).toUpperCase() + words.slice(1)
}

function fieldLabel(field: string): string {
  const m = /^([a-z_]+)(?:\[(\d+)\])?$/.exec(field)
  if (!m || !FIELD_LABEL[m[1]]) return ''
  return m[2] !== undefined ? `${FIELD_LABEL[m[1]]}, item ${Number(m[2]) + 1}` : FIELD_LABEL[m[1]]
}

/**
 * What the hand-off still fails after the agent's one correction pass (F3). Shown to the
 * recipient before Accept; never a block. A summary without the field (older event) shows
 * nothing; `violations: []` shows the green row. `detail` is agent text: a React text node.
 * Rules read as words; the rule name and field stay in the tooltip for anyone grouping reports.
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
        {vs.map((v, i) => {
          const where = fieldLabel(v.field)
          return (
            <li key={i} className="flex items-start gap-2 text-sm">
              <TriangleAlert className="mt-0.5 size-3.5 shrink-0 text-warning-foreground" aria-hidden="true" />
              <span className="min-w-0 break-words">
                <span className="font-medium" title={`${v.rule} · ${v.field}`}>
                  {ruleLabel(v.rule)}
                </span>
                {where && <span className="text-subtle-foreground"> · {where}</span>}
                <span className="block text-muted-foreground">{v.detail}</span>
              </span>
            </li>
          )
        })}
      </ul>
    </div>
  )
}

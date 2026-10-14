import type { ChecklistStepDonePayload } from '@/api/types'
import { checklistKey } from '@/state/types'
import type { ChecklistTick } from '@/state/types'
import { runbookHits } from '@/lib/f8'
import { timeAgo } from '@/lib/time'
import { Button } from '@/components/ui/button'

/** What the room lets this person do with a checklist. */
export interface ChecklistAccess {
  /** Tick boxes: a member of an open room. A viewer sees disabled boxes. */
  canTick: boolean
  /** Offer "Ask the agent": a member of an open room (the composer sends it as a steer or a suggestion). */
  canAsk: boolean
}

// The items of a runbook_lookup result as checkboxes. Items are plain text from an embedded
// runbook (a trusted source, still rendered as text). The ticked state is the room's
// (checklist.step_done), so every tab shows who did what.
export function Checklist({
  stepId,
  toolUseId,
  data,
  done,
  access,
  onTick,
  onAsk,
  now = Date.now(),
}: {
  stepId: string
  toolUseId: string
  data: unknown
  done: Record<string, ChecklistTick>
  access: ChecklistAccess
  onTick: (p: ChecklistStepDonePayload) => void
  onAsk: (text: string) => void
  now?: number
}) {
  const hits = runbookHits(data)
  if (!hits.some((h) => h.items.length > 0)) return null
  return (
    <div className="flex flex-col gap-3">
      {hits.map((hit, hi) =>
        hit.items.length === 0 ? null : (
          <section key={hi} aria-label={`Checklist: ${hit.section || `runbook ${hi + 1}`}`} className="flex flex-col gap-1">
            <h3 className="m-0 text-xs font-medium text-muted-foreground">{hit.section || 'Runbook steps'}</h3>
            <ul className="m-0 flex list-none flex-col p-0">
              {hit.items.map((item, ii) => {
                const tick = done[checklistKey(toolUseId, hi, ii)]
                const id = `chk-${toolUseId}-${hi}-${ii}`
                return (
                  <li key={ii} className="flex min-h-11 flex-wrap items-center gap-x-3 gap-y-1">
                    <label htmlFor={id} className="flex min-w-0 flex-1 items-center gap-2.5 py-1">
                      <input
                        id={id}
                        type="checkbox"
                        className="size-4 shrink-0 accent-[hsl(var(--primary))]"
                        checked={tick !== undefined}
                        disabled={!access.canTick}
                        onChange={(e) =>
                          onTick({ step_id: stepId, tool_use_id: toolUseId, hit: hi, item: ii, done: e.target.checked })
                        }
                      />
                      <span className="min-w-0 break-words">{item}</span>
                    </label>
                    {tick && (
                      <span className="text-xs text-subtle-foreground">
                        {tick.by.name || tick.by.id} · {timeAgo(tick.at, now)}
                      </span>
                    )}
                    {access.canAsk && (
                      <Button
                        type="button"
                        size="sm"
                        variant="ghost"
                        aria-label={`Ask the agent to do step ${ii + 1}`}
                        onClick={() => onAsk(`Do step ${ii + 1} of "${hit.section || 'the runbook'}": ${item}`)}
                      >
                        Ask the agent
                      </Button>
                    )}
                  </li>
                )
              })}
            </ul>
          </section>
        ),
      )}
    </div>
  )
}

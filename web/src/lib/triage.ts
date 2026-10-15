// The triage pass's first look (F6): helpers shared by the room header card and the list row.
import type { TriageSummary } from '../api/types'
import type { StepState, TriageState } from '../state/types'

/** DOM id of a step row in the timeline, the target of "based on steps" chips. */
export function stepAnchor(stepId: string): string {
  return `step-${stepId}`
}

/** A failed first look stays visible this long, then the card hides (ms). */
export const TRIAGE_FAILED_SHOW_MS = 60_000
/** A room this young with no first look yet shows the "in progress" shimmer in the list (ms). */
export const TRIAGE_PENDING_MS = 2 * 60_000

/** The reason codes of a failed first look, in words. */
const REASON_TEXT: Record<string, string> = {
  timeout: 'timed out',
  model_error: 'model error',
  budget: 'budget spent',
  refused: 'model refused',
  no_tools: 'no tools',
  off: 'turned off',
}

export function triageReasonText(reason: string | undefined): string {
  if (!reason) return 'unknown reason'
  return REASON_TEXT[reason] ?? reason
}

/** The step numbers (1-based, timeline order) the first look rests on; unknown ids are dropped. */
export function triageStepNumbers(triage: TriageState, steps: StepState[]): Array<{ id: string; n: number }> {
  const out: Array<{ id: string; n: number }> = []
  for (const id of triage.evidence_refs) {
    const i = steps.findIndex((s) => s.id === id)
    if (i >= 0) out.push({ id, n: i + 1 })
  }
  return out
}

/** What the list row's second line shows for a room. */
export type TriageLine =
  | { kind: 'headline'; headline: string; layer?: string }
  | { kind: 'pending' }
  | { kind: 'none' }

export function triageLine(
  room: { status: string; opened_at: string; triage?: TriageSummary | null },
  now: number,
): TriageLine {
  const t = room.triage
  if (t) {
    if (t.status === 'ok' && t.headline) return { kind: 'headline', headline: t.headline, layer: t.suspected_layer || undefined }
    return { kind: 'none' }
  }
  if (room.status !== 'open') return { kind: 'none' }
  const opened = Date.parse(room.opened_at)
  if (Number.isFinite(opened) && now - opened < TRIAGE_PENDING_MS) return { kind: 'pending' }
  return { kind: 'none' }
}

/**
 * Scrolls to and briefly highlights a step of the room. Step links are not `#step-` hrefs: the app
 * uses HashRouter, where a fragment is the route, so following one leaves the room (QA F19).
 */
export function goToStep(stepId: string): void {
  const el = document.getElementById(stepAnchor(stepId))
  if (!el) return
  el.scrollIntoView?.({ block: 'start', behavior: 'smooth' })
  el.setAttribute('tabindex', '-1')
  el.focus({ preventScroll: true })
  el.classList.add('ring-2', 'ring-ring')
  window.setTimeout(() => el.classList.remove('ring-2', 'ring-ring'), 2000)
}

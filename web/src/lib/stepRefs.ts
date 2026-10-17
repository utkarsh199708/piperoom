// Agent prose names steps by their ids (s_ and a ULID). These helpers find those
// ids so the UI can show "step N" links instead (G11). Every other character of
// the text is kept as it was.
import type { StepState } from '../state/types'

/**
 * A step id as the server writes it: "s_" and 26 Crockford base32 characters (no I, L, O or U).
 * The lookaround guards keep it from matching inside a longer word.
 */
const STEP_ID = /(?<![A-Za-z0-9_])s_[0-9A-HJKMNP-TV-Z]{26}(?![A-Za-z0-9_])/g

/** Step id -> its 1-based place in the room's timeline. */
export type StepNumbers = ReadonlyMap<string, number>

/** The 1-based number of each step, in the order the room started them. */
export function stepNumbers(steps: readonly Pick<StepState, 'id'>[]): StepNumbers {
  return new Map(steps.map((s, i) => [s.id, i + 1]))
}

/** A run of plain text, or a step id of the room with its number. */
export type TextPart = string | { id: string; n: number }

/**
 * Splits text at the step ids of the room. A matched id that is not a step of the
 * room stays in the plain text, so an unknown id reads as it always did.
 */
export function splitStepRefs(text: string, numbers: StepNumbers): TextPart[] {
  const out: TextPart[] = []
  let last = 0
  for (const m of text.matchAll(STEP_ID)) {
    const id = m[0]
    const n = numbers.get(id)
    if (n === undefined) continue
    const at = m.index ?? 0
    if (at > last) out.push(text.slice(last, at))
    out.push({ id, n })
    last = at + id.length
  }
  if (last < text.length) out.push(text.slice(last))
  return out
}

/**
 * A proposal id as the server writes it: "p_" and 26 Crockford base32 characters.
 * Same guards as STEP_ID.
 */
const PROPOSAL_ID = /(?<![A-Za-z0-9_])p_[0-9A-HJKMNP-TV-Z]{26}(?![A-Za-z0-9_])/g

/** A run of plain text, or a proposal id of the room with its 1-based place. */
export type ProposalTextPart = string | { proposalId: string; n: number }

/** Splits text at the proposal ids of the room (T12); an id the room does not know stays text. */
export function splitProposalRefs(text: string, ids: readonly string[]): ProposalTextPart[] {
  const out: ProposalTextPart[] = []
  let last = 0
  for (const m of text.matchAll(PROPOSAL_ID)) {
    const i = ids.indexOf(m[0])
    if (i < 0) continue
    const at = m.index ?? 0
    if (at > last) out.push(text.slice(last, at))
    out.push({ proposalId: m[0], n: i + 1 })
    last = at + m[0].length
  }
  if (last < text.length) out.push(text.slice(last))
  return out
}

import type { LabelInput } from '../api/types'
import type { RoomState } from '../state/types'

export const MAX_CAUSE = 500
export const MAX_FIX = 500

export const LABEL_LAYERS = ['postgres', 'kafka-connect', 'schema-registry', 'kafka', 'consumer', 'warehouse'] as const

// Words that point at a layer, matched against the final hypothesis to pre-fill the layer select.
const LAYER_WORDS: Record<(typeof LABEL_LAYERS)[number], RegExp> = {
  postgres: /\b(postgres|wal|replication slot|publication|ddl)\b/i,
  'kafka-connect': /\b(connector|kafka[- ]connect|debezium|task)\b/i,
  'schema-registry': /\b(schema|registry|compatib\w*|avro|subject)\b/i,
  kafka: /\b(broker|partition|topic|offset|lag)\b/i,
  consumer: /\b(consumer|sink|group)\b/i,
  warehouse: /\b(warehouse|snowflake|bigquery|redshift)\b/i,
}

/** The one layer whose words the text uses; '' when none or more than one matches. */
export function guessLayer(text: string): string {
  const hits = LABEL_LAYERS.filter((l) => LAYER_WORDS[l].test(text))
  return hits.length === 1 ? hits[0] : ''
}

/**
 * The proposal "fix that worked" starts from: the newest one whose verification reported recovered
 * (what the close nudge's resolution names), else the last executed proposal whose step ran ok.
 * `hypothesis` is the hypothesis text that was current for that proposal, when the log recorded one.
 */
export function executedFix(state: RoomState): { id: string; title: string; hypothesis?: string } | null {
  const pick = (p: RoomState['proposals'][number]) => ({ id: p.proposal_id, title: p.title, hypothesis: p.hypothesis_text })
  for (let i = state.proposals.length - 1; i >= 0; i--) {
    const p = state.proposals[i]
    if (p.verification?.recovered === true) return pick(p)
  }
  for (let i = state.proposals.length - 1; i >= 0; i--) {
    const p = state.proposals[i]
    if (p.executed.some((e) => e.ok)) return pick(p)
  }
  return null
}

export type Helped = 'yes' | 'no' | 'skip'

export interface LabelDraft {
  layer: string
  cause: string
  fix: string
  helped: Helped
}

/** The form's starting values: the hypothesis text (cause, layer) and the executed proposal (fix). */
export function initialDraft(state: RoomState): LabelDraft {
  const fix = executedFix(state)
  const recovered = state.proposals.some((p) => p.verification?.recovered === true)
  // The cause is the hypothesis behind the recovering fix; without a recovery, or without a
  // recorded hypothesis, it is the latest one.
  const hyp = (recovered ? fix?.hypothesis : undefined) || state.hypothesis?.text || ''
  return {
    layer: guessLayer(hyp),
    cause: hyp.slice(0, MAX_CAUSE),
    fix: (fix?.title ?? '').slice(0, MAX_FIX),
    helped: 'skip',
  }
}

/** The request body for a draft, or null when it has no cause (nothing to label). */
export function draftToInput(state: RoomState, d: LabelDraft): LabelInput | null {
  const cause = d.cause.trim()
  if (!cause) return null
  const fix = executedFix(state)
  const fixText = d.fix.trim()
  const input: LabelInput = { cause }
  if (d.layer) input.actual_layer = d.layer
  if (fixText) {
    input.fix_that_worked = fixText
    // The id only goes along while the text is still the proposal's own title.
    if (fix && fixText === fix.title.trim()) input.fix_proposal_id = fix.id
  }
  if (d.helped !== 'skip') input.agent_helped = d.helped === 'yes'
  return input
}


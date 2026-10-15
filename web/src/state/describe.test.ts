import { describe, expect, it } from 'vitest'
import type { Event } from '../api/types'
import { clip, describeEvent } from './describe'
import type { DescribeContext } from './describe'

const arjun = { kind: 'human' as const, id: 'arjun', name: 'Arjun' }
const agent = { kind: 'agent' as const, id: 'agent', name: 'Agent' }
const system = { kind: 'system' as const, id: 'system', name: 'System' }
const ctx: DescribeContext = {
  driverBefore: 'arjun',
  stepTitles: { s_aaaaaaaa: 'Check the connector' },
  proposalTitles: { p_01M4ABCDEFGHJKMNPQRSTVWXYZ: 'Set compat' },
  stepIndex: { s_aaaaaaaa: 2 },
  suggesters: { g_bbbbbbbb: 'Kabir' },
  confidenceBefore: 'low',
  runbookItem: () => null,
}
const ev = (type: Event['type'], payload: unknown, actor: Event['actor'] = arjun): Event => ({
  room_id: 'r1',
  seq: 1,
  type,
  actor,
  payload,
  at: '2026-10-11T10:00:00Z',
})
const ID = /\b[pgsh]_[0-9A-Za-z]{6,}\b/

describe('describeEvent', () => {
  const longText = 'x'.repeat(300)
  const cases: [string, Event, Partial<ReturnType<typeof describeEvent>>][] = [
    ['room.created human', ev('room.created', { title: 'Lag', severity: 'sev2', pipeline_id: 'orders' }), { text: 'Room opened by Arjun', kind: 'system' }],
    ['room.created system', ev('room.created', { title: 'Lag', severity: 'sev2', pipeline_id: 'orders' }, system), { text: 'Room opened — Lag', kind: 'system' }],
    ['agent.status', ev('agent.status', { state: 'waiting_approval', detail: 'one proposal open' }, agent), { text: 'Agent waiting for approval', detail: 'one proposal open', kind: 'system' }],
    ['agent.step.started', ev('agent.step.started', { step_id: 's_aaaaaaaa', title: 'Check the connector' }, agent), { text: 'Agent started step 2 · Check the connector', kind: 'step' }],
    ['agent.tool_call', ev('agent.tool_call', { step_id: 's_aaaaaaaa', tool_use_id: 'tu', tool: 'connect_status', input: { connector: 'pg' } }, agent), { text: 'Step 2 · ran {tool}', tool: 'connect_status', detail: 'connector=pg', kind: 'system' }],
    ['agent.tool_result ok', ev('agent.tool_result', { step_id: 's_aaaaaaaa', tool_use_id: 'tu', tool: 'connect_status', summary: 'fine', is_error: false, took_ms: 1500 }, agent), { text: 'Step 2 done · 1.5s', detail: 'fine', kind: 'step' }],
    ['agent.tool_result failed', ev('agent.tool_result', { step_id: 's_aaaaaaaa', tool_use_id: 'tu', tool: 'connect_status', summary: longText, is_error: true, took_ms: 5 }, agent), { text: 'Step 2 · {tool} failed', kind: 'step' }],
    ['agent.message', ev('agent.message', { text: 'hello' }, agent), { text: 'Agent: hello', kind: 'step' }],
    ['agent.message reply', ev('agent.message', { text: 'hello', reply_to: 'c_1', step_id: 's_aaaaaaaa' }, agent), { text: 'Agent replied on step 2', kind: 'step' }],
    ['agent.hypothesis raised', ev('agent.hypothesis', { text: 'it broke', based_on_steps: [], confidence: 'high', confirmed: false }, agent), { text: 'Hypothesis raised to high confidence', detail: 'it broke', kind: 'step' }],
    ['comment on step', ev('comment.added', { comment_id: 'c_1', step_id: 's_aaaaaaaa', text: 'look', mentions_agent: false }), { text: 'Arjun commented on step 2', detail: 'look', kind: 'human' }],
    ['comment unknown step', ev('comment.added', { comment_id: 'c_1', step_id: 's_zzzzzzzz', text: 'look', mentions_agent: false }), { text: 'Arjun commented on a step', kind: 'human' }],
    ['steer.suggested', ev('steer.suggested', { suggestion_id: 'g_bbbbbbbb', text: 'try X' }), { text: 'Arjun suggested: "try X"', kind: 'human' }],
    ['steer.sent', ev('steer.sent', { text: 'go' }), { text: 'Arjun steered: "go"', kind: 'human' }],
    ['steer.sent suggestion', ev('steer.sent', { suggestion_id: 'g_bbbbbbbb', text: 'try X' }), { text: "Arjun sent Kabir's suggestion", kind: 'human' }],
    ['steer.declined', ev('steer.declined', { suggestion_id: 'g_bbbbbbbb', reason: 'not now' }), { text: "Arjun declined Kabir's suggestion", detail: 'not now', kind: 'human' }],
    ['control.requested', ev('control.requested', {}), { text: 'Arjun asked for control', kind: 'control' }],
    ['control.granted renew', ev('control.granted', { to: arjun, lease_until: 'x', stages: [] }), { text: 'Arjun renewed the lease', kind: 'system' }],
    ['control.granted take', ev('control.granted', { to: { id: 'kabir', name: 'Kabir' }, lease_until: 'x' }, { kind: 'human', id: 'kabir', name: 'Kabir' }), { text: 'Kabir took control', kind: 'control' }],
    ['control.granted hand', ev('control.granted', { to: { id: 'kabir', name: 'Kabir' }, lease_until: 'x' }), { text: 'Arjun handed control to Kabir', kind: 'control' }],
    ['control.lease_expired', ev('control.lease_expired', { driver: arjun, lease_until: 'x' }, system), { text: "Arjun's lease expired · no one is driving", kind: 'control' }],
    ['proposal.created', ev('proposal.created', { proposal_id: 'p_01M4ABCDEFGHJKMNPQRSTVWXYZ', title: 'Set compat', steps: [], risk: 'low', rollback: '', required_layers: [] }, agent), { text: 'Agent proposed a fix · Set compat', kind: 'decision' }],
    ['approval.given', ev('approval.given', { proposal_id: 'p_1', layer: 'kafka-connect' }), { text: 'Arjun approved kafka-connect', kind: 'decision' }],
    ['approval.rejected', ev('approval.rejected', { proposal_id: 'p_1', layer: 'kafka-connect', reason: 'risky' }), { text: 'Arjun rejected kafka-connect', detail: 'risky', kind: 'decision' }],
    ['action ok', ev('action.executed', { proposal_id: 'p_1', step_index: 0, ok: true, output: 'done' }, system), { text: 'Step 1 ran', detail: 'done', kind: 'decision' }],
    ['action failed', ev('action.executed', { proposal_id: 'p_1', step_index: 1, ok: false, output: 'boom' }, system), { text: 'Step 2 failed', kind: 'decision' }],
    ['handoff.requested', ev('handoff.requested', { request_id: 'h_1', to: { id: 'kabir', name: 'Kabir' }, stay_as_watcher: true, expires_at: 'x' }), { text: 'Arjun asked for a hand-off to Kabir', kind: 'control' }],
    ['handoff.failed', ev('handoff.failed', { request_id: 'h_1', reason: 'x' }, system), { text: 'The hand-off could not be written', kind: 'control' }],
    ['handoff.created', ev('handoff.created', { handoff_id: 'h_1', to: { id: 'kabir', name: 'Kabir' }, summary: {}, stay_as_watcher: false }), { text: 'Arjun started a hand-off to Kabir', kind: 'control' }],
    ['handoff.accepted', ev('handoff.accepted', { handoff_id: 'h_1' }), { text: 'Arjun accepted the hand-off', kind: 'control' }],
    ['trace.pinned', ev('trace.pinned', { table: 'orders', key: '42', stages: [] }), { text: 'Arjun traced orders · 42', kind: 'human' }],
    ['room.closed', ev('room.closed', { resolution: 'fixed' }), { text: 'Arjun closed the room', detail: 'fixed', kind: 'decision' }],
    ['room.labelled', ev('room.labelled', { cause: 'schema change', fix_that_worked: 'restart task' }), { text: 'Arjun labelled the room: cause schema change · fix restart task', kind: 'human' }],
    ['checklist tick unknown item', ev('checklist.step_done', { tool_use_id: 'tu', hit: 0, item: 1, done: true }), { text: 'Arjun ticked step 2 of a runbook', kind: 'human' }],
    ['hypothesis.verdict', ev('hypothesis.verdict', { verdict: 'disputed', note: 'DDL never ran', hypothesis_seq: 1 }), { text: 'Arjun disputed the hypothesis: DDL never ran', kind: 'human' }],
    ['approval.nudged', ev('approval.nudged', { proposal_id: 'p_01M4ABCDEFGHJKMNPQRSTVWXYZ', layer: 'kafka-connect' }), { text: 'Arjun nudged kafka-connect owners', kind: 'human' }],
    ['unknown', ev('agent.usage' as Event['type'], {}, agent), { text: 'agent.usage', kind: 'system' }],
  ]
  it.each(cases)('%s', (_n, e, want) => {
    const d = describeEvent(e, ctx)
    expect(d).toMatchObject(want)
    expect(d.text).not.toMatch(ID)
    expect(d.detail ?? '').not.toMatch(ID)
  })

  it('clips rune-safe and keeps detail within 200 characters', () => {
    const d = describeEvent(ev('agent.tool_result', { step_id: 's_aaaaaaaa', tool_use_id: 'tu', tool: 'x', summary: '😀'.repeat(300), is_error: false, took_ms: 1 }, agent), ctx)
    expect(Array.from(d.detail ?? '')).toHaveLength(200)
    expect(clip('😀'.repeat(10), 5)).toBe('😀😀😀😀…')
    expect(describeEvent(ev('steer.suggested', { suggestion_id: 'g', text: 'y'.repeat(200) }), ctx).text).toBe(`Arjun suggested: "${'y'.repeat(79)}…"`)
  })

  it('keeps an unsafe tool name out of <code> and ids out of free text', () => {
    const d = describeEvent(ev('agent.tool_call', { step_id: 's_aaaaaaaa', tool_use_id: 'tu', tool: '<b>Bad</b>', input: {} }, agent), ctx)
    expect(d.tool).toBeUndefined()
    expect(d.text).not.toContain('{tool}')
    const c = describeEvent(ev('comment.added', { comment_id: 'c', text: 'see p_01M4ABCDEFGHJKMNPQRSTVWXYZ now', mentions_agent: false }), ctx)
    expect(c.detail).toBe('see … now')
  })
})

describe('describeEvent agent.verification', () => {
  const v = (p: object) => describeEvent(ev('agent.verification', { proposal_id: 'p_1', checks: [], ...p }, agent), ctx)

  // A repeat (same last-step result delivered again) must not announce the recovery a second time.
  it('keeps a repeat out of the default feed', () => {
    expect(v({ recovered: true, verdict: 'already verified', repeat: true })).toEqual({
      text: 'Verification already recorded',
      kind: 'system',
    })
  })

  it.each([
    [true, 'recovered: every layer check passed', 'Verification: recovered'],
    [false, 'NOT recovered: a layer check did not pass.', 'Verification: not recovered'],
    [false, 'not verified: a step outcome is unknown; recovery is not claimed.', 'Verification: not verified'],
    [false, 'inconclusive: no source changes observed', 'Verification: inconclusive'],
  ])('recovered=%s %j reads %j', (recovered, verdict, text) => {
    expect(v({ recovered, verdict })).toEqual({ text, kind: 'decision' })
  })
})

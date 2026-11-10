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
    ['setup.secret_set', ev('setup.secret_set', { name: 'db_pass' }, { kind: 'human', id: 'priya', name: 'priya' }), { text: 'priya set secret db_pass', kind: 'system' }],
    ['setup.secret_set never a value', ev('setup.secret_set', { name: 'db_pass', value: 'hunter2', secret: 'hunter2' }, { kind: 'human', id: 'priya', name: 'priya' }), { text: 'priya set secret db_pass' }],
    ['setup.target_registered', ev('setup.target_registered', { kind: 'postgres', address: 'pg.example.test:5432', class: 'shared' }), { text: 'Target pg.example.test:5432 registered as shared', kind: 'system' }],
    ['setup.target_classed', ev('setup.target_classed', { target: 'pg.example.test:5432', class: 'shared', reason: 'matches a shared host', by: 'server' }, system), { text: 'Target pg.example.test:5432 classed shared', detail: 'matches a shared host', kind: 'system' }],
    ['setup.pipeline_extended', ev('setup.pipeline_extended', { add_connectors: ['c1'], add_tables: ['a', 'b'] }), { text: 'Pipeline extended with 1 connector, 2 tables', kind: 'system' }],
    ['setup.pipeline_extended empty', ev('setup.pipeline_extended', {}), { text: 'Pipeline extended', kind: 'system' }],
    ['setup.maintenance_window', ev('setup.maintenance_window', { start: '2026-10-11T02:00:00Z', end: '2026-10-11T04:00:00Z' }), { text: 'Maintenance window set, 2026-10-11T02:00:00Z to 2026-10-11T04:00:00Z', kind: 'system' }],
    ['setup.runner_attached', ev('setup.runner_attached', { runner_label: 'qa6', os: 'linux', arch: 'amd64' }, system), { text: 'Runner qa6 attached', kind: 'system' }],
    ['setup.runner_lost', ev('setup.runner_lost', { runner_label: 'qa6', last_seen: 'x' }, system), { text: 'Runner qa6 lost', kind: 'system' }],
    ['setup.pairing ok', ev('setup.pairing', { outcome: 'ok', source: 'x', runner_label: 'qa6' }, system), { text: 'Paired with runner qa6', kind: 'system' }],
    ['setup.pairing bad_code', ev('setup.pairing', { outcome: 'bad_code', source: 'x' }, system), { text: 'Pairing failed: wrong code' }],
    ['setup.pairing expired', ev('setup.pairing', { outcome: 'expired', source: 'x' }, system), { text: 'Pairing failed: the code expired' }],
    ['setup.pairing rate_limited', ev('setup.pairing', { outcome: 'rate_limited', source: 'x' }, system), { text: 'Pairing paused: too many attempts' }],
    ['setup.pairing replaced', ev('setup.pairing', { outcome: 'replaced', source: 'x', runner_label: 'qa7' }, system), { text: 'Replaced the lost runner with runner qa7' }],
    ['setup.pairing refused', ev('setup.pairing', { outcome: 'refused', source: 'x' }, system), { text: 'Pairing refused' }],
    ['setup.runner_refused', ev('setup.runner_refused', { op: 'psql.exec', reason: 'not_in_catalogue' }, system), { text: 'Runner refused to run psql.exec', detail: 'not_in_catalogue', kind: 'system' }],
    ['setup.plan', ev('setup.plan', { plan_id: 'x', phases: [{ name: 'a', steps: [{}, {}] }, { name: 'b', steps: [{}] }] }, agent), { text: 'Agent proposed a setup plan (3 steps)', kind: 'step' }],
    ['setup.plan superseding', ev('setup.plan', { plan_id: 'y', supersedes: 'x', phases: [{ name: 'a', steps: [{}] }] }, agent), { text: 'Agent replaced its setup plan (1 step)' }],
    ['setup.credentials_registered names only', ev('setup.credentials_registered', { pipeline_id: 'orders', names: ['db_pass', 'api_key'], runner_label: 'qa6', values: ['hunter2'] }, system), { text: '2 read credentials registered', kind: 'system' }],
    ['setup.verified all', ev('setup.verified', { plan_id: 'x', nonce_hash: 'h', rungs: [{ name: 'a', ok: true }, { name: 'b', ok: true }] }, system), { text: 'Setup verified: 2 of 2 checks passed', kind: 'decision' }],
    ['setup.verified partial', ev('setup.verified', { plan_id: 'x', nonce_hash: 'h', rungs: [{ name: 'a', ok: true }, { name: 'b', ok: false }] }, system), { text: 'Setup not verified: 1 of 2 checks passed' }],
    ['setup.awaiting_change', ev('setup.awaiting_change', { table: 'public.orders', example_sql: 'UPDATE x' }, system), { text: 'Waiting for a change to public.orders', kind: 'system' }],
    ['setup.blocked', ev('setup.blocked', { reason: 'budget', question: 'Raise the budget?' }, agent), { text: 'Setup paused: the token budget was reached', detail: 'Raise the budget?', kind: 'system' }],
    ['setup.verification_requested', ev('setup.verification_requested' as Event['type'], {}, agent), { text: 'Agent asked to verify the setup', kind: 'step' }],
  ]
  it.each(cases)('%s', (_n, e, want) => {
    const d = describeEvent(e, ctx)
    expect(d).toMatchObject(want)
    expect(d.text).not.toMatch(ID)
    expect(d.detail ?? '').not.toMatch(ID)
  })

  // Every setup.* type in internal/events/events.go; none may fall through to the raw type name.
  const SETUP_TYPES = [
    'setup.pairing', 'setup.runner_attached', 'setup.runner_lost', 'setup.runner_refused', 'setup.plan',
    'setup.target_registered', 'setup.target_classed', 'setup.pipeline_extended', 'setup.secret_set',
    'setup.credentials_registered', 'setup.maintenance_window', 'setup.verified', 'setup.awaiting_change',
    'setup.blocked', 'setup.verification_requested',
  ]
  it.each(SETUP_TYPES)('%s never renders as its raw type name', (type) => {
    for (const payload of [{}, { outcome: 'ok', name: 'n', rungs: [], phases: [], names: [] }]) {
      const d = describeEvent(ev(type as Event['type'], payload, system), ctx)
      expect(d.text).not.toContain(type)
      expect(d.text).not.toMatch(/setup\.[a-z_]+/)
      expect(d.text.trim()).not.toBe('')
    }
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

  // X2: a recovery proposal's verdict reads by its counted outcome (partial is not "not recovered").
  it.each([
    [{ recovered: false, outcome: 'partial', before: 4, after: 1, unit: 'rows' }, 'Verification: partially recovered'],
    [{ recovered: true, outcome: 'recovered', before: 4, after: 0, unit: 'rows' }, 'Verification: recovered'],
    [{ recovered: false, outcome: 'unverifiable' }, 'Verification: could not verify'],
    // Counts back to 0 but a layer check failed: the agent says NOT recovered, so the feed does too.
    [{ recovered: false, outcome: 'recovered', before: 4, after: 0, unit: 'rows' }, 'Verification: not recovered: a check failed'],
  ])('%j reads %j', (p, text) => {
    expect(v({ verdict: 'v', ...p })).toEqual({ text, kind: 'decision' })
  })
})

describe('describeEvent agent.recovery_check (X2)', () => {
  it('names the poll and the gap; no ids reach the text', () => {
    const d = describeEvent(
      ev('agent.recovery_check', { proposal_id: 'p_01M4ABCDEFGHJKMNPQRSTVWXYZ', step_index: 0, poll: 2, max_polls: 8, before: 412, after: 120, unit: 'rows' }, agent),
      ctx,
    )
    expect(d).toEqual({ text: 'Recovery checking (2 of 8) · 412 missing → 120 rows', kind: 'system' })
    expect(d.text).not.toMatch(ID)
  })
})

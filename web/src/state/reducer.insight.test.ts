import { describe, expect, it } from 'vitest'
import type { Event } from '../api/types'
import { FIXTURE_ROOM_ID, fixtureEvents } from '../fixtures/player'
import { applyEvent } from './reducer'
import { emptyRoomState } from './types'

// Same table as internal/room/snapshot_insight_test.go: the live fold and the
// server snapshot follow one set of rules.
const agent = { kind: 'agent' as const, id: 'agent', name: 'Agent' }
const t0 = Date.parse('2026-10-14T10:00:00Z')
const at = (ms: number) => new Date(t0 + ms).toISOString()
const ev = (seq: number, type: Event['type'], payload: unknown, ms = 0): Event => ({
  room_id: 'r1',
  seq,
  type,
  actor: agent,
  payload,
  at: at(ms),
})
const usage = (cost?: number) => ({
  model: 'm',
  role: 'main',
  input_tokens: 100,
  cache_creation_input_tokens: 10,
  cache_read_input_tokens: 20,
  output_tokens: 5,
  latency_ms: 700,
  ...(cost === undefined ? {} : { cost_usd: cost }),
})
const opened = (shadow = false) =>
  applyEvent(
    emptyRoomState('r1'),
    ev(1, 'room.created', { title: 't', severity: 'sev2', pipeline_id: 'orders', shadow }),
  )

describe('insight fold', () => {
  it('starts empty', () => {
    const i = opened().insight
    expect(i.time_to_first_hypothesis_s).toBe(-1)
    expect(i.first_hypothesis_at).toBeNull()
    expect(i.usage.cost_usd).toBeNull()
  })

  it('counts steps, tool calls and tool errors', () => {
    let s = opened()
    s = applyEvent(s, ev(2, 'agent.step.started', { step_id: 's_1', title: 'x' }))
    s = applyEvent(s, ev(3, 'agent.tool_call', { step_id: 's_1', tool_use_id: 'u1', tool: 'x', input: {} }))
    s = applyEvent(s, ev(4, 'agent.tool_result', { step_id: 's_1', tool_use_id: 'u1', tool: 'x', summary: '', is_error: true }))
    s = applyEvent(s, ev(5, 'agent.tool_result', { step_id: 's_1', tool_use_id: 'u1', tool: 'x', summary: '', is_error: false }))
    expect([s.insight.steps, s.insight.tool_calls, s.insight.tool_errors]).toEqual([1, 1, 1])
  })

  it('keeps the first hypothesis time, floors seconds, never negative', () => {
    let s = opened()
    s = applyEvent(s, ev(2, 'agent.hypothesis', { text: 'h', based_on_steps: [], confidence: 'medium', confirmed: false }, 48_900))
    s = applyEvent(s, ev(3, 'agent.hypothesis', { text: 'h', based_on_steps: [], confidence: 'high', confirmed: false }, 300_000))
    expect(s.insight.time_to_first_hypothesis_s).toBe(48)
    expect(s.insight.first_hypothesis_at).toBe(at(48_900))
    const early = applyEvent(opened(), ev(2, 'agent.hypothesis', { text: 'h', based_on_steps: [], confidence: 'low', confirmed: false }, -5000))
    expect(early.insight.time_to_first_hypothesis_s).toBe(0)
  })

  it('usage: null cost while unpriced, lower bound once mixed', () => {
    let s = opened()
    s = applyEvent(s, ev(2, 'agent.usage', usage()))
    expect(s.insight.usage).toMatchObject({ calls: 1, unpriced_calls: 1, cost_usd: null, tokens: 135, latency_ms_total: 700 })
    s = applyEvent(s, ev(3, 'agent.usage', usage(0.25)))
    s = applyEvent(s, ev(4, 'agent.usage', usage(0.17)))
    const u = s.insight.usage
    expect(u.calls).toBe(3)
    expect(u.unpriced_calls).toBe(1)
    expect(u.cost_usd).toBeCloseTo(0.42, 6)
    expect(u.tokens).toBe(405)
    expect(u.cache_read_tokens).toBe(60)
    expect(u.models).toEqual({ m: 3 })
  })

  it('usage still adds no activity entry and no step', () => {
    const s0 = opened()
    const s = applyEvent(s0, ev(2, 'agent.usage', usage(1)))
    expect(s.activity).toBe(s0.activity)
    expect(s.steps).toBe(s0.steps)
    expect(s.last_seq).toBe(2)
  })

  it('a shadow room stays at zero', () => {
    let s = opened(true)
    s = applyEvent(s, ev(2, 'agent.usage', usage(1)))
    s = applyEvent(s, ev(3, 'agent.step.started', { step_id: 's_1', title: 'x' }))
    s = applyEvent(s, ev(4, 'agent.hypothesis', { text: 'h', based_on_steps: [], confidence: 'high', confirmed: false }, 9000))
    expect(s.shadow).toBe(true)
    expect(s.insight.usage.calls).toBe(0)
    expect(s.insight.steps).toBe(0)
    expect(s.insight.time_to_first_hypothesis_s).toBe(-1)
  })

  it('the schema-change fixture folds a priced and an unpriced call', () => {
    let s = emptyRoomState(FIXTURE_ROOM_ID)
    for (const e of fixtureEvents(FIXTURE_ROOM_ID)) s = applyEvent(s, e)
    expect(s.insight.usage).toMatchObject({ calls: 2, unpriced_calls: 1 })
    expect(s.insight.usage.cost_usd).toBeCloseTo(0.32, 6)
    expect(s.insight.steps).toBeGreaterThan(0)
  })
})

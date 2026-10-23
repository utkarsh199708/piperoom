import { describe, expect, it } from 'vitest'
import type { Topology } from '../api/types'
import cdc from '../fixtures/topology.json'
import evidence from '../fixtures/watcher-drift.json'
import { cleanText, driftFacts, driftLabel, driftMeasure, driftPhrase, placeDrift, windowClock } from './drift'
import type { DriftFact } from './drift'

const topology = cdc as unknown as Topology

const W = { from: '2026-10-21T10:00:00Z', to: '2026-10-21T11:00:00Z' }

function entry(over: Record<string, unknown>): Record<string, unknown> {
  return { signal: 'count_drift', severity: 'sev2', window: W, source: 0, sink: 0, missing: 0, mismatched: 0, sample: 0, ...over }
}

function one(over: Record<string, unknown>): DriftFact {
  const facts = driftFacts({ signals: [entry(over)] }, 'watcher')
  expect(facts).toHaveLength(1)
  return facts[0]
}

describe('driftFacts', () => {
  it('pins the documented flat DriftSignal shape (docs/api.md Watcher alerts)', () => {
    // The watcher adds since and firing_for_seconds to every entry; every other key is events.DriftSignal's.
    const keys = ['signal', 'severity', 'table', 'topic', 'window', 'source', 'sink', 'missing', 'mismatched', 'sample', 'detail']
    for (const e of evidence.signals.filter((s) => s.signal === 'count_drift' || s.signal === 'dup_keys')) {
      for (const k of Object.keys(e)) expect([...keys, 'since', 'firing_for_seconds']).toContain(k)
      expect(Object.keys(e.window as object).sort()).toEqual(['from', 'to'])
    }
  })

  it('reads the drift entries of a watcher room and skips the other signals', () => {
    const facts = driftFacts(evidence, 'watcher')
    expect(facts.map((f) => f.kind)).toEqual(['count_drift', 'dup_keys'])
    expect(facts[0]).toMatchObject({ table: 'public.orders', source: 100000, sink: 97900, missing: 2100, from: W.from, to: W.to })
  })

  it.each([
    ['webhook', 'webhook'],
    ['no source', undefined],
    ['empty source', ''],
  ])('reads nothing when the server did not say the watcher opened the room (%s)', (_, src) => {
    expect(driftFacts(evidence, src)).toEqual([])
  })

  it.each([
    ['null', null],
    ['a string', 'count_drift'],
    ['no signals', { source: 'watcher' }],
    ['signals not a list', { signals: { signal: 'count_drift' } }],
    ['entries not objects', { signals: [null, 3, 'x', []] }],
  ])('never throws on a malformed body (%s)', (_, body) => {
    expect(driftFacts(body, 'watcher')).toEqual([])
  })

  it('keeps a missing or unknown kind with a drift window as a neutral fact, and drops one without', () => {
    const facts = driftFacts(
      {
        signals: [
          entry({ signal: 'null_drift' }),
          entry({ signal: undefined }),
          { signal: 'mystery', window: '15m' },
          { signal: 'slot_wal', window: W },
        ],
      },
      'watcher',
    )
    expect(facts.map((f) => [f.kind, f.signal])).toEqual([
      [null, 'null_drift'],
      [null, ''],
    ])
    expect(facts.map(driftLabel)).toEqual(['null drift', 'drift'])
    expect(driftPhrase(facts[1])).toBe('drift over 1 h')
  })

  it('turns bad numbers and windows into nulls instead of NaN', () => {
    const f = one({ source: '100', sink: -1, missing: Number.NaN, window: { from: 'yesterday', to: 7 } })
    expect([f.source, f.sink, f.missing]).toEqual([null, null, null])
    expect([f.from, f.to]).toEqual(['', ''])
    expect(driftPhrase(f)).toBe('row counts differ')
    expect(windowClock(f)).toBe('')
  })

  it('cleans untrusted text: control and bidi characters, and a 10 KB detail capped', () => {
    const f = one({ table: 'public.‮orders\u0000x', detail: 'a\nb\tc' + 'z'.repeat(10_000) })
    expect(f.table).toBe('public.orders x')
    expect(f.detail.startsWith('a b c')).toBe(true)
    expect(Array.from(f.detail)).toHaveLength(200)
    expect(cleanText(42, 10)).toBe('')
  })
})

describe('drift phrases', () => {
  it.each([
    [{ signal: 'count_drift', source: 100000, sink: 97900, missing: 2100 }, '2.1 % rows missing over 1 h'],
    [{ signal: 'count_drift', source: 1000000, sink: 999999, missing: 1 }, '<0.1 % rows missing over 1 h'],
    [{ signal: 'count_drift', source: 200, sink: 0, missing: 200 }, '100 % rows missing over 1 h'],
    [{ signal: 'count_drift', source: 1000, sink: 1050 }, '5 % extra rows on the sink over 1 h'],
    [{ signal: 'row_drift', sample: 200, missing: 4, mismatched: 1 }, '4 of 200 sampled rows missing, 1 of 200 sampled rows differ over 1 h'],
    [{ signal: 'dup_keys', source: 48210, missing: 3, sample: 2 }, '3 duplicate events on 2 keys over 1 h'],
    [{ signal: 'dup_keys', missing: 1, sample: 1 }, '1 duplicate event on 1 key over 1 h'],
    [{ signal: 'order_skew', mismatched: 1500, sample: 7 }, '1,500 out-of-order events on 7 keys over 1 h'],
    [{ signal: 'count_drift', window: { from: W.from, to: '2026-10-21T10:15:00Z' }, source: 10, missing: 1 }, '10 % rows missing over 15 min'],
  ])('%j reads "%s"', (over, phrase) => {
    expect(driftPhrase(one(over))).toBe(phrase)
  })

  it('names the window in UTC, with dates when it crosses a day', () => {
    expect(windowClock(one({}))).toBe('10:00–11:00 UTC')
    expect(windowClock(one({ window: { from: '2026-10-21T23:30:00Z', to: '2026-10-22T00:30:00Z' } }))).toBe(
      '2026-10-21 23:30–2026-10-22 00:30 UTC',
    )
  })

  it('falls back to the label when a known kind has no counts', () => {
    expect(driftMeasure(one({ signal: 'order_skew' }))).toBe('events out of order')
    expect(driftMeasure(one({ signal: 'row_drift' }))).toBe('sampled rows differ')
  })
})

describe('placeDrift', () => {
  const at = (over: Record<string, unknown>) => [...placeDrift(topology, [one(over)]).keys()]

  it('puts table kinds on the sink node of the table and topic kinds on the topic node', () => {
    expect(at({ signal: 'count_drift', table: 'public.orders' })).toEqual(['wh:analytics.orders'])
    expect(at({ signal: 'row_drift', table: 'analytics.orders' })).toEqual(['wh:analytics.orders'])
    expect(at({ signal: 'dup_keys', topic: 'pg.public.orders' })).toEqual(['topic:pg.public.orders'])
    expect(at({ signal: 'order_skew', topic: 'pg.public.orders' })).toEqual(['topic:pg.public.orders'])
  })

  it('uses the only sink or topic when no name is given, and no node for a name it cannot find', () => {
    expect(at({ signal: 'count_drift' })).toEqual(['wh:analytics.orders'])
    expect(at({ signal: 'dup_keys' })).toEqual(['topic:pg.public.orders'])
    expect(at({ signal: 'count_drift', table: 'public.other' })).toEqual([])
    expect(at({ signal: 'dup_keys', topic: 'other' })).toEqual([])
  })

  it('places an unknown kind by the name it carries, or nowhere', () => {
    expect(at({ signal: 'null_drift', table: 'public.orders' })).toEqual(['wh:analytics.orders'])
    expect(at({ signal: 'null_drift', topic: 'pg.public.orders' })).toEqual(['topic:pg.public.orders'])
    expect(at({ signal: 'null_drift' })).toEqual([])
  })

  it('does not follow depends_on edges to a sink', () => {
    const t: Topology = {
      pipeline_id: 'p',
      nodes: [
        { id: 'a', kind: 'source_table', name: 'public.a', layer: 'postgres', owner: 'o', health: 'ok' },
        { id: 's', kind: 'sink_table', name: 'w.a', layer: 'warehouse', owner: 'o', health: 'ok' },
      ],
      edges: [{ from: 'a', to: 's', kind: 'depends_on' }],
    }
    expect([...placeDrift(t, [one({ table: 'public.a' })]).keys()]).toEqual([])
  })
})

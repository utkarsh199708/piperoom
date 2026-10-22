import { describe, expect, it } from 'vitest'
import type { Event, RoomSnapshot, TopologyNode, TrendPoint } from '../api/types'
import { applyEvent } from '../state/reducer'
import { emptyRoomState, fromSnapshot } from '../state/types'
import {
  MAX_TREND_POINTS,
  cleanForecast,
  cleanSeries,
  durationLabel,
  etaBadge,
  formatMetricValue,
  laneTrends,
  sparklineLabel,
  sparklinePath,
  sparklineXY,
} from './trends'

const NOW = Date.parse('2026-10-22T10:00:00Z')
const MIN = 60_000
const HOUR = 60 * MIN
const DAY = 24 * HOUR

/** n samples 15 min apart ending at NOW, rising by `step`. */
function series(n: number, step = 1): TrendPoint[] {
  return Array.from({ length: n }, (_, i) => ({
    at: new Date(NOW - (n - 1 - i) * 15 * MIN).toISOString(),
    value: 1000 + i * step,
  }))
}

// The docs/api.md X3 snapshot example, pinned: the web reads exactly these keys.
const FORECAST = {
  metric: 'slot_wal_bytes',
  object: 'orders_slot',
  current: 64424509440,
  limit: 107374182400,
  eta: '2026-10-24T03:00:00Z',
  slope_per_hour: 1073741824,
  r2: 0.97,
  at: '2026-10-22T09:45:00Z',
}

describe('durationLabel — the ETA formatting table (TC2)', () => {
  it.each([
    ['30 min', 30 * MIN, '30 min'],
    ['under a minute', 20_000, '1 min'],
    ['59 min', 59 * MIN + 59_000, '59 min'],
    ['18 h', 18 * HOUR, '18 h'],
    ['47 h', 47 * HOUR + 59 * MIN, '47 h'],
    ['3 d', 3 * DAY, '3 d'],
    ['exactly 7 d', 7 * DAY, '7 d'],
    ['> 7 d: no badge', 7 * DAY + MIN, null],
    ['not a number', Number.NaN, null],
  ])('%s', (_name, ms, want) => {
    expect(durationLabel(ms)).toBe(want)
  })
})

describe('etaBadge', () => {
  const at = (ms: number) => new Date(NOW + ms).toISOString()

  it('says "fills in" for a slot and "data loss in" for a topic past retention', () => {
    expect(etaBadge({ ...FORECAST, eta: at(3 * DAY) }, NOW)?.short).toBe('fills in 3 d')
    expect(etaBadge({ ...FORECAST, metric: 'topic_oldest_unread_age', eta: at(18 * HOUR) }, NOW)?.short).toBe(
      'data loss in 18 h',
    )
    expect(etaBadge({ ...FORECAST, eta: at(30 * MIN) }, NOW)?.short).toBe('fills in 30 min')
  })

  it('has no badge beyond a week, for a bad time, or for a metric it does not know', () => {
    expect(etaBadge({ ...FORECAST, eta: at(8 * DAY) }, NOW)).toBeNull()
    expect(etaBadge({ ...FORECAST, eta: 'soon' }, NOW)).toBeNull()
    expect(etaBadge({ ...FORECAST, metric: 'disk_free' }, NOW)).toBeNull()
  })

  it('says the limit is reached once the ETA has passed', () => {
    const b = etaBadge({ ...FORECAST, eta: at(-HOUR) }, NOW)
    expect(b).toMatchObject({ short: 'limit reached', due: true })
    expect(etaBadge({ ...FORECAST, metric: 'topic_oldest_unread_age', eta: at(-1) }, NOW)?.short).toBe('data loss now')
  })

  it('has a long text naming the metric, the object and the UTC time (TC4)', () => {
    const b = etaBadge(FORECAST, NOW)
    expect(b?.long).toContain('WAL retained by the slot (orders_slot)')
    expect(b?.long).toContain('2026-10-24 03:00')
    expect(b?.long).toContain('in about 41 h')
  })
})

describe('sparkline geometry (TC1)', () => {
  it('draws one path command per point', () => {
    const d = sparklinePath(series(96), 64, 16)
    expect(d.match(/[ML]/g)).toHaveLength(96)
    expect(d.startsWith('M')).toBe(true)
  })

  it('draws nothing for an empty series', () => {
    expect(sparklinePath([], 64, 16)).toBe('')
    expect(sparklineXY([], 64, 16)).toEqual([])
  })

  it('keeps every point inside the box, flat and single-point series included', () => {
    for (const pts of [series(96, 37), series(5, 0), series(1)]) {
      for (const [x, y] of sparklineXY(pts, 64, 16)) {
        expect(x).toBeGreaterThanOrEqual(0)
        expect(x).toBeLessThanOrEqual(64)
        expect(y).toBeGreaterThanOrEqual(0)
        expect(y).toBeLessThanOrEqual(16)
      }
    }
  })

  it('places x by sample time, so a gap in sampling is a long segment', () => {
    const pts = [
      { at: '2026-10-22T00:00:00Z', value: 1 },
      { at: '2026-10-22T01:00:00Z', value: 2 },
      { at: '2026-10-22T10:00:00Z', value: 3 },
    ]
    const xs = sparklineXY(pts, 102, 10).map(([x]) => x)
    expect(xs).toEqual([1, 11, 101])
  })

  it('a limit widens the range: the series sits below it', () => {
    const [[, y]] = sparklineXY([{ at: '2026-10-22T00:00:00Z', value: 50 }, { at: '2026-10-22T01:00:00Z', value: 50 }], 10, 102, 100)
    expect(y).toBe(101)
  })
})

describe('cleanSeries', () => {
  it('drops points that are not finite numbers with a time, and non-array bodies', () => {
    expect(cleanSeries({ error: 'x' })).toEqual([])
    expect(cleanSeries(null)).toEqual([])
    expect(
      cleanSeries([
        { at: '2026-10-22T00:00:00Z', value: 1 },
        { at: 'yesterday', value: 2 },
        { at: '2026-10-22T00:15:00Z', value: '3' },
        { at: '2026-10-22T00:30:00Z', value: null },
        '<img src=x onerror=alert(1)>',
        null,
        { at: '2026-10-22T00:45:00Z', value: 5 },
      ]),
    ).toEqual([
      { at: '2026-10-22T00:00:00Z', value: 1 },
      { at: '2026-10-22T00:45:00Z', value: 5 },
    ])
  })

  it('keeps at most the newest 672 points', () => {
    const got = cleanSeries(series(700))
    expect(got).toHaveLength(MAX_TREND_POINTS)
    expect(got[got.length - 1].value).toBe(1000 + 699)
  })
})

describe('formatMetricValue and sparklineLabel', () => {
  it('uses the metric unit', () => {
    expect(formatMetricValue('slot_wal_bytes', 64424509440)).toBe('60 GiB')
    expect(formatMetricValue('group_lag', 1204)).toBe('1,204 records')
    expect(formatMetricValue('topic_oldest_unread_age', 5 * 3600)).toBe('5 h')
  })

  it('names the metric and its newest value', () => {
    expect(sparklineLabel('dlq_records', series(3))).toBe('Dead-letter records, last 24 h, now 1,002 records')
    expect(sparklineLabel('dlq_records', [])).toBe('Dead-letter records, last 24 h')
  })
})

const node = (id: string, kind: TopologyNode['kind'], name = id): TopologyNode => ({
  id,
  kind,
  name,
  layer: 'kafka',
  owner: 'o',
  health: 'ok',
})

describe('laneTrends — which node carries which metric', () => {
  const nodes = [
    node('pg', 'source_table'),
    node('c', 'connector'),
    node('t1', 'topic', 'pg.public.orders'),
    node('t2', 'topic', 'pg.public.items'),
    node('dlq', 'dlq_topic'),
    node('g', 'consumer_group'),
    node('wh', 'sink_table'),
  ]

  it('puts each metric on the first node of its kind; empty series draw nothing', () => {
    const got = laneTrends(nodes, {
      slot_wal_bytes: series(4),
      db_size_bytes: series(4),
      dlq_records: [],
      group_lag: series(2),
      topic_oldest_unread_age: series(3),
    })
    expect([...got.entries()].map(([id, t]) => [id, t.metric])).toEqual([
      ['pg', 'slot_wal_bytes'],
      ['g', 'group_lag'],
      ['t1', 'topic_oldest_unread_age'],
    ])
  })

  it('puts the forecast on the node named like its object, even with no samples', () => {
    const f = { ...FORECAST, metric: 'topic_oldest_unread_age', object: 'pg.public.items' }
    const got = laneTrends(nodes, { topic_oldest_unread_age: series(3) }, f)
    expect(got.get('t2')).toMatchObject({ metric: 'topic_oldest_unread_age', forecast: f })
    expect(got.has('t1')).toBe(false)
    expect(laneTrends(nodes, {}, f).get('t2')?.points).toEqual([])
  })

  it('a forecast metric wins its node over the default metric', () => {
    const f = { ...FORECAST, metric: 'db_size_bytes', object: 'orders' }
    const got = laneTrends(nodes, { slot_wal_bytes: series(4), db_size_bytes: series(4) }, f)
    expect(got.get('pg')?.metric).toBe('db_size_bytes')
  })

  it('draws nothing for a metric whose node kind is not in the lane, or an unknown forecast metric', () => {
    expect(laneTrends([node('wh', 'sink_table')], { slot_wal_bytes: series(4) }, FORECAST).size).toBe(0)
    expect(laneTrends(nodes, {}, { ...FORECAST, metric: 'disk_free' }).size).toBe(0)
  })
})

describe('the room carries origin and forecast (E8)', () => {
  const snapshot = {
    id: 'r1',
    title: 'WAL growth',
    severity: 'sev4',
    pipeline_id: 'orders',
    status: 'open',
    opened_at: '2026-10-22T09:45:00Z',
    last_seq: 3,
    driver: null,
    agent_status: { state: 'idle' },
    hypothesis: null,
    open_proposals: [],
    pending_suggestions: [],
    members: [],
    origin: 'forecast',
    forecast: FORECAST,
  } as RoomSnapshot

  it('fromSnapshot keeps the contract fields', () => {
    const s = fromSnapshot(snapshot)
    expect(s.origin).toBe('forecast')
    expect(s.forecast).toEqual(FORECAST)
  })

  it('an older snapshot or a malformed forecast leaves them off', () => {
    const old: Partial<RoomSnapshot> = { ...snapshot }
    delete old.origin
    delete old.forecast
    const s = fromSnapshot(old as RoomSnapshot)
    expect('origin' in s).toBe(false)
    expect('forecast' in s).toBe(false)
    expect(cleanForecast({ ...FORECAST, current: 'big' })).toBeUndefined()
    expect(cleanForecast({ ...FORECAST, limit: null })).toBeUndefined()
    expect(cleanForecast('x')).toBeUndefined()
  })

  it('room.created folds origin', () => {
    const ev = {
      seq: 1,
      room_id: 'r1',
      type: 'room.created',
      at: '2026-10-22T09:45:00Z',
      actor: { kind: 'system', id: 'watcher', name: 'watcher' },
      payload: { title: 't', severity: 'sev4', pipeline_id: 'orders', origin: 'forecast' },
    } as unknown as Event
    expect(applyEvent(emptyRoomState('r1'), ev).origin).toBe('forecast')
  })
})

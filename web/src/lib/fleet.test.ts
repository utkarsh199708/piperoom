import { describe, expect, it } from 'vitest'
import { ApiClientError } from '../api/client'
import type { FleetPipeline, PipelineChange } from '../api/types'
import fixture from '../fixtures/fleet.json'
import {
  changeLink,
  cleanChangesPage,
  cleanFleet,
  facetValues,
  filterFleet,
  fleetErrorText,
  landingHash,
  laneHref,
  sortFleet,
  verdictLabel,
} from './fleet'

const row = (over: Partial<FleetPipeline>): FleetPipeline => ({
  pipeline_id: 'p',
  title: '',
  kind: 'cdc',
  tenant: '',
  team: '',
  health: 'ok',
  worst_node: '',
  open_rooms: 0,
  signals_firing: [],
  changes_24h: 0,
  forecasts: [],
  last_verdict: null,
  ...over,
})

const ids = (rows: FleetPipeline[]) => rows.map((r) => r.pipeline_id)

describe('cleanFleet', () => {
  it('keeps the docs/api.md FleetPipeline shape field for field (E8)', () => {
    const rows = cleanFleet(fixture)
    expect(rows[0]).toEqual(fixture[0])
    expect(rows).toHaveLength(3)
    expect(rows[2].checked_at).toBeUndefined()
    expect(rows[2].last_verdict).toEqual({ room_id: 'r_02AA', verdict: '', layer_correct: false, at: '2026-10-20T07:00:00Z' })
  })

  it('answers [] for anything but an array and drops rows without an id', () => {
    expect(cleanFleet(null)).toEqual([])
    expect(cleanFleet({ rows: [] })).toEqual([])
    expect(cleanFleet([{ title: 'x' }, 'x', null])).toEqual([])
  })

  it('normalises unknown words, bad numbers and lists, and strips bidi controls', () => {
    const [r] = cleanFleet([
      {
        pipeline_id: 'a',
        title: 'safe‮evil',
        kind: 'weird',
        team: 'ops⁦x',
        health: 'on-fire',
        open_rooms: -3,
        changes_24h: 'many',
        signals_firing: ['slot_wal', 3, '', null],
        forecasts: [{ metric: 'slot_wal_bytes', eta: 'soon' }, { metric: 'dlq_records', eta: '2026-10-23T00:00:00Z' }],
        last_verdict: { verdict: 'confirmed' },
        checked_at: 'yesterday',
      },
    ])
    expect(r.title).toBe('safe evil')
    expect(r.team).toBe('opsx')
    expect(r.kind).toBe('cdc')
    expect(r.health).toBe('unknown')
    expect(r.open_rooms).toBe(0)
    expect(r.changes_24h).toBe(0)
    expect(r.signals_firing).toEqual(['slot_wal'])
    expect(r.forecasts).toEqual([{ metric: 'dlq_records', eta: '2026-10-23T00:00:00Z' }])
    expect(r.last_verdict).toBeNull()
    expect(r.checked_at).toBeUndefined()
  })
})

describe('sortFleet', () => {
  const rows = [
    row({ pipeline_id: 'b', health: 'ok', changes_24h: 1 }),
    row({ pipeline_id: 'a', health: 'warn', forecasts: [{ metric: 'dlq_records', eta: '2026-10-25T00:00:00Z' }] }),
    row({ pipeline_id: 'c', health: 'fail', changes_24h: 5, forecasts: [{ metric: 'group_lag', eta: '2026-10-23T00:00:00Z' }] }),
    row({ pipeline_id: 'd', health: 'unknown' }),
  ]

  it('orders health worst first, ties by id', () => {
    expect(ids(sortFleet(rows, 'health', 'asc'))).toEqual(['c', 'a', 'd', 'b'])
    expect(ids(sortFleet(rows, 'health', 'desc'))).toEqual(['b', 'd', 'a', 'c'])
  })

  it('orders by the soonest ETA and keeps rows without a forecast last either way', () => {
    expect(ids(sortFleet(rows, 'eta', 'asc'))).toEqual(['c', 'a', 'b', 'd'])
    expect(ids(sortFleet(rows, 'eta', 'desc'))).toEqual(['a', 'c', 'b', 'd'])
  })

  it('orders by name and by changes', () => {
    expect(ids(sortFleet(rows, 'name', 'asc'))).toEqual(['a', 'b', 'c', 'd'])
    expect(ids(sortFleet(rows, 'changes', 'desc'))).toEqual(['c', 'b', 'a', 'd'])
  })
})

describe('filters', () => {
  const rows = cleanFleet(fixture)

  it('matches each set field exactly', () => {
    expect(ids(filterFleet(rows, { tenant: 'payments', team: '', kind: '' }))).toEqual(['orders', 'refunds'])
    expect(ids(filterFleet(rows, { tenant: '', team: 'refunds', kind: '' }))).toEqual(['refunds'])
    expect(ids(filterFleet(rows, { tenant: '', team: '', kind: 'batch' }))).toEqual(['nightly'])
    expect(ids(filterFleet(rows, { tenant: 'Payments', team: '', kind: '' }))).toEqual([])
  })

  it('offers the distinct non-empty values', () => {
    expect(facetValues(rows, 'tenant')).toEqual(['payments'])
    expect(facetValues(rows, 'team')).toEqual(['orders-core', 'refunds'])
  })
})

describe('changes', () => {
  const change = (after: unknown): PipelineChange => ({
    id: 1,
    pipeline_id: 'orders',
    kind: 'deploy',
    object: 'svc',
    at: '2026-10-22T10:00:00Z',
    source: 'webhook',
    fingerprint: 'f',
    after,
    summary: 's',
  })

  it('links only an absolute http(s) url without user info', () => {
    expect(changeLink(change({ url: 'https://ci.example/run/1' }))).toBe('https://ci.example/run/1')
    expect(changeLink(change({ url: 'javascript:alert(1)' }))).toBe('')
    expect(changeLink(change({ url: 'https://me:pw@ci.example/' }))).toBe('')
    expect(changeLink(change({ url: '/relative' }))).toBe('')
    expect(changeLink(change('https://ci.example'))).toBe('')
  })

  it('checks a page: rows without kind, object or time are dropped; a summary is capped', () => {
    const page = cleanChangesPage({
      changes: [
        { id: 1, pipeline_id: 'orders', kind: 'ddl', object: 'public.orders', at: '2026-10-22T10:00:00Z', summary: 'x'.repeat(10_000) },
        { id: 2, kind: 'ddl', object: '', at: '2026-10-22T10:00:00Z' },
        { id: 3, kind: 'ddl', object: 'o', at: 'never' },
      ],
      next: 'c2',
    })
    expect(page.next).toBe('c2')
    expect(page.changes).toHaveLength(1)
    expect(page.changes[0].summary.length).toBeLessThanOrEqual(300)
    expect(cleanChangesPage('nope')).toEqual({ changes: [], next: '' })
  })
})

describe('helpers', () => {
  it('words the last verdict', () => {
    expect(verdictLabel(null)).toBe('')
    expect(verdictLabel({ room_id: 'r', verdict: 'disputed', layer_correct: null, at: '' })).toBe('Disputed')
    expect(verdictLabel({ room_id: 'r', verdict: '', layer_correct: true, at: '' })).toBe('Graded: right layer')
    expect(verdictLabel({ room_id: 'r', verdict: '', layer_correct: null, at: '' })).toBe('')
  })

  it('lands a bare URL on the fleet and leaves every route alone', () => {
    expect(landingHash('')).toBe('#/fleet')
    expect(landingHash('#')).toBe('#/fleet')
    expect(landingHash('#/')).toBeNull()
    expect(landingHash('#/?pipeline=orders')).toBeNull()
    expect(landingHash('#/rooms/r1')).toBeNull()
  })

  it('links the lane with the node id exactly as given', () => {
    expect(laneHref('orders')).toBe('/pipelines/orders')
    expect(laneHref('a/b', 'pg:public.orders')).toBe('/pipelines/a%2Fb?node=pg%3Apublic.orders')
  })

  it('says a missing fleet route plainly and keeps other API errors', () => {
    expect(fleetErrorText(new ApiClientError(404, 'not_found', 'not found'))).toBe('This server does not serve the fleet view.')
    expect(fleetErrorText(new ApiClientError(403, 'viewer', 'viewers cannot'))).toBe('viewers cannot')
  })
})

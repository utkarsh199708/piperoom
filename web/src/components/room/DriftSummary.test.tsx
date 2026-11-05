import { afterEach, describe, expect, it, vi } from 'vitest'
import { render, screen, within } from '@testing-library/react'
import type { Event } from '@/api/types'
import evidence from '@/fixtures/watcher-drift.json'
import { applyEvent } from '@/state/reducer'
import { emptyRoomState } from '@/state/types'
import type { RoomState } from '@/state/types'
import { RoomHeader } from './RoomHeader'

const SYSTEM = { kind: 'system', id: 'system', name: 'Piperoom' } as const

function created(payload: Record<string, unknown>): Event {
  return { room_id: 'r_1', seq: 1, type: 'room.created', actor: SYSTEM, payload, at: '2026-10-21T11:20:00Z' }
}

function watcherRoom(alert: unknown = evidence, alertSource = 'watcher'): RoomState {
  return applyEvent(
    emptyRoomState('r_1'),
    created({ title: 'orders: count drift', severity: 'sev2', pipeline_id: 'orders', alert, alert_source: alertSource }),
  )
}

function setWidth(px: number) {
  vi.stubGlobal('matchMedia', (q: string) => {
    const min = /min-width:\s*(\d+)px/.exec(q)
    return {
      matches: min ? px >= Number(min[1]) : false,
      media: q,
      addEventListener: () => undefined,
      removeEventListener: () => undefined,
    }
  })
}

function header(state: RoomState) {
  return render(
    <RoomHeader
      state={state}
      user={{ id: 'kabir', name: 'Kabir', layers: ['kafka'] }}
      members={[]}
      conn="live"
      copied={false}
      onHandoff={() => undefined}
      onCloseRoom={() => undefined}
      onInvite={() => undefined}
    />,
  )
}

afterEach(() => {
  vi.unstubAllGlobals()
})

describe('room header drift line (X1)', () => {
  it.each([
    ['desktop', 1280, 'More room actions'],
    ['mobile', 390, 'More actions'],
  ])('leads a watcher room with the signal kind and window at %s width', (_, px, menu) => {
    setWidth(px)
    header(watcherRoom())
    const list = screen.getByRole('list', { name: 'Drift the watcher saw' })
    const lines = within(list).getAllByRole('listitem')
    expect(lines).toHaveLength(2)
    expect(lines[0]).toHaveTextContent('count drift')
    expect(lines[0]).toHaveTextContent('public.orders')
    expect(lines[0]).toHaveTextContent('2.1 % rows missing over 1 h')
    expect(lines[0]).toHaveTextContent('10:00–11:00 UTC')
    expect(lines[1]).toHaveTextContent('duplicate events')
    // The line comes right after the title row; no room control goes missing at this width.
    expect(screen.getByRole('heading', { level: 1 })).toHaveTextContent('orders: count drift')
    expect(screen.getByRole('button', { name: menu })).toBeInTheDocument()
    expect(list.outerHTML).toMatchSnapshot()
  })

  it('renders hostile detail as text and an unknown kind as a neutral line', () => {
    setWidth(1280)
    const { container } = header(
      watcherRoom({
        signals: [
          { signal: 'count_drift', severity: 'sev2', table: '<b>t</b>', window: { from: '2026-10-21T10:00:00Z', to: '2026-10-21T11:00:00Z' }, source: 10, sink: 9, missing: 1, mismatched: 0, sample: 0, detail: '<img src=x onerror=alert(1)>' },
          { signal: 'brand_new_kind', severity: 'sev3', window: { from: '2026-10-21T10:00:00Z', to: '2026-10-21T10:15:00Z' } },
        ],
      }),
    )
    expect(container.querySelector('img')).toBeNull()
    expect(container.querySelector('b')).toBeNull()
    const lines = within(screen.getByRole('list', { name: 'Drift the watcher saw' })).getAllByRole('listitem')
    expect(lines[0]).toHaveTextContent('<img src=x onerror=alert(1)>')
    expect(lines[0]).toHaveTextContent('<b>t</b>')
    expect(lines[1]).toHaveTextContent('brand new kind')
    expect(lines[1]).toHaveTextContent('15 min')
    expect(lines[1]).toHaveAttribute('data-drift', 'unknown')
  })

  it('shows no drift line for a room the watcher did not open, or one with no drift', () => {
    setWidth(1280)
    const { unmount } = header(watcherRoom(evidence, 'webhook'))
    expect(screen.queryByRole('list', { name: 'Drift the watcher saw' })).toBeNull()
    unmount()
    header(watcherRoom({ source: 'watcher', signals: [{ signal: 'slot_wal', severity: 'sev2' }] }))
    expect(screen.queryByRole('list', { name: 'Drift the watcher saw' })).toBeNull()
  })

  it('caps the header at three lines and counts the rest', () => {
    setWidth(1280)
    const w = { from: '2026-10-21T10:00:00Z', to: '2026-10-21T11:00:00Z' }
    header(watcherRoom({ signals: [1, 2, 3, 4, 5].map((n) => ({ signal: 'dup_keys', topic: 't', window: w, missing: n })) }))
    const list = screen.getByRole('list', { name: 'Drift the watcher saw' })
    expect(within(list).getAllByRole('listitem')).toHaveLength(4)
    expect(list).toHaveTextContent('+2 more drift signals')
  })
})

describe('room.escalated (X3)', () => {
  const at = '2026-10-21T12:00:00Z'
  const esc = (seq: number, severity: string): Event => ({
    room_id: 'r_1',
    seq,
    type: 'room.escalated',
    actor: SYSTEM,
    payload: { severity, from: 'sev4', signal: 'slot_wal' },
    at,
  })

  it('raises the severity on replay and never lowers it', () => {
    let s = applyEvent(emptyRoomState('r_1'), created({ title: 't', severity: 'sev4', pipeline_id: 'orders', origin: 'forecast' }))
    s = applyEvent(s, esc(2, 'sev2'))
    expect(s.severity).toBe('sev2')
    s = applyEvent(s, esc(3, 'sev3'))
    expect(s.severity).toBe('sev2')
    s = applyEvent(s, esc(4, 'bogus'))
    expect(s.severity).toBe('sev2')
    expect(s.last_seq).toBe(4)
  })
})

import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { act, render, screen } from '@testing-library/react'
import { MemoryRouter } from 'react-router-dom'
import type { RoomSnapshot } from '../api/types'

// The real list hook over a fake push socket: a room opened on the server shows up as a row
// as soon as its frame arrives, with no poll.
vi.mock('../context/user', () => ({
  useCurrentUser: () => ({
    user: { id: 'arjun', name: 'Arjun', layers: [] },
    setUserId: vi.fn(),
    logout: vi.fn(),
    sessionEpoch: 0,
  }),
}))
vi.mock('../hooks/usePipelines', () => ({ usePipelines: () => ({ status: 'loading', reload: () => undefined }) }))
vi.mock('sonner', () => ({ toast: { success: vi.fn(), error: vi.fn() } }))
vi.mock('../api/client', () => ({
  api: { rooms: vi.fn(), createRoom: vi.fn(), grantControl: vi.fn() },
  isFixtures: () => false,
  openListWs: vi.fn(),
  ApiClientError: class extends Error {},
}))

import { api, openListWs } from '../api/client'
import { RoomList } from './RoomList'

class FakeSocket {
  readyState = 1
  onopen: (() => void) | null = null
  onmessage: ((m: { data: string }) => void) | null = null
  onclose: ((ev: { reason?: string }) => void) | null = null
  send() {}
  close() {}
  frame(msg: unknown) {
    this.onmessage?.({ data: JSON.stringify(msg) })
  }
}

function room(id: string, title: string, extra: Partial<RoomSnapshot> = {}): RoomSnapshot {
  return {
    id,
    title,
    severity: 'sev2',
    pipeline_id: 'orders',
    status: 'open',
    opened_at: new Date().toISOString(),
    last_seq: 1,
    driver: null,
    agent_status: { state: 'queued' },
    hypothesis: null,
    triage: null,
    open_proposals: [],
    pending_suggestions: [],
    members: [],
    ...extra,
  }
}

let sock: FakeSocket

beforeEach(() => {
  sock = new FakeSocket()
  vi.stubGlobal('WebSocket', FakeSocket)
  vi.mocked(openListWs).mockReturnValue(sock as unknown as WebSocket)
})

afterEach(() => {
  vi.unstubAllGlobals()
})

describe('RoomList push', () => {
  it('shows a new watcher room the moment its frame arrives, then its first look', () => {
    render(
      <MemoryRouter>
        <RoomList />
      </MemoryRouter>,
    )
    act(() => sock.frame({ type: 'rooms', rooms: [room('r_1', 'billing: lag')] }))
    expect(screen.getByText('billing: lag')).toBeInTheDocument()

    act(() => sock.frame({ type: 'room', room: room('r_2', 'orders: slot debezium_orders inactive 6m') }))
    expect(screen.getByText('orders: slot debezium_orders inactive 6m')).toBeInTheDocument()
    expect(screen.getAllByLabelText('First look in progress')).toHaveLength(2)

    act(() =>
      sock.frame({
        type: 'room',
        room: room('r_2', 'orders: slot debezium_orders inactive 6m', {
          triage: {
            status: 'ok',
            headline: 'task 0 FAILED on MISSING_UNION_BRANCH after DDL on public.orders',
            suspected_layer: 'kafka-connect',
            confidence: 'medium',
            evidence_refs: [],
            at: new Date().toISOString(),
          },
        }),
      }),
    )
    expect(screen.getByText('task 0 FAILED on MISSING_UNION_BRANCH after DDL on public.orders')).toBeInTheDocument()
    expect(api.rooms).not.toHaveBeenCalled()
  })
})

import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { act, renderHook, waitFor } from '@testing-library/react'
import type { Event, RoomSnapshot } from '../api/types'
import { ApiClientError } from '../api/client'

// P2c: useRoom's approveAll is POST /api/proposals/{pid}/approve-all with the hash on screen, and it
// sorts the route's answers the way the card needs them.

vi.mock('sonner', () => ({ toast: Object.assign(vi.fn(), { warning: vi.fn(), dismiss: vi.fn() }) }))
vi.mock('../context/user', () => ({
  useCurrentUser: () => ({
    user: { id: 'sam', name: 'Sam', layers: ['schema-registry', 'kafka-connect'] },
    setUserId: vi.fn(),
    logout: vi.fn(),
    sessionEpoch: 0,
  }),
}))
vi.mock('../api/client', async (importOriginal) => {
  const actual = await importOriginal<typeof import('../api/client')>()
  return {
    ...actual,
    isFixtures: () => false,
    openRoomWs: vi.fn(),
    api: { ...actual.api, me: vi.fn(), room: vi.fn(), events: vi.fn(), approveAll: vi.fn() },
  }
})

import { api, openRoomWs } from '../api/client'
import { useRoom } from './useRoom'

class FakeWebSocket {
  static CONNECTING = 0
  static OPEN = 1
  static CLOSING = 2
  static CLOSED = 3
  readyState = FakeWebSocket.CONNECTING
  onopen: (() => void) | null = null
  onmessage: ((m: { data: string }) => void) | null = null
  onclose: ((ev: { reason: string }) => void) | null = null
  onerror: (() => void) | null = null
  send() {}
  close() {
    this.readyState = FakeWebSocket.CLOSED
  }
}

const HASH = 'abc123'
const snap: RoomSnapshot = {
  id: 'r1',
  title: 'T',
  severity: 'sev2',
  pipeline_id: 'orders',
  status: 'open',
  opened_at: '2026-10-18T10:00:00Z',
  last_seq: 0,
  driver: null,
  agent_status: { state: 'idle' },
  hypothesis: null,
  open_proposals: [],
  pending_suggestions: [],
  members: [],
  mode: { write: true, solo: false, auto_approve: [] },
}
const created: Event = {
  room_id: 'r1',
  seq: 1,
  type: 'proposal.created',
  actor: { kind: 'agent', id: 'agent', name: 'Agent' },
  payload: {
    proposal_id: 'p1',
    title: 'Fix',
    steps: [{ action: 'restart_connector_task', layer: 'kafka-connect', params: {}, description: '' }],
    risk: 'low',
    rollback: '',
    required_layers: ['schema-registry', 'kafka-connect'],
    params_hash: HASH,
  },
  at: '2026-10-18T10:00:01Z',
}

async function live() {
  vi.mocked(openRoomWs).mockImplementation(() => {
    const sock = new FakeWebSocket()
    queueMicrotask(() => {
      sock.readyState = FakeWebSocket.OPEN
      sock.onopen?.()
      sock.onmessage?.({ data: JSON.stringify({ type: 'event', event: created }) })
    })
    return sock as unknown as WebSocket
  })
  vi.mocked(api.me).mockResolvedValue(undefined as never)
  vi.mocked(api.room).mockResolvedValue(snap)
  const hook = renderHook(() => useRoom('r1'))
  await waitFor(() => expect(hook.result.current.state?.last_seq).toBe(1))
  return hook
}

beforeEach(() => {
  vi.stubGlobal('WebSocket', FakeWebSocket)
  vi.resetAllMocks()
})
afterEach(() => {
  vi.unstubAllGlobals()
})

describe('useRoom approveAll', () => {
  it('posts the hash on screen (and the typed confirm) and answers ok', async () => {
    const hook = await live()
    vi.mocked(api.approveAll).mockResolvedValue([])
    let res: Awaited<ReturnType<NonNullable<typeof hook.result.current.actions.approveAll>>> | undefined
    await act(async () => {
      res = await hook.result.current.actions.approveAll?.('p1', { params_hash: HASH, confirm: 'orders/*' })
    })
    expect(api.approveAll).toHaveBeenCalledWith('p1', { params_hash: HASH, confirm: 'orders/*' })
    expect(res).toEqual({ ok: true })
    expect(hook.result.current.error).toBeNull()
  })

  it('403 not_layer_owner is a refusal kept on the room state, not a banner', async () => {
    const hook = await live()
    const msg = 'layer "schema-registry": user sam does not own layer "schema-registry" (approve-all needs every required layer)'
    vi.mocked(api.approveAll).mockRejectedValue(new ApiClientError(403, 'not_layer_owner', msg))
    let res: unknown
    await act(async () => {
      res = await hook.result.current.actions.approveAll?.('p1', { params_hash: HASH })
    })
    expect(res).toEqual({ ok: false, refused: true, error: msg })
    expect(hook.result.current.state?.approve_all_refused).toEqual({ p1: msg })
    expect(hook.result.current.error).toBeNull()
  })

  it('409 already_decided goes to the banner and back to the dialog', async () => {
    const hook = await live()
    vi.mocked(api.approveAll).mockRejectedValue(
      new ApiClientError(409, 'already_decided', 'every layer of this proposal is already approved'),
    )
    let res: unknown
    await act(async () => {
      res = await hook.result.current.actions.approveAll?.('p1', { params_hash: HASH })
    })
    expect(res).toEqual({ ok: false, error: 'every layer of this proposal is already approved' })
    expect(hook.result.current.error).toBe('every layer of this proposal is already approved')
    expect(hook.result.current.state?.approve_all_refused).toBeUndefined()
  })

  it('409 params_mismatch raises the stale banner with the hash sent, and reloads', async () => {
    const hook = await live()
    vi.mocked(api.approveAll).mockRejectedValue(new ApiClientError(409, 'params_mismatch', 'the fix changed'))
    let res: unknown
    await act(async () => {
      res = await hook.result.current.actions.approveAll?.('p1', { params_hash: HASH })
    })
    expect(res).toEqual({ ok: false, mismatch: true })
    expect(hook.result.current.stale).toEqual({ proposalId: 'p1', sentHash: HASH })
    expect(hook.result.current.error).toBeNull()
    expect(api.approveAll).toHaveBeenCalledTimes(1)
  })

  it('the solo 400 asks for the typed text and is not an error', async () => {
    const hook = await live()
    vi.mocked(api.approveAll).mockRejectedValue(
      new ApiClientError(400, 'validation', 'solo mode: type confirm "orders/*" to approve'),
    )
    let res: unknown
    await act(async () => {
      res = await hook.result.current.actions.approveAll?.('p1', { params_hash: HASH })
    })
    expect(res).toEqual({ ok: false, needsConfirm: true })
    expect(hook.result.current.error).toBeNull()
  })
})

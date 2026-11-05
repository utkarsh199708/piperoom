import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { act, renderHook, waitFor } from '@testing-library/react'
import type { Event, RoomSnapshot } from '../api/types'
import { ApiClientError } from '../api/client'
import { stagingProposal } from '../fixtures/staging'

// X8e: useRoom's stage action, the 409 staging_required that approve answers, and the room GET it asks
// for again when a proposal or a stage arrives live (what can be staged is not in the log).

vi.mock('sonner', () => ({ toast: Object.assign(vi.fn(), { warning: vi.fn(), dismiss: vi.fn() }) }))
vi.mock('../context/user', () => ({
  useCurrentUser: () => ({
    user: { id: 'arjun', name: 'Arjun', layers: ['kafka-connect'] },
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
    api: { ...actual.api, me: vi.fn(), room: vi.fn(), events: vi.fn(), approve: vi.fn(), approveAll: vi.fn(), stageProposal: vi.fn() },
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
const snap = (over: Partial<RoomSnapshot> = {}): RoomSnapshot => ({
  id: 'r1',
  title: 'T',
  severity: 'sev2',
  pipeline_id: 'orders',
  status: 'open',
  opened_at: '2026-10-22T10:00:00Z',
  last_seq: 0,
  driver: null,
  agent_status: { state: 'idle' },
  hypothesis: null,
  open_proposals: [],
  pending_suggestions: [],
  members: [],
  mode: { write: true, solo: false, auto_approve: [], staging: true },
  ...over,
})
const created: Event = {
  room_id: 'r1',
  seq: 1,
  type: 'proposal.created',
  actor: { kind: 'agent', id: 'agent', name: 'Agent' },
  payload: {
    proposal_id: 'p1',
    title: 'Fix',
    steps: [{ action: 'connect_update_connector_config', layer: 'kafka-connect', params: {}, description: '' }],
    risk: 'low',
    rollback: '',
    required_layers: ['kafka-connect'],
    params_hash: HASH,
  },
  at: '2026-10-22T10:00:01Z',
}

async function live(first: RoomSnapshot = snap(), ...later: RoomSnapshot[]) {
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
  const room = vi.mocked(api.room).mockResolvedValueOnce(first)
  for (const s of later) room.mockResolvedValueOnce(s)
  room.mockResolvedValue(later[later.length - 1] ?? first)
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

describe('useRoom stage', () => {
  it('posts the stage and answers ok; the events do the rest', async () => {
    const hook = await live()
    vi.mocked(api.stageProposal).mockResolvedValue({} as never)
    let res: unknown
    await act(async () => {
      res = await hook.result.current.actions.stage?.('p1')
    })
    expect(api.stageProposal).toHaveBeenCalledWith('p1')
    expect(res).toEqual({ ok: true })
    expect(hook.result.current.error).toBeNull()
  })

  it.each([
    [409, 'stage_in_progress'],
    [409, 'no_staging'],
    [409, 'not_stageable'],
    [429, 'rate_limited'],
    [403, 'not_driver'],
  ])('%i %s comes back to the card with its code and stays out of the banner', async (status, code) => {
    const hook = await live()
    vi.mocked(api.stageProposal).mockRejectedValue(new ApiClientError(status, code, `server says ${code}`))
    let res: unknown
    await act(async () => {
      res = await hook.result.current.actions.stage?.('p1')
    })
    expect(res).toEqual({ ok: false, code, error: `server says ${code}` })
    expect(hook.result.current.error).toBeNull()
  })

  it('a network failure comes back as text', async () => {
    const hook = await live()
    vi.mocked(api.stageProposal).mockRejectedValue(new TypeError('Failed to fetch'))
    let res: { ok: boolean; error?: string } | undefined
    await act(async () => {
      res = await hook.result.current.actions.stage?.('p1')
    })
    expect(res?.ok).toBe(false)
    expect(res?.error).toMatch(/Can't reach Piperoom/)
  })
})

describe('409 staging_required', () => {
  const text = 'step 0 (connect_update_connector_config) needs a healthy stage first'

  it('approve marks the proposal held with the server’s text, and is not a banner', async () => {
    const hook = await live()
    vi.mocked(api.approve).mockRejectedValue(new ApiClientError(409, 'staging_required', text))
    let res: unknown
    await act(async () => {
      res = await hook.result.current.actions.approve('p1', 'kafka-connect', { params_hash: HASH })
    })
    expect(res).toEqual({ ok: false, stagingRequired: true })
    expect(hook.result.current.error).toBeNull()
    expect(hook.result.current.state?.proposals[0]).toMatchObject({ staging_required: true, staging_message: text })
  })

  it('approve-all does the same', async () => {
    const hook = await live()
    vi.mocked(api.approveAll).mockRejectedValue(new ApiClientError(409, 'staging_required', text))
    let res: unknown
    await act(async () => {
      res = await hook.result.current.actions.approveAll?.('p1', { params_hash: HASH })
    })
    expect(res).toEqual({ ok: false, stagingRequired: true, error: text })
    expect(hook.result.current.error).toBeNull()
    expect(hook.result.current.state?.proposals[0].staging_required).toBe(true)
  })
})

describe('the room GET after a live proposal', () => {
  it('takes what can be staged and whether a stage is owed', async () => {
    const stageable = { ...stagingProposal, proposal_id: 'p1', params_hash: HASH, stageable: [0], staging_required: true }
    const hook = await live(snap(), snap({ last_seq: 1, open_proposals: [stageable] }))
    await waitFor(() => expect(hook.result.current.state?.proposals[0].stageable).toEqual([0]))
    expect(hook.result.current.state?.proposals[0].staging_required).toBe(true)
    expect(api.room).toHaveBeenCalledTimes(2)
  })

  it('is not asked for a proposal the first snapshot already covers (replay)', async () => {
    const hook = await live(snap({ last_seq: 1 }))
    expect(hook.result.current.state?.last_seq).toBe(1)
    expect(api.room).toHaveBeenCalledTimes(1)
  })

  it('is not asked when the room has no staging', async () => {
    await live(snap({ mode: { write: true, solo: false, auto_approve: [] } }))
    expect(api.room).toHaveBeenCalledTimes(1)
  })
})

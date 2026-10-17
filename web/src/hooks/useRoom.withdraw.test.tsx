import { beforeEach, describe, expect, it, vi } from 'vitest'
import { act, renderHook, waitFor } from '@testing-library/react'
import type { Event, RoomSnapshot } from '../api/types'

vi.mock('sonner', () => ({
  toast: Object.assign(vi.fn(), { warning: vi.fn(), dismiss: vi.fn() }),
}))

vi.mock('../context/user', () => ({
  useCurrentUser: () => ({
    user: { id: 'kabir', name: 'Kabir', layers: ['consumer'] },
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
    api: { ...actual.api, me: vi.fn(), room: vi.fn(), events: vi.fn(), withdrawProposal: vi.fn(), closeRoom: vi.fn(), approve: vi.fn() },
  }
})

import { ApiClientError, api, openRoomWs } from '../api/client'
import { useRoom } from './useRoom'

class FakeWebSocket {
  static OPEN = 1
  readyState = 1
  onopen: (() => void) | null = null
  onmessage: ((m: { data: string }) => void) | null = null
  onclose: ((ev: { reason: string }) => void) | null = null
  onerror: (() => void) | null = null
  send() {}
  close() {
    this.readyState = 3
  }
}

const snapshot: RoomSnapshot = {
  id: 'r1',
  title: 'T',
  severity: 'sev2',
  pipeline_id: 'orders',
  status: 'open',
  opened_at: '2026-10-15T10:00:00Z',
  last_seq: 0,
  driver: { id: 'kabir', name: 'Kabir' },
  agent_status: { state: 'idle' },
  hypothesis: null,
  open_proposals: [],
  pending_suggestions: [],
  members: [],
}

async function setup() {
  const socket = new FakeWebSocket()
  vi.mocked(api.room).mockResolvedValue(snapshot)
  vi.mocked(openRoomWs).mockReturnValue(socket as unknown as WebSocket)
  const hook = renderHook(() => useRoom('r1'))
  await waitFor(() => expect(openRoomWs).toHaveBeenCalledWith('r1', 0))
  socket.onopen?.()
  return hook
}

beforeEach(() => {
  vi.stubGlobal('WebSocket', FakeWebSocket)
  vi.resetAllMocks()
})

describe('useRoom withdraw (Q0, QA G9)', () => {
  it('sends the reason and resolves ok; the event that follows is what changes the proposal', async () => {
    vi.mocked(api.withdrawProposal).mockResolvedValue({} as Event)
    const { result } = await setup()
    let res: unknown
    await act(async () => {
      res = await result.current.actions.withdraw?.('p1', 'wrong connector')
    })
    expect(res).toEqual({ ok: true })
    expect(api.withdrawProposal).toHaveBeenCalledWith('r1', 'p1', 'wrong connector')
    expect(result.current.error).toBeNull()
  })

  it.each([
    [409, 'proposal_running', 'This fix is already approved or running, so it can no longer be withdrawn'],
    [409, 'proposal_withdrawn', 'This fix was already withdrawn, so it can no longer be decided on'],
    [403, 'not_driver', 'Only the driver can withdraw a fix.'],
    [409, 'room_closed', 'The room is closed'],
    [404, 'not_found', 'No such proposal'],
  ])('a %i %s comes back as a sentence for the card, not in the room banner', async (status, code, text) => {
    vi.mocked(api.withdrawProposal).mockRejectedValue(
      new ApiClientError(status, code, code === 'room_closed' ? 'the room is closed' : code === 'not_found' ? 'No such proposal' : 'raw server text'),
    )
    const { result } = await setup()
    let res: unknown
    await act(async () => {
      res = await result.current.actions.withdraw?.('p1')
    })
    expect(res).toEqual({ ok: false, error: text })
    expect(result.current.error).toBeNull()
  })

  it('a call that never reached the server says so and keeps nothing to retry', async () => {
    vi.mocked(api.withdrawProposal).mockRejectedValue(new TypeError('Failed to fetch'))
    const { result } = await setup()
    let res: { ok: boolean; error?: string } | undefined
    await act(async () => {
      res = await result.current.actions.withdraw?.('p1')
    })
    expect(res?.ok).toBe(false)
    expect(res?.error).toMatch(/not withdrawn/)
    expect(res?.error).not.toMatch(/kept/)
  })
})

describe('useRoom close and approve error text (Q0)', () => {
  it('a 403 not_driver on close says who may close (the layer owners can now)', async () => {
    vi.mocked(api.closeRoom).mockRejectedValue(new ApiClientError(403, 'not_driver', 'Only the driver can close.'))
    const { result } = await setup()
    let res: { ok: boolean; error?: string } | undefined
    await act(async () => {
      res = await result.current.actions.close('fixed')
    })
    expect(res?.ok).toBe(false)
    expect(res?.error).toBe("Only the driver or an approver of one of this pipeline's layers can close the room.")
    expect(result.current.error).toBe(res?.error)
  })

  it('approving a withdrawn proposal shows a readable line in the banner', async () => {
    vi.mocked(api.approve).mockRejectedValue(new ApiClientError(409, 'proposal_withdrawn', 'raw'))
    const { result } = await setup()
    await act(async () => {
      await result.current.actions.approve('p1', 'consumer', { params_hash: 'h' })
    })
    expect(result.current.error).toBe('this fix was already withdrawn, so it can no longer be decided on')
  })
})

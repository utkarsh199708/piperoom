import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { act, renderHook, waitFor } from '@testing-library/react'
import type { RoomSnapshot } from '../api/types'

// KAN-582: a network error during a room action that the send ledger does not keep says the action may not
// have happened. The "kept. Retrying" line is only true for steers and comments (useRoom.offline.test.tsx).

vi.mock('sonner', () => ({ toast: Object.assign(vi.fn(), { warning: vi.fn(), dismiss: vi.fn() }) }))
vi.mock('../context/user', () => ({
  useCurrentUser: () => ({
    user: { id: 'arjun', name: 'Arjun', layers: ['consumer'] },
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
    api: {
      ...actual.api,
      me: vi.fn(),
      room: vi.fn(),
      events: vi.fn(),
      approve: vi.fn(),
      reject: vi.fn(),
      nudge: vi.fn(),
      closeRoom: vi.fn(),
      label: vi.fn(),
      handoff: vi.fn(),
      acceptHandoff: vi.fn(),
      approveAll: vi.fn(),
      cancelProposal: vi.fn(),
      stageProposal: vi.fn(),
      declineSuggestion: vi.fn(),
      sendSuggestion: vi.fn(),
      requestControl: vi.fn(),
      grantControl: vi.fn(),
      hypothesis: vi.fn(),
      checklist: vi.fn(),
    },
  }
})

import { api, openRoomWs } from '../api/client'
import { ACTION_OFFLINE_TEXT, OFFLINE_TEXT } from '../lib/errors'
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

const snapshot: RoomSnapshot = {
  id: 'r1',
  title: 'T',
  severity: 'sev2',
  pipeline_id: 'orders',
  status: 'open',
  opened_at: '2026-10-19T09:58:00Z',
  last_seq: 0,
  driver: null,
  agent_status: { state: 'idle' },
  hypothesis: null,
  open_proposals: [],
  pending_suggestions: [],
  members: [],
  mode: { write: true, solo: false, auto_approve: [] },
}

async function live(snap: RoomSnapshot = snapshot) {
  vi.mocked(openRoomWs).mockImplementation(() => {
    const sock = new FakeWebSocket()
    queueMicrotask(() => {
      sock.readyState = FakeWebSocket.OPEN
      sock.onopen?.()
    })
    return sock as unknown as WebSocket
  })
  vi.mocked(api.me).mockResolvedValue(undefined as never)
  vi.mocked(api.room).mockResolvedValue(snap)
  const hook = renderHook(() => useRoom('r1'))
  await waitFor(() => expect(hook.result.current.conn).toBe('live'))
  return hook
}

function expectActionOffline(text: string | null | undefined) {
  expect(text).toBe(ACTION_OFFLINE_TEXT)
  expect(text).not.toBe(OFFLINE_TEXT)
  expect(text).not.toMatch(/kept|retrying/i)
}

beforeEach(() => {
  vi.stubGlobal('WebSocket', FakeWebSocket)
  vi.resetAllMocks()
})
afterEach(() => {
  vi.unstubAllGlobals()
})

describe('useRoom actions on a network error', () => {
  const down = () => new TypeError('Failed to fetch')

  it('approve', async () => {
    const hook = await live()
    vi.mocked(api.approve).mockRejectedValue(down())
    await act(async () => {
      await hook.result.current.actions.approve('p1', 'consumer', {} as never)
    })
    expectActionOffline(hook.result.current.error)
  })

  it('reject', async () => {
    const hook = await live()
    vi.mocked(api.reject).mockRejectedValue(down())
    act(() => hook.result.current.actions.reject('p1', 'consumer', 'no', 'h'))
    await waitFor(() => expect(hook.result.current.error).not.toBeNull())
    expectActionOffline(hook.result.current.error)
  })

  it('nudge', async () => {
    const hook = await live()
    vi.mocked(api.nudge).mockRejectedValue(down())
    await act(async () => {
      await hook.result.current.actions.nudge('p1', 'consumer')
    })
    expectActionOffline(hook.result.current.error)
  })

  it('close', async () => {
    const hook = await live()
    vi.mocked(api.closeRoom).mockRejectedValue(down())
    let res: { ok: boolean; error?: string } | undefined
    await act(async () => {
      res = await hook.result.current.actions.close('fixed')
    })
    expectActionOffline(res?.error)
    expectActionOffline(hook.result.current.error)
  })

  it('label', async () => {
    const hook = await live()
    vi.mocked(api.label).mockRejectedValue(down())
    let res: { ok: boolean; error?: string } | undefined
    await act(async () => {
      res = await hook.result.current.actions.label({} as never)
    })
    expectActionOffline(res?.error)
  })

  it('handoff', async () => {
    const hook = await live()
    vi.mocked(api.handoff).mockRejectedValue(down())
    act(() => hook.result.current.actions.handoff('bea', false))
    await waitFor(() => expect(hook.result.current.error).not.toBeNull())
    expectActionOffline(hook.result.current.error)
  })

  it('acceptHandoff', async () => {
    const hook = await live()
    vi.mocked(api.acceptHandoff).mockRejectedValue(down())
    await act(async () => {
      await hook.result.current.actions.acceptHandoff('h1')
    })
    expectActionOffline(hook.result.current.error)
  })

  it('approveAll', async () => {
    const hook = await live()
    vi.mocked(api.approveAll).mockRejectedValue(down())
    let res: { ok: boolean; error?: string } | undefined
    await act(async () => {
      res = await hook.result.current.actions.approveAll!('p1', {} as never)
    })
    expectActionOffline(res?.error)
    expectActionOffline(hook.result.current.error)
  })

  it('cancelSchedule', async () => {
    const hook = await live()
    vi.mocked(api.cancelProposal).mockRejectedValue(down())
    let res: { ok: boolean; error?: string } | undefined
    await act(async () => {
      res = await hook.result.current.actions.cancelSchedule!('p1', 'x')
    })
    expectActionOffline(res?.error)
    expectActionOffline(hook.result.current.error)
  })

  it('stage', async () => {
    const hook = await live()
    vi.mocked(api.stageProposal).mockRejectedValue(down())
    let res: { ok: boolean; error?: string } | undefined
    await act(async () => {
      res = await hook.result.current.actions.stage!('p1')
    })
    expectActionOffline(res?.error)
  })

  it('declineSuggestion', async () => {
    const hook = await live()
    vi.mocked(api.declineSuggestion).mockRejectedValue(down())
    act(() => hook.result.current.actions.declineSuggestion('s1', 'no'))
    await waitFor(() => expect(hook.result.current.error).not.toBeNull())
    expectActionOffline(hook.result.current.error)
  })

  it('sendSuggestion', async () => {
    const hook = await live()
    vi.mocked(api.sendSuggestion).mockRejectedValue(down())
    act(() => hook.result.current.actions.sendSuggestion('s1'))
    await waitFor(() => expect(hook.result.current.error).not.toBeNull())
    expectActionOffline(hook.result.current.error)
  })

  it('requestControl', async () => {
    const hook = await live()
    vi.mocked(api.requestControl).mockRejectedValue(down())
    act(() => hook.result.current.actions.requestControl())
    await waitFor(() => expect(hook.result.current.error).not.toBeNull())
    expectActionOffline(hook.result.current.error)
  })

  it('grantControl', async () => {
    const hook = await live()
    vi.mocked(api.grantControl).mockRejectedValue(down())
    act(() => hook.result.current.actions.grantControl('bea'))
    await waitFor(() => expect(hook.result.current.error).not.toBeNull())
    expectActionOffline(hook.result.current.error)
  })

  it('hypothesisVerdict', async () => {
    const hook = await live({
      ...snapshot,
      hypothesis: { text: 'h', based_on_steps: [], confidence: 'high', confirmed: false, seq: 3 },
    })
    vi.mocked(api.hypothesis).mockRejectedValue(down())
    act(() => hook.result.current.actions.hypothesisVerdict('confirm' as never))
    await waitFor(() => expect(hook.result.current.error).not.toBeNull())
    expectActionOffline(hook.result.current.error)
  })

  it('checklist', async () => {
    const hook = await live()
    vi.mocked(api.checklist).mockRejectedValue(down())
    act(() =>
      hook.result.current.actions.checklist({ step_id: 's', tool_use_id: 't', hit: 0, item: 0, done: true }),
    )
    await waitFor(() => expect(hook.result.current.error).not.toBeNull())
    expectActionOffline(hook.result.current.error)
  })

  it('keeps a server error message unchanged', async () => {
    const hook = await live()
    vi.mocked(api.closeRoom).mockRejectedValue(new Error('boom'))
    await act(async () => {
      await hook.result.current.actions.close('fixed')
    })
    expect(hook.result.current.error).toBe('boom')
  })
})

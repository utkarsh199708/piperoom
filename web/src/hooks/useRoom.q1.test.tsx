// Q1-web-b: what page, fixNote and handoff return to the dialogs that call them, and what each puts in the room banner.
import { beforeEach, describe, expect, it, vi } from 'vitest'
import { act, renderHook, waitFor } from '@testing-library/react'
import type { RoomSnapshot } from '../api/types'

vi.mock('sonner', () => ({
  toast: Object.assign(vi.fn(), { warning: vi.fn(), dismiss: vi.fn() }),
}))

vi.mock('../context/user', () => ({
  useCurrentUser: () => ({
    user: { id: 'arjun', name: 'Arjun', layers: [] },
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
    api: { ...actual.api, room: vi.fn(), events: vi.fn(), me: vi.fn(), page: vi.fn(), comment: vi.fn(), handoff: vi.fn() },
  }
})

import { ApiClientError, api, openRoomWs } from '../api/client'
import { useRoom } from './useRoom'

class FakeWebSocket {
  static OPEN = 1
  readyState = FakeWebSocket.OPEN
  onopen: (() => void) | null = null
  onmessage: ((m: { data: string }) => void) | null = null
  onclose: ((ev: { reason: string }) => void) | null = null
  onerror: (() => void) | null = null
  send() {}
  close() {}
}

const snapshot: RoomSnapshot = {
  id: 'r1',
  title: 'T',
  severity: 'sev2',
  pipeline_id: 'orders',
  status: 'open',
  opened_at: '2026-10-11T10:00:00Z',
  last_seq: 0,
  driver: null,
  agent_status: { state: 'idle' },
  hypothesis: null,
  open_proposals: [],
  pending_suggestions: [],
  members: [],
}

async function ready() {
  vi.mocked(api.room).mockResolvedValue(snapshot)
  vi.mocked(openRoomWs).mockReturnValue(new FakeWebSocket() as unknown as WebSocket)
  const hook = renderHook(() => useRoom('r1'))
  await waitFor(() => expect(hook.result.current.state).not.toBeNull())
  return hook
}

beforeEach(() => {
  vi.stubGlobal('WebSocket', FakeWebSocket)
  vi.resetAllMocks()
})

describe('page', () => {
  it('posts the page and resolves ok, leaving the room banner alone', async () => {
    const { result } = await ready()
    vi.mocked(api.page).mockResolvedValue({} as never)
    let res
    await act(async () => {
      res = await result.current.actions.page?.({ layer: 'consumer', reason: 'lag' })
    })
    expect(res).toEqual({ ok: true })
    expect(api.page).toHaveBeenCalledWith('r1', { layer: 'consumer', reason: 'lag' })
    expect(result.current.error).toBeNull()
  })

  it('returns already_paged in the server words and does not put it in the banner', async () => {
    const { result } = await ready()
    vi.mocked(api.page).mockRejectedValue(new ApiClientError(409, 'already_paged', 'consumer was paged 3 min ago'))
    let res
    await act(async () => {
      res = await result.current.actions.page?.({ layer: 'consumer', reason: 'lag' })
    })
    expect(res).toEqual({ ok: false, error: 'consumer was paged 3 min ago' })
    expect(result.current.error).toBeNull()
  })

  it('a network failure reads as such', async () => {
    const { result } = await ready()
    vi.mocked(api.page).mockRejectedValue(new TypeError('Failed to fetch'))
    let res: { ok: boolean; error?: string } | undefined
    await act(async () => {
      res = await result.current.actions.page?.({ layer: 'kafka', reason: 'r' })
    })
    expect(res?.ok).toBe(false)
    expect(res?.error).toMatch(/Can't reach Piperoom/)
  })
})

describe('fixNote', () => {
  it('sends a comment of kind fix_note', async () => {
    const { result } = await ready()
    vi.mocked(api.comment).mockResolvedValue({} as never)
    let res
    await act(async () => {
      res = await result.current.actions.fixNote?.('Restarted order-sync by hand')
    })
    expect(res).toEqual({ ok: true })
    expect(api.comment).toHaveBeenCalledWith('r1', { text: 'Restarted order-sync by hand', kind: 'fix_note' })
  })

  it('returns a refusal to the dialog instead of queueing it for a retry', async () => {
    const { result } = await ready()
    vi.mocked(api.comment).mockRejectedValue(new ApiClientError(403, 'not_allowed', 'only the closer can add a fix note'))
    let res
    await act(async () => {
      res = await result.current.actions.fixNote?.('x')
    })
    expect(res).toEqual({ ok: false, error: 'only the closer can add a fix note' })
    expect(result.current.error).toBeNull()
  })
})

describe('handoff', () => {
  it('resolves ok when the server took it', async () => {
    const { result } = await ready()
    vi.mocked(api.handoff).mockResolvedValue({} as never)
    let res
    await act(async () => {
      res = await result.current.actions.handoff('priya', true)
    })
    expect(res).toEqual({ ok: true })
    expect(api.handoff).toHaveBeenCalledWith('r1', 'priya', true, undefined)
  })

  it('passes the summary the driver wrote', async () => {
    const { result } = await ready()
    vi.mocked(api.handoff).mockResolvedValue({} as never)
    const summary = { what_broke: 'b', why: 'w', done: [], ruled_out: [], open: [], risks: [] }
    await act(async () => {
      await result.current.actions.handoff('priya', false, summary)
    })
    expect(api.handoff).toHaveBeenCalledWith('r1', 'priya', false, summary)
  })

  it('503 summary_failed: the code reaches the dialog, and the banner says what to do', async () => {
    const { result } = await ready()
    vi.mocked(api.handoff).mockRejectedValue(new ApiClientError(503, 'summary_failed', 'the agent could not write a summary'))
    let res: { ok: boolean; code?: string; error?: string } | undefined
    await act(async () => {
      res = await result.current.actions.handoff('priya', false)
    })
    expect(res?.ok).toBe(false)
    expect(res?.code).toBe('summary_failed')
    expect(result.current.error).toMatch(/write the summary yourself/)
  })

  it('a refusal of the driver\'s own summary comes back to the dialog and stays out of the banner', async () => {
    const { result } = await ready()
    vi.mocked(api.handoff).mockRejectedValue(new ApiClientError(400, 'validation', 'what_broke is too long'))
    let res: { ok: boolean; code?: string; error?: string } | undefined
    await act(async () => {
      res = await result.current.actions.handoff('priya', false, { what_broke: 'b', why: 'w', done: [], ruled_out: [], open: [], risks: [] })
    })
    expect(res).toEqual({ ok: false, code: 'validation', error: 'what_broke is too long' })
    expect(result.current.error).toBeNull()
  })

  it('a refusal without a code still comes back as a failure', async () => {
    const { result } = await ready()
    vi.mocked(api.handoff).mockRejectedValue(new TypeError('Failed to fetch'))
    let res: { ok: boolean; code?: string } | undefined
    await act(async () => {
      res = await result.current.actions.handoff('priya', false)
    })
    expect(res?.ok).toBe(false)
    expect(res?.code).toBeUndefined()
  })
})

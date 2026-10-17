import { describe, expect, it, vi } from 'vitest'
import { act, fireEvent, render, screen } from '@testing-library/react'
import { SteerBox } from './SteerBox'
import type { DeliveredListener, RoomActions, SendControls } from '../hooks/useRoom'
import type { CurrentUser } from '../state/permissions'
import { emptyRoomState } from '../state/types'
import { readSteerAction, writeSteerAction } from './room/steerAction'
import type { SteerAction } from './room/steerAction'

// #550 review: the guards on an action a remounted SteerBox takes over from the room's record.
// The record is a pointer; the ledger (here a fake) decides whether the action is still owed.

const arjun: CurrentUser = { id: 'arjun', name: 'Arjun', layers: ['kafka'] }
const room = () => ({
  ...emptyRoomState('r1'),
  driver: { id: 'arjun', name: 'Arjun', lease_until: new Date(Date.now() + 20 * 60_000).toISOString() },
})

/** A fake ledger handle: `owed` is what isPending answers; cancel removes from it. */
function fakeSends(owed: string[]) {
  const pending = new Set(owed)
  const listeners = new Set<DeliveredListener>()
  const cancelled: string[] = []
  const sends: SendControls = {
    onDelivered: (l) => {
      listeners.add(l)
      return () => {
        listeners.delete(l)
      }
    },
    cancel: (id) => {
      cancelled.push(id)
      pending.delete(id)
    },
    isPending: (id) => pending.has(id),
  }
  return { sends, cancelled }
}

function actionsWith(sends: SendControls, steer = vi.fn().mockResolvedValue(true)): RoomActions {
  return {
    comment: vi.fn(),
    steer,
    sends,
    sendSuggestion: vi.fn(),
    declineSuggestion: vi.fn(),
    requestControl: vi.fn(),
    grantControl: vi.fn(),
    close: vi.fn(),
    approve: vi.fn(),
    reject: vi.fn(),
    handoff: vi.fn(),
    acceptHandoff: vi.fn(),
    label: vi.fn(),
    checklist: vi.fn(),
    trace: vi.fn(),
    hypothesisVerdict: vi.fn(),
    nudge: vi.fn().mockResolvedValue({ ok: true }),
  }
}

/** A kept steer left by a box that a reload unmounted, with its words still in the draft. */
function keptRecord(over: Partial<SteerAction> = {}) {
  const a: SteerAction = {
    clientId: 'X',
    userId: 'arjun',
    kind: 'steer',
    text: 'stop the connector',
    typed: 'stop the connector',
    phase: 'kept',
    ...over,
  }
  writeSteerAction('r1', a)
  localStorage.setItem('piperoom.draft.r1', a.typed)
}

const field = () => screen.getByLabelText<HTMLTextAreaElement>('Steer the agent')
const retry = () => screen.queryByRole('button', { name: 'Retry' })
const sentIds = (steer: ReturnType<typeof vi.fn>) =>
  steer.mock.calls.map((c) => (c[1] as { clientId?: string } | undefined)?.clientId)

async function clickSend() {
  fireEvent.focus(field())
  await act(async () => {
    fireEvent.click(screen.getByRole('button', { name: /Send to agent/ }))
    await Promise.resolve()
  })
}

describe('SteerBox takes over only an action the ledger still owes to this user (#550)', () => {
  // Guard: takeOverSteerAction's `sends.isPending` check.
  it('(a) a record the ledger no longer owes is not taken over: no Retry, record dropped, Send is a new action', async () => {
    keptRecord()
    const { sends } = fakeSends([])
    const steer = vi.fn().mockResolvedValue(true)
    render(<SteerBox state={room()} user={arjun} actions={actionsWith(sends, steer)} />)
    expect(field().value).toBe('stop the connector') // the words stay as a draft
    expect(retry()).toBeNull()
    expect(readSteerAction('r1')).toBeNull()
    await clickSend()
    expect(steer).toHaveBeenCalledTimes(1)
    expect(sentIds(steer)[0]).not.toBe('X')
  })

  // Guard: takeOverSteerAction's `userId` check.
  it("(a2) another user's record is never taken over, even while the ledger answers isPending for its id", async () => {
    keptRecord({ userId: 'priya', typed: 'priya says stop', text: 'priya says stop' })
    localStorage.setItem('piperoom.draft.r1', 'priya says stop')
    const { sends } = fakeSends(['X'])
    const steer = vi.fn().mockResolvedValue(true)
    render(<SteerBox state={room()} user={arjun} actions={actionsWith(sends, steer)} />)
    expect(retry()).toBeNull()
    expect(readSteerAction('r1')).toBeNull()
    await clickSend()
    expect(sentIds(steer)[0]).not.toBe('X')
  })

  it("(a3) another user's delivered record does not clear this user's field", () => {
    keptRecord({ userId: 'priya', phase: 'delivered' })
    const { sends } = fakeSends([])
    render(<SteerBox state={room()} user={arjun} actions={actionsWith(sends)} />)
    expect(field().value).toBe('stop the connector')
    expect(readSteerAction('r1')).toBeNull()
  })

  // Guard: dropAdopted in `edit` (typing, chip, recall) and in the prefill effect.
  it('(b) an edit cancels the taken-over action and drops its record', () => {
    keptRecord()
    const fake = fakeSends(['X'])
    render(<SteerBox state={room()} user={arjun} actions={actionsWith(fake.sends)} />)
    expect(retry()).toBeInTheDocument()
    fireEvent.change(field(), { target: { value: 'do NOT stop the connector' } })
    expect(fake.cancelled).toEqual(['X'])
    expect(readSteerAction('r1')).toBeNull()
  })

  it('(b) a chip cancels the taken-over action and drops its record', () => {
    keptRecord()
    const fake = fakeSends(['X'])
    render(<SteerBox state={room()} user={arjun} actions={actionsWith(fake.sends)} />)
    fireEvent.click(screen.getByRole('button', { name: 'Check the connector status' }))
    expect(fake.cancelled).toEqual(['X'])
    expect(readSteerAction('r1')).toBeNull()
  })

  it('(b) a prefill cancels the taken-over action and drops its record', () => {
    keptRecord()
    const fake = fakeSends(['X'])
    const actions = actionsWith(fake.sends)
    const view = render(<SteerBox state={room()} user={arjun} actions={actions} />)
    view.rerender(<SteerBox state={room()} user={arjun} actions={actions} prefill={{ text: 'Why is lag growing?', n: 1 }} />)
    expect(field().value).toBe('Why is lag growing?')
    expect(fake.cancelled).toEqual(['X'])
    expect(readSteerAction('r1')).toBeNull()
  })

  // Guard: `adoptedRef.current = null` when the ledger changes (#536).
  it('(c) a new ledger owes nothing of the old one: no Retry, and the next Send does not reuse the id', async () => {
    keptRecord()
    const old = fakeSends(['X'])
    const steer = vi.fn().mockResolvedValue(true)
    const view = render(<SteerBox state={room()} user={arjun} actions={actionsWith(old.sends, steer)} />)
    expect(retry()).toBeInTheDocument()
    // The new ledger answers isPending for X too, so only the reset keeps X from being reused.
    const next = fakeSends(['X'])
    view.rerender(<SteerBox state={room()} user={arjun} actions={actionsWith(next.sends, steer)} />)
    expect(retry()).toBeNull()
    // The old box's send ending late (its record says kept) does not bring the strip back.
    act(() => keptRecord())
    expect(retry()).toBeNull()
    // An edit has nothing of the new ledger's to cancel.
    fireEvent.change(field(), { target: { value: 'stop the connector now' } })
    expect(next.cancelled).toEqual([])
    await clickSend()
    expect(steer).toHaveBeenCalledTimes(1)
    expect(sentIds(steer)[0]).not.toBe('X')
  })

  // Guard: the `retryAdopted` path in send: a Retry of the taken-over action keeps its id.
  it('(d) a Retry of the taken-over action that fails again keeps the strip and the id', async () => {
    keptRecord()
    const fake = fakeSends(['X'])
    const steer = vi.fn().mockResolvedValue(false)
    render(<SteerBox state={room()} user={arjun} actions={actionsWith(fake.sends, steer)} />)
    await act(async () => {
      fireEvent.click(retry()!)
      await Promise.resolve()
    })
    expect(retry()).toBeInTheDocument()
    await act(async () => {
      fireEvent.click(retry()!)
      await Promise.resolve()
    })
    expect(sentIds(steer)).toEqual(['X', 'X'])
    expect(retry()).toBeInTheDocument()
    expect(readSteerAction('r1')?.phase).toBe('kept')
  })

  // Nit 4: an edit of the composer's own kept action drops its record (no lingering watcher).
  it("an edit of the box's own kept action drops its record", async () => {
    const fake = fakeSends([])
    const steer = vi.fn().mockImplementation(async (_t: string, o?: { clientId?: string }) => {
      if (o?.clientId) fake.sends.isPending = (id) => id === o.clientId // kept by the ledger
      return false
    })
    render(<SteerBox state={room()} user={arjun} actions={actionsWith(fake.sends, steer)} />)
    fireEvent.change(field(), { target: { value: 'restart the connector' } })
    await clickSend()
    expect(readSteerAction('r1')?.phase).toBe('kept')
    fireEvent.change(field(), { target: { value: 'do NOT restart the connector' } })
    expect(readSteerAction('r1')).toBeNull()
  })
})

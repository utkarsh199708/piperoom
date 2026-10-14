import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { act, fireEvent, render, renderHook, screen } from '@testing-library/react'
import { CatchUpStrip } from './CatchUpStrip'
import { useCatchUp } from '@/hooks/useCatchUp'
import type { ConnState } from '@/hooks/useRoom'
import type { CurrentUser } from '@/state/permissions'
import { emptyRoomState } from '@/state/types'
import type { RoomState } from '@/state/types'

const me: CurrentUser = { id: 'arjun', name: 'Arjun', layers: ['kafka-connect'] }
const step = (id: string) => ({ id, title: id, tool_calls: [], messages: [] })
const hyp = { text: 'h', based_on_steps: [], confidence: 'high', confirmed: false }

function setHidden(hidden: boolean) {
  Object.defineProperty(document, 'hidden', { configurable: true, get: () => hidden })
  document.dispatchEvent(new Event('visibilitychange'))
}

beforeEach(() => {
  vi.useFakeTimers()
  setHidden(false)
})
afterEach(() => {
  vi.useRealTimers()
  setHidden(false)
})

function setup(initial: RoomState, conn: ConnState = 'live') {
  return renderHook(({ state, conn }) => useCatchUp(state, conn, me), { initialProps: { state: initial, conn } })
}

describe('useCatchUp', () => {
  it('summarises a 60 s+ hide with at least one item, exactly as the strip reads', () => {
    const start = emptyRoomState('r1')
    const { result, rerender } = setup(start)
    act(() => setHidden(true))
    act(() => void vi.advanceTimersByTime(61_000))
    rerender({ state: { ...start, steps: [step('a'), step('b'), step('c')], hypothesis: hyp }, conn: 'live' })
    act(() => setHidden(false))
    expect(result.current.strip).toEqual({
      kind: 'away',
      text: 'While you were away (1m): 3 steps · hypothesis → high',
      target: 'hypothesis',
    })
  })

  it('shows nothing under 60 s, or with nothing to say', () => {
    const start = emptyRoomState('r1')
    const { result, rerender } = setup(start)
    act(() => setHidden(true))
    act(() => void vi.advanceTimersByTime(30_000))
    rerender({ state: { ...start, steps: [step('a')] }, conn: 'live' })
    act(() => setHidden(false))
    expect(result.current.strip).toBeNull()

    act(() => setHidden(true))
    act(() => void vi.advanceTimersByTime(120_000))
    act(() => setHidden(false))
    expect(result.current.strip).toBeNull()
  })

  it('stays dismissed for the same hide and comes back for the next one', () => {
    const start = emptyRoomState('r1')
    const { result, rerender } = setup(start)
    act(() => setHidden(true))
    act(() => void vi.advanceTimersByTime(90_000))
    rerender({ state: { ...start, steps: [step('a')] }, conn: 'live' })
    act(() => setHidden(false))
    expect(result.current.strip?.kind).toBe('away')
    act(() => result.current.dismiss())
    expect(result.current.strip).toBeNull()
    rerender({ state: { ...start, steps: [step('a'), step('b')] }, conn: 'live' })
    expect(result.current.strip).toBeNull()

    act(() => setHidden(true))
    act(() => void vi.advanceTimersByTime(90_000))
    rerender({ state: { ...start, steps: [step('a'), step('b'), step('c')] }, conn: 'live' })
    act(() => setHidden(false))
    expect(result.current.strip?.text).toBe('While you were away (2m): 1 step')
  })

  it('shows the pending reconnect with the last seq, then "Caught up" for 2 s', () => {
    const start = { ...emptyRoomState('r1'), last_seq: 31 }
    const { result, rerender } = setup(start)
    rerender({ state: start, conn: 'reconnecting' })
    expect(result.current.strip).toEqual({ kind: 'reconnecting', text: 'Reconnecting… events after #31 pending' })
    rerender({ state: start, conn: 'live' })
    expect(result.current.strip).toEqual({ kind: 'caught_up', text: 'Caught up' })
    act(() => void vi.advanceTimersByTime(2_100))
    expect(result.current.strip).toBeNull()
  })
})

describe('CatchUpStrip', () => {
  it('renders nothing without a strip', () => {
    const { container } = render(<CatchUpStrip strip={null} onDismiss={vi.fn()} />)
    expect(container).toBeEmptyDOMElement()
  })

  it('Show me scrolls to the target, X and Esc dismiss', () => {
    const target = document.createElement('div')
    target.id = 'hypothesis-heading'
    target.scrollIntoView = vi.fn()
    document.body.appendChild(target)
    const onDismiss = vi.fn()
    render(<CatchUpStrip strip={{ kind: 'away', text: 'While you were away (2m): 1 step', target: 'hypothesis' }} onDismiss={onDismiss} />)
    const strip = screen.getByRole('status')
    expect(strip).toHaveTextContent('While you were away (2m): 1 step')
    fireEvent.click(screen.getByRole('button', { name: 'Show me' }))
    expect(target.scrollIntoView).toHaveBeenCalled()
    fireEvent.click(screen.getByRole('button', { name: 'Dismiss' }))
    fireEvent.keyDown(strip, { key: 'Escape' })
    expect(onDismiss).toHaveBeenCalledTimes(2)
    target.remove()
  })

  it('has no buttons for the reconnect text', () => {
    render(<CatchUpStrip strip={{ kind: 'reconnecting', text: 'Reconnecting… events after #31 pending' }} onDismiss={vi.fn()} />)
    expect(screen.queryByRole('button')).not.toBeInTheDocument()
  })
})

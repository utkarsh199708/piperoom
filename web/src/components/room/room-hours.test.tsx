import { act, fireEvent, render, renderHook, screen } from '@testing-library/react'
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { emptyRoomState, fromSnapshot } from '../../state/types'
import type { RoomState } from '../../state/types'
import type { RoomSnapshot } from '../../api/types'
import { useRoomAttention } from '../../hooks/useRoomAttention'
import { useJumpToLatest } from '../../hooks/useJumpToLatest'
import { CollapsibleText } from './CollapsibleText'
import { NotifyToggle } from './NotifyToggle'
import { ReplayProgress } from './ReplayProgress'
import { SuggestionsPanel } from './SuggestionsPanel'

const kabir = { kind: 'human' as const, id: 'kabir', name: 'Kabir' }

describe('CollapsibleText', () => {
  it('short text has no toggle', () => {
    render(<CollapsibleText text="all good" />)
    expect(screen.queryByRole('button')).toBeNull()
  })
  it('long text starts collapsed and expands on click', () => {
    render(<CollapsibleText text={'word '.repeat(100)} />)
    const btn = screen.getByRole('button', { name: 'Show more' })
    expect(btn).toHaveAttribute('aria-expanded', 'false')
    fireEvent.click(btn)
    expect(screen.getByRole('button', { name: 'Show less' })).toHaveAttribute('aria-expanded', 'true')
  })
})

describe('SuggestionsPanel', () => {
  it('shows pending and accepted; says nothing about declined until an event exists', () => {
    render(
      <SuggestionsPanel
        suggestions={[
          { suggestion_id: 'g1', from: kabir, text: 'check config', status: 'pending' },
          { suggestion_id: 'g2', from: kabir, text: 'restart task', status: 'accepted' },
        ]}
      />,
    )
    expect(screen.getByText('Pending')).toBeInTheDocument()
    expect(screen.getByText('Sent to agent')).toBeInTheDocument()
    expect(screen.queryByText('Declined')).toBeNull()
  })
  it('renders nothing without suggestions', () => {
    const { container } = render(<SuggestionsPanel suggestions={[]} />)
    expect(container).toBeEmptyDOMElement()
  })
})

describe('ReplayProgress', () => {
  it('shows event n of total', () => {
    render(<ReplayProgress done={30} total={120} />)
    expect(screen.getByRole('status')).toHaveTextContent('event 30 of 120')
  })
})

describe('replay_target', () => {
  it('comes from the snapshot last_seq while last_seq stays 0', () => {
    const snap = { id: 'r1', title: 't', severity: 'sev2', pipeline_id: 'orders', status: 'open', last_seq: 42,
      agent_status: { state: 'idle' } } as unknown as RoomSnapshot
    const s = fromSnapshot(snap)
    expect(s.replay_target).toBe(42)
    expect(s.last_seq).toBe(0)
  })
})

describe('NotifyToggle', () => {
  it('asks only when the button is clicked', () => {
    const enable = vi.fn()
    render(<NotifyToggle permission="default" onEnable={enable} />)
    expect(enable).not.toHaveBeenCalled()
    fireEvent.click(screen.getByRole('button', { name: /Notify me/ }))
    expect(enable).toHaveBeenCalledTimes(1)
  })
  it('hides when unsupported and explains a block', () => {
    const { container, rerender } = render(<NotifyToggle permission="unsupported" onEnable={vi.fn()} />)
    expect(container).toBeEmptyDOMElement()
    rerender(<NotifyToggle permission="denied" onEnable={vi.fn()} />)
    expect(screen.getByText(/blocked in your browser/)).toBeInTheDocument()
  })
})

describe('useRoomAttention', () => {
  const user = { id: 'meera', name: 'Meera', layers: ['warehouse'] }
  const proposal = {
    proposal_id: 'p1', title: 'Reset', steps: [], risk: '', rollback: '', required_layers: ['warehouse'],
    approvals: { warehouse: 'pending' as const }, decisions: {}, params_hash: '', status: 'open' as const, executed: [],
  }
  let hidden = true
  const notes: Array<[string, unknown]> = []
  beforeEach(() => {
    hidden = true
    notes.length = 0
    document.title = 'Piperoom'
    Object.defineProperty(document, 'hidden', { configurable: true, get: () => hidden })
    class N {
      static permission = 'granted'
      static requestPermission = vi.fn()
      constructor(title: string, opts: unknown) {
        notes.push([title, opts])
      }
    }
    vi.stubGlobal('Notification', N)
  })
  afterEach(() => vi.unstubAllGlobals())

  const run = (replaying: boolean, states: RoomState[]) =>
    renderHook(({ s, r }) => useRoomAttention(s, user, r), { initialProps: { s: states[0], r: replaying } })

  it('badges the title and notifies while hidden, then clears when shown', () => {
    const a = { ...emptyRoomState('r1'), title: 'Orders' }
    const h = run(false, [a])
    act(() => h.rerender({ s: { ...a, proposals: [proposal] }, r: false }))
    expect(h.result.current.unread).toBe(1)
    expect(document.title).toBe('(1) Piperoom')
    expect(notes[0][0]).toMatch(/Your approval is needed/)
    hidden = false
    act(() => {
      document.dispatchEvent(new Event('visibilitychange'))
    })
    expect(h.result.current.unread).toBe(0)
    expect(document.title).toBe('Piperoom')
  })

  it('stays quiet during replay and while the tab is visible', () => {
    const a = emptyRoomState('r1')
    const h = run(true, [a])
    act(() => h.rerender({ s: { ...a, proposals: [proposal] }, r: true }))
    expect(h.result.current.unread).toBe(0)
    hidden = false
    act(() => h.rerender({ s: { ...a, proposals: [proposal, { ...proposal, proposal_id: 'p2' }] }, r: false }))
    expect(h.result.current.unread).toBe(0)
    expect(notes).toHaveLength(0)
  })
})

describe('useJumpToLatest', () => {
  let cb: (e: Array<{ isIntersecting: boolean }>) => void
  beforeEach(() => {
    vi.stubGlobal(
      'IntersectionObserver',
      class {
        constructor(c: typeof cb) {
          cb = c
        }
        observe() {}
        disconnect() {}
      },
    )
  })
  afterEach(() => vi.unstubAllGlobals())

  const act1 = (seq: number) => ({ seq, at: '', actor: kabir, text: 'x' })

  function Harness({ activity, replaying = false }: { activity: ReturnType<typeof act1>[]; replaying?: boolean }) {
    const { sentinelRef, unseen, jump } = useJumpToLatest(activity, replaying)
    return (
      <>
        <div ref={sentinelRef} />
        {unseen > 0 && <button onClick={jump}>Jump {unseen}</button>}
      </>
    )
  }

  it('counts events that arrive while scrolled up and clears on jump', () => {
    const { rerender } = render(<Harness activity={[act1(1)]} />)
    act(() => cb([{ isIntersecting: false }]))
    rerender(<Harness activity={[act1(1), act1(2), act1(3)]} />)
    const btn = screen.getByRole('button', { name: 'Jump 2' })
    fireEvent.click(btn)
    expect(screen.queryByRole('button')).toBeNull()
  })

  it('shows nothing when at the bottom or while replaying', () => {
    const { rerender } = render(<Harness activity={[act1(1)]} />)
    rerender(<Harness activity={[act1(1), act1(2)]} />)
    expect(screen.queryByRole('button')).toBeNull()
    act(() => cb([{ isIntersecting: false }]))
    rerender(<Harness activity={[act1(1), act1(2), act1(3)]} replaying />)
    expect(screen.queryByRole('button')).toBeNull()
  })
})

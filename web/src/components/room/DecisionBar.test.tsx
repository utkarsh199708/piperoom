import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { act, fireEvent, render, screen } from '@testing-library/react'
import { DecisionBar } from './DecisionBar'
import { DecisionHandleContext } from '../shell/context'
import type { DecisionHandle } from '../shell/context'
import { emptyRoomState } from '../../state/types'
import type { ProposalState, RoomState } from '../../state/types'
import type { CurrentUser } from '../../state/permissions'

const arjun: CurrentUser = { id: 'arjun', name: 'Arjun', layers: ['kafka-connect'] }
const step = (layer: string, action: string) => ({ action, layer, params: {}, description: '' })
const prop = (over: Partial<ProposalState> = {}): ProposalState => ({
  proposal_id: 'p1',
  title: 'Restart the task',
  steps: [step('schema-registry', 'set_compatibility'), step('kafka-connect', 'restart_connector_task')],
  risk: 'low',
  rollback: '',
  required_layers: ['schema-registry', 'kafka-connect'],
  approvals: { 'schema-registry': 'approved', 'kafka-connect': 'pending' },
  decisions: {},
  params_hash: 'h',
  status: 'open',
  executed: [],
  ...over,
})
const room = (p: ProposalState): RoomState => ({ ...emptyRoomState('r'), proposals: [p] })
const ex = (i: number, ok = true) => ({ proposal_id: 'p1', step_index: i, ok, output: '' })
const handle: DecisionHandle = { approve: vi.fn(), reject: vi.fn(), scrollTo: vi.fn() }
const show = (state: RoomState, user = arjun) =>
  render(
    <DecisionHandleContext.Provider value={{ get: () => handle, set: () => undefined }}>
      <DecisionBar state={state} user={user} />
    </DecisionHandleContext.Provider>,
  )

beforeEach(() => {
  vi.mocked(handle.approve).mockClear()
  vi.mocked(handle.reject).mockClear()
  vi.mocked(handle.scrollTo).mockClear()
  window.sessionStorage.clear()
})
afterEach(() => vi.useRealTimers())

describe('DecisionBar', () => {
  it('needs-you: copy, and the buttons call the card handle', () => {
    show(room(prop()))
    const bar = screen.getByRole('region', { name: 'Decision' })
    expect(bar).toHaveTextContent('Needs your approval · kafka-connect — Restart the task')
    fireEvent.click(screen.getByRole('button', { name: /^Approve kafka-connect/ }))
    fireEvent.click(screen.getByRole('button', { name: /^Reject/ }))
    fireEvent.click(screen.getByRole('button', { name: /View steps/ }))
    expect(handle.approve).toHaveBeenCalledTimes(1)
    expect(handle.reject).toHaveBeenCalledTimes(1)
    expect(handle.scrollTo).toHaveBeenCalledTimes(1)
  })

  it('waiting, executing, failed and rejected copy', () => {
    const other = { ...arjun, layers: ['consumer'] }
    const { unmount } = show(room(prop()), other)
    expect(screen.getByRole('region', { name: 'Decision' })).toHaveTextContent('Waiting for kafka-connect · 1 of 2 approved')
    expect(screen.queryByRole('button', { name: /Approve/ })).not.toBeInTheDocument()
    unmount()

    const approved = prop({ status: 'approved', approvals: { 'schema-registry': 'approved', 'kafka-connect': 'approved' } })
    const r2 = show(room({ ...approved, executed: [ex(0)] }))
    expect(screen.getByRole('region', { name: 'Decision' })).toHaveTextContent('Running step 2 of 2 · restart_connector_task')
    r2.unmount()

    const r3 = show(room({ ...approved, executed: [ex(0), ex(1, false)] }))
    expect(screen.getByRole('region', { name: 'Decision' })).toHaveTextContent('Fix failed at step 2 — see the card')
    r3.unmount()

    const rejected = prop({
      status: 'rejected',
      approvals: { 'schema-registry': 'approved', 'kafka-connect': 'rejected' },
      decisions: { 'kafka-connect': { outcome: 'rejected', reason: 'no', by: { kind: 'human', id: 'k', name: 'Kabir' }, params_hash: 'h' } },
    })
    show(room(rejected))
    expect(screen.getByRole('region', { name: 'Decision' })).toHaveTextContent('Proposal rejected by Kabir · kafka-connect')
    fireEvent.click(screen.getByRole('button', { name: 'Dismiss' }))
    expect(screen.queryByRole('region', { name: 'Decision' })).not.toBeInTheDocument()
    expect(window.sessionStorage.getItem('piperoom.dismissed.p1')).toBe('1')
  })

  it('done ok shows for 3 s after the fix ran, then goes', () => {
    vi.useFakeTimers()
    const approved = prop({ status: 'approved', approvals: { 'schema-registry': 'approved', 'kafka-connect': 'approved' } })
    const view = show(room({ ...approved, executed: [ex(0)] }))
    view.rerender(
      <DecisionHandleContext.Provider value={{ get: () => handle, set: () => undefined }}>
        <DecisionBar state={room({ ...approved, executed: [ex(0), ex(1)] })} user={arjun} />
      </DecisionHandleContext.Provider>,
    )
    expect(screen.getByRole('region', { name: 'Decision' })).toHaveTextContent('Fix ran · 2 of 2 steps')
    act(() => void vi.advanceTimersByTime(3100))
    expect(screen.queryByRole('region', { name: 'Decision' })).not.toBeInTheDocument()
  })

  it('a finished fix seen only on load does not flash', () => {
    const approved = prop({ status: 'approved', approvals: { 'schema-registry': 'approved', 'kafka-connect': 'approved' } })
    show(room({ ...approved, executed: [ex(0), ex(1)] }))
    expect(screen.queryByRole('region', { name: 'Decision' })).not.toBeInTheDocument()
  })
})

describe('DecisionBar as a bottom sheet', () => {
  it('is fixed at the bottom with full-width Reject / Approve buttons and no key caps', () => {
    render(
      <DecisionHandleContext.Provider value={{ get: () => handle, set: () => undefined }}>
        <DecisionBar state={room(prop())} user={arjun} placement="sheet" />
      </DecisionHandleContext.Provider>,
    )
    const bar = screen.getByRole('region', { name: 'Decision' })
    expect(bar).toHaveAttribute('data-placement', 'sheet')
    expect(bar.className).toContain('fixed')
    expect(bar.className).toContain('bottom-0')
    const approve = screen.getByRole('button', { name: 'Approve kafka-connect' })
    const reject = screen.getByRole('button', { name: 'Reject' })
    expect(approve.className).toContain('h-11')
    expect(reject.className).toContain('h-11')
    expect(bar.querySelector('kbd')).toBeNull()
    expect(screen.queryByRole('button', { name: /View steps/ })).not.toBeInTheDocument()
    fireEvent.click(approve)
    expect(handle.approve).toHaveBeenCalledTimes(1)
  })
})

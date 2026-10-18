import { beforeEach, describe, expect, it, vi } from 'vitest'
import { fireEvent, render, screen } from '@testing-library/react'
import { ProposalsPanel } from './ProposalsPanel'
import type { RoomActions } from '../hooks/useRoom'
import type { CurrentUser } from '../state/permissions'
import { emptyRoomState, fromSnapshot } from '../state/types'
import type { ProposalState, RoomState } from '../state/types'
import { applyEvent } from '../state/reducer'
import type { Event, RoomMode, RoomSnapshot } from '../api/types'

const approve = vi.fn()
const actions: RoomActions = {
  comment: vi.fn(),
  steer: vi.fn(),
  sendSuggestion: vi.fn(),
  declineSuggestion: vi.fn(),
  requestControl: vi.fn(),
  grantControl: vi.fn(),
  close: vi.fn(),
  approve,
  reject: vi.fn(),
  handoff: vi.fn(),
  acceptHandoff: vi.fn(),
  label: vi.fn(),
  checklist: vi.fn(),
  trace: vi.fn(),
  hypothesisVerdict: vi.fn(),
  nudge: vi.fn().mockResolvedValue({ ok: true }),
}

const HASH = '9f2c1e7ab04d58c3d6e1f0a7b92c4d835e6a1f0b7c3d9e2a48b5c6d7e8f90a1b'
const WAITING = 'Approved · waiting for write mode'
const off: RoomMode = { write: false, solo: false, auto_approve: [] }
const on: RoomMode = { write: true, solo: false, auto_approve: [] }

const arjun: CurrentUser = { id: 'arjun', name: 'Arjun', layers: ['kafka-connect'] }
const soloArjun: CurrentUser = { ...arjun, solo: true }

const step = (layer: string, action: string) => ({ action, layer, params: {}, description: '' })
function proposal(over: Partial<ProposalState> = {}): ProposalState {
  return {
    proposal_id: 'p1',
    title: 'Restart the task',
    steps: [step('schema-registry', 'set_compatibility'), step('kafka-connect', 'restart_connector_task')],
    risk: 'low',
    rollback: 'undo',
    required_layers: ['schema-registry', 'kafka-connect'],
    approvals: { 'schema-registry': 'approved', 'kafka-connect': 'pending' },
    decisions: {},
    params_hash: HASH,
    status: 'open',
    executed: [],
    ...over,
  }
}
const approvedAll = { approvals: { 'schema-registry': 'approved', 'kafka-connect': 'approved' }, status: 'approved' } as const
function room(p: ProposalState, mode?: RoomMode): RoomState {
  return { ...emptyRoomState('r1'), pipeline_id: 'orders', proposals: [p], ...(mode ? { mode } : {}) }
}
const show = (state: RoomState, user = arjun) => render(<ProposalsPanel state={state} user={user} actions={actions} />)

beforeEach(() => {
  approve.mockReset()
})

describe('approve button copy', () => {
  it('reads "Approve (will not run)" while write mode is off', () => {
    show(room(proposal(), off))
    expect(screen.getByRole('button', { name: 'Approve (will not run)' })).toBeEnabled()
    expect(screen.queryByRole('button', { name: 'Approve' })).not.toBeInTheDocument()
    expect(screen.getByRole('button', { name: /^Reject/ })).toBeInTheDocument()
  })

  it.each([
    ['write mode on', on],
    ['no mode at all (an older server)', undefined],
  ])('reads plain "Approve" with %s', (_name, mode) => {
    show(room(proposal(), mode))
    expect(screen.getByRole('button', { name: 'Approve' })).toBeEnabled()
    expect(screen.queryByText(/will not run/)).not.toBeInTheDocument()
  })

  it('still approves with the hash on screen', () => {
    approve.mockResolvedValue({ ok: true })
    // A third layer still pending: this approval does not complete the set, so no run confirm.
    const open = proposal({
      required_layers: ['schema-registry', 'kafka-connect', 'consumer'],
      approvals: { 'schema-registry': 'approved', 'kafka-connect': 'pending', consumer: 'pending' },
    })
    show(room(open, off))
    fireEvent.click(screen.getByRole('button', { name: 'Approve (will not run)' }))
    expect(approve).toHaveBeenCalledWith('p1', 'kafka-connect', { params_hash: HASH })
  })

  it('the solo confirm keeps its text and its Approve button', () => {
    show(room(proposal({ required_layers: ['kafka-connect', 'consumer'], approvals: { 'kafka-connect': 'pending', consumer: 'pending' } }), off), soloArjun)
    fireEvent.click(screen.getByRole('button', { name: 'Approve (will not run)' }))
    expect(screen.getByLabelText('Solo mode: type orders/kafka-connect to approve')).toBeInTheDocument()
    expect(screen.getByRole('button', { name: 'Approve' })).toBeDisabled() // the dialog's own button
  })

  it('the run confirm for the approval that completes the set keeps its text', () => {
    show(room(proposal(), off))
    fireEvent.click(screen.getByRole('button', { name: 'Approve (will not run)' }))
    expect(screen.getByText('Run this fix now?')).toBeInTheDocument()
    expect(screen.getByRole('button', { name: 'Approve and run' })).toBeInTheDocument()
  })
})

describe('"Approved · waiting for write mode"', () => {
  it('replaces the execution card when the snapshot says the proposal is blocked', () => {
    show(room(proposal({ ...approvedAll, blocked: 'write_mode_off' }), off))
    const line = screen.getByRole('status')
    expect(line).toHaveTextContent(WAITING)
    // No run, so none of the run's words and no progress bar.
    expect(screen.queryByText(/Running/)).not.toBeInTheDocument()
    expect(screen.queryByText(/Ran \d/)).not.toBeInTheDocument()
    expect(screen.queryByText(/verifying/)).not.toBeInTheDocument()
    expect(screen.queryByRole('progressbar')).not.toBeInTheDocument()
    expect(screen.queryByTestId('exec-step-0')).not.toBeInTheDocument()
  })

  it('uses neutral styling, not the error or warning one', () => {
    show(room(proposal({ ...approvedAll, blocked: 'write_mode_off' }), off))
    const line = screen.getByRole('status')
    expect(line).toHaveClass('text-muted-foreground')
    expect(line.className).not.toMatch(/destructive|warning|success/)
    expect(line.parentElement?.className ?? '').not.toMatch(/destructive|warning|success/)
    expect(line.querySelector('svg')).toHaveAttribute('aria-hidden', 'true')
  })

  it('shows for an approval that arrived over the socket, where the proposal carries no blocked', () => {
    const snap: RoomSnapshot = {
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
      mode: off,
    }
    const ev = (seq: number, type: Event['type'], payload: unknown): Event => ({
      room_id: 'r1',
      seq,
      type,
      actor: { kind: 'human', id: 'arjun', name: 'Arjun' },
      payload,
      at: `2026-10-11T10:00:0${seq}Z`,
    })
    const { proposal_id, title, steps, risk, rollback, required_layers, params_hash } = proposal()
    const created = { proposal_id, title, steps, risk, rollback, required_layers, params_hash }
    let s = applyEvent(fromSnapshot(snap), ev(1, 'proposal.created', created))
    s = applyEvent(s, ev(2, 'approval.given', { proposal_id: 'p1', layer: 'schema-registry' }))
    const view = show(s)
    expect(screen.queryByText(WAITING)).not.toBeInTheDocument() // one layer still pending
    view.unmount()

    s = applyEvent(s, ev(3, 'approval.given', { proposal_id: 'p1', layer: 'kafka-connect' }))
    expect(s.proposals[0].blocked).toBe('write_mode_off')
    show(s)
    expect(screen.getByRole('status')).toHaveTextContent(WAITING)
    expect(screen.queryByRole('progressbar')).not.toBeInTheDocument()
  })

  it('does not show with write mode on: the run card with its progress bar, as before', () => {
    show(room(proposal({ ...approvedAll }), on))
    expect(screen.queryByText(WAITING)).not.toBeInTheDocument()
    expect(screen.getByText('Running · step 1 of 2 · set_compatibility')).toBeInTheDocument()
    expect(screen.getByRole('progressbar')).toBeInTheDocument()
  })

  it('does not show when the snapshot has no mode, even for an approved proposal', () => {
    show(room(proposal({ ...approvedAll })))
    expect(screen.queryByText(WAITING)).not.toBeInTheDocument()
    expect(screen.getByText(/^Running · step 1 of 2/)).toBeInTheDocument()
  })

  it('gives way to the execution card once a step ran', () => {
    const ran = { proposal_id: 'p1', step_index: 0, ok: true, output: 'ok' }
    show(room(proposal({ ...approvedAll, blocked: 'write_mode_off', executed: [ran] }), off))
    expect(screen.queryByText(WAITING)).not.toBeInTheDocument()
    expect(screen.getByText(/^Running · step 2 of 2/)).toBeInTheDocument()
  })
})

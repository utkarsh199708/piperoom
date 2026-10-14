import { beforeEach, describe, expect, it, vi } from 'vitest'
import { fireEvent, render, screen, waitFor, within } from '@testing-library/react'
import { ProposalsPanel } from './ProposalsPanel'
import type { RoomActions } from '../hooks/useRoom'
import type { CurrentUser } from '../state/permissions'
import { emptyRoomState } from '../state/types'
import type { ProposalState } from '../state/types'

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
  trace: vi.fn(),
}

const solo: CurrentUser = { id: 'sam', name: 'Sam', layers: ['kafka-connect'] }
const soloKnown: CurrentUser = { ...solo, solo: true }
const HASH = 'abc123abc123def456abc123abc123def456abc123abc123def456abc123abc1'
const BOX = /Solo mode: type orders\/kafka-connect to approve/

const proposal: ProposalState = {
  proposal_id: 'p1',
  title: 'Restart the task',
  steps: [],
  risk: 'low',
  rollback: 'none',
  // A second layer still pending: this approval does not complete the set, so no run confirm.
  required_layers: ['kafka-connect', 'consumer'],
  approvals: { 'kafka-connect': 'pending', consumer: 'pending' },
  status: 'open',
  decisions: {},
  params_hash: HASH,
  executed: [],
}
const state = { ...emptyRoomState('r1'), pipeline_id: 'orders', proposals: [proposal] }

beforeEach(() => {
  approve.mockReset()
})

describe('solo-mode confirm', () => {
  it('a plain approve sends no confirm and shows no solo UI', async () => {
    approve.mockResolvedValue({ ok: true })
    render(<ProposalsPanel state={state} user={solo} actions={actions} />)
    fireEvent.click(screen.getByRole('button', { name: 'Approve' }))
    await waitFor(() => expect(approve).toHaveBeenCalledWith('p1', 'kafka-connect', { params_hash: HASH }))
    expect(screen.queryByText(/Solo mode/)).not.toBeInTheDocument()
  })

  it('a solo 400 opens the confirm box and the typed text retries with confirm', async () => {
    approve.mockResolvedValueOnce({ ok: false, needsConfirm: true })
    approve.mockResolvedValueOnce({ ok: true })
    render(<ProposalsPanel state={state} user={solo} actions={actions} />)

    fireEvent.click(screen.getByRole('button', { name: 'Approve' }))

    const box = await screen.findByLabelText(BOX)
    // The solo banner appears once the server has asked. (The open dialog hides the page behind it
    // from the accessibility tree, hence `hidden`.)
    expect(screen.getByRole('status', { hidden: true })).toHaveTextContent(/Solo mode/)
    // The text to type is shown, with no copy button.
    expect(screen.getByText('orders/kafka-connect')).toBeInTheDocument()
    // (The proposal's hash has its own copy button, outside this form.)
    expect(within(box.closest('form') as HTMLElement).queryByRole('button', { name: /copy/i })).not.toBeInTheDocument()

    // Approve stays off until the text matches exactly.
    const go = screen.getAllByRole('button', { name: 'Approve' }).at(-1) as HTMLElement
    expect(go).toBeDisabled()
    fireEvent.change(box, { target: { value: 'orders/kafka' } })
    expect(go).toBeDisabled()
    fireEvent.change(box, { target: { value: 'orders/kafka-connect' } })
    expect(go).toBeEnabled()

    fireEvent.click(go)
    await waitFor(() =>
      expect(approve).toHaveBeenLastCalledWith('p1', 'kafka-connect', { params_hash: HASH, confirm: 'orders/kafka-connect' }),
    )
    expect(approve).toHaveBeenCalledTimes(2)
    // The box closes after a good answer.
    await waitFor(() =>
      expect(screen.queryByLabelText(BOX)).not.toBeInTheDocument(),
    )
  })

  it('blocks paste into the confirm box (typed by hand)', async () => {
    approve.mockResolvedValue({ ok: false, needsConfirm: true })
    render(<ProposalsPanel state={state} user={solo} actions={actions} />)
    fireEvent.click(screen.getByRole('button', { name: 'Approve' }))
    const box = await screen.findByLabelText(BOX)
    // A visible hint says paste is off, and the input points at it.
    expect(screen.getByText('Type it by hand. Paste is off.')).toBeInTheDocument()
    expect(box).toHaveAccessibleDescription('Type it by hand. Paste is off.')
    const notCancelled = fireEvent.paste(box, { clipboardData: { getData: () => 'orders/kafka-connect' } })
    expect(notCancelled).toBe(false) // preventDefault was called
  })

  it('keeps the box open and says so when the server still refuses the text', async () => {
    approve.mockResolvedValue({ ok: false, needsConfirm: true })
    render(<ProposalsPanel state={state} user={solo} actions={actions} />)
    fireEvent.click(screen.getByRole('button', { name: 'Approve' }))
    const box = await screen.findByLabelText(BOX)
    fireEvent.change(box, { target: { value: 'orders/kafka-connect' } })
    fireEvent.click(screen.getAllByRole('button', { name: 'Approve' }).at(-1) as HTMLElement)
    expect(await screen.findByRole('alert')).toHaveTextContent('The server did not accept that text.')
    expect(screen.getByLabelText(BOX)).toBeInTheDocument()
  })

  it('Cancel closes the box without a second call', async () => {
    approve.mockResolvedValue({ ok: false, needsConfirm: true })
    render(<ProposalsPanel state={state} user={solo} actions={actions} />)
    fireEvent.click(screen.getByRole('button', { name: 'Approve' }))
    await screen.findByLabelText(BOX)
    fireEvent.click(screen.getByRole('button', { name: 'Cancel' }))
    expect(screen.queryByLabelText(BOX)).not.toBeInTheDocument()
    expect(approve).toHaveBeenCalledTimes(1)
  })
})

describe('solo mode known from /api/me.solo', () => {
  it('shows the solo banner up front, before any approve call', () => {
    render(<ProposalsPanel state={state} user={soloKnown} actions={actions} />)
    expect(screen.getByRole('status')).toHaveTextContent(/Solo mode/)
    expect(approve).not.toHaveBeenCalled()
  })

  it('Approve opens the confirm box without a call, then sends confirm once', async () => {
    approve.mockResolvedValue({ ok: true })
    render(<ProposalsPanel state={state} user={soloKnown} actions={actions} />)
    fireEvent.click(screen.getByRole('button', { name: 'Approve' }))
    expect(approve).not.toHaveBeenCalled()

    const box = screen.getByLabelText(BOX)
    fireEvent.change(box, { target: { value: 'orders/kafka-connect' } })
    fireEvent.click(screen.getAllByRole('button', { name: 'Approve' }).at(-1) as HTMLElement)
    await waitFor(() =>
      expect(approve).toHaveBeenCalledWith('p1', 'kafka-connect', { params_hash: HASH, confirm: 'orders/kafka-connect' }),
    )
    expect(approve).toHaveBeenCalledTimes(1)
  })

  it('a non-solo user still sends a plain approve (the error-triggered path is the fallback)', async () => {
    approve.mockResolvedValue({ ok: true })
    render(<ProposalsPanel state={state} user={solo} actions={actions} />)
    fireEvent.click(screen.getByRole('button', { name: 'Approve' }))
    await waitFor(() => expect(approve).toHaveBeenCalledWith('p1', 'kafka-connect', { params_hash: HASH }))
  })
})

import { beforeEach, describe, expect, it, vi } from 'vitest'
import { fireEvent, render, screen, waitFor } from '@testing-library/react'
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

const proposal: ProposalState = {
  proposal_id: 'p1',
  title: 'Restart the task',
  steps: [],
  risk: 'low',
  rollback: 'none',
  required_layers: ['kafka-connect'],
  approvals: { 'kafka-connect': 'pending' },
  status: 'open',
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
    await waitFor(() => expect(approve).toHaveBeenCalledWith('p1', 'kafka-connect', undefined))
    expect(screen.queryByText(/Solo mode/)).not.toBeInTheDocument()
  })

  it('a solo 400 opens the confirm box and the typed text retries with confirm', async () => {
    approve.mockResolvedValueOnce({ ok: false, needsConfirm: true })
    approve.mockResolvedValueOnce({ ok: true })
    render(<ProposalsPanel state={state} user={solo} actions={actions} />)

    fireEvent.click(screen.getByRole('button', { name: 'Approve' }))

    const box = await screen.findByLabelText('Type the pipeline and layer to confirm')
    // The solo banner appears once the server has asked.
    expect(screen.getByRole('status')).toHaveTextContent(/Solo mode/)
    // The text to type is shown, with no copy button.
    expect(screen.getByText('orders/kafka-connect')).toBeInTheDocument()
    expect(screen.queryByRole('button', { name: /copy/i })).not.toBeInTheDocument()

    // Approve stays off until the text matches exactly.
    const go = screen.getAllByRole('button', { name: 'Approve' }).at(-1) as HTMLElement
    expect(go).toBeDisabled()
    fireEvent.change(box, { target: { value: 'orders/kafka' } })
    expect(go).toBeDisabled()
    fireEvent.change(box, { target: { value: 'orders/kafka-connect' } })
    expect(go).toBeEnabled()

    fireEvent.click(go)
    await waitFor(() =>
      expect(approve).toHaveBeenLastCalledWith('p1', 'kafka-connect', { confirm: 'orders/kafka-connect' }),
    )
    expect(approve).toHaveBeenCalledTimes(2)
    // The box closes after a good answer.
    await waitFor(() =>
      expect(screen.queryByLabelText('Type the pipeline and layer to confirm')).not.toBeInTheDocument(),
    )
  })

  it('blocks paste into the confirm box (typed by hand)', async () => {
    approve.mockResolvedValue({ ok: false, needsConfirm: true })
    render(<ProposalsPanel state={state} user={solo} actions={actions} />)
    fireEvent.click(screen.getByRole('button', { name: 'Approve' }))
    const box = await screen.findByLabelText('Type the pipeline and layer to confirm')
    const notCancelled = fireEvent.paste(box, { clipboardData: { getData: () => 'orders/kafka-connect' } })
    expect(notCancelled).toBe(false) // preventDefault was called
  })

  it('keeps the box open and says so when the server still refuses the text', async () => {
    approve.mockResolvedValue({ ok: false, needsConfirm: true })
    render(<ProposalsPanel state={state} user={solo} actions={actions} />)
    fireEvent.click(screen.getByRole('button', { name: 'Approve' }))
    const box = await screen.findByLabelText('Type the pipeline and layer to confirm')
    fireEvent.change(box, { target: { value: 'orders/kafka-connect' } })
    fireEvent.click(screen.getAllByRole('button', { name: 'Approve' }).at(-1) as HTMLElement)
    expect(await screen.findByText('The server did not accept that text.')).toBeInTheDocument()
    expect(screen.getByLabelText('Type the pipeline and layer to confirm')).toBeInTheDocument()
  })

  it('Cancel closes the box without a second call', async () => {
    approve.mockResolvedValue({ ok: false, needsConfirm: true })
    render(<ProposalsPanel state={state} user={solo} actions={actions} />)
    fireEvent.click(screen.getByRole('button', { name: 'Approve' }))
    await screen.findByLabelText('Type the pipeline and layer to confirm')
    fireEvent.click(screen.getByRole('button', { name: 'Cancel' }))
    expect(screen.queryByLabelText('Type the pipeline and layer to confirm')).not.toBeInTheDocument()
    expect(approve).toHaveBeenCalledTimes(1)
  })
})

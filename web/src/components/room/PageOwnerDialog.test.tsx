import { describe, expect, it, vi } from 'vitest'
import { fireEvent, render, screen, waitFor, within } from '@testing-library/react'
import { PageOwnerDialog } from './PageOwnerDialog'
import type { RoomActions } from '@/hooks/useRoom'
import { emptyRoomState } from '@/state/types'
import type { ProposalState, RoomState } from '@/state/types'

const proposal = (over: Partial<ProposalState> = {}): ProposalState => ({
  proposal_id: 'p_1',
  title: 'Restart the consumer',
  steps: [],
  risk: '',
  rollback: '',
  required_layers: ['consumer', 'warehouse'],
  approvals: { consumer: 'approved', warehouse: 'pending' },
  decisions: {},
  params_hash: 'h',
  status: 'open',
  executed: [],
  ...over,
})

function setup(state: RoomState = emptyRoomState('r1'), page?: RoomActions['page'], people?: Parameters<typeof PageOwnerDialog>[0]['people']) {
  const onClose = vi.fn()
  render(<PageOwnerDialog state={state} actions={{ page } as RoomActions} people={people} onClose={onClose} />)
  return { onClose }
}

const layer = () => screen.getByLabelText('Layer') as HTMLSelectElement
const reason = () => screen.getByLabelText('Why now') as HTMLInputElement
const send = () => screen.getByRole('button', { name: /Page owner|Paging/ })

describe('PageOwnerDialog', () => {
  it('is a named dialog; the page button waits for a layer and a reason', () => {
    setup(undefined, vi.fn())
    expect(screen.getByRole('dialog', { name: 'Page an owner' })).toBeInTheDocument()
    expect(send()).toBeDisabled()
    fireEvent.change(layer(), { target: { value: 'consumer' } })
    expect(send()).toBeDisabled()
    fireEvent.change(reason(), { target: { value: '   ' } })
    expect(send()).toBeDisabled()
    fireEvent.change(reason(), { target: { value: 'Lag is growing' } })
    expect(send()).toBeEnabled()
  })

  it('offers the six layers and starts from the layer a fix is waiting on', () => {
    setup({ ...emptyRoomState('r1'), proposals: [proposal()] }, vi.fn())
    expect(within(layer()).getAllByRole('option').map((o) => o.textContent)).toEqual([
      'Choose a layer…',
      'postgres',
      'kafka-connect',
      'schema-registry',
      'kafka',
      'consumer',
      'warehouse',
    ])
    expect(layer().value).toBe('warehouse')
  })

  it('sends the layer and the trimmed reason, then closes', async () => {
    const page = vi.fn().mockResolvedValue({ ok: true })
    const { onClose } = setup(undefined, page)
    fireEvent.change(layer(), { target: { value: 'consumer' } })
    fireEvent.change(reason(), { target: { value: '  Lag is growing  ' } })
    fireEvent.click(send())
    await waitFor(() => expect(onClose).toHaveBeenCalled())
    expect(page).toHaveBeenCalledWith({ layer: 'consumer', reason: 'Lag is growing' })
  })

  it('can name the open fix it is about, and only open ones are offered', async () => {
    const page = vi.fn().mockResolvedValue({ ok: true })
    const state = {
      ...emptyRoomState('r1'),
      proposals: [proposal(), proposal({ proposal_id: 'p_2', title: 'Old fix', status: 'rejected' })],
    }
    setup(state, page)
    const about = screen.getByLabelText('About a fix (optional)')
    expect(within(about).getAllByRole('option').map((o) => o.textContent)).toEqual(['No specific fix', 'Restart the consumer'])
    fireEvent.change(about, { target: { value: 'p_1' } })
    fireEvent.change(reason(), { target: { value: 'needs your layer' } })
    fireEvent.click(send())
    await waitFor(() => expect(page).toHaveBeenCalledWith({ layer: 'warehouse', reason: 'needs your layer', proposal_id: 'p_1' }))
  })

  it('has no proposal field when nothing is open', () => {
    setup(undefined, vi.fn())
    expect(screen.queryByLabelText('About a fix (optional)')).toBeNull()
  })

  it('shows the server message for already_paged and keeps the form for another try', async () => {
    const message = 'consumer was already paged 3 min ago'
    const page = vi.fn().mockResolvedValue({ ok: false, error: message })
    const { onClose } = setup(undefined, page)
    fireEvent.change(layer(), { target: { value: 'consumer' } })
    fireEvent.change(reason(), { target: { value: 'again' } })
    fireEvent.click(send())
    expect(await screen.findByRole('alert')).toHaveTextContent(message)
    expect(onClose).not.toHaveBeenCalled()
    expect(reason().value).toBe('again')
    expect(send()).toBeEnabled()
  })

  it('names the approvers of the chosen layer when the people list is known', () => {
    const people = [
      { id: 'kabir', name: 'Kabir', layers: ['consumer'] },
      { id: 'meera', name: 'Meera', layers: ['warehouse'] },
      { id: 'zed', name: 'Zed' },
    ]
    setup(undefined, vi.fn(), people)
    expect(screen.queryByText(/Approvers for/)).toBeNull()
    fireEvent.change(layer(), { target: { value: 'consumer' } })
    expect(screen.getByText('Approvers for consumer: Kabir')).toBeInTheDocument()
  })

  it('says paging is unavailable instead of failing when the room cannot page', () => {
    setup(undefined, undefined)
    expect(screen.getByRole('alert')).toHaveTextContent('Paging is not available here.')
    expect(send()).toBeDisabled()
  })

  it('caps the reason at 300 characters', () => {
    setup(undefined, vi.fn())
    expect(reason()).toHaveAttribute('maxlength', '300')
  })

  it('Cancel closes without paging', () => {
    const page = vi.fn()
    const { onClose } = setup(undefined, page)
    fireEvent.click(screen.getByRole('button', { name: 'Cancel' }))
    expect(onClose).toHaveBeenCalled()
    expect(page).not.toHaveBeenCalled()
  })
})

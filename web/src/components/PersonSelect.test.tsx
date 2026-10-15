import { beforeEach, describe, expect, it, vi } from 'vitest'
import { fireEvent, render, screen, waitFor } from '@testing-library/react'
import type { RoomActions } from '../hooks/useRoom'
import type { CurrentUser } from '../state/permissions'
import { emptyRoomState } from '../state/types'

vi.mock('../api/client', async (importOriginal) => {
  const actual = await importOriginal<typeof import('../api/client')>()
  return { ...actual, isFixtures: () => false, api: { ...actual.api, people: vi.fn() } }
})

import { ApiClientError, api } from '../api/client'
import { HandoffDialog } from './HandoffDialog'
import { SteerBox } from './SteerBox'
import { PersonSelect } from './PersonSelect'

const actions: RoomActions = {
  comment: vi.fn(),
  steer: vi.fn(),
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

const arjun: CurrentUser = { id: 'arjun', name: 'Arjun', layers: [] }

// What the server sends: users.yaml users have no email and are verified; IdP users may have
// an email and are not.
const people = [
  { id: 'arjun', name: 'Arjun', verified: true },
  { id: 'priya', name: 'Priya', verified: true },
  { id: 'u_0a1b2c3d4e5f', name: 'Meera Rao', email: 'meera@example.com', verified: false },
]

beforeEach(() => {
  vi.mocked(api.people).mockReset()
  vi.mocked(actions.handoff).mockReset()
  vi.mocked(actions.grantControl).mockReset()
})

describe('hand-off picker', () => {
  it('lists /api/people as "name (id)" or "name <email>" and leaves out yourself', async () => {
    vi.mocked(api.people).mockResolvedValue(people)
    render(<HandoffDialog state={emptyRoomState('r1')} user={arjun} actions={actions} onClose={() => {}} />)

    const select = await screen.findByLabelText('Hand off to')
    const labels = Array.from(select.querySelectorAll('option')).map((o) => o.textContent)
    expect(labels).toEqual(['Priya (priya)', 'Meera Rao <meera@example.com> — unverified'])
    // The option text cannot draw a badge, so the chosen person's drawn badge sits under the list.
    expect((await screen.findByTestId('handoff-to-selected')).querySelector('[data-slot="verified-badge"]')).toHaveAttribute(
      'data-verification',
      'verified',
    )
    expect(api.people).toHaveBeenCalledTimes(1)
  })

  it('hands off to the chosen person id', async () => {
    vi.mocked(api.people).mockResolvedValue(people)
    render(<HandoffDialog state={emptyRoomState('r1')} user={arjun} actions={actions} onClose={() => {}} />)

    const select = await screen.findByLabelText('Hand off to')
    fireEvent.change(select, { target: { value: 'u_0a1b2c3d4e5f' } })
    fireEvent.click(screen.getByRole('button', { name: 'Start hand-off' }))
    expect(actions.handoff).toHaveBeenCalledWith('u_0a1b2c3d4e5f', false)
  })

  it('shows no picker for a viewer (403 viewer)', async () => {
    vi.mocked(api.people).mockRejectedValue(new ApiClientError(403, 'viewer', 'read-only'))
    render(<HandoffDialog state={emptyRoomState('r1')} user={arjun} actions={actions} onClose={() => {}} />)

    expect(await screen.findByText(/View only/)).toBeInTheDocument()
    expect(screen.queryByLabelText('Hand off to')).not.toBeInTheDocument()
    expect(screen.getByRole('button', { name: 'Start hand-off' })).toBeDisabled()
  })

  it('shows an error and can try again when the list fails', async () => {
    vi.mocked(api.people).mockRejectedValueOnce(new ApiClientError(500, 'internal', 'boom'))
    vi.mocked(api.people).mockResolvedValueOnce(people)
    render(<HandoffDialog state={emptyRoomState('r1')} user={arjun} actions={actions} onClose={() => {}} />)

    expect(await screen.findByText(/Could not load people: boom/)).toBeInTheDocument()
    fireEvent.click(screen.getByRole('button', { name: 'Try again' }))
    expect(await screen.findByLabelText('Hand off to')).toBeInTheDocument()
  })
})

describe('grant-control picker', () => {
  const driving = { ...emptyRoomState('r1'), driver: { id: 'arjun', name: 'Arjun' } }

  it('does not load people until the driver opens it', () => {
    render(<SteerBox state={driving} user={arjun} actions={actions} />)
    expect(api.people).not.toHaveBeenCalled()
    expect(screen.getByRole('button', { name: /Give control to/ })).toBeInTheDocument()
  })

  it('gives control to the person picked from /api/people', async () => {
    vi.mocked(api.people).mockResolvedValue(people)
    render(<SteerBox state={driving} user={arjun} actions={actions} />)

    fireEvent.click(screen.getByRole('button', { name: /Give control to/ }))
    const select = await screen.findByLabelText('Give control to')
    expect(screen.getByRole('option', { name: 'Meera Rao <meera@example.com> — unverified' })).toBeInTheDocument()
    fireEvent.change(select, { target: { value: 'priya' } })
    fireEvent.click(screen.getByRole('button', { name: 'Give control' }))
    await waitFor(() => expect(actions.grantControl).toHaveBeenCalledWith('priya'))
  })
})

describe('PersonSelect default and a late effect', () => {
  // CI flake (#373): the "pick the first person" effect from a render that still had value ''
  // ran after the user's change and handed off to priya instead of the chosen person.
  it('a default effect that runs after the pick does not undo it', async () => {
    vi.mocked(api.people).mockResolvedValue(people)
    const first = vi.fn()
    const ui = (onChange: (id: string) => void) => (
      <>
        <label htmlFor="p">pick</label>
        <PersonSelect id="p" selfId="arjun" value="" onChange={onChange} />
      </>
    )
    const { rerender } = render(ui(first))
    const select = await screen.findByLabelText('pick')
    expect(select).toHaveValue('priya') // the default shows at once, before the parent stores it
    await waitFor(() => expect(first).toHaveBeenCalledWith('priya'))

    fireEvent.change(select, { target: { value: 'u_0a1b2c3d4e5f' } })
    expect(first).toHaveBeenLastCalledWith('u_0a1b2c3d4e5f')

    // The parent has not stored the pick yet (value is still ''), and the effect runs again.
    const late = vi.fn()
    rerender(ui(late))
    expect(late).not.toHaveBeenCalledWith('priya')
  })
})

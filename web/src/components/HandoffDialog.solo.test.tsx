import { beforeEach, describe, expect, it, vi } from 'vitest'
import { fireEvent, render, screen, waitFor } from '@testing-library/react'
import type { RoomActions } from '../hooks/useRoom'
import type { CurrentUser } from '../state/permissions'
import { emptyRoomState } from '../state/types'

vi.mock('../api/client', async (importOriginal) => {
  const actual = await importOriginal<typeof import('../api/client')>()
  return { ...actual, isFixtures: () => false, api: { ...actual.api, people: vi.fn() } }
})

import { api } from '../api/client'
import { HandoffDialog } from './HandoffDialog'

// KAN-394: in solo mode the dev login picker lists only the solo user, but /api/people still
// lists every users.yaml user. A hand-off to Priya could never be accepted by a real login.
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

const people = [
  { id: 'arjun', name: 'Arjun', verified: true },
  { id: 'priya', name: 'Priya', verified: true },
  { id: 'meera', name: 'Meera', verified: true },
  { id: 'kabir', name: 'Kabir', verified: true },
]

const arjun: CurrentUser = { id: 'arjun', name: 'Arjun', layers: ['kafka-connect'] }
const soloArjun: CurrentUser = { ...arjun, solo: true }

const SOLO_TEXT = 'No other person can sign in on this server (solo mode).'

beforeEach(() => {
  vi.mocked(api.people).mockReset()
  vi.mocked(actions.handoff).mockReset()
})

describe('hand-off recipients must be able to sign in (KAN-394)', () => {
  it('solo mode: offers nobody else, says why, and offers Close the room instead', async () => {
    vi.mocked(api.people).mockResolvedValue(people)
    const onCloseRoom = vi.fn()
    render(
      <HandoffDialog
        state={emptyRoomState('r1')}
        user={soloArjun}
        actions={actions}
        onClose={() => {}}
        onCloseRoom={onCloseRoom}
      />,
    )

    expect(await screen.findByText(SOLO_TEXT)).toBeInTheDocument()
    // No empty combobox: the picker is gone, and none of the other users is offered.
    expect(screen.queryByLabelText('Hand off to')).not.toBeInTheDocument()
    for (const name of ['Priya', 'Meera', 'Kabir']) {
      expect(screen.queryByRole('option', { name: new RegExp(name) })).not.toBeInTheDocument()
    }
    // No target, so the hand-off itself is disabled.
    expect(screen.getByRole('button', { name: /Start hand-off/ })).toBeDisabled()

    fireEvent.click(screen.getByRole('button', { name: 'Close the room' }))
    expect(onCloseRoom).toHaveBeenCalledTimes(1)
    expect(actions.handoff).not.toHaveBeenCalled()
  })

  it('multi-user mode: everyone who can sign in is still offered as before', async () => {
    vi.mocked(api.people).mockResolvedValue(people)
    render(<HandoffDialog state={emptyRoomState('r1')} user={arjun} actions={actions} onClose={() => {}} />)

    const select = await screen.findByLabelText('Hand off to')
    const labels = Array.from(select.querySelectorAll('option')).map((o) => o.textContent)
    expect(labels).toEqual(['Priya (priya)', 'Meera (meera)', 'Kabir (kabir)'])
    await waitFor(() => expect(select).toHaveValue('priya'))
    expect(screen.getByRole('button', { name: /Start hand-off/ })).toBeEnabled()
    expect(screen.queryByText(SOLO_TEXT)).not.toBeInTheDocument()
    expect(screen.queryByRole('button', { name: 'Close the room' })).not.toBeInTheDocument()
  })

  it('solo mode keeps the form (not an empty dialog) when /api/people has nobody to offer', async () => {
    vi.mocked(api.people).mockResolvedValue([{ id: 'arjun', name: 'Arjun', verified: true }])
    render(<HandoffDialog state={emptyRoomState('r1')} user={soloArjun} actions={actions} onClose={() => {}} />)

    expect(await screen.findByText(SOLO_TEXT)).toBeInTheDocument()
    expect(screen.getByRole('button', { name: /Start hand-off/ })).toBeDisabled()
  })
})

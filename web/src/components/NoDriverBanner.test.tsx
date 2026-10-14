import { beforeEach, describe, expect, it, vi } from 'vitest'
import { fireEvent, render, screen } from '@testing-library/react'
import { NoDriverBanner } from './NoDriverBanner'
import type { RoomActions } from '../hooks/useRoom'
import type { CurrentUser } from '../state/permissions'
import { emptyRoomState } from '../state/types'

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
}

const arjun: CurrentUser = { id: 'arjun', name: 'Arjun', layers: ['kafka'] }

beforeEach(() => {
  vi.resetAllMocks()
})

describe('NoDriverBanner', () => {
  it('prompts to take control when the room has no driver', () => {
    render(<NoDriverBanner state={emptyRoomState('r_webhook')} user={arjun} actions={actions} />)
    expect(screen.getByText(/No one is driving/)).toBeInTheDocument()
    expect(screen.getByRole('button', { name: 'Take control' })).toBeInTheDocument()
  })

  it('grants control to the current user on click', () => {
    render(<NoDriverBanner state={emptyRoomState('r_webhook')} user={arjun} actions={actions} />)
    fireEvent.click(screen.getByRole('button', { name: 'Take control' }))
    expect(actions.grantControl).toHaveBeenCalledTimes(1)
    expect(actions.grantControl).toHaveBeenCalledWith('arjun')
  })

  it('stays hidden when a driver is present', () => {
    const state = { ...emptyRoomState('r1'), driver: { id: 'kabir', name: 'Kabir' } }
    render(<NoDriverBanner state={state} user={arjun} actions={actions} />)
    expect(screen.queryByText(/No one is driving/)).not.toBeInTheDocument()
  })

  it('stays hidden when the room is closed', () => {
    const state = { ...emptyRoomState('r1'), status: 'closed' as const }
    render(<NoDriverBanner state={state} user={arjun} actions={actions} />)
    expect(screen.queryByText(/No one is driving/)).not.toBeInTheDocument()
  })
})

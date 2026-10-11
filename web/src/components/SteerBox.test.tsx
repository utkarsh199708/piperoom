import { describe, expect, it, vi } from 'vitest'
import { render, screen } from '@testing-library/react'
import { SteerBox } from './SteerBox'
import type { RoomActions } from '../hooks/useRoom'
import type { CurrentUser } from '../state/permissions'
import { emptyRoomState } from '../state/types'

const actions: RoomActions = {
  comment: vi.fn(),
  steer: vi.fn(),
  sendSuggestion: vi.fn(),
  requestControl: vi.fn(),
  grantControl: vi.fn(),
  close: vi.fn(),
  approve: vi.fn(),
  reject: vi.fn(),
  handoff: vi.fn(),
  acceptHandoff: vi.fn(),
  trace: vi.fn(),
}

const arjun: CurrentUser = { id: 'arjun', name: 'Arjun', layers: ['kafka'] }
const kabir: CurrentUser = { id: 'kabir', name: 'Kabir', layers: ['consumer'] }

describe('SteerBox', () => {
  it('driver sees "Send to agent" and no "Request control"', () => {
    const state = { ...emptyRoomState('r1'), driver: { id: 'arjun', name: 'Arjun' } }
    render(<SteerBox state={state} user={arjun} actions={actions} />)
    expect(screen.getByRole('button', { name: 'Send to agent' })).toBeInTheDocument()
    expect(screen.queryByText('Request control')).not.toBeInTheDocument()
  })

  it('watcher sees "Suggest", "Request control", and the driver queue hint', () => {
    const state = { ...emptyRoomState('r1'), driver: { id: 'arjun', name: 'Arjun' } }
    render(<SteerBox state={state} user={kabir} actions={actions} />)
    expect(screen.getByText(/Arjun's queue/)).toBeInTheDocument()
    expect(screen.getByRole('button', { name: 'Suggest' })).toBeInTheDocument()
    expect(screen.getByRole('button', { name: 'Request control' })).toBeInTheDocument()
  })

  it('driver sees pending suggestions with a forward button', () => {
    const state = {
      ...emptyRoomState('r1'),
      driver: { id: 'arjun', name: 'Arjun' },
      suggestions: [
        {
          suggestion_id: 'g1',
          from: { kind: 'human' as const, id: 'kabir', name: 'Kabir' },
          text: 'check the config',
        },
      ],
    }
    render(<SteerBox state={state} user={arjun} actions={actions} />)
    expect(screen.getAllByRole('button', { name: 'Send to agent' }).length).toBeGreaterThanOrEqual(2)
    expect(screen.getByText('check the config')).toBeInTheDocument()
  })
})

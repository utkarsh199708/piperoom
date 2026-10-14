import { describe, expect, it, vi } from 'vitest'
import { act, fireEvent, render, screen, waitFor } from '@testing-library/react'
import { stripIsolates } from '../lib/bidi'
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
    expect(screen.getByTestId('steer-status')).toHaveTextContent(/Arjun's queue/)
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

const inAnHour = () => new Date(Date.now() + 24 * 60_000).toISOString()

describe('SteerBox composer (U3)', () => {
  it('the driver sees "You are driving" with the lease and sends with the button', () => {
    vi.mocked(actions.steer).mockClear()
    const state = { ...emptyRoomState('r1'), driver: { id: 'arjun', name: 'Arjun', lease_until: inAnHour() } }
    render(<SteerBox state={state} user={arjun} actions={actions} />)
    expect(screen.getByText(/You are driving · lease 2[34]m/)).toBeInTheDocument()
    const box = screen.getByLabelText('Steer the agent')
    expect(box).toBeEnabled()
    const send = screen.getByRole('button', { name: 'Send to agent' })
    expect(send).toBeDisabled()
    fireEvent.change(box, { target: { value: 'check the DDL log' } })
    fireEvent.click(send)
    expect(actions.steer).toHaveBeenCalledWith('check the DDL log')
    return waitFor(() => expect(box).toHaveValue(''))
  })

  it('Ctrl+Enter sends too', () => {
    vi.mocked(actions.steer).mockClear()
    const state = { ...emptyRoomState('r1'), driver: { id: 'arjun', name: 'Arjun' } }
    render(<SteerBox state={state} user={arjun} actions={actions} />)
    const box = screen.getByLabelText('Steer the agent')
    fireEvent.change(box, { target: { value: 'go' } })
    fireEvent.keyDown(box, { key: 'Enter', ctrlKey: true })
    expect(actions.steer).toHaveBeenCalledWith('go')
  })

  it('a watcher is told whose queue the message goes to, and the lease', () => {
    const state = { ...emptyRoomState('r1'), driver: { id: 'arjun', name: 'Arjun', lease_until: inAnHour() } }
    render(<SteerBox state={state} user={kabir} actions={actions} />)
    expect(screen.getByTestId('steer-status')).toHaveTextContent(
      /^Arjun is driving · lease 2[34]m left · your message goes to Arjun's queue$/,
    )
    expect(stripIsolates(screen.getByLabelText('Steer the agent').getAttribute('placeholder') ?? '')).toBe(
      'Suggest a step for Arjun…',
    )
  })

  it('with no driver the field is disabled and says how to start', () => {
    const state = emptyRoomState('r1')
    render(<SteerBox state={state} user={kabir} actions={actions} />)
    expect(screen.getByLabelText('Steer the agent')).toBeDisabled()
    expect(screen.getByText(/No driver · take control/)).toBeInTheDocument()
    expect(screen.queryByRole('button', { name: 'Suggest' })).not.toBeInTheDocument()
  })

  it('with no driver the composer has no Take control (the banner offers it); with an expired lease it does', () => {
    const { rerender } = render(<SteerBox state={emptyRoomState('r1')} user={kabir} actions={actions} />)
    expect(screen.queryByRole('button', { name: 'Take control' })).not.toBeInTheDocument()
    const expired = { ...emptyRoomState('r1'), driver: { id: 'arjun', name: 'Arjun', lease_until: '2020-01-01T00:00:00Z' } }
    rerender(<SteerBox state={expired} user={kabir} actions={actions} />)
    expect(screen.getByRole('button', { name: 'Take control' })).toBeInTheDocument()
    expect(screen.getByText(/lease expired/)).toBeInTheDocument()
  })

  it('a viewer gets a read-only bar and no field', () => {
    const state = { ...emptyRoomState('r1'), driver: { id: 'arjun', name: 'Arjun' } }
    render(<SteerBox state={state} user={{ ...kabir, role: 'viewer' }} actions={actions} />)
    expect(screen.getByText(/View only: you can read this room but not steer it/)).toBeInTheDocument()
    expect(screen.queryByLabelText('Steer the agent')).not.toBeInTheDocument()
    expect(screen.queryByRole('button')).not.toBeInTheDocument()
  })

  it('a closed room has no composer', () => {
    const state = { ...emptyRoomState('r1'), status: 'closed' as const, driver: { id: 'arjun', name: 'Arjun' } }
    render(<SteerBox state={state} user={arjun} actions={actions} />)
    expect(screen.queryByLabelText('Steer the agent')).not.toBeInTheDocument()
    expect(screen.getByText(/This room is closed/)).toBeInTheDocument()
  })

  it('the driver can grant a control request', () => {
    vi.mocked(actions.grantControl).mockClear()
    const state = {
      ...emptyRoomState('r1'),
      driver: { id: 'arjun', name: 'Arjun' },
      control_requests: [{ from: { kind: 'human' as const, id: 'kabir', name: 'Kabir' } }],
    }
    render(<SteerBox state={state} user={arjun} actions={actions} />)
    fireEvent.click(screen.getByRole('button', { name: 'Grant control' }))
    expect(actions.grantControl).toHaveBeenCalledWith('kabir')
  })
})

describe('SteerBox keeps the text until the steer is accepted (QA9)', () => {
  const driving = { ...emptyRoomState('r1'), driver: { id: 'arjun', name: 'Arjun' } }

  it('keeps the text while the request runs and after it fails', async () => {
    let finish: (ok: boolean) => void = () => undefined
    const steer = vi.fn(() => new Promise<boolean>((resolve) => (finish = resolve)))
    render(<SteerBox state={driving} user={arjun} actions={{ ...actions, steer }} />)
    const box = screen.getByLabelText('Steer the agent')
    fireEvent.change(box, { target: { value: 'restart the task' } })
    fireEvent.click(screen.getByRole('button', { name: 'Send to agent' }))
    expect(steer).toHaveBeenCalledWith('restart the task')
    // In flight: the text is still there and a second send is off.
    expect(box).toHaveValue('restart the task')
    expect(screen.getByRole('button', { name: 'Send to agent' })).toBeDisabled()
    await act(async () => finish(false))
    expect(box).toHaveValue('restart the task')
    expect(screen.getByRole('button', { name: 'Send to agent' })).toBeEnabled()
  })

  it('keeps the text when the request rejects', async () => {
    const steer = vi.fn(() => Promise.reject(new Error('500')))
    render(<SteerBox state={driving} user={arjun} actions={{ ...actions, steer }} />)
    const box = screen.getByLabelText('Steer the agent')
    fireEvent.change(box, { target: { value: 'check lag' } })
    await act(async () => {
      fireEvent.keyDown(box, { key: 'Enter', ctrlKey: true })
    })
    expect(steer).toHaveBeenCalledTimes(1)
    expect(box).toHaveValue('check lag')
  })

  it('clears the text once the server has it', async () => {
    const steer = vi.fn(() => Promise.resolve(true))
    render(<SteerBox state={driving} user={arjun} actions={{ ...actions, steer }} />)
    const box = screen.getByLabelText('Steer the agent')
    fireEvent.change(box, { target: { value: 'go' } })
    fireEvent.click(screen.getByRole('button', { name: 'Send to agent' }))
    await waitFor(() => expect(box).toHaveValue(''))
  })

  it('isolates a driver name with a bidi override', () => {
    const spoof = { ...emptyRoomState('r1'), driver: { id: 'x', name: 'Eve\u202E' } }
    render(<SteerBox state={spoof} user={kabir} actions={actions} />)
    const name = screen.getByTestId('steer-status').querySelector('bdi')
    expect(name).toHaveTextContent('Eve\u202E')
  })
})

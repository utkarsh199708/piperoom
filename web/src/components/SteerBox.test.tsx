import { afterEach, describe, expect, it, vi } from 'vitest'
import { fireEvent, render, screen, waitFor } from '@testing-library/react'
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
    fireEvent.focus(screen.getByLabelText('Steer the agent')) // idle is one row without the button
    expect(screen.getByRole('button', { name: 'Send to agent' })).toBeInTheDocument()
    expect(screen.queryByText('Request control')).not.toBeInTheDocument()
  })

  it('watcher sees "Suggest", "Request control", and the driver queue hint', () => {
    const state = { ...emptyRoomState('r1'), driver: { id: 'arjun', name: 'Arjun' } }
    render(<SteerBox state={state} user={kabir} actions={actions} />)
    expect(screen.getByText(/Arjun's queue/)).toBeInTheDocument()
    fireEvent.focus(screen.getByLabelText('Steer the agent'))
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
          status: 'pending' as const,
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
  it('the driver sees "You are driving" with the lease and sends with the button', async () => {
    vi.mocked(actions.steer).mockReset().mockResolvedValue(true)
    const state = { ...emptyRoomState('r1'), driver: { id: 'arjun', name: 'Arjun', lease_until: inAnHour() } }
    render(<SteerBox state={state} user={arjun} actions={actions} />)
    expect(screen.getByText(/You are driving · lease 2[34]m/)).toBeInTheDocument()
    const box = screen.getByLabelText('Steer the agent')
    expect(box).toBeEnabled()
    fireEvent.focus(box)
    const send = screen.getByRole('button', { name: 'Send to agent' })
    expect(send).toBeDisabled()
    fireEvent.change(box, { target: { value: 'check the DDL log' } })
    fireEvent.click(send)
    expect(actions.steer).toHaveBeenCalledWith('check the DDL log')
    await waitFor(() => expect(box).toHaveValue(''))
  })

  it('keeps the text, shows the offline strip and retries when the send did not reach the server', async () => {
    vi.mocked(actions.steer).mockReset().mockResolvedValueOnce(false).mockResolvedValueOnce(true)
    const state = { ...emptyRoomState('r1'), driver: { id: 'arjun', name: 'Arjun', lease_until: inAnHour() } }
    render(<SteerBox state={state} user={arjun} actions={actions} />)
    const box = screen.getByLabelText('Steer the agent')
    fireEvent.focus(box)
    fireEvent.change(box, { target: { value: 'look at the slot' } })
    fireEvent.click(screen.getByRole('button', { name: 'Send to agent' }))
    const alert = await screen.findByRole('alert')
    expect(alert).toHaveTextContent("Can't reach Piperoom")
    expect(box).toHaveValue('look at the slot')
    fireEvent.click(screen.getByRole('button', { name: 'Retry' }))
    await waitFor(() => expect(box).toHaveValue(''))
    expect(screen.queryByRole('alert')).not.toBeInTheDocument()
    expect(actions.steer).toHaveBeenCalledTimes(2)
  })

  it('says messages are kept while the socket reconnects', () => {
    const state = { ...emptyRoomState('r1'), driver: { id: 'arjun', name: 'Arjun', lease_until: inAnHour() } }
    render(<SteerBox state={state} user={arjun} actions={actions} conn="reconnecting" />)
    expect(screen.getByText(/messages are kept until the room is back/)).toBeInTheDocument()
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
    expect(screen.getByText(/Arjun is driving · lease 2[34]m left · your message goes to Arjun's queue/)).toBeInTheDocument()
    expect(screen.getByPlaceholderText(/Suggest a step for Arjun/)).toBeInTheDocument()
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

describe('SteerBox idle row (F1b)', () => {
  const driving = { ...emptyRoomState('r1'), driver: { id: 'arjun', name: 'Arjun' } }
  afterEach(() => {
    vi.unstubAllGlobals()
    document.documentElement.style.removeProperty('--composer-h')
  })

  it('idle composer is one row and grows on focus', () => {
    render(<SteerBox state={driving} user={arjun} actions={actions} />)
    const section = screen.getByRole('region', { name: 'Steer' })
    expect(section).toHaveAttribute('data-idle', 'true')
    expect(screen.queryByRole('button', { name: 'Send to agent' })).not.toBeInTheDocument()
    fireEvent.focus(screen.getByLabelText('Steer the agent'))
    expect(section).not.toHaveAttribute('data-idle')
    expect(screen.getByRole('button', { name: 'Send to agent' })).toBeInTheDocument()
    fireEvent.blur(screen.getByLabelText('Steer the agent'))
    expect(section).toHaveAttribute('data-idle', 'true')
    fireEvent.change(screen.getByLabelText('Steer the agent'), { target: { value: 'x' } })
    expect(section).not.toHaveAttribute('data-idle')
  })

  it('sets --composer-h from the section height and clears it on unmount', () => {
    let notify: () => void = () => undefined
    vi.stubGlobal(
      'ResizeObserver',
      class {
        constructor(cb: () => void) {
          notify = cb
        }
        observe() {}
        disconnect() {}
      },
    )
    vi.spyOn(HTMLElement.prototype, 'getBoundingClientRect').mockReturnValue({ height: 57 } as DOMRect)
    const { unmount } = render(<SteerBox state={driving} user={arjun} actions={actions} />)
    expect(document.documentElement.style.getPropertyValue('--composer-h')).toBe('57px')
    vi.spyOn(HTMLElement.prototype, 'getBoundingClientRect').mockReturnValue({ height: 120 } as DOMRect)
    notify()
    expect(document.documentElement.style.getPropertyValue('--composer-h')).toBe('120px')
    unmount()
    expect(document.documentElement.style.getPropertyValue('--composer-h')).toBe('')
    vi.restoreAllMocks()
  })
})

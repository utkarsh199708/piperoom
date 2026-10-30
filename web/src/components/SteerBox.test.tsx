import { afterEach, describe, expect, it, vi } from 'vitest'
import { act, fireEvent, render, screen, waitFor } from '@testing-library/react'
import { SteerBox } from './SteerBox'
import type { DeliveredListener, RoomActions, SendControls } from '../hooks/useRoom'
import type { Sent } from '../hooks/sendLedger'
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
  hypothesisVerdict: vi.fn(),
  nudge: vi.fn().mockResolvedValue({ ok: true }),
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
    expect(screen.getByText(/your message goes to their queue/)).toBeInTheDocument()
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
    expect(actions.steer).toHaveBeenCalledWith('check the DDL log', { clientId: expect.any(String) })
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

  // #459: a Retry is the same action, so it keeps the failed send's client_id.
  it('the Retry keeps the failed send\'s client_id', async () => {
    vi.mocked(actions.steer).mockReset().mockResolvedValueOnce(false).mockResolvedValueOnce(true)
    const state = { ...emptyRoomState('r1'), driver: { id: 'arjun', name: 'Arjun', lease_until: inAnHour() } }
    render(<SteerBox state={state} user={arjun} actions={actions} />)
    const box = screen.getByLabelText('Steer the agent')
    fireEvent.change(box, { target: { value: 'look at the slot' } })
    fireEvent.click(screen.getByRole('button', { name: 'Send to agent' }))
    fireEvent.click(await screen.findByRole('button', { name: 'Retry' }))
    await waitFor(() => expect(box).toHaveValue(''))
    const [first, second] = vi.mocked(actions.steer).mock.calls
    expect(first[0]).toBe('look at the slot')
    expect(second[0]).toBe('look at the slot')
    expect(second[1]?.clientId).toBeTruthy()
    expect(second[1]?.clientId).toBe(first[1]?.clientId)
  })

  it('edited words sent while the Retry strip is up are a new message, not a retry', async () => {
    vi.mocked(actions.steer).mockReset().mockResolvedValueOnce(false).mockResolvedValueOnce(true)
    const state = { ...emptyRoomState('r1'), driver: { id: 'arjun', name: 'Arjun', lease_until: inAnHour() } }
    render(<SteerBox state={state} user={arjun} actions={actions} />)
    const box = screen.getByLabelText('Steer the agent')
    fireEvent.change(box, { target: { value: 'look at the slot' } })
    fireEvent.click(screen.getByRole('button', { name: 'Send to agent' }))
    await screen.findByRole('button', { name: 'Retry' })
    fireEvent.change(box, { target: { value: 'look at the lag' } })
    fireEvent.click(screen.getByRole('button', { name: 'Send to agent' }))
    await waitFor(() => expect(box).toHaveValue(''))
    const [first, second] = vi.mocked(actions.steer).mock.calls
    expect(second[0]).toBe('look at the lag')
    expect(second[1]?.clientId).not.toBe(first[1]?.clientId)
  })

  // A fake ledger handle: one listener (the composer's), every action still owed unless cancelled.
  function fakeSends() {
    // A Set, like the ledger: the composer's own listener and the #550 delivery watcher.
    const listeners = new Set<DeliveredListener>()
    const cancelled: string[] = []
    const sends: SendControls = {
      onDelivered: (l) => {
        listeners.add(l)
        return () => {
          listeners.delete(l)
        }
      },
      cancel: (id) => {
        cancelled.push(id)
      },
      isPending: (id) => !cancelled.includes(id),
    }
    return { sends, cancelled, deliver: (sent: Sent) => [...listeners].map((l) => l(sent)).some(Boolean) }
  }

  // #390 scenario B: the failed send landed (reconnect copy or its event); the field and the Retry
  // strip go away without waiting for the person.
  it('clears the field and the Retry strip when its failed send landed', async () => {
    const fake = fakeSends()
    const withSignal: RoomActions = { ...actions, steer: vi.fn().mockResolvedValue(false), sends: fake.sends }
    const state = { ...emptyRoomState('r1'), driver: { id: 'arjun', name: 'Arjun', lease_until: inAnHour() } }
    render(<SteerBox state={state} user={arjun} actions={withSignal} />)
    const box = screen.getByLabelText('Steer the agent')
    fireEvent.change(box, { target: { value: 'look at the slot' } })
    fireEvent.click(screen.getByRole('button', { name: 'Send to agent' }))
    await screen.findByRole('button', { name: 'Retry' })
    const clientId = vi.mocked(withSignal.steer).mock.calls[0][1]!.clientId!
    // Another action is not the composer's, even with the same words.
    let taken = true
    act(() => {
      taken = fake.deliver({ clientId: 'other', kind: 'steer', text: 'look at the slot' })
    })
    expect(taken).toBe(false)
    act(() => {
      taken = fake.deliver({ clientId, kind: 'steer', text: 'look at the slot' })
    })
    expect(taken).toBe(true)
    expect(box).toHaveValue('')
    expect(screen.queryByRole('button', { name: 'Retry' })).not.toBeInTheDocument()
    expect(withSignal.steer).toHaveBeenCalledTimes(1)
  })

  // #535: an edit gives up on the failed action. It is cancelled in the ledger at once, so no
  // Retry, reconnect or live retry sends the old words after the new ones.
  it('an edit after a failed send cancels it; the edited words are kept', async () => {
    const fake = fakeSends()
    const withSignal: RoomActions = { ...actions, steer: vi.fn().mockResolvedValue(false), sends: fake.sends }
    const state = { ...emptyRoomState('r1'), driver: { id: 'arjun', name: 'Arjun', lease_until: inAnHour() } }
    render(<SteerBox state={state} user={arjun} actions={withSignal} />)
    const box = screen.getByLabelText('Steer the agent')
    fireEvent.change(box, { target: { value: 'restart the connector' } })
    fireEvent.click(screen.getByRole('button', { name: 'Send to agent' }))
    await screen.findByRole('button', { name: 'Retry' })
    expect(screen.getByText('Editing discards the unsent message.')).toBeInTheDocument()
    const clientId = vi.mocked(withSignal.steer).mock.calls[0][1]!.clientId!
    fireEvent.change(box, { target: { value: 'do NOT restart the connector' } })
    expect(fake.cancelled).toEqual([clientId])
    // Had it been on the wire and landed anyway, it is no longer the composer's.
    let taken = true
    act(() => {
      taken = fake.deliver({ clientId, kind: 'steer', text: 'restart the connector' })
    })
    expect(taken).toBe(false)
    expect(box).toHaveValue('do NOT restart the connector')
  })

  it('a prefill ("Ask the agent") while the Retry strip is up cancels the failed send too', async () => {
    const fake = fakeSends()
    const withSignal: RoomActions = { ...actions, steer: vi.fn().mockResolvedValue(false), sends: fake.sends }
    const state = { ...emptyRoomState('r1'), driver: { id: 'arjun', name: 'Arjun', lease_until: inAnHour() } }
    const { rerender } = render(<SteerBox state={state} user={arjun} actions={withSignal} />)
    fireEvent.change(screen.getByLabelText('Steer the agent'), { target: { value: 'look at the slot' } })
    fireEvent.click(screen.getByRole('button', { name: 'Send to agent' }))
    await screen.findByRole('button', { name: 'Retry' })
    const clientId = vi.mocked(withSignal.steer).mock.calls[0][1]!.clientId!
    expect(fake.cancelled).toEqual([])
    rerender(<SteerBox state={state} user={arjun} actions={withSignal} prefill={{ text: 'check the lag', n: 1 }} />)
    expect(screen.getByLabelText('Steer the agent')).toHaveValue('check the lag')
    expect(fake.cancelled).toEqual([clientId])
  })

  // #535 (low): after 409 client_id_reused the ledger has dropped the entry, so the same id could
  // only be refused again. No Retry strip is offered for it, and the next send is a new action.
  it('an action the ledger no longer owes gets no Retry strip; sending again uses a new client_id', async () => {
    const fake = fakeSends()
    const withSignal: RoomActions = {
      ...actions,
      steer: vi.fn().mockResolvedValue(false),
      sends: { ...fake.sends, isPending: () => false },
    }
    const state = { ...emptyRoomState('r1'), driver: { id: 'arjun', name: 'Arjun', lease_until: inAnHour() } }
    render(<SteerBox state={state} user={arjun} actions={withSignal} />)
    fireEvent.change(screen.getByLabelText('Steer the agent'), { target: { value: 'look at the slot' } })
    fireEvent.click(screen.getByRole('button', { name: 'Send to agent' }))
    await waitFor(() => expect(withSignal.steer).toHaveBeenCalledTimes(1))
    await act(async () => {})
    expect(screen.queryByRole('button', { name: 'Retry' })).not.toBeInTheDocument()
    fireEvent.click(screen.getByRole('button', { name: 'Send to agent' }))
    await waitFor(() => expect(withSignal.steer).toHaveBeenCalledTimes(2))
    const [first, second] = vi.mocked(withSignal.steer).mock.calls
    expect(second[0]).toBe('look at the slot')
    expect(second[1]?.clientId).not.toBe(first[1]?.clientId)
  })

  // Review nit 3 (#536): a new ledger (another user or session) takes the old Retry strip down.
  it('a replaced send ledger takes the Retry strip down', async () => {
    const fake = fakeSends()
    const withSignal: RoomActions = { ...actions, steer: vi.fn().mockResolvedValue(false), sends: fake.sends }
    const state = { ...emptyRoomState('r1'), driver: { id: 'arjun', name: 'Arjun', lease_until: inAnHour() } }
    const { rerender } = render(<SteerBox state={state} user={arjun} actions={withSignal} />)
    fireEvent.change(screen.getByLabelText('Steer the agent'), { target: { value: 'look at the slot' } })
    fireEvent.click(screen.getByRole('button', { name: 'Send to agent' }))
    await screen.findByRole('button', { name: 'Retry' })
    rerender(<SteerBox state={state} user={arjun} actions={{ ...withSignal, sends: fakeSends().sends }} />)
    expect(screen.queryByRole('button', { name: 'Retry' })).not.toBeInTheDocument()
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
    expect(actions.steer).toHaveBeenCalledWith('go', { clientId: expect.any(String) })
  })

  it('a watcher is told whose queue the message goes to, and the lease', () => {
    const state = { ...emptyRoomState('r1'), driver: { id: 'arjun', name: 'Arjun', lease_until: inAnHour() } }
    render(<SteerBox state={state} user={kabir} actions={actions} />)
    expect(document.querySelector('[data-slot="steer-status"]')).toHaveTextContent(
      /^Arjun.* is driving · lease 2[34]m left · your message goes to their queue/,
    )
    expect(screen.getByPlaceholderText(/Suggest a step for Arjun/)).toBeInTheDocument()
  })

  it('with no driver a member can suggest', () => {
    const state = emptyRoomState('r1')
    render(<SteerBox state={state} user={kabir} actions={actions} />)
    const field = screen.getByLabelText('Steer the agent')
    expect(field).toBeEnabled()
    expect(field).toHaveAttribute('placeholder', 'Suggest a first step…')
    expect(screen.getByText(/No driver · your suggestion is queued for whoever takes control/)).toBeInTheDocument()
    fireEvent.change(field, { target: { value: 'check the connector' } })
    fireEvent.click(screen.getByRole('button', { name: 'Suggest' }))
    expect(actions.steer).toHaveBeenCalledWith('check the connector', { clientId: expect.any(String) })
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

  it('idle composer stacks on phones and goes one row from sm up (KAN-564)', () => {
    render(<SteerBox state={driving} user={arjun} actions={actions} />)
    const section = screen.getByRole('region', { name: 'Steer' })
    // Phone: column, so the composer gets the full width on its own row. sm+: the one-row bar.
    expect(section).toHaveClass('flex-col', 'sm:flex-row', 'sm:h-14')
    expect(section).not.toHaveClass('flex-row', 'h-14')
    const form = screen.getByLabelText('Steer the agent').closest('form')
    expect(form).toHaveClass('sm:flex-1')
    expect(form).not.toHaveClass('flex-1')
    expect(section.querySelector('[data-slot="steer-status"]')?.parentElement).not.toHaveClass('max-w-[55%]')
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

  describe('F5 composer', () => {
    const driverState = { ...emptyRoomState('r1'), driver: { id: 'arjun', name: 'Arjun' } }
    const field = () => screen.getByLabelText('Steer the agent')

    afterEach(() => {
      localStorage.clear()
      sessionStorage.clear()
      vi.useRealTimers()
      vi.clearAllMocks()
    })

    it('names the mode before sending', () => {
      render(<SteerBox state={driverState} user={kabir} actions={actions} />)
      fireEvent.focus(field())
      expect(screen.getByText(/Suggesting to Arjun/)).toBeInTheDocument()
      fireEvent.change(field(), { target: { value: '?why is task 0 failing' } })
      expect(screen.getByText('Asking the agent · it answers under Room comments')).toBeInTheDocument()
      fireEvent.click(screen.getByRole('button', { name: 'Ask' }))
      expect(actions.comment).toHaveBeenCalledWith('@agent why is task 0 failing', undefined, { clientId: expect.any(String) })
      expect(actions.steer).not.toHaveBeenCalled()
    })

    it('a driver question is a steer', () => {
      render(<SteerBox state={driverState} user={arjun} actions={actions} />)
      fireEvent.change(field(), { target: { value: '?why is task 0 failing' } })
      fireEvent.click(screen.getByRole('button', { name: 'Ask' }))
      expect(actions.steer).toHaveBeenCalledWith('Question: why is task 0 failing', { clientId: expect.any(String) })
    })

    it('a bare ? sends nothing', () => {
      render(<SteerBox state={driverState} user={arjun} actions={actions} />)
      fireEvent.change(field(), { target: { value: '?' } })
      fireEvent.keyDown(field(), { key: 'Enter', ctrlKey: true })
      expect(actions.steer).not.toHaveBeenCalled()
    })

    it('chips fill the field and never send', () => {
      render(<SteerBox state={driverState} user={arjun} actions={actions} />)
      fireEvent.focus(field())
      fireEvent.click(screen.getByRole('button', { name: 'Check the connector status' }))
      expect(field()).toHaveValue('Check the connector status')
      expect(actions.steer).not.toHaveBeenCalled()
    })

    it('keeps the draft across a remount and clears it on send', async () => {
      vi.mocked(actions.steer).mockResolvedValue(true)
      vi.useFakeTimers()
      const first = render(<SteerBox state={driverState} user={arjun} actions={actions} />)
      fireEvent.change(field(), { target: { value: 'half a thought' } })
      act(() => {
        vi.advanceTimersByTime(400)
      })
      first.unmount()
      render(<SteerBox state={driverState} user={arjun} actions={actions} />)
      expect(field()).toHaveValue('half a thought')
      await act(async () => {
        fireEvent.click(screen.getByRole('button', { name: 'Send to agent' }))
      })
      expect(localStorage.getItem('piperoom.draft.r1')).toBeNull()
    })

    it('arrow up on an empty field recalls the last sent message', async () => {
      vi.mocked(actions.steer).mockResolvedValue(true)
      render(<SteerBox state={driverState} user={arjun} actions={actions} />)
      fireEvent.change(field(), { target: { value: 'look at lag' } })
      fireEvent.keyDown(field(), { key: 'Enter', ctrlKey: true })
      await waitFor(() => expect(field()).toHaveValue(''))
      fireEvent.keyDown(field(), { key: 'ArrowUp' })
      expect(field()).toHaveValue('look at lag')
    })

    it('storage that throws does not break the composer', () => {
      const spy = vi.spyOn(Storage.prototype, 'getItem').mockImplementation(() => {
        throw new Error('blocked')
      })
      render(<SteerBox state={driverState} user={arjun} actions={actions} />)
      expect(field()).toHaveValue('')
      spy.mockRestore()
    })

    it('the driver can decline a suggestion with a reason', () => {
      const state = {
        ...driverState,
        suggestions: [
          { suggestion_id: 'g1', from: { kind: 'human' as const, id: 'kabir', name: 'Kabir' }, text: 'restart it', status: 'pending' as const },
        ],
      }
      render(<SteerBox state={state} user={arjun} actions={actions} />)
      fireEvent.click(screen.getByRole('button', { name: 'Decline' }))
      const input = screen.getByLabelText(/Reason for declining/)
      fireEvent.change(input, { target: { value: 'too risky' } })
      fireEvent.submit(input)
      expect(actions.declineSuggestion).toHaveBeenCalledWith('g1', 'too risky')
    })

    it('shows the suggester what became of their suggestions', () => {
      const me = { kind: 'human' as const, id: 'kabir', name: 'Kabir' }
      const driver = { kind: 'human' as const, id: 'arjun', name: 'Arjun' }
      const state = {
        ...driverState,
        suggestions: [
          { suggestion_id: 'g1', from: me, text: 'a', status: 'pending' as const },
          { suggestion_id: 'g2', from: me, text: 'b', status: 'accepted' as const, sent_by: driver, sent_at: '2026-10-14T10:02:00Z' },
          { suggestion_id: 'g3', from: me, text: 'c', status: 'declined' as const, declined_by: driver, reason: 'not now' },
        ],
      }
      render(<SteerBox state={state} user={kabir} actions={actions} />)
      fireEvent.focus(field())
      expect(screen.getByText(/^Queued for/)).toHaveTextContent(/^Queued for Arjun/)
      expect(screen.getByText(/^Sent by/)).toHaveTextContent(/^Sent by Arjun.* · /)
      expect(screen.getByText(/^Declined by/)).toHaveTextContent(/^Declined by Arjun/)
      expect(screen.getByText(/^Declined by/)).toHaveAttribute('title', 'not now')
    })

    it('with no driver a pending chip says no driver yet', () => {
      const me = { kind: 'human' as const, id: 'kabir', name: 'Kabir' }
      const state = { ...emptyRoomState('r1'), suggestions: [{ suggestion_id: 'g1', from: me, text: 'a', status: 'pending' as const }] }
      render(<SteerBox state={state} user={kabir} actions={actions} />)
      fireEvent.focus(field())
      expect(screen.getByText('Queued · no driver yet')).toBeInTheDocument()
    })
  })
})

describe('SteerBox lease copy', () => {
  const drive = (leaseUntil: string) => ({ ...emptyRoomState('r1'), driver: { id: 'arjun', name: 'Arjun', lease_until: leaseUntil } })
  it('a fresh 30-minute lease reads 30m', () => {
    render(<SteerBox state={drive(new Date(Date.now() + 30 * 60_000 + 5000).toISOString())} user={arjun} actions={actions} />)
    expect(screen.getByText('You are driving · lease 30m')).toBeInTheDocument()
  })
  it('under a minute is not expired', () => {
    render(<SteerBox state={drive(new Date(Date.now() + 30_000).toISOString())} user={arjun} actions={actions} />)
    expect(screen.getByText('Lease ends in under a minute — any action renews')).toBeInTheDocument()
  })
  it('a lapsed lease reads expired', () => {
    render(<SteerBox state={drive(new Date(Date.now() - 1000).toISOString())} user={arjun} actions={actions} />)
    expect(screen.getByText('You are driving · lease expired')).toBeInTheDocument()
  })
})

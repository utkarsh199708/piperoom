import { beforeEach, describe, expect, it, vi } from 'vitest'
import { fireEvent, render, screen } from '@testing-library/react'
import { SteerBox } from './SteerBox'
import type { RoomActions } from '../hooks/useRoom'
import type { CurrentUser } from '../state/permissions'
import { emptyRoomState } from '../state/types'

vi.mock('../hooks/usePeople', async (importOriginal) => {
  const real = await importOriginal<typeof import('../hooks/usePeople')>()
  return {
    ...real,
    usePeople: () => ({
      status: 'ready' as const,
      people: [{ id: 'kabir', name: 'Kabir' }],
      reload: vi.fn(),
    }),
  }
})

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
const vera: CurrentUser = { id: 'vera', name: 'Vera', layers: [], role: 'viewer' }

beforeEach(() => {
  vi.clearAllMocks()
})

describe('SteerBox — sticky composer', () => {
  it('is the sticky composer panel for a member', () => {
    const state = { ...emptyRoomState('r1'), driver: { id: 'arjun', name: 'Arjun' } }
    const { container } = render(<SteerBox state={state} user={arjun} actions={actions} />)
    expect(container.querySelector('section')).toHaveClass('steer', 'composer')
  })

  it('tells the driver they are driving, with the lease', () => {
    const lease = new Date(Date.now() + 24 * 60_000 + 10_000).toISOString()
    const state = { ...emptyRoomState('r1'), driver: { id: 'arjun', name: 'Arjun', lease_until: lease } }
    render(<SteerBox state={state} user={arjun} actions={actions} />)
    expect(screen.getByText('You are driving · lease 24m')).toBeInTheDocument()
  })

  it('warns when the driver lease has expired', () => {
    const lease = new Date(Date.now() - 60_000).toISOString()
    const state = { ...emptyRoomState('r1'), driver: { id: 'arjun', name: 'Arjun', lease_until: lease } }
    render(<SteerBox state={state} user={kabir} actions={actions} />)
    expect(screen.getByText("Arjun's lease expired")).toHaveClass('composer__expired')
    expect(screen.getByRole('button', { name: 'Take control' })).toBeInTheDocument()
  })

  it('Ctrl+Enter sends the steer and clears the box', () => {
    const state = { ...emptyRoomState('r1'), driver: { id: 'arjun', name: 'Arjun' } }
    render(<SteerBox state={state} user={arjun} actions={actions} />)
    const box = screen.getByLabelText('Steer the agent')
    fireEvent.change(box, { target: { value: 'check the config' } })
    fireEvent.keyDown(box, { key: 'Enter', ctrlKey: true })
    expect(actions.steer).toHaveBeenCalledWith('check the config')
    expect(box).toHaveValue('')
  })

  it('Cmd+Enter also sends, and a plain Enter does not', () => {
    const state = { ...emptyRoomState('r1'), driver: { id: 'arjun', name: 'Arjun' } }
    render(<SteerBox state={state} user={arjun} actions={actions} />)
    const box = screen.getByLabelText('Steer the agent')
    fireEvent.change(box, { target: { value: 'one' } })
    fireEvent.keyDown(box, { key: 'Enter' })
    expect(actions.steer).not.toHaveBeenCalled()
    fireEvent.keyDown(box, { key: 'Enter', metaKey: true })
    expect(actions.steer).toHaveBeenCalledWith('one')
  })

  it('shows the driver the grant picker in the footer', () => {
    const state = { ...emptyRoomState('r1'), driver: { id: 'arjun', name: 'Arjun' } }
    render(<SteerBox state={state} user={arjun} actions={actions} />)
    fireEvent.click(screen.getByRole('button', { name: 'Give control to…' }))
    fireEvent.click(screen.getByRole('button', { name: 'Give control' }))
    expect(actions.grantControl).toHaveBeenCalledWith('kabir')
  })

  it('lists suggestions and control requests above the box', () => {
    const state = {
      ...emptyRoomState('r1'),
      driver: { id: 'arjun', name: 'Arjun' },
      suggestions: [
        { suggestion_id: 'g1', from: { kind: 'human' as const, id: 'kabir', name: 'Kabir' }, text: 'look at lag' },
      ],
      control_requests: [{ from: { kind: 'human' as const, id: 'priya', name: 'Priya' } }],
    }
    const { container } = render(<SteerBox state={state} user={arjun} actions={actions} />)
    const lists = container.querySelectorAll('.suggestions')
    expect(lists).toHaveLength(2)
    const form = container.querySelector('.steer__form') as HTMLElement
    for (const l of Array.from(lists)) {
      expect(l.compareDocumentPosition(form) & Node.DOCUMENT_POSITION_FOLLOWING).toBeTruthy()
    }
    fireEvent.click(screen.getByRole('button', { name: 'Grant control' }))
    expect(actions.grantControl).toHaveBeenCalledWith('priya')
  })

  it('a viewer gets one quiet line and no box', () => {
    const state = { ...emptyRoomState('r1'), driver: { id: 'arjun', name: 'Arjun' } }
    const { container } = render(<SteerBox state={state} user={vera} actions={actions} />)
    expect(screen.getByText(/View only: you can read this room but not steer it\./)).toBeInTheDocument()
    expect(screen.queryByLabelText('Steer the agent')).not.toBeInTheDocument()
    expect(container.querySelector('.composer')).toBeNull()
  })
})

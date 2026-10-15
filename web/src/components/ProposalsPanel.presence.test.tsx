import { beforeEach, describe, expect, it, vi } from 'vitest'
import { fireEvent, render, screen, waitFor } from '@testing-library/react'
import { toast } from 'sonner'
import { ProposalsPanel } from './ProposalsPanel'
import type { RoomActions } from '../hooks/useRoom'
import type { CurrentUser } from '../state/permissions'
import { emptyRoomState } from '../state/types'
import type { ProposalState } from '../state/types'

vi.mock('sonner', () => ({ toast: Object.assign(vi.fn(), { warning: vi.fn(), dismiss: vi.fn(), error: vi.fn() }) }))

const nudge = vi.fn()
const actions = {
  comment: vi.fn(), steer: vi.fn(), sendSuggestion: vi.fn(), requestControl: vi.fn(), grantControl: vi.fn(),
  close: vi.fn(), approve: vi.fn(), reject: vi.fn(), handoff: vi.fn(), acceptHandoff: vi.fn(), trace: vi.fn(),
  hypothesisVerdict: vi.fn(), nudge,
} as unknown as RoomActions

const proposal: ProposalState = {
  proposal_id: 'p1', title: 'Restart the connector', steps: [], risk: 'low', rollback: 'undo',
  required_layers: ['kafka-connect', 'postgres'], approvals: { 'kafka-connect': 'pending', postgres: 'pending' },
  status: 'open', decisions: {}, params_hash: 'abc123abc123', executed: [],
}
const priya: CurrentUser = { id: 'priya', name: 'Priya', layers: ['postgres'] }
const people = [
  { id: 'arjun', name: 'Arjun', layers: ['kafka-connect'] },
  { id: 'priya', name: 'Priya', layers: ['postgres'] },
]
const since = () => new Date(Date.now() - 12 * 60_000).toISOString()
const room = (extra = {}) => ({ ...emptyRoomState('r_1'), title: 'Warehouse "orders"\nstale', proposals: [proposal], ...extra })

beforeEach(() => {
  nudge.mockReset()
  vi.mocked(toast).mockReset()
  Object.assign(navigator, { clipboard: { writeText: vi.fn().mockResolvedValue(undefined) } })
})

describe('pending layer row (F9)', () => {
  it('names the owner with presence', () => {
    render(<ProposalsPanel state={room()} user={priya} actions={actions} people={people} presence={[{ id: 'arjun', name: 'Arjun', state: 'away', since: since() }]} />)
    expect(screen.getByText(/Waiting for/)).toHaveTextContent(/Waiting for Arjun.* · away 12m/)
  })

  it('falls back to "a kafka-connect owner" when nobody in the list owns it', () => {
    render(<ProposalsPanel state={room()} user={priya} actions={actions} people={[people[1]]} presence={[]} />)
    expect(screen.getByText('Waiting for a kafka-connect owner')).toBeInTheDocument()
  })

  it('Nudge copies a sanitized deep link, calls the server and toasts', async () => {
    nudge.mockResolvedValue({ ok: true })
    render(<ProposalsPanel state={room()} user={priya} actions={actions} people={people} presence={[]} />)
    fireEvent.click(screen.getByRole('button', { name: 'Nudge kafka-connect owners' }))
    await waitFor(() => expect(nudge).toHaveBeenCalledWith('p1', 'kafka-connect'))
    const text = vi.mocked(navigator.clipboard.writeText).mock.calls[0][0]
    expect(text).toBe(`Your approval is needed for kafka-connect on "Warehouse 'orders' stale": ${window.location.origin}/rooms/r_1`)
    await waitFor(() => expect(toast).toHaveBeenCalledWith('Copied — paste it to Arjun'))
  })

  it('answers 409 already_nudged with the "link copied anyway" toast', async () => {
    nudge.mockResolvedValue({ ok: false, code: 'already_nudged' })
    render(<ProposalsPanel state={room()} user={priya} actions={actions} people={people} presence={[]} />)
    fireEvent.click(screen.getByRole('button', { name: 'Nudge kafka-connect owners' }))
    await waitFor(() => expect(toast).toHaveBeenCalledWith('Already nudged a few minutes ago — link copied anyway'))
  })

  it('shows when it was nudged and disables the button for 10 minutes', () => {
    const state = room({ nudges: { 'p1/kafka-connect': { at: new Date(Date.now() - 2 * 60_000).toISOString(), by: { kind: 'human', id: 'priya', name: 'Priya' } } } })
    render(<ProposalsPanel state={state} user={priya} actions={actions} people={people} presence={[]} />)
    expect(screen.getByText(/^nudged /)).toBeInTheDocument()
    expect(screen.getByRole('button', { name: 'Nudge kafka-connect owners' })).toBeDisabled()
  })

  it('has no Nudge for a viewer or in a closed room', () => {
    const { unmount } = render(<ProposalsPanel state={room()} user={{ ...priya, role: 'viewer' }} actions={actions} people={people} presence={[]} />)
    expect(screen.queryByRole('button', { name: /^Nudge/ })).not.toBeInTheDocument()
    unmount()
    render(<ProposalsPanel state={room({ status: 'closed' })} user={priya} actions={actions} people={people} presence={[]} />)
    expect(screen.queryByRole('button', { name: /^Nudge/ })).not.toBeInTheDocument()
  })
})

import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { fireEvent, render, screen, waitFor, within } from '@testing-library/react'
import { ProposalsPanel } from './ProposalsPanel'
import type { RoomActions } from '../hooks/useRoom'
import type { CurrentUser } from '../state/permissions'
import { emptyRoomState } from '../state/types'
import type { ProposalState } from '../state/types'

// KAN-573: every confirm dialog in the proposals panel names the modifier of the platform (Ctrl,
// or ⌘ on a Mac), and the shortcut it names confirms.

const approve = vi.fn()
const approveAll = vi.fn()
const actions: RoomActions = {
  comment: vi.fn(),
  steer: vi.fn(),
  sendSuggestion: vi.fn(),
  declineSuggestion: vi.fn(),
  requestControl: vi.fn(),
  grantControl: vi.fn(),
  close: vi.fn(),
  approve,
  approveAll,
  reject: vi.fn(),
  handoff: vi.fn(),
  acceptHandoff: vi.fn(),
  label: vi.fn(),
  checklist: vi.fn(),
  trace: vi.fn(),
  hypothesisVerdict: vi.fn(),
  nudge: vi.fn().mockResolvedValue({ ok: true }),
}

const HASH = 'abc123abc123def456abc123abc123def456abc123abc123def456abc123abc1'
const sam: CurrentUser = { id: 'sam', name: 'Sam', layers: ['schema-registry', 'kafka-connect'] }
const arjun: CurrentUser = { id: 'arjun', name: 'Arjun', layers: ['kafka-connect'] }

function proposal(over: Partial<ProposalState> = {}): ProposalState {
  return {
    proposal_id: 'p1',
    title: 'Restart the task',
    steps: [
      { action: 'set_compatibility', layer: 'schema-registry', params: {}, description: 'Allow the schema.' },
      { action: 'restart_connector_task', layer: 'kafka-connect', params: {}, description: 'Restart the task.' },
    ],
    risk: 'low',
    rollback: 'none',
    required_layers: ['schema-registry', 'kafka-connect'],
    approvals: { 'schema-registry': 'pending', 'kafka-connect': 'pending' },
    status: 'open',
    decisions: {},
    params_hash: HASH,
    executed: [],
    ...over,
  }
}
const show = (user: CurrentUser, p: ProposalState) =>
  render(<ProposalsPanel state={{ ...emptyRoomState('r1'), pipeline_id: 'orders', proposals: [p] }} user={user} actions={actions} />)

const setPlatform = (value: string) => vi.spyOn(navigator, 'platform', 'get').mockReturnValue(value)
const kbds = (d: HTMLElement) => within(d).getAllByText(/⏎/).map((e) => e.textContent)

beforeEach(() => {
  approve.mockReset()
  approveAll.mockReset()
})
afterEach(() => vi.restoreAllMocks())

describe.each([
  ['Win32', 'Ctrl⏎'],
  ['Linux x86_64', 'Ctrl⏎'],
  ['MacIntel', '⌘⏎'],
])('confirm-dialog hints on %s', (platform, hint) => {
  beforeEach(() => setPlatform(platform))

  it('the solo typed confirm names the platform modifier', () => {
    show({ ...arjun, solo: true }, proposal({ approvals: { 'schema-registry': 'approved', 'kafka-connect': 'pending' } }))
    fireEvent.click(screen.getByRole('button', { name: 'Approve' }))
    expect(kbds(screen.getByRole('dialog'))).toEqual([hint])
  })

  it('the run confirm names the platform modifier, and that chord confirms', async () => {
    approve.mockResolvedValue({ ok: true })
    show(arjun, proposal({ approvals: { 'schema-registry': 'approved', 'kafka-connect': 'pending' } }))
    fireEvent.keyDown(window, { key: 'a' })
    const dialog = screen.getByRole('alertdialog')
    expect(kbds(dialog)).toEqual([hint])
    fireEvent.keyDown(dialog, { key: 'Enter', ctrlKey: platform !== 'MacIntel', metaKey: platform === 'MacIntel' })
    await waitFor(() => expect(approve).toHaveBeenCalledTimes(1))
  })

  it('the approve-all confirm names the platform modifier, and that chord confirms from the button', async () => {
    approveAll.mockResolvedValue({ ok: true })
    show(sam, proposal())
    fireEvent.click(screen.getByRole('button', { name: 'Approve and run' }))
    const dialog = screen.getByRole('alertdialog')
    expect(kbds(dialog)).toEqual([hint])
    fireEvent.keyDown(within(dialog).getByRole('button', { name: 'Approve and run' }), {
      key: 'Enter',
      ctrlKey: platform !== 'MacIntel',
      metaKey: platform === 'MacIntel',
    })
    await waitFor(() => expect(approveAll).toHaveBeenCalledTimes(1))
  })
})

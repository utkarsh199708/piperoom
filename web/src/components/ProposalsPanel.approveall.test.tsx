import { beforeEach, describe, expect, it, vi } from 'vitest'
import { fireEvent, render, screen, waitFor, within } from '@testing-library/react'
import { ProposalsPanel } from './ProposalsPanel'
import type { RoomActions } from '../hooks/useRoom'
import type { CurrentUser } from '../state/permissions'
import { emptyRoomState } from '../state/types'
import type { ProposalState, RoomState } from '../state/types'
import type { RoomMode } from '../api/types'

// P2c "Approve and run": one person who may approve every layer of a proposal gets one button, one
// confirm and one approve-all call, in place of a click and a confirm per layer.

const approve = vi.fn()
const approveAll = vi.fn()
const reject = vi.fn()
const base: Omit<RoomActions, 'approveAll'> = {
  comment: vi.fn(),
  steer: vi.fn(),
  sendSuggestion: vi.fn(),
  declineSuggestion: vi.fn(),
  requestControl: vi.fn(),
  grantControl: vi.fn(),
  close: vi.fn(),
  approve,
  reject,
  handoff: vi.fn(),
  acceptHandoff: vi.fn(),
  label: vi.fn(),
  checklist: vi.fn(),
  trace: vi.fn(),
  hypothesisVerdict: vi.fn(),
  nudge: vi.fn().mockResolvedValue({ ok: true }),
}
const actions: RoomActions = { ...base, approveAll }

const HASH = '9f2c1e7ab04d58c3d6e1f0a7b92c4d835e6a1f0b7c3d9e2a48b5c6d7e8f90a1b'
const RUN = 'Approve and run'
const WILL_NOT_RUN = 'Approve (will not run)'
const TYPE_BOX = /Solo mode: type orders\/\* to approve every layer/

// Sam owns both layers of the schema-change fix (a two-person team).
const sam: CurrentUser = { id: 'sam', name: 'Sam', layers: ['schema-registry', 'kafka-connect'] }
// Arjun owns only kafka-connect: the per-layer ceremony.
const arjun: CurrentUser = { id: 'arjun', name: 'Arjun', layers: ['kafka-connect', 'kafka'] }
const solo: RoomMode = { write: true, solo: true, auto_approve: [] }
const writeOff: RoomMode = { write: false, solo: false, auto_approve: [] }

function proposal(over: Partial<ProposalState> = {}): ProposalState {
  return {
    proposal_id: 'p1',
    title: 'Set subject compatibility to NONE, then restart the connector task',
    steps: [
      {
        action: 'set_compatibility',
        layer: 'schema-registry',
        params: { subject: 'pg.public.orders-value', compatibility: { from: 'BACKWARD', to: 'NONE' } },
        description: 'Allow the new schema to register.',
      },
      {
        action: 'restart_connector_task',
        layer: 'kafka-connect',
        params: { connector: 'pg-orders-connector', task: 0 },
        description: 'Restart the task.',
      },
    ],
    risk: 'low',
    rollback: 'set compatibility back to BACKWARD',
    required_layers: ['schema-registry', 'kafka-connect'],
    approvals: { 'schema-registry': 'pending', 'kafka-connect': 'pending' },
    decisions: {},
    params_hash: HASH,
    status: 'open',
    executed: [],
    ...over,
  }
}
function room(p: ProposalState = proposal(), over: Partial<RoomState> = {}): RoomState {
  return { ...emptyRoomState('r1'), pipeline_id: 'orders', proposals: [p], ...over }
}
const show = (state: RoomState = room(), user: CurrentUser = sam, acts: RoomActions = actions) =>
  render(<ProposalsPanel state={state} user={user} actions={acts} />)
const dialog = () => screen.getByRole('alertdialog')
const openConfirm = () => fireEvent.click(screen.getByRole('button', { name: RUN }))

beforeEach(() => {
  approve.mockReset()
  approveAll.mockReset()
  reject.mockReset()
})

describe('Approve and run: when it is offered', () => {
  it('the owner of every required layer gets one button and no per-layer Approve', () => {
    show()
    expect(screen.getAllByRole('button', { name: RUN })).toHaveLength(1)
    expect(screen.queryByRole('button', { name: 'Approve' })).not.toBeInTheDocument()
    expect(screen.getAllByRole('button', { name: /^Reject/ })).toHaveLength(1)
    // Each row still says whose layer it is, and the ribbon names both.
    expect(screen.getAllByText('pending · your layer')).toHaveLength(2)
    expect(screen.getByText('Needs your approval · schema-registry · kafka-connect')).toBeInTheDocument()
    expect(screen.getByTestId('approve-all')).toHaveTextContent('Records your approval for schema-registry, kafka-connect at once')
  })

  it('mode.solo offers it even when /api/me lists none of the layers', () => {
    show(room(proposal(), { mode: solo }), { id: 'sam', name: 'Sam', layers: [] })
    expect(screen.getByRole('button', { name: RUN })).toBeEnabled()
    expect(screen.getAllByText('pending · your layer')).toHaveLength(2)
  })

  it('an owner of one layer keeps the per-layer rows', () => {
    show(room(), arjun)
    expect(screen.queryByRole('button', { name: RUN })).not.toBeInTheDocument()
    expect(screen.queryByTestId('approve-all')).not.toBeInTheDocument()
    expect(within(document.querySelector('[data-layer="kafka-connect"]') as HTMLElement).getByRole('button', { name: 'Approve' })).toBeInTheDocument()
  })

  it('one pending layer left: the row keeps its own Approve', () => {
    show(room(proposal({ approvals: { 'schema-registry': 'approved', 'kafka-connect': 'pending' } })))
    expect(screen.queryByRole('button', { name: RUN })).not.toBeInTheDocument()
    expect(screen.getByRole('button', { name: 'Approve' })).toBeInTheDocument()
  })

  it('a viewer, a closed room and a host without approveAll get no button', () => {
    const { unmount } = show(room(proposal(), { mode: solo }), { ...sam, role: 'viewer' })
    expect(screen.queryByRole('button', { name: RUN })).not.toBeInTheDocument()
    unmount()

    const closed = show(room(proposal(), { status: 'closed', mode: solo }))
    expect(screen.queryByRole('button', { name: RUN })).not.toBeInTheDocument()
    expect(screen.queryByRole('button', { name: 'Approve' })).not.toBeInTheDocument()
    closed.unmount()

    show(room(), sam, base as RoomActions)
    expect(screen.queryByRole('button', { name: RUN })).not.toBeInTheDocument()
    expect(screen.getAllByRole('button', { name: 'Approve' })).toHaveLength(2)
  })

  it('no params hash: the button is off and points at the note', () => {
    show(room(proposal({ params_hash: '' })))
    const button = screen.getByRole('button', { name: RUN })
    expect(button).toBeDisabled()
    expect(button).toHaveAccessibleDescription(/can't be approved here/)
  })
})

describe('Approve and run: the confirm', () => {
  it('names each layer with its action and target, the fingerprint and the void rule', () => {
    show()
    openConfirm()
    const box = dialog()
    expect(within(box).getByRole('heading', { name: 'Approve and run this fix?' })).toBeInTheDocument()
    expect(box).toHaveTextContent('This records your approval for all 2 layers below, then Piperoom runs 2 steps right away.')
    const layers = within(within(box).getByRole('list', { name: 'Layers you are approving' })).getAllByRole('listitem')
    expect(layers.map((li) => li.textContent)).toEqual([
      'schema-registry · set_compatibility · pg.public.orders-value',
      'kafka-connect · restart_connector_task · pg-orders-connector task 0',
    ])
    expect(box).toHaveTextContent('fingerprint 9f2c 1e7a … 0a1b')
    expect(box).toHaveTextContent('if the steps change, this fingerprint changes and your approval is void')
    expect(approveAll).not.toHaveBeenCalled()
  })

  it('confirm makes one approve-all call with the hash on screen, and no per-layer call', async () => {
    approveAll.mockResolvedValue({ ok: true })
    show()
    openConfirm()
    fireEvent.click(within(dialog()).getByRole('button', { name: RUN }))
    await waitFor(() => expect(approveAll).toHaveBeenCalledWith('p1', { params_hash: HASH }))
    expect(approveAll).toHaveBeenCalledTimes(1)
    expect(approve).not.toHaveBeenCalled()
    await waitFor(() => expect(screen.queryByRole('alertdialog')).not.toBeInTheDocument())
  })

  it('Cancel closes it without a call', () => {
    show()
    openConfirm()
    fireEvent.click(within(dialog()).getByRole('button', { name: /Cancel/ }))
    expect(screen.queryByRole('alertdialog')).not.toBeInTheDocument()
    expect(approveAll).not.toHaveBeenCalled()
  })

  it('a layer someone else approved is named as done, and only the rest is recorded', async () => {
    approveAll.mockResolvedValue({ ok: true })
    const three = proposal({
      required_layers: ['postgres', 'schema-registry', 'kafka-connect'],
      approvals: { postgres: 'approved', 'schema-registry': 'pending', 'kafka-connect': 'pending' },
    })
    show(room(three), { ...sam, layers: ['postgres', 'schema-registry', 'kafka-connect'] })
    openConfirm()
    const box = dialog()
    expect(within(box).getAllByRole('listitem').map((li) => li.getAttribute('data-layer'))).toEqual(['schema-registry', 'kafka-connect'])
    expect(box).toHaveTextContent('Already approved: postgres')
    fireEvent.click(within(box).getByRole('button', { name: RUN }))
    await waitFor(() => expect(approveAll).toHaveBeenCalledWith('p1', { params_hash: HASH }))
  })

  it('A opens the confirm and R the reason; the rejection goes on the first pending layer with the hash', () => {
    show()
    fireEvent.keyDown(window, { key: 'a' })
    expect(dialog()).toBeInTheDocument()
    fireEvent.click(within(dialog()).getByRole('button', { name: /Cancel/ }))

    fireEvent.keyDown(window, { key: 'r' })
    const reason = screen.getByLabelText('Rejection reason')
    fireEvent.change(reason, { target: { value: 'wrong subject' } })
    fireEvent.click(screen.getByRole('button', { name: 'Confirm' }))
    expect(reject).toHaveBeenCalledWith('p1', 'schema-registry', 'wrong subject', HASH)
    expect(approveAll).not.toHaveBeenCalled()
  })

  it('a held Enter does not confirm it', () => {
    show()
    openConfirm()
    const go = within(dialog()).getByRole('button', { name: RUN })
    const notCancelled = fireEvent.keyDown(go, { key: 'Enter', repeat: true })
    expect(notCancelled).toBe(false)
    expect(approveAll).not.toHaveBeenCalled()
  })
})

describe('Approve and run with write mode off', () => {
  it('the button and the confirm say the fix will not run', async () => {
    approveAll.mockResolvedValue({ ok: true })
    show(room(proposal(), { mode: writeOff }))
    expect(screen.queryByRole('button', { name: RUN })).not.toBeInTheDocument()
    fireEvent.click(screen.getByRole('button', { name: WILL_NOT_RUN }))
    const box = dialog()
    expect(within(box).getByRole('heading', { name: 'Approve this fix?' })).toBeInTheDocument()
    expect(box).toHaveTextContent(
      'This records your approval for all 2 layers below. Write mode is off, so nothing runs: the fix waits for write mode.',
    )
    expect(box).not.toHaveTextContent(/runs 2 steps|right away/)
    fireEvent.click(within(box).getByRole('button', { name: WILL_NOT_RUN }))
    await waitFor(() => expect(approveAll).toHaveBeenCalledWith('p1', { params_hash: HASH }))
  })
})

describe('Approve and run: the solo user', () => {
  it('types "<pipeline>/*" once, by hand; the call carries it', async () => {
    approveAll.mockResolvedValue({ ok: true })
    show(room(proposal(), { mode: solo }), { ...sam, solo: true })
    openConfirm()
    const box = screen.getByLabelText(TYPE_BOX)
    const go = within(dialog()).getByRole('button', { name: RUN })
    expect(go).toBeDisabled()
    expect(fireEvent.paste(box, { clipboardData: { getData: () => 'orders/*' } })).toBe(false)
    fireEvent.change(box, { target: { value: 'orders/' } })
    expect(go).toBeDisabled()
    fireEvent.change(box, { target: { value: 'orders/*' } })
    expect(go).toBeEnabled()
    fireEvent.click(go)
    await waitFor(() => expect(approveAll).toHaveBeenCalledWith('p1', { params_hash: HASH, confirm: 'orders/*' }))
    expect(approveAll).toHaveBeenCalledTimes(1)
  })

  it('a server that asks for the text gets the box; a refused text says so and keeps it open', async () => {
    approveAll.mockResolvedValueOnce({ ok: false, needsConfirm: true })
    approveAll.mockResolvedValueOnce({ ok: false, needsConfirm: true })
    show()
    openConfirm()
    fireEvent.click(within(dialog()).getByRole('button', { name: RUN }))
    const box = await screen.findByLabelText(TYPE_BOX)
    expect(approveAll).toHaveBeenLastCalledWith('p1', { params_hash: HASH })
    // The solo banner appears once the server has asked (the open dialog hides the page, hence `hidden`).
    expect(screen.getByRole('status', { hidden: true })).toHaveTextContent(/Solo mode/)

    fireEvent.change(box, { target: { value: 'orders/*' } })
    fireEvent.click(within(dialog()).getByRole('button', { name: RUN }))
    expect(await within(dialog()).findByRole('alert')).toHaveTextContent('The server did not accept that text.')
    expect(approveAll).toHaveBeenLastCalledWith('p1', { params_hash: HASH, confirm: 'orders/*' })
    expect(screen.getByLabelText(TYPE_BOX)).toBeInTheDocument()
  })
})

describe('Approve and run: errors from the route', () => {
  it('a 409 is shown in the confirm, which stays open for Cancel', async () => {
    approveAll.mockResolvedValue({ ok: false, error: 'every layer of this proposal is already approved' })
    show()
    openConfirm()
    fireEvent.click(within(dialog()).getByRole('button', { name: RUN }))
    expect(await within(dialog()).findByRole('alert')).toHaveTextContent('every layer of this proposal is already approved')
    expect(approveAll).toHaveBeenCalledTimes(1)
    fireEvent.click(within(dialog()).getByRole('button', { name: /Cancel/ }))
    expect(screen.queryByRole('alertdialog')).not.toBeInTheDocument()
  })

  it('a 409 params_mismatch closes the confirm and is never retried', async () => {
    approveAll.mockResolvedValue({ ok: false, mismatch: true })
    show()
    openConfirm()
    fireEvent.click(within(dialog()).getByRole('button', { name: RUN }))
    await waitFor(() => expect(screen.queryByRole('alertdialog')).not.toBeInTheDocument())
    expect(approveAll).toHaveBeenCalledTimes(1)
  })

  it('a 403 not_layer_owner closes the confirm; the refused card says why and offers the per-layer rows', async () => {
    const text = 'layer "schema-registry": user sam does not own layer "schema-registry" (approve-all needs every required layer)'
    approveAll.mockResolvedValue({ ok: false, refused: true, error: text })
    const { rerender } = show()
    openConfirm()
    fireEvent.click(within(dialog()).getByRole('button', { name: RUN }))
    await waitFor(() => expect(screen.queryByRole('alertdialog')).not.toBeInTheDocument())

    // useRoom records the refusal on the room state (useRoom.approveall.test.tsx).
    rerender(<ProposalsPanel state={room(proposal(), { approve_all_refused: { p1: text } })} user={sam} actions={actions} />)
    expect(screen.getByRole('alert')).toHaveTextContent(`Approve and run was refused: ${text}. Approve the layers you own one at a time below.`)
    expect(screen.queryByRole('button', { name: RUN })).not.toBeInTheDocument()
    expect(screen.getAllByRole('button', { name: 'Approve' })).toHaveLength(2)
  })
})

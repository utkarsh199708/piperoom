import { beforeEach, describe, expect, it, vi } from 'vitest'
import { fireEvent, render, screen, waitFor } from '@testing-library/react'
import { ApiClientError, api } from '@/api/client'
import type { ActionInfo, Event } from '@/api/types'
import { applyEvent } from '@/state/reducer'
import { emptyRoomState } from '@/state/types'
import type { RoomState } from '@/state/types'
import type { CurrentUser } from '@/state/permissions'
import { describeEvent } from '@/state/describe'
import type { RoomActions } from '@/hooks/useRoom'
import { ProposalsPanel } from '../ProposalsPanel'
import { DecisionBar } from './DecisionBar'
import { ProposeFixDialog } from './ProposeFixDialog'

vi.mock('@/api/client', async (importOriginal) => {
  const real = await importOriginal<typeof import('@/api/client')>()
  return { ...real, api: { ...real.api, actions: vi.fn(), proposeFix: vi.fn() } }
})

const arjun: CurrentUser = { id: 'arjun', name: 'Arjun', layers: ['kafka-connect'] }
const priya: CurrentUser = { id: 'priya', name: 'Priya', layers: ['postgres'] }
const arjunActor = { kind: 'human' as const, id: 'arjun', name: 'Arjun', verified: true }
const driven = (over: Partial<RoomState> = {}): RoomState => ({
  ...emptyRoomState('r1'),
  driver: { id: 'arjun', name: 'Arjun' },
  ...over,
})

const ACTIONS: ActionInfo[] = [
  {
    name: 'connect_resume',
    layer: 'kafka-connect',
    describe: 'Resume a paused connector.',
    params_schema: { type: 'object', properties: { connector: { type: 'string', description: 'connector name' } }, required: ['connector'] },
    preview: true,
  },
  {
    name: 'pg_drop_replication_slot',
    layer: 'postgres',
    describe: 'Drop an inactive replication slot.',
    params_schema: {
      type: 'object',
      properties: { connector: { type: 'string' }, slot: { type: 'string' }, force: { type: 'boolean' } },
      required: ['connector', 'slot'],
    },
    preview: true,
  },
]

const created = (seq: number, over: Record<string, unknown> = {}): Event => ({
  room_id: 'r1',
  seq,
  type: 'proposal.created',
  actor: arjunActor,
  // Now: the card's "Previewing…" waits 20 s from proposal.created before it says "unavailable".
  at: new Date().toISOString(),
  payload: {
    proposal_id: 'p_d1',
    title: 'Resume the orders connector',
    steps: [{ action: 'connect_resume', layer: 'kafka-connect', params: { connector: 'pg-orders' }, description: '' }],
    risk: 'low',
    rollback: 'pause it again',
    required_layers: ['kafka-connect'],
    params_hash: 'h1',
    origin: 'driver',
    ...over,
  },
})

beforeEach(() => {
  vi.mocked(api.actions).mockReset().mockResolvedValue(ACTIONS)
  vi.mocked(api.proposeFix).mockReset()
})

describe('DecisionBar: Propose a fix yourself', () => {
  it('the driver gets the button, even with nothing to decide', () => {
    const onProposeFix = vi.fn()
    render(<DecisionBar state={driven()} user={arjun} onProposeFix={onProposeFix} />)
    const bar = screen.getByRole('region', { name: 'Decision' })
    expect(bar).toHaveAttribute('data-kind', 'idle')
    fireEvent.click(screen.getByRole('button', { name: 'Propose a fix yourself' }))
    expect(onProposeFix).toHaveBeenCalledTimes(1)
  })

  it.each<[string, RoomState, CurrentUser]>([
    ['a member who is not the driver', driven(), priya],
    ['a viewer', driven(), { ...arjun, role: 'viewer' }],
    ['a closed room', driven({ status: 'closed' }), arjun],
    ['a shadow room', driven({ shadow: true }), arjun],
  ])('no button for %s', (_n, state, user) => {
    render(<DecisionBar state={state} user={user} onProposeFix={vi.fn()} />)
    expect(screen.queryByRole('button', { name: 'Propose a fix yourself' })).not.toBeInTheDocument()
    expect(screen.queryByRole('region', { name: 'Decision' })).not.toBeInTheDocument()
  })

  it('sits beside the decision when there is one', () => {
    let state = driven()
    state = applyEvent(state, created(1, { origin: undefined }))
    render(<DecisionBar state={state} user={arjun} onProposeFix={vi.fn()} />)
    expect(screen.getByRole('region', { name: 'Decision' })).toHaveAttribute('data-kind', 'needs-you')
    expect(screen.getByRole('button', { name: 'Propose a fix yourself' })).toBeInTheDocument()
  })

  it('not on the phone sheet, and not without a handler', () => {
    const { unmount } = render(<DecisionBar state={driven()} user={arjun} placement="sheet" onProposeFix={vi.fn()} />)
    expect(screen.queryByRole('button', { name: 'Propose a fix yourself' })).not.toBeInTheDocument()
    unmount()
    render(<DecisionBar state={driven()} user={arjun} />)
    expect(screen.queryByRole('region', { name: 'Decision' })).not.toBeInTheDocument()
  })
})

async function openDialog(onCreated = vi.fn(), onClose = vi.fn()) {
  render(<ProposeFixDialog roomId="r1" onClose={onClose} onCreated={onCreated} />)
  const select = await screen.findByRole('combobox', { name: 'Action' })
  return { select, onCreated, onClose }
}

describe('ProposeFixDialog', () => {
  it('lists the actions, generates the params form and posts the fix', async () => {
    const ev = created(7)
    vi.mocked(api.proposeFix).mockResolvedValue(ev)
    const { select, onCreated, onClose } = await openDialog()
    await waitFor(() => expect(select).toHaveFocus())
    expect([...select.querySelectorAll('option')].map((o) => o.textContent)).toEqual([
      'Choose an action…',
      'connect_resume · kafka-connect',
      'pg_drop_replication_slot · postgres',
    ])
    fireEvent.change(select, { target: { value: 'pg_drop_replication_slot' } })
    expect(screen.getByText('Drop an inactive replication slot.')).toBeInTheDocument()
    fireEvent.change(screen.getByRole('textbox', { name: 'connector' }), { target: { value: 'pg-orders' } })
    fireEvent.change(screen.getByRole('textbox', { name: 'slot' }), { target: { value: 'dbz_orders' } })
    fireEvent.click(screen.getByRole('checkbox', { name: 'force' }))
    fireEvent.change(screen.getByRole('textbox', { name: 'Title' }), { target: { value: 'Drop the dead slot' } })
    fireEvent.change(screen.getByRole('combobox', { name: 'Risk' }), { target: { value: 'high' } })
    fireEvent.change(screen.getByRole('textbox', { name: 'Rollback' }), { target: { value: 'the connector recreates it' } })
    fireEvent.click(screen.getByRole('button', { name: /Propose fix/ }))
    await waitFor(() => expect(onClose).toHaveBeenCalled())
    expect(api.proposeFix).toHaveBeenCalledWith('r1', {
      title: 'Drop the dead slot',
      risk: 'high',
      rollback: 'the connector recreates it',
      steps: [{ action: 'pg_drop_replication_slot', layer: 'postgres', params: { connector: 'pg-orders', slot: 'dbz_orders', force: true } }],
    })
    expect(onCreated).toHaveBeenCalledWith(ev)
  })

  it('checks the form first and sends nothing while it is incomplete', async () => {
    const { select } = await openDialog()
    fireEvent.click(screen.getByRole('button', { name: /Propose fix/ }))
    expect(screen.getByText('Choose an action.')).toBeInTheDocument()
    fireEvent.change(select, { target: { value: 'connect_resume' } })
    fireEvent.click(screen.getByRole('button', { name: /Propose fix/ }))
    expect(screen.getByText('Give the fix a title.')).toBeInTheDocument()
    expect(screen.getByText('Required.')).toBeInTheDocument()
    expect(api.proposeFix).not.toHaveBeenCalled()
  })

  it.each([
    [new ApiClientError(403, 'not_driver', 'not the driver'), 'Only the driver can propose a fix. Take control of the room first.'],
    [new ApiClientError(429, 'too_many_proposals', 'too many'), 'This room already has 5 open fixes you proposed. Approve, reject or wait for one before proposing another.'],
    [new ApiClientError(400, 'validation', 'step 0: connector "nope" is not one of the pipeline\'s connectors'), 'The server refused the fix: step 0: connector "nope" is not one of the pipeline\'s connectors'],
  ])('shows %s plainly and keeps the typed values', async (err, text) => {
    vi.mocked(api.proposeFix).mockRejectedValue(err)
    const { select, onClose } = await openDialog()
    fireEvent.change(select, { target: { value: 'connect_resume' } })
    fireEvent.change(screen.getByRole('textbox', { name: 'connector' }), { target: { value: 'nope' } })
    fireEvent.change(screen.getByRole('textbox', { name: 'Title' }), { target: { value: 'Resume' } })
    // Enter in a one-line field submits, as in any form.
    fireEvent.submit(screen.getByRole('textbox', { name: 'Title' }).closest('form') as HTMLFormElement)
    expect(await screen.findByRole('alert')).toHaveTextContent(text)
    expect(onClose).not.toHaveBeenCalled()
    expect(screen.getByRole('textbox', { name: 'connector' })).toHaveValue('nope')
    expect(screen.getByRole('textbox', { name: 'Title' })).toHaveValue('Resume')
  })

  it('Ctrl+Enter submits from a one-line field', async () => {
    vi.mocked(api.proposeFix).mockResolvedValue(created(7))
    const { select, onClose } = await openDialog()
    fireEvent.change(select, { target: { value: 'connect_resume' } })
    fireEvent.change(screen.getByRole('textbox', { name: 'connector' }), { target: { value: 'pg-orders' } })
    const title = screen.getByRole('textbox', { name: 'Title' })
    fireEvent.change(title, { target: { value: 'Resume' } })
    fireEvent.keyDown(title, { key: 'Enter', ctrlKey: true })
    await waitFor(() => expect(onClose).toHaveBeenCalled())
    expect(api.proposeFix).toHaveBeenCalledTimes(1)
  })

  it('says so when the actions cannot be loaded', async () => {
    vi.mocked(api.actions).mockRejectedValue(new ApiClientError(500, 'internal', 'registry unavailable'))
    render(<ProposeFixDialog roomId="r1" onClose={vi.fn()} />)
    expect(await screen.findByRole('alert')).toHaveTextContent('The actions could not be loaded: registry unavailable')
  })
})

const roomActions: RoomActions = {
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

describe('a driver-authored proposal', () => {
  it('folds origin and author; agent proposals stay unmarked', () => {
    const s = applyEvent(driven(), created(1))
    expect(s.proposals[0]).toMatchObject({ origin: 'driver', author: arjunActor })
    const a = applyEvent(driven(), { ...created(1, { origin: 'agent' }), actor: { kind: 'agent', id: 'agent', name: 'Agent' } })
    expect(a.proposals[0]).not.toHaveProperty('origin')
    expect(a.proposals[0]).not.toHaveProperty('author')
  })

  it('reads as the driver in the activity line', () => {
    expect(describeEvent(created(1)).text).toBe('Arjun proposed a fix · Resume the orders connector')
    expect(describeEvent({ ...created(1, { origin: undefined }), actor: { kind: 'agent', id: 'agent', name: 'Agent' } }).text).toBe(
      'Agent proposed a fix · Resume the orders connector',
    )
  })

  it('the card shows "authored by <driver>" and its preview fills in like the agent\'s', () => {
    let state = applyEvent(driven(), created(1))
    const { rerender } = render(<ProposalsPanel state={state} user={priya} actions={roomActions} />)
    const badge = screen.getByTestId('authored-by')
    expect(badge).toHaveTextContent('authored by Arjun')
    expect(screen.queryByText('proposed by the agent')).not.toBeInTheDocument()
    expect(screen.getByText('Previewing what this will do…')).toBeInTheDocument()

    state = applyEvent(state, {
      room_id: 'r1',
      seq: 2,
      type: 'proposal.previewed',
      actor: { kind: 'system', id: 'system', name: 'system' },
      at: '2026-10-23T10:00:02Z',
      payload: {
        proposal_id: 'p_d1',
        at: '2026-10-23T10:00:02Z',
        steps: [{ step_index: 0, preview: { target: 'pg-orders', current: 'PAUSED', expected: 'RUNNING', reversible: true } }],
      },
    })
    rerender(<ProposalsPanel state={state} user={priya} actions={roomActions} />)
    expect(screen.queryByText('Previewing what this will do…')).not.toBeInTheDocument()
    expect(screen.getByText('This touches')).toBeInTheDocument()
    expect(screen.getByRole('region', { name: 'What this touches' })).toHaveTextContent('RUNNING')
  })

  it('an agent proposal keeps "proposed by the agent" and no badge', () => {
    const state = applyEvent(driven(), { ...created(1, { origin: undefined }), actor: { kind: 'agent', id: 'agent', name: 'Agent' } })
    // Arjun owns the layer, so the card shows no "waiting for" line in place of the author line.
    render(<ProposalsPanel state={state} user={arjun} actions={roomActions} />)
    expect(screen.queryByTestId('authored-by')).not.toBeInTheDocument()
    expect(screen.getByText('proposed by the agent')).toBeInTheDocument()
  })
})

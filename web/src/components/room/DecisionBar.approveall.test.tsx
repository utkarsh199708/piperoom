import { describe, expect, it, vi } from 'vitest'
import { fireEvent, render, screen, waitFor, within } from '@testing-library/react'
import { DecisionBar } from './DecisionBar'
import { ProposalsPanel } from '../ProposalsPanel'
import { DecisionHandleContext } from '../shell/context'
import type { DecisionHandle } from '../shell/context'
import type { RoomActions } from '../../hooks/useRoom'
import { emptyRoomState } from '../../state/types'
import type { ProposalState, RoomState } from '../../state/types'
import type { CurrentUser } from '../../state/permissions'
import type { RoomMode } from '../../api/types'
import { decisionFor } from '../../lib/decision'

// P2c: the bar asks what the card asks. Where the card shows one "Approve and run", the bar names
// every layer and its button opens the card's one confirm.

const sam: CurrentUser = { id: 'sam', name: 'Sam', layers: ['schema-registry', 'kafka-connect'] }
const step = (layer: string, action: string) => ({ action, layer, params: {}, description: '' })
const prop = (over: Partial<ProposalState> = {}): ProposalState => ({
  proposal_id: 'p1',
  title: 'Restart the task',
  steps: [step('schema-registry', 'set_compatibility'), step('kafka-connect', 'restart_connector_task')],
  risk: 'low',
  rollback: '',
  required_layers: ['schema-registry', 'kafka-connect'],
  approvals: { 'schema-registry': 'pending', 'kafka-connect': 'pending' },
  decisions: {},
  params_hash: 'h',
  status: 'open',
  executed: [],
  ...over,
})
const room = (p: ProposalState, mode?: RoomMode, over: Partial<RoomState> = {}): RoomState => ({
  ...emptyRoomState('r'),
  pipeline_id: 'orders',
  proposals: [p],
  ...(mode ? { mode } : {}),
  ...over,
})

function holder() {
  let h: DecisionHandle | null = null
  return { get: () => h, set: (next: DecisionHandle | null) => void (h = next) }
}

describe('DecisionBar with Approve and run', () => {
  it('names every layer and offers the one button', () => {
    const handle: DecisionHandle = { approve: vi.fn(), reject: vi.fn(), scrollTo: vi.fn() }
    render(
      <DecisionHandleContext.Provider value={{ get: () => handle, set: () => undefined }}>
        <DecisionBar state={room(prop())} user={sam} />
      </DecisionHandleContext.Provider>,
    )
    const bar = screen.getByRole('region', { name: 'Decision' })
    expect(bar).toHaveTextContent('Needs your approval · schema-registry · kafka-connect — Restart the task')
    fireEvent.click(within(bar).getByRole('button', { name: /^Approve and run/ }))
    expect(handle.approve).toHaveBeenCalledTimes(1)
  })

  it('says "will not run" with write mode off', () => {
    render(<DecisionBar state={room(prop(), { write: false, solo: false, auto_approve: [] })} user={sam} />)
    expect(screen.getByRole('button', { name: /^Approve \(will not run\)/ })).toBeInTheDocument()
    expect(screen.queryByRole('button', { name: /Approve and run/ })).not.toBeInTheDocument()
  })

  it('keeps the per-layer words where the card does (one owner of one layer, or a refused approve-all)', () => {
    const arjun: CurrentUser = { id: 'arjun', name: 'Arjun', layers: ['kafka-connect'] }
    expect(decisionFor(room(prop()), arjun)).toMatchObject({ kind: 'needs-you', layer: 'kafka-connect' })
    expect(decisionFor(room(prop()), arjun)).not.toHaveProperty('all')

    const refused = room(prop(), undefined, { approve_all_refused: { p1: 'no' } })
    expect(decisionFor(refused, sam)).toMatchObject({ kind: 'needs-you', layer: 'schema-registry' })
    expect(decisionFor(refused, sam)).not.toHaveProperty('all')
    expect(decisionFor(room(prop()), sam)).toMatchObject({ kind: 'needs-you', layer: 'schema-registry', all: ['schema-registry', 'kafka-connect'] })
  })

  it("the bar's button opens the card's confirm, and confirming makes the one call", async () => {
    const approveAll = vi.fn().mockResolvedValue({ ok: true })
    const actions = new Proxy({ approveAll } as unknown as RoomActions, {
      get: (target, key: string) => (key in target ? target[key as keyof RoomActions] : key === 'sends' ? undefined : vi.fn()),
    })
    const state = room(prop(), { write: true, solo: true, auto_approve: [] })
    render(
      <DecisionHandleContext.Provider value={holder()}>
        <DecisionBar state={state} user={{ ...sam, layers: [] }} />
        <ProposalsPanel state={state} user={{ ...sam, layers: [] }} actions={actions} ribbon={false} />
      </DecisionHandleContext.Provider>,
    )
    const bar = screen.getByRole('region', { name: 'Decision' })
    fireEvent.click(within(bar).getByRole('button', { name: /^Approve and run/ }))
    const box = screen.getByRole('alertdialog')
    fireEvent.click(within(box).getByRole('button', { name: 'Approve and run' }))
    await waitFor(() => expect(approveAll).toHaveBeenCalledWith('p1', { params_hash: 'h' }))
  })
})

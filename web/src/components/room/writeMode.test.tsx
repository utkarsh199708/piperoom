import { beforeEach, describe, expect, it, vi } from 'vitest'
import { render, screen } from '@testing-library/react'
import { ProposalsSummary } from './ProposalsSummary'
import { DecisionBar } from './DecisionBar'
import { DecisionHandleContext } from '../shell/context'
import type { DecisionHandle } from '../shell/context'
import { emptyRoomState } from '../../state/types'
import type { ProposalState, RoomState } from '../../state/types'
import type { CurrentUser } from '../../state/permissions'

const WAITING = 'Approved · waiting for write mode'
const arjun: CurrentUser = { id: 'arjun', name: 'Arjun', layers: ['kafka-connect'] }
const step = (layer: string, action: string) => ({ action, layer, params: {}, description: '' })
function prop(over: Partial<ProposalState> = {}): ProposalState {
  return {
    proposal_id: 'p1',
    title: 'Restart the task',
    steps: [step('schema-registry', 'a'), step('kafka-connect', 'b')],
    risk: 'low',
    rollback: '',
    required_layers: ['schema-registry', 'kafka-connect'],
    approvals: { 'schema-registry': 'approved', 'kafka-connect': 'pending' },
    decisions: {},
    params_hash: 'h',
    status: 'open',
    executed: [],
    ...over,
  }
}
const allApproved = { approvals: { 'schema-registry': 'approved', 'kafka-connect': 'approved' }, status: 'approved' } as const

describe('ProposalsSummary — waiting for write mode', () => {
  it('is one neutral line, with the count', () => {
    render(<ProposalsSummary proposals={[prop({ ...allApproved, blocked: 'write_mode_off' })]} />)
    const line = screen.getByText(WAITING)
    expect(line).toHaveClass('text-muted-foreground')
    expect(line.className).not.toMatch(/destructive|warning|success/)
    expect(screen.getByText('2 of 2')).toBeInTheDocument()
    expect(screen.queryByText(/Running|Ran |failed/)).not.toBeInTheDocument()
  })

  it('an approved proposal without blocked reads Approved, as before', () => {
    render(<ProposalsSummary proposals={[prop({ ...allApproved })]} />)
    expect(screen.getByText('Approved')).toBeInTheDocument()
    expect(screen.queryByText(WAITING)).not.toBeInTheDocument()
  })
})

describe('DecisionBar with write mode off', () => {
  const handle: DecisionHandle = { approve: vi.fn(), reject: vi.fn(), scrollTo: vi.fn() }
  const show = (state: RoomState) =>
    render(
      <DecisionHandleContext.Provider value={{ get: () => handle, set: () => undefined }}>
        <DecisionBar state={state} user={arjun} />
      </DecisionHandleContext.Provider>,
    )
  const room = (p: ProposalState, mode?: RoomState['mode']): RoomState => ({
    ...emptyRoomState('r'),
    proposals: [p],
    ...(mode ? { mode } : {}),
  })

  beforeEach(() => {
    window.sessionStorage.clear()
  })

  it('the approve button says it will not run', () => {
    show(room(prop(), { write: false, solo: false, auto_approve: [] }))
    expect(screen.getByRole('button', { name: 'Approve kafka-connect (will not run)' })).toBeInTheDocument()
  })

  it.each([
    ['write mode on', { write: true, solo: false, auto_approve: [] }],
    ['no mode', undefined],
  ])('is unchanged with %s', (_name, mode) => {
    show(room(prop(), mode))
    expect(screen.getByRole('button', { name: 'Approve kafka-connect' })).toBeInTheDocument()
    expect(screen.queryByText(/will not run/)).not.toBeInTheDocument()
  })

  it('does not claim a blocked fix is running: no bar, no progress', () => {
    const { container } = show(room(prop({ ...allApproved, blocked: 'write_mode_off' }), { write: false, solo: false, auto_approve: [] }))
    expect(container).toBeEmptyDOMElement()
    expect(screen.queryByRole('progressbar')).not.toBeInTheDocument()
  })
})

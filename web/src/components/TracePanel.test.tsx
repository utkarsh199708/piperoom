import { describe, expect, it, vi } from 'vitest'
import { fireEvent, render, screen, within } from '@testing-library/react'
import { TracePanel } from './TracePanel'
import type { RoomActions } from '../hooks/useRoom'
import type { TraceState } from '../state/types'

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
  label: vi.fn(),
  checklist: vi.fn(),
  trace: vi.fn(),
}

const trace: TraceState = {
  table: 'public.orders',
  key: '10042',
  stages: [
    { layer: 'postgres', name: 'public.orders', status: 'seen', facts: { updated_at: '2026-10-11T09:47:00Z' } },
    { layer: 'kafka-connect', name: 'pg-orders-connector', status: 'seen' },
    { layer: 'kafka', name: 'pg.public.orders', status: 'seen', facts: { offset: '48213', partition: '0' } },
    { layer: 'consumer', name: 'order-sync', status: 'seen', facts: { committed: '48214' } },
    { layer: 'warehouse', name: 'analytics.orders', status: 'stopped_here' },
  ],
}

describe('TracePanel', () => {
  it('renders stages and highlights exactly one stopped_here', () => {
    render(<TracePanel trace={trace} actions={actions} onClose={vi.fn()} />)
    // The dialog is a Radix portal: it renders in document.body, not in the render container.
    const container = document.body
    const stopped = container.querySelectorAll('.trace-stage--stopped_here')
    expect(stopped).toHaveLength(1)
    expect(stopped[0].textContent).toContain('analytics.orders')
  })
})

describe('TracePanel as a dialog', () => {
  it('is a named modal dialog with the stopped stage marked', () => {
    render(<TracePanel trace={trace} actions={actions} onClose={vi.fn()} />)
    const dialog = screen.getByRole('dialog', { name: 'Trace a record' })
    expect(within(dialog).getByText('stopped here')).toBeInTheDocument()
    // No made-up table or key: the person types both.
    expect(within(dialog).getByLabelText('Table')).toHaveValue('')
    expect(within(dialog).getByLabelText('Key')).toHaveValue('')
  })

  it('Esc and the X both ask to close', () => {
    const onClose = vi.fn()
    render(<TracePanel trace={null} actions={actions} onClose={onClose} />)
    fireEvent.keyDown(document.activeElement ?? document.body, { key: 'Escape' })
    expect(onClose).toHaveBeenCalledTimes(1)
    fireEvent.click(screen.getByRole('button', { name: 'Close trace panel' }))
    expect(onClose).toHaveBeenCalledTimes(2)
  })

  it('submitting traces the table and key', () => {
    render(<TracePanel trace={null} actions={actions} onClose={vi.fn()} />)
    expect(screen.getByRole('button', { name: 'Trace' })).toBeDisabled()
    fireEvent.change(screen.getByLabelText('Table'), { target: { value: 'public.orders' } })
    fireEvent.change(screen.getByLabelText('Key'), { target: { value: '7' } })
    fireEvent.click(screen.getByRole('button', { name: 'Trace' }))
    expect(actions.trace).toHaveBeenCalledWith('public.orders', '7')
  })
})

describe('TracePanel verdict and layout', () => {
  it('leads with the verdict, before the stages, and indexes the stages', () => {
    render(<TracePanel trace={trace} actions={actions} onClose={vi.fn()} />)
    const dialog = screen.getByRole('dialog', { name: 'Trace a record' })
    const verdict = within(dialog).getByRole('status')
    expect(verdict).toHaveTextContent('Stopped at warehouse analytics.orders')
    const stages = within(dialog).getByRole('group', { name: /Trace stages/ })
    expect(verdict.compareDocumentPosition(stages) & Node.DOCUMENT_POSITION_FOLLOWING).toBeTruthy()
    expect(within(dialog).getByLabelText('Stage 5 of 5')).toBeInTheDocument()
    expect(stages.className).not.toContain('overflow-x-auto')
    expect(dialog.querySelectorAll('[data-worst]')).toHaveLength(1)
    expect(dialog.className).toContain('right-0')
  })
})

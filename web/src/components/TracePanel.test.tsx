import { describe, expect, it, vi } from 'vitest'
import { act, fireEvent, render, screen, waitFor, within } from '@testing-library/react'
import { TracePanel } from './TracePanel'
import type { RoomActions, TraceResult } from '../hooks/useRoom'
import type { TraceState } from '../state/types'

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
  trace: vi.fn().mockResolvedValue({ ok: true }),
  hypothesisVerdict: vi.fn(),
  nudge: vi.fn().mockResolvedValue({ ok: true }),
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

const NOT_IN_PIPELINE = 'topology: bad input: no connector streams table "public.nope" in pipeline "orders"'

function fill(table: string, key: string) {
  fireEvent.change(screen.getByLabelText('Table'), { target: { value: table } })
  fireEvent.change(screen.getByLabelText('Key'), { target: { value: key } })
}

describe('TracePanel when the trace fails', () => {
  it('names the problem under the form and keeps what was typed', async () => {
    const trace = vi.fn().mockResolvedValue({ ok: false, error: NOT_IN_PIPELINE })
    render(<TracePanel trace={null} actions={{ ...actions, trace }} onClose={vi.fn()} />)
    fill('public.nope', '1')
    fireEvent.click(screen.getByRole('button', { name: 'Trace' }))

    const dialog = screen.getByRole('dialog', { name: 'Trace a record' })
    const alert = await within(dialog).findByRole('alert')
    expect(alert).toHaveTextContent('public.nope')
    expect(alert).toHaveTextContent('pipeline "orders"')
    // The error sits under the form, not above it.
    const form = dialog.querySelector('form')!
    expect(form.compareDocumentPosition(alert) & Node.DOCUMENT_POSITION_FOLLOWING).toBeTruthy()
    expect(within(dialog).getByLabelText('Table')).toHaveValue('public.nope')
    expect(within(dialog).getByLabelText('Key')).toHaveValue('1')
    expect(within(dialog).getByLabelText('Table')).toHaveAccessibleDescription(NOT_IN_PIPELINE)
    // The person can fix a field and try again.
    expect(within(dialog).getByRole('button', { name: 'Trace' })).toBeEnabled()
  })

  it('changing the table or the key clears the error', async () => {
    for (const field of ['Table', 'Key']) {
      const trace = vi.fn().mockResolvedValue({ ok: false, error: NOT_IN_PIPELINE })
      const { unmount } = render(<TracePanel trace={null} actions={{ ...actions, trace }} onClose={vi.fn()} />)
      fill('public.nope', '1')
      fireEvent.click(screen.getByRole('button', { name: 'Trace' }))
      await screen.findByRole('alert')

      fireEvent.change(screen.getByLabelText(field), { target: { value: 'public.orders' } })
      expect(screen.queryByRole('alert')).not.toBeInTheDocument()
      expect(screen.getByLabelText('Table')).not.toHaveAttribute('aria-invalid')
      unmount()
    }
  })

  it('a new attempt clears the old error, and a success leaves none', async () => {
    const trace = vi
      .fn()
      .mockResolvedValueOnce({ ok: false, error: NOT_IN_PIPELINE })
      .mockResolvedValueOnce({ ok: true })
    render(<TracePanel trace={null} actions={{ ...actions, trace }} onClose={vi.fn()} />)
    fill('public.nope', '1')
    fireEvent.click(screen.getByRole('button', { name: 'Trace' }))
    await screen.findByRole('alert')

    fireEvent.click(screen.getByRole('button', { name: 'Trace' }))
    expect(screen.queryByRole('alert')).not.toBeInTheDocument()
    await waitFor(() => expect(trace).toHaveBeenCalledTimes(2))
    await waitFor(() => expect(screen.getByRole('button', { name: 'Trace' })).toBeEnabled())
    expect(screen.queryByRole('alert')).not.toBeInTheDocument()
  })

  it('drops an answer for words the person has since changed', async () => {
    let answer: (r: TraceResult) => void = () => {}
    const trace = vi.fn().mockReturnValue(new Promise<TraceResult>((resolve) => (answer = resolve)))
    render(<TracePanel trace={null} actions={{ ...actions, trace }} onClose={vi.fn()} />)
    fill('public.nope', '1')
    fireEvent.click(screen.getByRole('button', { name: 'Trace' }))
    expect(screen.getByRole('button', { name: 'Trace' })).toBeDisabled()

    fireEvent.change(screen.getByLabelText('Table'), { target: { value: 'public.orders' } })
    // act() flushes the continuation of submit(): without it the assertions below run before the late
    // answer is handled, and they would pass whether or not the panel drops it.
    await act(async () => {
      answer({ ok: false, error: NOT_IN_PIPELINE })
    })
    expect(trace).toHaveBeenCalledTimes(1)
    expect(screen.queryByRole('alert')).not.toBeInTheDocument()
    expect(screen.getByLabelText('Table')).not.toHaveAttribute('aria-invalid')
    expect(screen.getByRole('button', { name: 'Trace' })).toBeEnabled()
  })
})

describe('TracePanel when the trace works', () => {
  it('shows no error and renders the pinned hop-by-hop result', async () => {
    const pinned: TraceState = { ...trace, table: 'public.orders', key: '42' }
    const run = vi.fn().mockResolvedValue({ ok: true })
    render(<TracePanel trace={pinned} actions={{ ...actions, trace: run }} onClose={vi.fn()} />)
    fill('public.orders', '42')
    fireEvent.click(screen.getByRole('button', { name: 'Trace' }))

    await waitFor(() => expect(run).toHaveBeenCalledWith('public.orders', '42'))
    await waitFor(() => expect(screen.getByRole('button', { name: 'Trace' })).toBeEnabled())
    const dialog = screen.getByRole('dialog', { name: 'Trace a record' })
    expect(within(dialog).queryByRole('alert')).not.toBeInTheDocument()
    expect(within(dialog).getByRole('status')).toHaveTextContent('Stopped at warehouse analytics.orders')
    expect(within(dialog).getByLabelText('Stage 5 of 5')).toBeInTheDocument()
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

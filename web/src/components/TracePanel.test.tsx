import { describe, expect, it, vi } from 'vitest'
import { fireEvent, render, screen } from '@testing-library/react'
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
    const { container } = render(<TracePanel trace={trace} actions={actions} onClose={vi.fn()} />)
    const stopped = container.querySelectorAll('.trace-stage--stopped_here')
    expect(stopped).toHaveLength(1)
    expect(stopped[0].textContent).toContain('analytics.orders')
  })
})

describe('TracePanel — stages (UI refresh §7.11)', () => {
  it('reuses the node look and draws a stalled edge right after the stopped_here stage', () => {
    const mid: TraceState = {
      ...trace,
      stages: [
        { layer: 'postgres', name: 'public.orders', status: 'seen' },
        { layer: 'kafka-connect', name: 'pg-orders-connector', status: 'stopped_here' },
        { layer: 'kafka', name: 'pg.public.orders', status: 'not_seen' },
        { layer: 'consumer', name: 'order-sync', status: 'unknown' },
      ],
    }
    const { container } = render(<TracePanel trace={mid} actions={actions} onClose={vi.fn()} />)
    const row = Array.from(container.querySelector('.trace__stages')!.children)
    expect(row.map((el) => el.className.split(' ')[0])).toEqual([
      'trace-stage',
      'edge',
      'trace-stage',
      'edge',
      'trace-stage',
      'edge',
      'trace-stage',
    ])
    const stopped = row.findIndex((el) => el.classList.contains('trace-stage--stopped_here'))
    expect(row[stopped]).toHaveClass('node-card--fail')
    expect(row[stopped]).toHaveTextContent('stopped here')
    expect(row[stopped + 1]).toHaveClass('edge--stalled')
    expect(row[stopped - 1]).toHaveClass('edge--flow')
    expect(row[stopped + 3]).toHaveClass('edge--flow')
    expect(container.querySelector('.trace-stage--not_seen')).toHaveClass('node-card--unknown')
    expect(container.querySelector('.trace-stage--not_seen')).toHaveTextContent('not seen')
    expect(container.querySelector('.trace-stage--seen')).toHaveClass('node-card--ok')
    expect(container.querySelector('.trace-stage--unknown')).toHaveClass('node-card--warn')
  })

  it('has no edge in front of the first stage', () => {
    const { container } = render(<TracePanel trace={trace} actions={actions} onClose={vi.fn()} />)
    expect(container.querySelector('.trace__stages')!.firstElementChild).toHaveClass('trace-stage')
    expect(container.querySelectorAll('.trace__stages .edge')).toHaveLength(4)
  })

  it('opens on the Table field and closes on Esc', () => {
    const onClose = vi.fn()
    render(<TracePanel trace={null} actions={actions} onClose={onClose} />)
    expect(screen.getByLabelText('Table')).toHaveFocus()
    fireEvent.keyDown(document, { key: 'Escape' })
    expect(onClose).toHaveBeenCalledTimes(1)
  })

  it('gives focus back to what opened it when it closes', () => {
    const opener = document.createElement('button')
    document.body.appendChild(opener)
    opener.focus()
    const { unmount } = render(<TracePanel trace={null} actions={actions} onClose={vi.fn()} />)
    expect(opener).not.toHaveFocus()
    unmount()
    expect(opener).toHaveFocus()
    opener.remove()
  })
})

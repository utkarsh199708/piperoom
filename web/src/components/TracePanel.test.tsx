import { describe, expect, it, vi } from 'vitest'
import { render } from '@testing-library/react'
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
  table: 'orders',
  key: 'id=10042',
  stages: [
    { layer: 'postgres', name: 'public.orders', status: 'seen' },
    { layer: 'kafka', name: 'pg.public.orders', status: 'seen' },
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

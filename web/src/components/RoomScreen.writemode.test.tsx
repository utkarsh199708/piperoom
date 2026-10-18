import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { fireEvent, render, screen, within } from '@testing-library/react'
import { MemoryRouter, Route, Routes } from 'react-router-dom'
import type { RoomActions } from '../hooks/useRoom'
import type { RoomState } from '../state/types'
import { emptyRoomState } from '../state/types'
import { QUICKSTART_DOC_PATH } from '../lib/writeMode'

vi.mock('../context/user', () => ({
  useCurrentUser: () => ({
    user: { id: 'arjun', name: 'Arjun', layers: ['kafka-connect', 'kafka'] },
    setUserId: vi.fn(),
    logout: vi.fn(),
    sessionEpoch: 0,
  }),
}))
vi.mock('../hooks/useRoom', () => ({ useRoom: vi.fn() }))
vi.mock('../hooks/useTopology', () => ({ useTopology: vi.fn() }))

import { useRoom } from '../hooks/useRoom'
import { useTopology } from '../hooks/useTopology'
import { RoomScreen } from './RoomScreen'

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
  trace: vi.fn(),
  hypothesisVerdict: vi.fn(),
  nudge: vi.fn().mockResolvedValue({ ok: true }),
}

function room(over: Partial<RoomState> = {}): RoomState {
  return {
    ...emptyRoomState('r1'),
    title: 'Orders connector stopped',
    severity: 'sev2',
    pipeline_id: 'orders',
    opened_at: '2026-10-11T10:05:00Z',
    driver: { id: 'kabir', name: 'Kabir' },
    ...over,
  }
}

function show(state: RoomState) {
  vi.mocked(useRoom).mockReturnValue({
    state,
    presence: [],
    conn: 'live',
    error: null,
    actions,
    stale: null,
    dismissStale: vi.fn(),
    reload: vi.fn(),
  })
  vi.mocked(useTopology).mockReturnValue({ topology: null, error: null, updatedAt: null, notFound: false, retry: () => undefined })
  return render(
    <MemoryRouter initialEntries={['/rooms/r1']}>
      <Routes>
        <Route path="/rooms/:roomId" element={<RoomScreen />} />
      </Routes>
    </MemoryRouter>,
  )
}

beforeEach(() => {
  vi.resetAllMocks()
  window.sessionStorage.clear()
})
afterEach(() => {
  vi.unstubAllEnvs()
})

describe('RoomScreen — read-only banner', () => {
  it('shows the banner when the room GET said write mode is off, naming the quickstart doc', () => {
    show(room({ mode: { write: false, solo: false, auto_approve: [] } }))
    const banner = screen.getByRole('region', { name: 'Write mode is off' })
    expect(banner).toHaveTextContent('Fixes are suggestions until write mode is on')
    expect(within(banner).getByText(QUICKSTART_DOC_PATH)).toBeInTheDocument()
    expect(within(banner).queryByRole('link')).not.toBeInTheDocument()
  })

  it('links to the doc in a new tab when the build knows where the docs are', () => {
    vi.stubEnv('VITE_DOCS_BASE_URL', 'https://example.test/piperoom/blob/main')
    show(room({ mode: { write: false, solo: false, auto_approve: [] } }))
    const link = within(screen.getByRole('region', { name: 'Write mode is off' })).getByRole('link', { name: 'How to turn it on' })
    expect(link).toHaveAttribute('href', 'https://example.test/piperoom/blob/main/docs/pilot/quickstart.md')
    expect(link).toHaveAttribute('target', '_blank')
  })

  it('sits under the room header', () => {
    show(room({ mode: { write: false, solo: false, auto_approve: [] } }))
    const banner = screen.getByRole('region', { name: 'Write mode is off' })
    const heading = screen.getByRole('heading', { name: 'Orders connector stopped' })
    expect(heading.compareDocumentPosition(banner) & Node.DOCUMENT_POSITION_FOLLOWING).toBeTruthy()
  })

  it('goes away on Dismiss and stays away for the browser session', () => {
    const first = show(room({ mode: { write: false, solo: false, auto_approve: [] } }))
    fireEvent.click(screen.getByRole('button', { name: 'Dismiss write-mode notice' }))
    expect(screen.queryByRole('region', { name: 'Write mode is off' })).not.toBeInTheDocument()
    first.unmount()
    show(room({ mode: { write: false, solo: false, auto_approve: [] } }))
    expect(screen.queryByRole('region', { name: 'Write mode is off' })).not.toBeInTheDocument()
  })

  it.each([
    ['write mode is on', { write: true, solo: false, auto_approve: [] }],
    ['the snapshot has no mode', undefined],
  ])('is absent when %s', (_name, mode) => {
    show(room(mode ? { mode } : {}))
    expect(screen.queryByRole('region', { name: 'Write mode is off' })).not.toBeInTheDocument()
    expect(screen.queryByText(/Fixes are suggestions/)).not.toBeInTheDocument()
  })
})

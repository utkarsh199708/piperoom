import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { act, fireEvent, render, screen, waitFor, within } from '@testing-library/react'
import { MemoryRouter, Route, Routes, useLocation } from 'react-router-dom'
import type { FleetPipeline, PipelineChange } from '../../api/types'
import fixture from '../../fixtures/fleet.json'

const me = vi.hoisted(() => ({ role: 'member' as 'member' | 'viewer' }))
vi.mock('../../context/user', () => ({
  useCurrentUser: () => ({
    user: { id: 'arjun', name: 'Arjun', layers: ['kafka'], role: me.role },
    setUserId: vi.fn(),
    logout: vi.fn(),
    sessionEpoch: 0,
  }),
}))

vi.mock('../../api/client', async (importOriginal) => {
  const actual = await importOriginal<typeof import('../../api/client')>()
  return {
    ...actual,
    isFixtures: () => false,
    api: { ...actual.api, fleet: vi.fn(), fleetChanges: vi.fn(), authUsers: vi.fn(), pipelines: vi.fn() },
  }
})

import { ApiClientError, api } from '../../api/client'
import { FIRST_RUN_TITLE } from '../FirstRunPanel'
import { FleetScreen } from './FleetScreen'

const NOW = new Date('2026-10-22T10:00:00Z').getTime()
const TEAMS = ['orders-core', 'refunds', 'growth']
const HEALTH = ['ok', 'warn', 'fail', 'unknown'] as const

// 50 rows in the API's shape: the docs/api.md example row, varied. Every 7th has a forecast, its ETA
// further out the higher the index, so the ETA order is known.
function fifty(): FleetPipeline[] {
  const base = fixture[0] as FleetPipeline
  return Array.from({ length: 50 }, (_, i) => ({
    ...base,
    pipeline_id: `p${String(i).padStart(2, '0')}`,
    title: `Pipeline ${i}`,
    team: TEAMS[i % 3],
    tenant: i % 2 ? 'payments' : 'growth',
    health: HEALTH[i % 4],
    forecasts: i % 7 === 0 ? [{ metric: 'slot_wal_bytes', eta: new Date(NOW + (50 - i) * 3_600_000).toISOString() }] : [],
  }))
}

function Where() {
  const loc = useLocation()
  return <div data-testid="where">{loc.pathname + loc.search}</div>
}

function renderFleet(path = '/fleet') {
  return render(
    <MemoryRouter initialEntries={[path]}>
      <Routes>
        <Route path="/fleet" element={<FleetScreen />} />
        <Route path="*" element={<Where />} />
      </Routes>
    </MemoryRouter>,
  )
}

const table = () => screen.getByRole('table', { name: 'Pipelines' })
const rowIds = () =>
  within(table())
    .getAllByRole('row')
    .slice(1)
    .map((r) => r.getAttribute('data-pipeline-id'))

function setWidth(px: number) {
  vi.stubGlobal('matchMedia', (q: string) => {
    const min = /min-width:\s*(\d+)px/.exec(q)
    return { matches: min ? px >= Number(min[1]) : false, media: q, addEventListener: () => {}, removeEventListener: () => {} }
  })
}

beforeEach(() => {
  vi.useFakeTimers({ shouldAdvanceTime: true })
  vi.setSystemTime(NOW)
  me.role = 'member'
  vi.mocked(api.fleet).mockResolvedValue(fifty())
  vi.mocked(api.fleetChanges).mockResolvedValue({ changes: [], next: '' })
  vi.mocked(api.authUsers).mockResolvedValue([])
  vi.mocked(api.pipelines).mockResolvedValue([])
  setWidth(1280)
})

afterEach(() => {
  vi.useRealTimers()
  vi.unstubAllGlobals()
  vi.resetAllMocks()
})

describe('FleetScreen pipelines', () => {
  it('renders 50 rows, worst health first (TC1, E1)', async () => {
    renderFleet()
    await screen.findByRole('table', { name: 'Pipelines' })
    const order = rowIds()
    expect(order).toHaveLength(50)
    // fail rows (i % 4 == 2) first, then warn, unknown, ok.
    expect(order[0]).toBe('p02')
    expect(order.slice(0, 12).every((id) => Number(id!.slice(1)) % 4 === 2)).toBe(true)
    expect(Number(order[12]!.slice(1)) % 4).toBe(1)
    expect(order[49]).toBe('p48')
    expect(screen.getByRole('columnheader', { name: /Health/ })).toHaveAttribute('aria-sort', 'ascending')
  })

  it('sorts by ETA, soonest first, rows without a forecast last (TC1)', async () => {
    renderFleet()
    await screen.findByRole('table', { name: 'Pipelines' })
    fireEvent.click(screen.getByRole('button', { name: 'Sort by forecast ETA' }))
    expect(rowIds().slice(0, 8)).toEqual(['p49', 'p42', 'p35', 'p28', 'p21', 'p14', 'p07', 'p00'])
    expect(screen.getByRole('columnheader', { name: /Forecast/ })).toHaveAttribute('aria-sort', 'ascending')
    expect(screen.getByRole('columnheader', { name: /Health/ })).not.toHaveAttribute('aria-sort')
    // Again: the other direction, still forecasts before the rest.
    fireEvent.click(screen.getByRole('button', { name: 'Sort by forecast ETA' }))
    expect(rowIds().slice(0, 2)).toEqual(['p00', 'p07'])
    expect(screen.getByRole('columnheader', { name: /Forecast/ })).toHaveAttribute('aria-sort', 'descending')
  })

  it('filters by team from the menu, leaving only matching rows (TC1)', async () => {
    renderFleet()
    await screen.findByRole('table', { name: 'Pipelines' })
    fireEvent.keyDown(screen.getByRole('button', { name: 'Filter by team' }), { key: 'Enter' })
    fireEvent.click(await screen.findByRole('menuitemradio', { name: 'refunds' }))
    const order = rowIds()
    expect(order).toHaveLength(17)
    expect(order.every((id) => Number(id!.slice(1)) % 3 === 1)).toBe(true)
    expect(screen.getByText('17 of 50 pipelines')).toBeInTheDocument()
    fireEvent.click(screen.getByRole('button', { name: 'Clear filters' }))
    expect(rowIds()).toHaveLength(50)
  })

  it('applies tenant and kind filters from the URL', async () => {
    renderFleet('/fleet?tenant=payments&kind=batch')
    expect(await screen.findByText('No pipeline matches these filters.')).toBeInTheDocument()
    expect(screen.getByRole('button', { name: 'Tenant: payments' })).toBeInTheDocument()
  })

  it('shows health, worst node, signals, ETA badge, rooms, changes and verdict with names (E4, E8)', async () => {
    vi.mocked(api.fleet).mockResolvedValue(fixture as FleetPipeline[])
    renderFleet()
    const row = (await screen.findByText('Orders CDC')).closest('tr')!
    const cells = within(row)
    expect(cells.getByText('Failing')).toBeInTheDocument()
    expect(cells.getByRole('link', { name: 'Worst node connect:pg-orders-connector on the orders lane' })).toHaveAttribute(
      'href',
      '/pipelines/orders?node=connect%3Apg-orders-connector',
    )
    expect(cells.getByLabelText('Signals: connector_down, slot_wal')).toBeInTheDocument()
    expect(cells.getByRole('img', { name: /Forecast: WAL retained by the slot: it reaches its limit in about 18 h/ })).toHaveTextContent(
      'fills in 18 h',
    )
    expect(cells.getByRole('link', { name: '1 open room on orders' })).toHaveAttribute('href', '/?pipeline=orders')
    expect(cells.getByRole('link', { name: /Confirmed/ })).toHaveAttribute('href', '/rooms/r_01J9')
    // count_drift's worst node is the pg:<source> node the API names; the link carries it unchanged.
    const refunds = (await screen.findByText('Refunds CDC')).closest('tr')!
    expect(within(refunds).getByRole('link', { name: /Worst node pg:public.refunds/ })).toHaveAttribute(
      'href',
      '/pipelines/refunds?node=pg%3Apublic.refunds',
    )
    const nightly = screen.getByText('Nightly load').closest('tr')!
    expect(within(nightly).getByText('Unknown')).toBeInTheDocument()
    expect(within(nightly).getByText('Graded: wrong layer')).toBeInTheDocument()
  })

  it('opens the pipeline lane on a row click', async () => {
    renderFleet()
    await screen.findByRole('table', { name: 'Pipelines' })
    fireEvent.click(screen.getAllByText('Failing')[0].closest('td')!)
    expect(screen.getByTestId('where')).toHaveTextContent('/pipelines/p02')
  })

  it('keeps sort controls real buttons with names, in header order (TC4)', async () => {
    renderFleet()
    await screen.findByRole('table', { name: 'Pipelines' })
    const sorts = within(table()).getAllByRole('button')
    expect(sorts.map((b) => b.getAttribute('aria-label'))).toEqual([
      'Sort by pipeline',
      'Sort by health',
      'Sort by forecast ETA',
      'Sort by changes in 24 h',
    ])
    for (const b of sorts) {
      expect(b.tagName).toBe('BUTTON')
      expect(b).toHaveAttribute('type', 'button')
      expect(b).not.toHaveAttribute('tabindex', '-1')
    }
    sorts[0].focus()
    expect(document.activeElement).toBe(sorts[0])
    fireEvent.click(sorts[0])
    expect(rowIds()[0]).toBe('p00')
    // The pipeline name is the row's first focusable control, a link to its lane.
    const firstRow = within(table()).getAllByRole('row')[1]
    expect(within(firstRow).getAllByRole('link')[0]).toHaveAttribute('href', '/pipelines/p00')
  })

  it('draws cards with every control at phone width (E5)', async () => {
    setWidth(390)
    renderFleet()
    const list = await screen.findByRole('list', { name: 'Pipelines' })
    expect(screen.queryByRole('table')).toBeNull()
    expect(within(list).getAllByRole('listitem')).toHaveLength(50)
    for (const name of ['Sort by pipeline', 'Sort by health', 'Sort by forecast ETA', 'Sort by changes in 24 h']) {
      expect(screen.getByRole('button', { name })).toBeInTheDocument()
    }
    expect(screen.getByRole('button', { name: 'Filter by team' })).toBeInTheDocument()
    fireEvent.click(screen.getByRole('button', { name: 'Sort by pipeline' }))
    expect(within(list).getAllByRole('listitem')[0]).toHaveAttribute('data-pipeline-id', 'p00')
  })

  it('shows the first-run panel for an empty fleet (TC2)', async () => {
    vi.mocked(api.fleet).mockResolvedValue([])
    renderFleet()
    expect(await screen.findByText(FIRST_RUN_TITLE)).toBeInTheDocument()
    expect(screen.queryByRole('table')).toBeNull()
  })

  it('shows a viewer an empty fleet without the test alert', async () => {
    me.role = 'viewer'
    vi.mocked(api.fleet).mockResolvedValue([])
    renderFleet()
    expect(await screen.findByText(/No pipelines yet/)).toBeInTheDocument()
    expect(screen.queryByText(FIRST_RUN_TITLE)).toBeNull()
  })

  it.each([
    [403, 'not_allowed', 'not allowed', 'not allowed'],
    [404, 'not_found', 'not found', 'This server does not serve the fleet view.'],
    [500, 'internal', 'internal error', 'internal error'],
  ])('shows a neutral error for %i and retries (E2)', async (status, code, message, text) => {
    vi.mocked(api.fleet).mockRejectedValueOnce(new ApiClientError(status, code, message))
    renderFleet()
    const alert = await screen.findByRole('alert')
    expect(alert).toHaveTextContent('Could not load the fleet')
    expect(alert).toHaveTextContent(text)
    fireEvent.click(within(alert).getByRole('button', { name: 'Retry' }))
    expect(await screen.findByRole('table', { name: 'Pipelines' })).toBeInTheDocument()
  })

  it('keeps the rows when a refresh fails', async () => {
    renderFleet()
    await screen.findByRole('table', { name: 'Pipelines' })
    vi.mocked(api.fleet).mockRejectedValueOnce(new ApiClientError(500, 'internal', 'boom'))
    await act(async () => {
      await vi.advanceTimersByTimeAsync(30_000)
    })
    expect(screen.getByRole('alert')).toHaveTextContent('Could not refresh the fleet: boom')
    expect(rowIds()).toHaveLength(50)
  })

  it('renders a hostile title and team as text (E3)', async () => {
    const evil = '<img src=x onerror=alert(1)>'
    vi.mocked(api.fleet).mockResolvedValue([
      { ...(fixture[0] as FleetPipeline), title: evil, team: `${'t'.repeat(10_000)}\u0007`, worst_node: `<b>${evil}</b>` },
    ])
    renderFleet()
    await screen.findByRole('table', { name: 'Pipelines' })
    expect(document.querySelector('img')).toBeNull()
    expect(document.querySelector('b')).toBeNull()
    expect(screen.getByText(evil)).toBeInTheDocument()
  })
})

const change = (i: number, over: Partial<PipelineChange> = {}): PipelineChange => ({
  id: i,
  pipeline_id: 'orders',
  kind: 'connector_config',
  object: 'pg-orders-connector',
  at: new Date(NOW - i * 60_000).toISOString(),
  source: 'watcher',
  fingerprint: `f${i}`,
  summary: `change ${i}`,
  ...over,
})

describe('FleetScreen changes tab', () => {
  it('pages with the cursor and renders hostile titles as text (TC3)', async () => {
    const evil = '<img src=x onerror=alert(1)><script>alert(2)</script>'
    vi.mocked(api.fleetChanges)
      .mockResolvedValueOnce({
        changes: [
          change(1, { kind: 'deploy', summary: evil, actor: '<b>bot</b>', after: { url: 'javascript:alert(1)' } }),
          change(2, { kind: 'vendor_incident', summary: 'MSK degraded', after: { url: 'https://status.example/i/1' } }),
        ],
        next: 'cur-2',
      })
      .mockResolvedValueOnce({ changes: [change(2), change(3)], next: '' })
    renderFleet('/fleet?tab=changes')
    const list = await screen.findByRole('list', { name: 'Changes this week' })
    expect(within(list).getAllByRole('listitem')).toHaveLength(2)
    expect(screen.getByText(evil)).toBeInTheDocument()
    expect(screen.getByText('<b>bot</b>:')).toBeInTheDocument()
    expect(document.querySelector('img, script, b')).toBeNull()
    // Only the https url became a link.
    const links = within(list).getAllByRole('link', { name: 'Open the source' })
    expect(links).toHaveLength(1)
    expect(links[0]).toHaveAttribute('href', 'https://status.example/i/1')
    expect(links[0]).toHaveAttribute('rel', 'noopener noreferrer nofollow')
    expect(api.fleetChanges).toHaveBeenLastCalledWith({ limit: 50 })

    fireEvent.click(screen.getByRole('button', { name: 'Load more' }))
    await waitFor(() => expect(within(list).getAllByRole('listitem')).toHaveLength(3))
    expect(api.fleetChanges).toHaveBeenLastCalledWith({ limit: 50, cursor: 'cur-2' })
    expect(screen.queryByRole('button', { name: 'Load more' })).toBeNull()
    expect(screen.getByText('3 changes')).toBeInTheDocument()
  })

  it('keeps the loaded rows when a later page fails', async () => {
    vi.mocked(api.fleetChanges)
      .mockResolvedValueOnce({ changes: [change(1)], next: 'cur-2' })
      .mockRejectedValueOnce(new ApiClientError(400, 'validation', 'bad cursor'))
    renderFleet('/fleet?tab=changes')
    await screen.findByRole('list', { name: 'Changes this week' })
    fireEvent.click(screen.getByRole('button', { name: 'Load more' }))
    expect(await screen.findByText('Could not load more: bad cursor')).toBeInTheDocument()
    expect(screen.getByText('change 1')).toBeInTheDocument()
    expect(screen.getByRole('button', { name: 'Load more' })).toBeEnabled()
  })

  it('shows an empty week and a first-page error neutrally (E2)', async () => {
    renderFleet('/fleet?tab=changes')
    expect(await screen.findByText(/Nothing changed in the last 7 days/)).toBeInTheDocument()
    vi.mocked(api.fleetChanges).mockRejectedValueOnce(new ApiClientError(500, 'internal', 'internal error'))
    fireEvent.mouseDown(screen.getByRole('tab', { name: /Pipelines/ }))
    fireEvent.mouseDown(screen.getByRole('tab', { name: 'What changed this week' }))
    const alert = await screen.findByRole('alert')
    expect(alert).toHaveTextContent('Could not load what changed')
  })

  it('does not load changes until the tab is opened', async () => {
    renderFleet()
    await screen.findByRole('table', { name: 'Pipelines' })
    expect(api.fleetChanges).not.toHaveBeenCalled()
  })
})

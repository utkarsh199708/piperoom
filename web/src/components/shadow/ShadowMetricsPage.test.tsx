import { afterEach, describe, expect, it, vi } from 'vitest'
import { fireEvent, render, screen, waitFor } from '@testing-library/react'
import { MemoryRouter } from 'react-router-dom'
import { ApiClientError, api } from '../../api/client'
import type { ShadowMetrics } from '../../api/types'
import { ShadowMetricsPage } from './ShadowMetricsPage'

const metrics = (over: Partial<ShadowMetrics> = {}): ShadowMetrics => ({
  rooms: 2,
  labelled: 2,
  graded: 1,
  layer_correct: 1,
  root_cause: { yes: 1, partial: 0, no: 0 },
  by_status: { hypothesised: 1, error: 1 },
  median_time_to_first_s: 90,
  median_time_to_final_s: 240,
  median_human_mttr_s: 3120,
  total_tokens: 20000,
  tokens_available: true,
  per_room: [
    {
      room_id: 'r1',
      pipeline_id: 'orders',
      alert_source: 'watcher',
      status: 'hypothesised',
      opened_at: '2026-10-14T10:00:00Z',
      closed_at: '2026-10-14T10:52:00Z',
      labelled: true,
      graded: true,
      layer_correct: true,
      root_cause: 'yes',
      time_to_first_s: 90,
      time_to_final_s: 240,
      human_mttr_s: 3120,
      tool_calls: 5,
      tokens: 20000,
      finished_after_close: false,
    },
    {
      room_id: 'r2',
      pipeline_id: 'orders',
      alert_source: '',
      status: 'error',
      opened_at: '2026-10-14T12:00:00Z',
      closed_at: '2026-10-14T12:10:00Z',
      labelled: false,
      graded: false,
      layer_correct: false,
      root_cause: '',
      time_to_first_s: -1,
      time_to_final_s: -1,
      human_mttr_s: 600,
      tool_calls: 0,
      tokens: 0,
      finished_after_close: false,
    },
  ],
  ...over,
})

const renderPage = () =>
  render(
    <MemoryRouter>
      <ShadowMetricsPage />
    </MemoryRouter>,
  )

afterEach(() => vi.restoreAllMocks())

describe('ShadowMetricsPage', () => {
  it('shows the headline, counts and one row per room', async () => {
    vi.spyOn(api, 'shadowMetrics').mockResolvedValue(metrics())
    renderPage()
    expect(await screen.findByTestId('shadow-headline')).toHaveTextContent('1 of 2 matched, median 4 min vs 52 min, 20k tokens')
    expect(screen.getByRole('link', { name: 'r1' })).toHaveAttribute('href', '/rooms/r1')
    expect(screen.getByText('layer ok, cause yes')).toBeInTheDocument()
    expect(screen.getByText('not labelled')).toBeInTheDocument()
  })

  it('refetches with the pipeline and since filters', async () => {
    const spy = vi.spyOn(api, 'shadowMetrics').mockResolvedValue(metrics())
    renderPage()
    await screen.findByTestId('shadow-headline')
    fireEvent.change(screen.getByLabelText('Pipeline'), { target: { value: 'orders' } })
    fireEvent.change(screen.getByLabelText('Closed since'), { target: { value: '2026-10-01' } })
    await waitFor(() =>
      expect(spy).toHaveBeenLastCalledWith({ pipeline: 'orders', since: '2026-10-01T00:00:00Z' }),
    )
  })

  it('explains an empty result', async () => {
    vi.spyOn(api, 'shadowMetrics').mockResolvedValue(metrics({ rooms: 0, per_room: [], by_status: {} }))
    renderPage()
    expect(await screen.findByText(/No closed shadow rooms match/)).toBeInTheDocument()
  })

  it('shows the server error', async () => {
    vi.spyOn(api, 'shadowMetrics').mockRejectedValue(new ApiClientError(400, 'validation', 'since: not RFC 3339'))
    renderPage()
    expect(await screen.findByText('since: not RFC 3339')).toBeInTheDocument()
  })

  it('copies the headline line', async () => {
    vi.spyOn(api, 'shadowMetrics').mockResolvedValue(metrics())
    const writeText = vi.fn().mockResolvedValue(undefined)
    Object.defineProperty(navigator, 'clipboard', { value: { writeText }, configurable: true })
    renderPage()
    fireEvent.click(await screen.findByRole('button', { name: 'Copy line' }))
    expect(writeText).toHaveBeenCalledWith('1 of 2 matched, median 4 min vs 52 min, 20k tokens')
  })
})

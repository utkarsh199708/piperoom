import { describe, expect, it } from 'vitest'
import { fireEvent, render, screen } from '@testing-library/react'
import type { ForecastSummary, TrendPoint } from '../../api/types'
import { EtaBadge, ForecastChart, Sparkline } from './TrendChart'

const NOW = Date.parse('2026-10-22T10:00:00Z')

function series(n: number): TrendPoint[] {
  return Array.from({ length: n }, (_, i) => ({
    at: new Date(NOW - (n - 1 - i) * 15 * 60_000).toISOString(),
    value: 60e9 + i * 1e8,
  }))
}

const FORECAST: ForecastSummary = {
  metric: 'slot_wal_bytes',
  object: 'orders_slot',
  current: 64424509440,
  limit: 107374182400,
  eta: '2026-10-25T10:00:00Z',
  slope_per_hour: 1073741824,
  r2: 0.97,
}

describe('Sparkline (TC1)', () => {
  it('renders a 96-point fixture as one path with 96 points', () => {
    const { container } = render(<Sparkline points={series(96)} label="WAL, last 24 h" />)
    const d = container.querySelector('path')?.getAttribute('d') ?? ''
    expect(d.match(/[ML]/g)).toHaveLength(96)
    expect(screen.getByRole('img', { name: 'WAL, last 24 h' })).toBeInTheDocument()
  })

  it('renders nothing for an empty series', () => {
    const { container } = render(<Sparkline points={[]} label="x" />)
    expect(container).toBeEmptyDOMElement()
  })
})

describe('EtaBadge', () => {
  it('shows the short text and carries a full text alternative (TC4)', () => {
    render(<EtaBadge forecast={FORECAST} now={NOW} />)
    const badge = document.querySelector('[data-eta]') as HTMLElement
    expect(badge).toHaveTextContent('fills in 3 d')
    expect(badge.title).toMatch(/^Forecast: WAL retained by the slot \(orders_slot\): it reaches its limit in about 3 d/)
    // The visible words are hidden from a screen reader, which reads the full sentence instead.
    expect(screen.getByText('fills in 3 d')).toHaveAttribute('aria-hidden', 'true')
    expect(screen.getByText(badge.title)).toHaveClass('sr-only')
  })

  it('has no badge past a week', () => {
    const { container } = render(<EtaBadge forecast={{ ...FORECAST, eta: '2026-11-22T10:00:00Z' }} now={NOW} />)
    expect(container).toBeEmptyDOMElement()
  })

  it('renders a hostile object name as text (E3)', () => {
    const hostile = `<img src=x onerror=alert(1)>\u0007${'A'.repeat(10_000)}`
    render(<EtaBadge forecast={{ ...FORECAST, object: hostile }} now={NOW} />)
    expect(document.querySelector('img')).toBeNull()
    expect(document.querySelector('[data-eta]')?.textContent).toContain('<img src=x')
  })
})

describe('ForecastChart', () => {
  const ready = (points: TrendPoint[] | undefined) => ({
    status: 'ready' as const,
    series: points ? { slot_wal_bytes: points } : {},
  })

  it('draws the trend against a dashed limit line, with the fit numbers', () => {
    render(<ForecastChart forecast={FORECAST} trends={ready(series(96))} now={NOW} />)
    expect(screen.getByRole('heading', { name: 'Forecast · WAL retained by the slot' })).toBeInTheDocument()
    const chart = screen.getByRole('img', { name: /last 24 h: now .* limit 100 GiB/ })
    expect(chart.querySelector('line[stroke-dasharray]')).not.toBeNull()
    expect(chart.querySelector('path')?.getAttribute('d')?.match(/[ML]/g)).toHaveLength(96)
    expect(screen.getByText('now 60 GiB')).toBeInTheDocument()
    expect(screen.getByText('fit r² 0.97')).toBeInTheDocument()
    expect(screen.getByText('fills in 3 d')).toBeInTheDocument()
  })

  it('shows the hovered sample', () => {
    render(<ForecastChart forecast={FORECAST} trends={ready(series(4))} now={NOW} />)
    const chart = screen.getByRole('img', { name: /last 24 h/ })
    chart.getBoundingClientRect = () => ({ left: 0, width: 600, top: 0, height: 96, right: 600, bottom: 96, x: 0, y: 0, toJSON: () => ({}) })
    fireEvent.pointerMove(chart, { clientX: 599 })
    expect(screen.queryByText('now 60 GiB')).not.toBeInTheDocument()
    expect(screen.getByText(/: 56 GiB$/)).toBeInTheDocument()
    fireEvent.pointerLeave(chart)
    expect(screen.getByText('now 60 GiB')).toBeInTheDocument()
  })

  it.each([
    ['loading', { status: 'loading' as const, series: {} }, 'Loading the last 24 h…'],
    ['no samples yet', ready([]), 'No trend samples to draw for the last 24 h.'],
    ['the request failed', ready(undefined), 'The trend is not available.'],
    ['not asked', { status: 'idle' as const, series: {} }, 'The trend is not available.'],
  ])('%s: a neutral line, the forecast numbers still shown (E2)', (_n, trends, text) => {
    render(<ForecastChart forecast={FORECAST} trends={trends} now={NOW} />)
    expect(screen.getByText(text)).toBeInTheDocument()
    expect(screen.queryByRole('img')).toBeNull()
    expect(screen.getByText('fills in 3 d')).toBeInTheDocument()
  })

  it('renders nothing for a metric it does not know', () => {
    const { container } = render(<ForecastChart forecast={{ ...FORECAST, metric: 'disk_free' }} trends={ready([])} now={NOW} />)
    expect(container).toBeEmptyDOMElement()
  })
})

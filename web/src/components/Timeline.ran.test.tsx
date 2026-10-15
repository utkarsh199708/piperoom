import { afterEach, describe, expect, it, vi } from 'vitest'
import { fireEvent, render, screen, waitFor, within } from '@testing-library/react'
import { Timeline } from './Timeline'
import type { StepState } from '../state/types'

const step = (provenance?: StepState['tool_calls'][number]['provenance']): StepState => ({
  id: 's1',
  title: 'Check the slots',
  messages: [],
  tool_calls: [
    {
      tool_use_id: 't1',
      tool: 'pg_replication_slots',
      input: { names: ['debezium_orders'] },
      summary: '1 slot',
      took_ms: 70,
      provenance,
    },
  ],
})

const sql = {
  kind: 'sql',
  target: 'postgres orders@db.internal:5432',
  text: 'SELECT slot_name, plugin, active FROM pg_replication_slots WHERE slot_name = ANY($1)',
  params_redacted: ['$1=[debezium_orders]'],
  at: '2026-10-14T03:00:00Z',
  took_ms: 12,
}

// A finished step starts closed; the evidence is in its opened part.
const renderTimeline = (s: StepState) => {
  const r = render(<Timeline steps={[s]} messages={[]} comments={[]} onComment={vi.fn()} />)
  fireEvent.click(screen.getByRole('button', { name: /Check the slots/ }))
  return r
}

afterEach(() => {
  vi.restoreAllMocks()
})

describe('Timeline "Ran" line', () => {
  it('shows the statement, kind, redacted params and time', () => {
    renderTimeline(step([sql]))
    const line = screen.getByTestId('ran-line')
    expect(within(line).getByText('Ran')).toBeInTheDocument()
    expect(within(line).getByText('sql')).toHaveAttribute('title', 'postgres orders@db.internal:5432')
    expect(line.querySelector('code')?.textContent).toBe(sql.text)
    expect(within(line).getByText('$1=[debezium_orders]')).toBeInTheDocument()
    expect(within(line).getByText('12 ms')).toBeInTheDocument()
  })

  it('copies the text only', async () => {
    const writeText = vi.fn().mockResolvedValue(undefined)
    Object.defineProperty(navigator, 'clipboard', { value: { writeText }, configurable: true })
    renderTimeline(step([sql]))
    fireEvent.click(screen.getByRole('button', { name: 'Copy query' }))
    expect(writeText).toHaveBeenCalledWith(sql.text)
    await waitFor(() => expect(screen.getByRole('button', { name: 'Copied' })).toBeInTheDocument())
  })

  it('has no line without provenance', () => {
    renderTimeline(step(undefined))
    expect(screen.queryByTestId('ran-line')).not.toBeInTheDocument()
  })

  it('has no line for an empty list', () => {
    renderTimeline(step([]))
    expect(screen.queryByTestId('ran-line')).not.toBeInTheDocument()
  })

  it('renders one line per call and never as markup or a link', () => {
    const html = { ...sql, text: '<a href="http://evil.example">x</a> [y](http://evil.example)' }
    const { container } = renderTimeline(step([sql, html]))
    expect(screen.getAllByTestId('ran-line')).toHaveLength(2)
    expect(container.querySelector('a')).toBeNull()
    expect(container.textContent).toContain('<a href="http://evil.example">x</a>')
  })
})

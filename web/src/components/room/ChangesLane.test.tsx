import { describe, expect, it } from 'vitest'
import { render, screen, within } from '@testing-library/react'
import { ChangesLane } from './ChangesLane'
import type { StepState } from '@/state/types'

const NOW = Date.parse('2026-10-14T03:30:00Z')

function stepWith(data: unknown, tool = 'what_changed'): StepState[] {
  return [
    {
      id: 's1',
      title: 'What changed',
      messages: [],
      tool_calls: [{ tool_use_id: 'tu1', tool, input: {}, summary: 'x', data }],
    },
  ]
}

const change = (o: Record<string, unknown>) => ({
  id: 1, kind: 'ddl', object: 'public.orders', at: '2026-10-14T03:04:11Z', actor: 'app_migrator', source: 'ddl_audit',
  fingerprint: 'f', summary: 'ALTER TABLE public.orders | ALTER COLUMN amount TYPE numeric', ...o,
})

describe('ChangesLane', () => {
  it('renders nothing without a what_changed result', () => {
    const { container } = render(<ChangesLane steps={stepWith({ changes: [] }, 'connect_status')} alertAt="2026-10-14T03:10:00Z" now={NOW} />)
    expect(container).toBeEmptyDOMElement()
  })

  it('is a region named "What changed" and lists newest first', () => {
    const data = {
      changes: [
        change({ id: 1, at: '2026-10-14T02:00:00Z', object: 'older' }),
        change({ id: 2, at: '2026-10-14T03:05:00Z', object: 'newer', kind: 'connector_state' }),
      ],
    }
    render(<ChangesLane steps={stepWith(data)} alertAt="2026-10-14T03:10:00Z" now={NOW} />)
    const region = screen.getByRole('region', { name: 'What changed' })
    const items = within(region).getAllByRole('listitem').filter((li) => li.dataset.testid !== 'alert-rule')
    expect(items[0]).toHaveTextContent('newer')
    expect(items[1]).toHaveTextContent('older')
    expect(within(items[0]).getByText('connector_state')).toBeInTheDocument()
  })

  it('puts the alert-fired rule between the changes after it and the suspects before it', () => {
    const data = {
      changes: [
        change({ id: 1, at: '2026-10-14T03:15:00Z', object: 'after-alert' }),
        change({ id: 2, at: '2026-10-14T03:04:00Z', object: 'before-alert' }),
      ],
    }
    render(<ChangesLane steps={stepWith(data)} alertAt="2026-10-14T03:10:00Z" now={NOW} />)
    const items = screen.getAllByRole('listitem').map((li) => li.textContent ?? '')
    const ruleAt = items.findIndex((t) => t.includes('alert fired'))
    expect(items[ruleAt - 1]).toContain('after-alert')
    expect(items[ruleAt + 1]).toContain('before-alert')
  })

  it('shows before and after as text: markup in a value appears literally', () => {
    const data = {
      changes: [
        change({
          kind: 'connector_config', object: 'pg-orders',
          before: { 'heartbeat.interval.ms': '<b>10000</b>' },
          after: { 'heartbeat.interval.ms': '<img src=x onerror=alert(1)>' },
        }),
      ],
    }
    const { container } = render(<ChangesLane steps={stepWith(data)} alertAt="2026-10-14T03:10:00Z" now={NOW} />)
    expect(screen.getByLabelText('Before')).toHaveTextContent('<b>10000</b>')
    expect(screen.getByLabelText('After')).toHaveTextContent('<img src=x onerror=alert(1)>')
    expect(container.querySelector('b, img')).toBeNull()
  })

  it('says so when the result has no change', () => {
    render(<ChangesLane steps={stepWith({ changes: [] })} alertAt="2026-10-14T03:10:00Z" now={NOW} />)
    expect(screen.getByText('nothing recorded')).toBeInTheDocument()
  })

  it('ignores rows that are not changes', () => {
    render(<ChangesLane steps={stepWith({ changes: [null, 5, { kind: 'ddl' }, change({})] })} alertAt="2026-10-14T03:10:00Z" now={NOW} />)
    expect(screen.getByText('1 change')).toBeInTheDocument()
  })
})

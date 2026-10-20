import { afterEach, describe, expect, it } from 'vitest'
import { fireEvent, render, screen, within } from '@testing-library/react'
import type { Topology } from '../api/types'
import batch from '../fixtures/topology-batch.json'
import { PipelineMap } from './PipelineMap'

// A batch pipeline (P4, internal/topology/batch.go): DAG → task → model → sink table, run states in facts.
const fixture = batch as unknown as Topology

afterEach(() => sessionStorage.clear())

const nodeByName = (name: string) => screen.getByText(name, { selector: '[title]' }).closest('button')!

describe('PipelineMap — batch lane', () => {
  it('labels each batch kind and draws the nodes in the server order', () => {
    const { container } = render(<PipelineMap topology={fixture} />)
    expect(screen.getAllByText('DAG')).toHaveLength(1)
    expect(screen.getAllByText('Task')).toHaveLength(3)
    expect(screen.getAllByText('Model')).toHaveLength(2)
    expect(screen.getAllByText('Sink')).toHaveLength(1)
    const names = [...container.querySelectorAll('[data-node] .font-mono.text-sm')].map((n) => n.textContent)
    expect(names).toEqual([
      'revenue_daily',
      'revenue_daily.extract_orders',
      'revenue_daily.fct_revenue_run',
      'revenue_daily.publish',
      'dim_customer',
      'fct_revenue',
      'analytics.fct_revenue',
    ])
  })

  it('leads each compact node with its run state and shows the health word', () => {
    render(<PipelineMap topology={fixture} />)
    const cases: [string, string, string, string][] = [
      ['revenue_daily', 'last run', 'failed', 'fail'],
      ['revenue_daily.extract_orders', 'state', 'success', 'ok'],
      ['revenue_daily.fct_revenue_run', 'state', 'failed', 'fail'],
      ['revenue_daily.publish', 'state', 'upstream_failed', 'warn'],
      ['dim_customer', 'last run', 'success', 'ok'],
      ['fct_revenue', 'last run', 'error', 'fail'],
    ]
    const word: Record<string, string> = { ok: 'healthy', warn: 'degraded', fail: 'failed' }
    for (const [name, label, state, health] of cases) {
      const node = nodeByName(name)
      expect(node).toHaveAttribute('data-health', health)
      expect(within(node).getByText(label)).toBeInTheDocument()
      expect(node.textContent).toContain(`${label} ${state}`)
      expect(within(node).getAllByText(word[health]).length).toBeGreaterThan(0)
    }
  })

  it('shows every fact in the expanded card, run state first, and only failed nodes get the danger border', () => {
    const { container } = render(<PipelineMap topology={fixture} />)
    fireEvent.click(screen.getByRole('button', { name: 'Expand' }))
    const task = nodeByName('revenue_daily.publish')
    expect(within(task).getByText('mapped_instances')).toBeInTheDocument()
    expect(within(task).getByText('operator')).toBeInTheDocument()
    const dag = nodeByName('revenue_daily')
    expect(within(dag).getByText('previous_run_state')).toBeInTheDocument()
    // The raw state key is not repeated under the readable label.
    expect(within(dag).queryByText('last_run_state')).not.toBeInTheDocument()
    const danger = [...container.querySelectorAll<HTMLElement>('[data-health]')].filter((n) =>
      n.className.includes('border-destructive'),
    )
    expect(danger.map((n) => n.dataset.health)).toEqual(['fail', 'fail', 'fail'])
  })

  it('shows the partial badge when the graph is partial, and not otherwise', () => {
    const { rerender } = render(<PipelineMap topology={fixture} />)
    expect(screen.getByText('partial')).toHaveAttribute('title', expect.stringMatching(/incomplete, not healthy/))
    rerender(<PipelineMap topology={{ ...fixture, partial: false }} />)
    expect(screen.queryByText('partial')).not.toBeInTheDocument()
  })

  it('shows tasks_skipped as a note on the DAG node, compact and expanded, not as a fact', () => {
    render(<PipelineMap topology={fixture} />)
    const dag = () => nodeByName('revenue_daily')
    expect(within(dag()).getByText('2 tasks not drawn')).toBeInTheDocument()
    fireEvent.click(screen.getByRole('button', { name: 'Expand' }))
    expect(within(dag()).getByText('2 tasks not drawn')).toBeInTheDocument()
    expect(within(dag()).queryByText('tasks_skipped')).not.toBeInTheDocument()
    expect(document.querySelectorAll('[data-skipped]')).toHaveLength(1)
  })

  it('says "1 task not drawn" for one, and nothing for zero or a non-number', () => {
    const withSkipped = (v: string): Topology => ({
      ...fixture,
      nodes: fixture.nodes.map((n) => (n.kind === 'dag' ? { ...n, facts: { ...n.facts, tasks_skipped: v } } : n)),
    })
    const { rerender } = render(<PipelineMap topology={withSkipped('1')} />)
    expect(screen.getByText('1 task not drawn')).toBeInTheDocument()
    rerender(<PipelineMap topology={withSkipped('0')} />)
    expect(document.querySelector('[data-skipped]')).toBeNull()
    rerender(<PipelineMap topology={withSkipped('lots')} />)
    expect(document.querySelector('[data-skipped]')).toBeNull()
  })

  it('renders a hostile node name and fact as text, never as HTML', () => {
    const hostile = '<img src=x onerror="alert(1)"><b>pwn</b>'
    const t: Topology = {
      ...fixture,
      nodes: [
        { ...fixture.nodes[0], name: hostile, facts: { last_run_state: '<script>alert(2)</script>' } },
        ...fixture.nodes.slice(1),
      ],
    }
    const { container } = render(<PipelineMap topology={t} />)
    fireEvent.click(screen.getByRole('button', { name: 'Expand' }))
    expect(container.querySelector('img')).toBeNull()
    expect(container.querySelector('b')).toBeNull()
    expect(container.querySelector('script')).toBeNull()
    expect(screen.getByText(hostile, { selector: '[title]' })).toHaveAttribute('title', hostile)
    expect(screen.getByText('<script>alert(2)</script>')).toBeInTheDocument()
  })
})

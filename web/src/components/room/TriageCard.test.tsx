import { afterEach, describe, expect, it, vi } from 'vitest'
import { fireEvent, render, screen } from '@testing-library/react'
import type { StepState, TriageState } from '@/state/types'
import { TriageCard } from './TriageCard'

const steps: StepState[] = [
  { id: 's_a', title: 'connect_status', tool_calls: [], messages: [] },
  { id: 's_b', title: 'pg_recent_ddl', tool_calls: [], messages: [] },
]

function triage(over: Partial<TriageState> = {}): TriageState {
  return {
    status: 'ok',
    headline: 'task 0 FAILED on MISSING_UNION_BRANCH <img src=x onerror=alert(1)>',
    suspected_layer: 'kafka-connect',
    confidence: 'medium',
    evidence_refs: ['s_b', 's_unknown'],
    next_check: 'schema_registry_diff orders-value',
    at: new Date().toISOString(),
    ...over,
  }
}

afterEach(() => {
  vi.useRealTimers()
})

describe('TriageCard', () => {
  it('labels the first look unverified, in one named region, as plain text', () => {
    const { container } = render(<TriageCard state={{ triage: triage(), hypothesis: null, steps }} />)
    const region = screen.getByRole('region', { name: 'First look' })
    expect(region).toHaveTextContent('triage, unverified')
    expect(region).toHaveTextContent('medium')
    expect(screen.getByText(/MISSING_UNION_BRANCH <img src=x/)).toBeInTheDocument()
    expect(container.querySelector('img')).toBeNull()
    expect(screen.getByText(/MISSING_UNION_BRANCH/)).toHaveAttribute('aria-live', 'polite')
    expect(screen.getByRole('button', { name: 'Go to step 2' })).toBeInTheDocument()
    expect(screen.queryByRole('button', { name: 'Go to step 1' })).toBeNull()
    expect(region).toHaveTextContent('Next: schema_registry_diff orders-value')
    expect(screen.getAllByRole('region')).toHaveLength(1)
  })

  // Under HashRouter a "#step-…" link replaces the route and drops the user on the room list.
  it('step chips jump through onCite and never change the URL hash', () => {
    window.location.hash = '#/rooms/r_x'
    const onCite = vi.fn()
    const { container } = render(<TriageCard state={{ triage: triage(), hypothesis: null, steps }} onCite={onCite} />)
    expect(container.querySelector('a[href^="#"]')).toBeNull()
    fireEvent.click(screen.getByRole('button', { name: 'Go to step 2' }))
    expect(onCite).toHaveBeenCalledWith('s_b')
    expect(window.location.hash).toBe('#/rooms/r_x')
  })

  it('collapses to one muted line once a hypothesis exists', () => {
    render(
      <TriageCard
        state={{ triage: triage(), hypothesis: { text: 'h', based_on_steps: [], confidence: 'high', confirmed: false }, steps }}
      />,
    )
    expect(screen.queryByRole('region')).toBeNull()
    expect(screen.getByTestId('triage-collapsed')).toHaveTextContent(/^First look: task 0 FAILED/)
  })

  it('a failed pass says why for a minute, then hides', () => {
    const at = new Date().toISOString()
    const { rerender } = render(
      <TriageCard state={{ triage: triage({ status: 'failed', reason: 'timeout', headline: undefined, at }), hypothesis: null, steps }} />,
    )
    expect(screen.getByTestId('triage-failed')).toHaveTextContent('First look unavailable · timed out')
    const old = new Date(Date.now() - 61_000).toISOString()
    rerender(
      <TriageCard state={{ triage: triage({ status: 'failed', reason: 'budget', headline: undefined, at: old }), hypothesis: null, steps }} />,
    )
    expect(screen.queryByTestId('triage-failed')).toBeNull()
  })

  it('renders nothing without a first look', () => {
    const { container } = render(<TriageCard state={{ triage: null, hypothesis: null, steps }} />)
    expect(container).toBeEmptyDOMElement()
  })
})

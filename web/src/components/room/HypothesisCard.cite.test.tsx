import { describe, expect, it, vi } from 'vitest'
import { fireEvent, render, screen, within } from '@testing-library/react'
import { HypothesisCard } from './HypothesisCard'
import type { HypothesisState, StepState } from '@/state/types'

const steps: StepState[] = [
  { id: 's1', title: 'Check the connector', tool_calls: [], messages: [] },
  { id: 's2', title: 'Check the DDL log', tool_calls: [], messages: [] },
]
const hyp = (over: Partial<HypothesisState> = {}): HypothesisState => ({
  text: 'DDL widened the column',
  based_on_steps: ['s1', 's2'],
  confidence: 'high',
  confirmed: false,
  seq: 5,
  verdicts: [],
  ...over,
})

describe('HypothesisCard citations', () => {
  it('renders a labelled chip per cited step and calls onCite with the id', () => {
    const onCite = vi.fn()
    render(<HypothesisCard hypothesis={hyp()} steps={steps} onCite={onCite} />)
    const chip = screen.getByRole('button', { name: 'Go to step 2: Check the DDL log' })
    expect(chip).toHaveTextContent('step 2')
    fireEvent.click(chip)
    expect(onCite).toHaveBeenCalledWith('s2')
  })

  it('shows a disabled step ? for an id not in the log, never the raw id', () => {
    render(<HypothesisCard hypothesis={hyp({ based_on_steps: ['s_01JXXXXXXXXXXXXXXXXXXXXXXX'] })} steps={steps} onCite={vi.fn()} />)
    const chip = screen.getByRole('button', { name: 'step ?' })
    expect(chip).toBeDisabled()
    expect(screen.queryByText(/s_01J/)).not.toBeInTheDocument()
  })

  it('shows nothing about citations when there are none', () => {
    render(<HypothesisCard hypothesis={hyp({ based_on_steps: [] })} steps={steps} />)
    expect(screen.queryByTestId('citations')).not.toBeInTheDocument()
  })
})

describe('HypothesisCard unbacked terms', () => {
  it('counts them and marks the first occurrence of each', () => {
    render(<HypothesisCard hypothesis={hyp({ text: 'order_sync fell behind by 4821 records', unbacked_terms: ['order_sync', '4821'] })} steps={steps} />)
    expect(screen.getByText('2 unbacked')).toHaveAttribute('title', 'Terms the cited steps do not show')
    const marks = document.querySelectorAll('mark.unbacked')
    expect([...marks].map((m) => m.textContent)).toEqual(['order_sync', '4821'])
    expect(marks[0]).toHaveAttribute('title', 'Not found in the cited steps')
    expect(screen.getByText(/fell behind by/).closest('p')?.textContent).toBe('order_sync fell behind by 4821 records')
  })

  it('does not throw on a regex-special term, and renders it as text', () => {
    expect(() =>
      render(<HypothesisCard hypothesis={hyp({ text: 'call a.b(c) now', unbacked_terms: ['a.b(c)', '(['] })} steps={steps} />),
    ).not.toThrow()
    expect(within(document.body).getByText('a.b(c)').tagName).toBe('MARK')
  })

  it('shows no badge or marks without terms', () => {
    render(<HypothesisCard hypothesis={hyp()} steps={steps} />)
    expect(screen.queryByText(/unbacked/)).not.toBeInTheDocument()
    expect(document.querySelector('mark')).toBeNull()
  })

  it('never builds markup from a term', () => {
    render(<HypothesisCard hypothesis={hyp({ text: 'see <b>bold</b> here', unbacked_terms: ['<b>bold</b>'] })} steps={steps} />)
    expect(document.querySelector('b')).toBeNull()
    expect(document.querySelector('mark')?.textContent).toBe('<b>bold</b>')
  })
})

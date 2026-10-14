import { describe, expect, it, vi } from 'vitest'
import { fireEvent, render, screen } from '@testing-library/react'
import { HypothesisCard } from './HypothesisCard'
import type { HypothesisState } from '@/state/types'

const hyp = (over: Partial<HypothesisState> = {}): HypothesisState => ({
  text: 'DDL widened the column',
  based_on_steps: [],
  confidence: 'high',
  confirmed: false,
  seq: 5,
  verdicts: [],
  ...over,
})

const kabir = { kind: 'human' as const, id: 'kabir', name: 'Kabir' }
const priya = { kind: 'human' as const, id: 'priya', name: 'Priya' }

describe('HypothesisCard verdicts', () => {
  it('shows the confidence path from two entries on, repeats already dropped by the reducer', () => {
    const history = [
      { seq: 1, at: '2026-10-11T10:00:00Z', confidence: 'low' },
      { seq: 3, at: '2026-10-11T10:01:00Z', confidence: 'medium' },
    ]
    const { rerender } = render(<HypothesisCard hypothesis={hyp()} steps={[]} history={history.slice(0, 1)} />)
    expect(screen.queryByTestId('confidence-path')).not.toBeInTheDocument()
    rerender(<HypothesisCard hypothesis={hyp()} steps={[]} history={history} />)
    expect(screen.getByTestId('confidence-path').textContent).toMatch(/^low .+ → medium /)
  })

  it('renders a chip per person, with the note as a title', () => {
    render(
      <HypothesisCard
        hypothesis={hyp({
          verdicts: [
            { verdict: 'confirmed', by: kabir, at: '2026-10-11T10:03:00Z' },
            { verdict: 'disputed', by: priya, note: 'DDL never ran', at: '2026-10-11T10:05:00Z' },
          ],
        })}
        steps={[]}
      />,
    )
    expect(screen.getByTestId('verdict-kabir').textContent).toMatch(/confirmed by Kabir/)
    const chip = screen.getByTestId('verdict-priya')
    expect(chip.textContent).toMatch(/disputed by Priya/)
    expect(chip).toHaveAttribute('title', 'DDL never ran')
  })

  it('Confirm sends a verdict; the pressed state makes a second click a no-op', () => {
    const onVerdict = vi.fn()
    const { rerender } = render(<HypothesisCard hypothesis={hyp()} steps={[]} selfId="kabir" canDecide onVerdict={onVerdict} />)
    const confirm = screen.getByRole('button', { name: /Confirm/ })
    expect(confirm).toHaveAttribute('aria-pressed', 'false')
    fireEvent.click(confirm)
    expect(onVerdict).toHaveBeenCalledWith('confirmed', undefined)

    rerender(
      <HypothesisCard
        hypothesis={hyp({ confirmed: true, verdicts: [{ verdict: 'confirmed', by: kabir, at: '2026-10-11T10:03:00Z' }] })}
        steps={[]}
        selfId="kabir"
        canDecide
        onVerdict={onVerdict}
      />,
    )
    expect(screen.getByRole('button', { name: /Confirm/ })).toHaveAttribute('aria-pressed', 'true')
    fireEvent.click(screen.getByRole('button', { name: /Confirm/ }))
    expect(onVerdict).toHaveBeenCalledTimes(1)
  })

  it('Dispute… opens an input: Enter sends the note, Esc cancels', () => {
    const onVerdict = vi.fn()
    render(<HypothesisCard hypothesis={hyp()} steps={[]} selfId="priya" canDecide onVerdict={onVerdict} />)
    fireEvent.click(screen.getByRole('button', { name: /Dispute/ }))
    const input = screen.getByPlaceholderText('Why? (optional, 500 chars)')
    fireEvent.keyDown(input, { key: 'Escape' })
    expect(screen.queryByPlaceholderText('Why? (optional, 500 chars)')).not.toBeInTheDocument()
    expect(onVerdict).not.toHaveBeenCalled()

    fireEvent.click(screen.getByRole('button', { name: /Dispute/ }))
    const again = screen.getByPlaceholderText('Why? (optional, 500 chars)')
    fireEvent.change(again, { target: { value: 'DDL never ran' } })
    fireEvent.submit(again.closest('form') as HTMLFormElement)
    expect(onVerdict).toHaveBeenCalledWith('disputed', 'DDL never ran')
    expect(screen.queryByPlaceholderText('Why? (optional, 500 chars)')).not.toBeInTheDocument()
  })

  it('shows no controls to a viewer or in a closed room', () => {
    render(<HypothesisCard hypothesis={hyp()} steps={[]} selfId="x" canDecide={false} onVerdict={vi.fn()} />)
    expect(screen.queryByRole('button', { name: /Confirm/ })).not.toBeInTheDocument()
    expect(screen.queryByRole('button', { name: /Dispute/ })).not.toBeInTheDocument()
  })
})

import { describe, expect, it } from 'vitest'
import { render, screen } from '@testing-library/react'
import { HandoffChecks } from './room/HandoffChecks'

describe('HandoffChecks', () => {
  it('lists each violation with its rule in words, where it is, and the detail as text', () => {
    render(
      <HandoffChecks
        summary={{
          violations: [
            { rule: 'unknown_identifier', field: 'open[0]', detail: '"decimal.handling.mode" does not appear in the log' },
            { rule: 'unknown_number', field: 'why', detail: '<b>number 4821</b> does not appear' },
          ],
        }}
      />,
    )
    expect(screen.getByRole('heading', { name: 'Checks' })).toBeInTheDocument()
    expect(screen.getAllByRole('listitem')).toHaveLength(2)
    expect(screen.getByText('Name not found in the room log')).toHaveAttribute('title', 'unknown_identifier · open[0]')
    expect(screen.getByText('· Still open, item 1')).toBeInTheDocument()
    expect(screen.queryByText('unknown_identifier')).toBeNull()
    expect(screen.getByText('"decimal.handling.mode" does not appear in the log')).toBeInTheDocument()
    expect(document.querySelector('b')).toBeNull()
  })

  it('shows the clean row for an empty list', () => {
    render(<HandoffChecks summary={{ violations: [] }} />)
    expect(screen.getByText('Checked against the room log · no issues')).toBeInTheDocument()
  })

  it('shows nothing for a summary without the field', () => {
    const { container } = render(<HandoffChecks summary={{}} />)
    expect(container).toBeEmptyDOMElement()
  })
})

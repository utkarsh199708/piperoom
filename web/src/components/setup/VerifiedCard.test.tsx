import { describe, expect, it } from 'vitest'
import { render, screen } from '@testing-library/react'
import { VerifiedCard } from './VerifiedCard'
import { setupState } from '../../test/setupRoom'
import { rungList, verified } from '../../fixtures/setup'

const runner = { label: 'arjun-laptop', connected: true, tools: {} }

describe('VerifiedCard (U1-12b)', () => {
  it('lists every rung with its outcome and counts the green ones', () => {
    const state = setupState({
      snapshot: { runner },
      events: [verified(rungList(), 'pl_1', 3)],
    })
    render(<VerifiedCard state={state} />)
    const card = screen.getByRole('region', { name: /Verification/ })
    expect(card).toHaveTextContent('7 of 8 rungs green')
    const rungs = screen.getAllByRole('listitem')
    expect(rungs).toHaveLength(8)
    expect(rungs[0]).toHaveTextContent('connector_running')
    expect(rungs[0]).toHaveTextContent('ok')
    expect(rungs[7]).toHaveTextContent('person_change')
    expect(rungs[7]).toHaveTextContent('failed')
    expect(rungs[7]).toHaveTextContent('timed out; rungs 1 to 7 stay green')
    expect(card).toHaveTextContent('the agent cannot claim a verification')
  })

  it('says every rung is green when the ladder finished clean', () => {
    const state = setupState({
      snapshot: { runner },
      events: [verified(rungList().slice(0, 7), 'pl_1', 3)],
    })
    render(<VerifiedCard state={state} />)
    expect(screen.getByText('7 of 7 rungs green')).toBeInTheDocument()
  })

  it('renders nothing before a verification exists', () => {
    const { container } = render(<VerifiedCard state={setupState({ snapshot: { runner } })} />)
    expect(container).toBeEmptyDOMElement()
  })
})

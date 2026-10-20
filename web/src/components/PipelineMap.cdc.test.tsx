import { afterEach, describe, expect, it } from 'vitest'
import { fireEvent, render, screen } from '@testing-library/react'
import type { Topology } from '../api/types'
import cdc from '../fixtures/topology.json'
import { PipelineMap } from './PipelineMap'

const cdcTopology = cdc as unknown as Topology

// The snapshot was recorded before the batch kinds (P4f-1) were added to the lane: a CDC
// pipeline must render exactly as it did, compact and expanded.

afterEach(() => sessionStorage.clear())

describe('PipelineMap — CDC lane is unchanged by the batch kinds', () => {
  it('renders the CDC fixture as before, compact', () => {
    const { container } = render(<PipelineMap topology={cdcTopology} />)
    expect(container.innerHTML).toMatchSnapshot()
  })

  it('renders the CDC fixture as before, expanded', () => {
    const { container } = render(<PipelineMap topology={cdcTopology} />)
    fireEvent.click(screen.getByRole('button', { name: 'Expand' }))
    expect(container.innerHTML).toMatchSnapshot()
  })

  it('shows no partial badge for a CDC graph', () => {
    render(<PipelineMap topology={cdcTopology} />)
    expect(screen.queryByText('partial')).not.toBeInTheDocument()
  })
})

import { describe, expect, it } from 'vitest'
import { render, screen } from '@testing-library/react'
import { MemoryRouter } from 'react-router-dom'
import { SimilarCard } from './SimilarCard'
import type { StepState } from '@/state/types'

function steps(hit: Record<string, unknown>): StepState[] {
  return [{ id: 's', title: 't', messages: [], tool_calls: [{ tool_use_id: 'tu', tool: 'similar_incidents', input: {}, data: { hits: [hit] } }] }]
}

const base = {
  room_id: 'r_01', title: 'orders', date: '2026-09-30T10:00:00Z', fix_that_worked: 'connect_resume_connector',
  duration_s: 720, resolution: 'resumed the connector',
}

const renderCard = (s: StepState[]) => render(<MemoryRouter><SimilarCard steps={s} /></MemoryRouter>)

describe('SimilarCard', () => {
  it('reads like the task file and links to the room', () => {
    renderCard(steps({ ...base, closed_by: 'Kabir' }))
    expect(screen.getByText('orders had this on 30 Sep · fixed by connect_resume_connector · 12 min · closed by Kabir')).toBeInTheDocument()
    expect(screen.getByRole('link', { name: /Open room/ })).toHaveAttribute('href', '/rooms/r_01')
  })

  it('shows no name when the hit has no verified closer', () => {
    renderCard(steps({ ...base }))
    expect(screen.queryByText(/closed by/)).toBeNull()
  })

  it('renders nothing without a similar_incidents hit', () => {
    const { container } = renderCard([{ id: 's', title: 't', messages: [], tool_calls: [{ tool_use_id: 'tu', tool: 'similar_incidents', input: {}, data: { hits: [] } }] }])
    expect(container).toBeEmptyDOMElement()
  })

  it('treats the room id and text as plain text', () => {
    const { container } = renderCard(steps({ ...base, room_id: 'r/../x', title: '<i>x</i>' }))
    expect(screen.getByRole('link', { name: /Open room/ })).toHaveAttribute('href', '/rooms/r%2F..%2Fx')
    expect(container.querySelector('i')).toBeNull()
  })
})

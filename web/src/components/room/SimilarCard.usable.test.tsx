// Q1-web-b (G7): "Seen before" offers a past room only when it has something to teach.
import { describe, expect, it } from 'vitest'
import { render, screen } from '@testing-library/react'
import { MemoryRouter } from 'react-router-dom'
import { SimilarCard } from './SimilarCard'
import { isUsableHit, topSimilarHit } from '@/lib/f8'
import type { SimilarHit } from '@/lib/f8'
import type { StepState } from '@/state/types'

const steps = (...hits: Record<string, unknown>[]): StepState[] => [
  { id: 's', title: 't', messages: [], tool_calls: [{ tool_use_id: 'tu', tool: 'similar_incidents', input: {}, data: { hits } }] },
]
const renderCard = (s: StepState[]) => render(<MemoryRouter><SimilarCard steps={s} /></MemoryRouter>)

const junk = { room_id: 'r_junk', title: 'test', date: '2026-10-15T04:09:50Z', resolution: 'Closed by the QA run', score: 1.48 }
const good = {
  room_id: 'r_good', title: 'orders', date: '2026-09-30T10:00:00Z', resolution: 'resumed the connector',
  fix_that_worked: 'connect_resume_connector', duration_s: 720,
}

describe('SimilarCard only shows history worth reading', () => {
  it('a room with a resolution and nothing else (QA: "test", "Closed by the QA run...") is not offered', () => {
    const { container } = renderCard(steps(junk))
    expect(container).toBeEmptyDOMElement()
  })

  it('a room with no resolution is not offered, even with a fix', () => {
    const { container } = renderCard(steps({ ...good, resolution: '   ' }))
    expect(container).toBeEmptyDOMElement()
  })

  it('a resolution plus a fix that worked is offered', () => {
    renderCard(steps(good))
    expect(screen.getByRole('region', { name: 'Similar incident' })).toBeInTheDocument()
  })

  it('a resolution plus the actions of an executed fix is offered', () => {
    renderCard(steps({ ...junk, fix_actions: ['connect_restart_task'] }))
    expect(screen.getByText(/fixed by connect_restart_task/)).toBeInTheDocument()
  })

  it('a resolution plus a label (the layer a person named) is offered', () => {
    renderCard(steps({ ...junk, layer: 'kafka-connect' }))
    expect(screen.getByRole('region', { name: 'Similar incident' })).toBeInTheDocument()
  })

  it('skips an unusable best hit and shows the next usable one', () => {
    renderCard(steps(junk, good))
    expect(screen.getByRole('link', { name: /Open room/ })).toHaveAttribute('href', '/rooms/r_good')
    expect(screen.queryByText(/Closed by the QA run/)).toBeNull()
  })

  it('shows nothing when every hit is unusable', () => {
    const { container } = renderCard(steps(junk, { ...junk, room_id: 'r_2' }))
    expect(container).toBeEmptyDOMElement()
  })
})

describe('isUsableHit / topSimilarHit', () => {
  const hit = (over: Partial<SimilarHit> = {}): SimilarHit => ({
    room_id: 'r', title: '', date: '', fix_that_worked: '', fix_actions: [], layer: '', duration_s: 0, closed_by: '', resolution: 'done', ...over,
  })

  it.each([
    [{}, false],
    [{ resolution: '' }, false],
    [{ fix_that_worked: 'x', resolution: '' }, false],
    [{ fix_that_worked: ' ' }, false],
    [{ fix_that_worked: 'x' }, true],
    [{ fix_actions: ['a'] }, true],
    [{ layer: 'kafka' }, true],
  ] as [Partial<SimilarHit>, boolean][])('%j -> %s', (over, want) => {
    expect(isUsableHit(hit(over))).toBe(want)
  })

  it('topSimilarHit reads the layer and ignores malformed hits', () => {
    expect(topSimilarHit({ hits: [null, 5, { title: 'no id' }, { ...junk, layer: 'kafka' }] })?.layer).toBe('kafka')
    expect(topSimilarHit({ hits: [] })).toBeNull()
    expect(topSimilarHit(null)).toBeNull()
    expect(topSimilarHit({})).toBeNull()
  })
})

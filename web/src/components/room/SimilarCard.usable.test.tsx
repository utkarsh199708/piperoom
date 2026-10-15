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

const empty = { room_id: 'r_empty', title: '', date: '', resolution: '' }
const placeholder = { room_id: 'r_ph', title: 'orders', layer: 'kafka-connect', resolution: '(no resolution text)' }

describe('SimilarCard trusts the server selection (QA F20)', () => {
  it('a hit with nothing at all to show is not offered', () => {
    const { container } = renderCard(steps(empty))
    expect(container).toBeEmptyDOMElement()
  })

  it('a hit the server judged an incident is shown even without a resolution (unverified closer)', () => {
    renderCard(steps({ ...good, resolution: '' }))
    expect(screen.getByRole('region', { name: 'Similar incident' })).toBeInTheDocument()
  })

  it('a hypothesis-only room (title, no fix, no label) is shown', () => {
    renderCard(steps({ ...junk, resolution: '' }))
    expect(screen.getByRole('region', { name: 'Similar incident' })).toBeInTheDocument()
  })

  it('never shows the "(no resolution text)" placeholder', () => {
    renderCard(steps(placeholder))
    expect(screen.getByRole('region', { name: 'Similar incident' })).toBeInTheDocument()
    expect(screen.queryByText(/no resolution text/)).toBeNull()
  })

  it('skips an empty best hit and shows the next one', () => {
    renderCard(steps(empty, good))
    expect(screen.getByRole('link', { name: /Open room/ })).toHaveAttribute('href', '/rooms/r_good')
  })
})

describe('isUsableHit / topSimilarHit', () => {
  const hit = (over: Partial<SimilarHit> = {}): SimilarHit => ({
    room_id: 'r', title: '', date: '', fix_that_worked: '', fix_actions: [], layer: '', duration_s: 0, closed_by: '', resolution: '', ...over,
  })

  it.each([
    [{}, false],
    [{ resolution: '  ' }, false],
    [{ resolution: '(no resolution text)' }, false],
    [{ resolution: 'done' }, true],
    [{ title: 'orders' }, true],
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

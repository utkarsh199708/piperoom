// Q1-web-b: owner pages (G2) and fix notes (G10) in the timeline.
import { describe, expect, it, vi } from 'vitest'
import { render, screen, within } from '@testing-library/react'
import { Timeline } from './Timeline'
import type { CommentState, PageState } from '../state/types'

const agent = { kind: 'agent' as const, id: 'agent', name: 'Agent' }
const arjun = { kind: 'human' as const, id: 'arjun', name: 'Arjun' }

const comment = (over: Partial<CommentState> = {}): CommentState => ({
  id: 'c1',
  text: 'a remark',
  mentions_agent: false,
  author: arjun,
  at: '2026-10-15T04:12:00Z',
  replies: [],
  ...over,
})

const page = (over: Partial<PageState> = {}): PageState => ({
  seq: 3,
  at: '2026-10-15T04:10:00Z',
  by: agent,
  layer: 'consumer',
  owners: ['kabir'],
  reason: 'Lag keeps growing',
  auto: false,
  ...over,
})

const timeline = (props: Partial<Parameters<typeof Timeline>[0]> = {}) =>
  render(<Timeline steps={[]} messages={[]} comments={[]} onComment={vi.fn()} {...props} />)

describe('fix notes', () => {
  it('stand apart from a plain comment: labelled, tinted, same words', () => {
    const { container } = timeline({
      comments: [comment({ id: 'c1', text: 'Restarted order-sync by hand', kind: 'fix_note' }), comment({ id: 'c2', text: 'just a remark' })],
    })
    const notes = container.querySelectorAll('[data-kind="fix_note"]')
    expect(notes).toHaveLength(1)
    expect(within(notes[0] as HTMLElement).getByText('Fix note')).toBeInTheDocument()
    expect(notes[0]).toHaveTextContent('Restarted order-sync by hand')
    expect(screen.getAllByText('Fix note')).toHaveLength(1) // the plain comment has no label
    expect(screen.getByText('just a remark')).toBeInTheDocument()
  })

  it('a fix note on a step sits under that step', () => {
    const steps = [{ id: 's1', title: 'Check the connector', tool_calls: [], messages: [] }]
    const { container } = timeline({ steps, comments: [comment({ step_id: 's1', text: 'fixed it', kind: 'fix_note' })] })
    expect(container.querySelector('li [data-kind="fix_note"]')).not.toBeNull()
  })

  it('is plain text', () => {
    const { container } = timeline({ comments: [comment({ text: '<a href="http://evil.example">x</a>', kind: 'fix_note' })] })
    expect(container.querySelector('a')).toBeNull()
  })
})

describe('owners paged', () => {
  it('lists who paged whom, on which layer, and why', () => {
    timeline({ pages: [page()], ownerNames: { kabir: 'Kabir' }, steps: [{ id: 's1', title: 't', tool_calls: [], messages: [] }] })
    const section = screen.getByRole('region', { name: 'Owners paged' })
    expect(section).toHaveTextContent('Agent paged Kabir consumer: Lag keeps growing')
  })

  it('says when the server paged, when a person did, and when the layer has nobody', () => {
    timeline({
      pages: [
        page({ seq: 1, auto: true, by: { kind: 'system', id: 'server', name: 'Piperoom' } }),
        page({ seq: 2, by: arjun, layer: 'kafka', owners: [] }),
      ],
      steps: [{ id: 's1', title: 't', tool_calls: [], messages: [] }],
    })
    const rows = screen.getAllByRole('listitem').filter((li) => li.textContent?.includes('paged'))
    expect(rows[0]).toHaveTextContent('Piperoom (auto) paged kabir')
    expect(rows[1]).toHaveTextContent('Arjun paged nobody (no owner is listed)')
  })

  it('shows before the agent has done anything, under the empty state', () => {
    timeline({ pages: [page()], agent: { state: 'idle' } })
    expect(screen.getByRole('region', { name: 'Owners paged' })).toBeInTheDocument()
  })

  it('is absent without pages', () => {
    timeline({ comments: [comment()] })
    expect(screen.queryByRole('region', { name: 'Owners paged' })).toBeNull()
  })

  it('shows the reason as text', () => {
    const { container } = timeline({ pages: [page({ reason: '<img src=x onerror=alert(1)>' })] })
    expect(container.querySelector('img')).toBeNull()
    expect(container).toHaveTextContent('<img src=x onerror=alert(1)>')
  })
})

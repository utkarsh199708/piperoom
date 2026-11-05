import { describe, expect, it } from 'vitest'
import { render, screen } from '@testing-library/react'
import type { ShadowResponse, ShadowRun } from '../../api/types'
import { RunReveal } from './RunReveal'

const run = (over: Partial<ShadowRun> = {}): ShadowRun => ({
  room_id: 'r1',
  status: 'hypothesised',
  model: 'm',
  pipeline_map: '',
  started_at: '2026-10-14T10:00:00Z',
  first_hypothesis_at: '2026-10-14T10:04:00Z',
  final_hypothesis_at: '2026-10-14T10:05:00Z',
  finished_at: '2026-10-14T10:05:00Z',
  finished_after_close: false,
  final_hypothesis: { text: 'A hypothesis.', based_on_steps: [], confidence: 'high', confirmed: false },
  tool_calls: 7,
  tokens: 12000,
  revealed_at: '2026-10-14T11:00:00Z',
  ...over,
})

function renderReveal(over: Partial<ShadowRun>) {
  const data: ShadowResponse = { run: run(over), events: [], room: {} }
  render(<RunReveal data={data} openedAt="2026-10-14T10:00:00Z" humanMttrS={3000} showData={false} onToggleData={() => {}} />)
}

const FINISHED = 'The agent finished after the room was closed.'
const STOPPED = 'The agent was stopped when the room closed.'

describe('RunReveal close note', () => {
  it('says an interrupted run was stopped, not finished', () => {
    renderReveal({ status: 'interrupted', finished_after_close: true, final_hypothesis: null })
    expect(screen.getByText(STOPPED)).toBeInTheDocument()
    expect(screen.queryByText(FINISHED)).not.toBeInTheDocument()
  })

  it('keeps "finished after the room was closed" for a completed run', () => {
    renderReveal({ status: 'hypothesised', finished_after_close: true })
    expect(screen.getByText(FINISHED)).toBeInTheDocument()
    expect(screen.queryByText(STOPPED)).not.toBeInTheDocument()
  })

  it('shows neither sentence when the run finished before the close', () => {
    renderReveal({ status: 'hypothesised', finished_after_close: false })
    expect(screen.queryByText(FINISHED)).not.toBeInTheDocument()
    expect(screen.queryByText(STOPPED)).not.toBeInTheDocument()
  })
})

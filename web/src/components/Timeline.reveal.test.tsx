// QA R18 (G11): a step link to a step that a layer filter hides clears the filter, so the step shows.
import { describe, expect, it, vi } from 'vitest'
import { act, render } from '@testing-library/react'
import { Timeline } from './Timeline'
import { goToStep } from '@/lib/triage'
import type { StepState } from '@/state/types'

const STEP = 's_01M4Y9X0ZPZ0NPJW00JGWVY8VP'
// connect_status belongs to kafka-connect, so a "postgres" filter hides this step.
const steps: StepState[] = [
  {
    id: STEP,
    title: 'connect_status',
    tool_calls: [{ tool_use_id: 'tu', tool: 'connect_status', input: {}, summary: 'ok', took_ms: 1 }],
    messages: [],
  },
]

describe('goToStep when a layer filter hides the step', () => {
  it('clears the filter so the step is rendered and highlighted', () => {
    const onClear = vi.fn()
    render(<Timeline steps={steps} messages={[]} comments={[]} onComment={vi.fn()} filterLayer="postgres" onClearFilter={onClear} />)
    expect(document.getElementById(`step-${STEP}`)).toBeNull()
    act(() => goToStep(STEP))
    expect(onClear).toHaveBeenCalledTimes(1)
  })

  it('does not clear the filter for a step the room does not have', () => {
    const onClear = vi.fn()
    render(<Timeline steps={steps} messages={[]} comments={[]} onComment={vi.fn()} filterLayer="postgres" onClearFilter={onClear} />)
    act(() => goToStep('s_01M4Y9X0ZPZ0NPJW00JGWVY8VZ'))
    expect(onClear).not.toHaveBeenCalled()
  })

  it('a visible step is highlighted without touching the filter', () => {
    const onClear = vi.fn()
    render(<Timeline steps={steps} messages={[]} comments={[]} onComment={vi.fn()} onClearFilter={onClear} />)
    act(() => goToStep(STEP))
    expect(document.getElementById(`step-${STEP}`)?.className).toContain('ring-2')
    expect(onClear).not.toHaveBeenCalled()
  })
})

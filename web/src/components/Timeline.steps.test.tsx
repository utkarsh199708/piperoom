import { describe, expect, it, vi } from 'vitest'
import { render, screen } from '@testing-library/react'
import { Timeline } from './Timeline'
import type { AgentMessageState, StepState } from '@/state/types'

// Real step ids: "s_" and 26 characters of the ULID alphabet.
const FIRST = 's_01M4Y9X0ZPZ0NPJW00JGWVY8VP'
const SECOND = 's_01M4Y9X0ZPZ0NPJW00JGWVY8VQ'

const steps: StepState[] = [
  { id: FIRST, title: 'connect_status', tool_calls: [], messages: [] },
  { id: SECOND, title: 'pg_recent_ddl', tool_calls: [], messages: [] },
]

describe('Timeline agent messages', () => {
  it('links a step id in an agent message to that step, numbered in the room', () => {
    const messages: AgentMessageState[] = [{ text: `The DDL ran after ${SECOND}, not before ${FIRST}`, at: '2026-10-15T10:00:00Z' }]
    render(<Timeline steps={steps} messages={messages} comments={[]} onComment={vi.fn()} />)
    expect(screen.getByRole('link', { name: 'step 2' })).toHaveAttribute('href', `#step-${SECOND}`)
    expect(screen.getByRole('link', { name: 'step 1' })).toHaveAttribute('href', `#step-${FIRST}`)
    expect(document.body.textContent).not.toContain(FIRST)
  })
})

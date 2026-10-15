import { describe, expect, it, vi } from 'vitest'
import { fireEvent, render, screen, within } from '@testing-library/react'
import { Timeline } from './Timeline'
import type { AgentMessageState, CommentState, StepState } from '../state/types'

// C13 prompt audit P5: agent-written messages must render as plain text —
// never as HTML and never as a Markdown link.
const HTML_PAYLOAD = '<a href="http://evil.example">click</a>'
const MD_PAYLOAD = '[x](http://evil.example)'

const messages: AgentMessageState[] = [
  { text: `I found this in the log: ${HTML_PAYLOAD}`, at: '2026-01-01T00:00:00Z' },
  { text: `Reference material: ${MD_PAYLOAD}`, at: '2026-01-01T00:00:01Z' },
]

describe('Timeline plain-text agent messages (P5)', () => {
  it('renders html- and markdown-looking agent text literally, with no links', () => {
    const { container } = render(
      <Timeline steps={[]} messages={messages} comments={[]} onComment={vi.fn()} />,
    )

    // Both payloads appear verbatim in the rendered text content.
    const text = container.textContent ?? ''
    expect(text).toContain(HTML_PAYLOAD)
    expect(text).toContain(MD_PAYLOAD)

    // No anchor element or link role was created from the message text.
    expect(container.querySelector('a')).toBeNull()
    expect(screen.queryAllByRole('link')).toHaveLength(0)
  })
})

const author = { kind: 'human' as const, id: 'priya', name: 'Priya' }
const steps: StepState[] = [
  {
    id: 's1',
    title: 'Check the Debezium connector',
    tool_calls: [
      {
        tool_use_id: 't1',
        tool: 'connect_status',
        input: { connector: 'pg-orders-connector' },
        summary: 'Task 0: FAILED',
        took_ms: 412,
      },
    ],
    messages: [],
  },
  {
    id: 's2',
    title: 'Confirm no rows reached the warehouse',
    tool_calls: [{ tool_use_id: 't2', tool: 'warehouse_freshness', input: { table: 'analytics.orders' } }],
    messages: [],
  },
]
const comments: CommentState[] = [
  {
    id: 'c1',
    step_id: 's1',
    text: 'That is the 10:58 migration.',
    mentions_agent: false,
    author,
    at: '2026-01-01T10:02:00Z',
    replies: [{ text: 'Thanks, noted.', at: '2026-01-01T10:03:00Z' }],
  },
]

describe('Timeline step stream', () => {
  it('shows a finished step as a compact row with its tool, args and time', () => {
    render(<Timeline steps={steps} messages={[]} comments={[]} onComment={vi.fn()} />)
    const row = screen.getByRole('button', { name: /Check the Debezium connector/ })
    expect(row).toHaveAttribute('aria-expanded', 'false')
    expect(within(row).getByText('connect_status')).toBeInTheDocument()
    expect(within(row).getByText('connector=pg-orders-connector')).toBeInTheDocument()
    expect(within(row).getByText('412 ms')).toBeInTheDocument()
    // Evidence is mounted but hidden until the row opens.
    expect(screen.getByText('Task 0: FAILED').closest('[hidden]')).not.toBeNull()
  })

  it('opens a row to show the evidence and the raw details', () => {
    render(<Timeline steps={steps} messages={[]} comments={[]} onComment={vi.fn()} />)
    const row = screen.getByRole('button', { name: /Check the Debezium connector/ })
    fireEvent.click(row)
    expect(row).toHaveAttribute('aria-expanded', 'true')
    expect(screen.getByText('Task 0: FAILED').closest('[hidden]')).toBeNull()
    const region = screen.getByText('Task 0: FAILED').closest('[id]') as HTMLElement
    expect(within(region).getByText('Raw input and result').closest('details')).not.toBeNull()
  })

  it('marks the last step live while the agent works: spinner, bar, no toggle row', () => {
    render(<Timeline steps={steps} messages={[]} comments={[]} onComment={vi.fn()} working />)
    expect(screen.getByRole('img', { name: 'Running' })).toBeInTheDocument()
    expect(screen.getByText('running')).toBeInTheDocument()
    // The live step is a card, not an expandable row; the finished one still is.
    expect(screen.queryByRole('button', { name: /Confirm no rows/ })).not.toBeInTheDocument()
    expect(screen.getByRole('button', { name: /Check the Debezium connector/ })).toBeInTheDocument()
    // Both animations are motion-safe only.
    const spinner = screen.getByRole('img', { name: 'Running' })
    expect(spinner.className).toContain('motion-safe:animate-spin')
    expect(document.querySelector('[data-slot="progress-indicator"]')?.className).toContain('motion-safe:animate-indeterminate')
  })

  it('no live step when the agent is idle', () => {
    render(<Timeline steps={steps} messages={[]} comments={[]} onComment={vi.fn()} />)
    expect(screen.queryByRole('img', { name: 'Running' })).not.toBeInTheDocument()
  })

  it('shows comments and the agent reply under their step, and a member can reply', () => {
    const onComment = vi.fn()
    render(<Timeline steps={steps} messages={[]} comments={comments} onComment={onComment} />)
    expect(screen.getByText('That is the 10:58 migration.')).toBeInTheDocument()
    expect(screen.getByText('Thanks, noted.')).toBeInTheDocument()
    fireEvent.click(screen.getByRole('button', { name: 'Reply' }))
    fireEvent.change(screen.getByLabelText('Reply to comment'), { target: { value: 'ack' } })
    fireEvent.click(screen.getByRole('button', { name: 'Send' }))
    expect(onComment).toHaveBeenCalledWith('ack', 's1')
  })

  it('a member comments on a step', () => {
    const onComment = vi.fn()
    render(<Timeline steps={steps.slice(0, 1)} messages={[]} comments={[]} onComment={onComment} />)
    // The box lives in the row's opened part (mounted while closed, so a draft survives).
    fireEvent.click(screen.getByRole('button', { name: /Check the Debezium connector/ }))
    fireEvent.change(screen.getByLabelText('Comment on this step'), { target: { value: 'looks right' } })
    fireEvent.click(screen.getByRole('button', { name: 'Comment' }))
    expect(onComment).toHaveBeenCalledWith('looks right', 's1')
  })

  it('read-only: comments show, no box and no reply button', () => {
    render(<Timeline steps={steps} messages={[]} comments={comments} onComment={vi.fn()} readOnly />)
    expect(screen.getByText('That is the 10:58 migration.')).toBeInTheDocument()
    expect(screen.queryByLabelText('Comment on this step')).not.toBeInTheDocument()
    expect(screen.queryByRole('button', { name: 'Reply' })).not.toBeInTheDocument()
  })

  it('an empty timeline says so and offers Take control and steer only when allowed', () => {
    const take = vi.fn()
    const { rerender } = render(<Timeline steps={[]} messages={[]} comments={[]} onComment={vi.fn()} />)
    expect(screen.getByText('The agent is idle and has not started yet.')).toBeInTheDocument()
    expect(screen.queryByRole('button')).not.toBeInTheDocument()
    rerender(<Timeline steps={[]} messages={[]} comments={[]} onComment={vi.fn()} onTakeControl={take} />)
    fireEvent.click(screen.getByRole('button', { name: 'Take control and steer' }))
    expect(take).toHaveBeenCalledTimes(1)
  })

  it('the empty state follows the agent status', () => {
    const take = vi.fn()
    const view = (state: string, detail?: string) =>
      render(
        <Timeline steps={[]} messages={[]} comments={[]} onComment={vi.fn()} onTakeControl={take} agent={{ state, detail }} />,
      )
    const a = view('working')
    expect(screen.getByText('The agent is investigating…')).toBeInTheDocument()
    expect(screen.queryByRole('button')).not.toBeInTheDocument()
    a.unmount()
    const b = view('starting')
    expect(screen.getByText('The agent is starting…')).toBeInTheDocument()
    b.unmount()
    const c = view('error', 'anthropic: 529 overloaded')
    expect(screen.getByText('The agent failed before its first step.')).toBeInTheDocument()
    expect(screen.getByText('anthropic: 529 overloaded')).toBeInTheDocument()
    expect(screen.getByRole('button', { name: 'Take control and steer' })).toBeInTheDocument()
    c.unmount()
    view('idle')
    expect(screen.getByRole('button', { name: 'Take control and steer' })).toBeInTheDocument()
  })

  it('queued and log_full are not "idle": no steer is offered', () => {
    const take = vi.fn()
    const view = (state: string, detail?: string) =>
      render(
        <Timeline steps={[]} messages={[]} comments={[]} onComment={vi.fn()} onTakeControl={take} agent={{ state, detail }} />,
      )
    const a = view('queued', 'waiting for an agent slot (2 ahead)')
    expect(screen.getByText('The agent is waiting for a free slot.')).toBeInTheDocument()
    expect(screen.getByText('waiting for an agent slot (2 ahead)')).toBeInTheDocument()
    expect(screen.queryByRole('button')).not.toBeInTheDocument()
    expect(screen.queryByText(/idle/)).not.toBeInTheDocument()
    a.unmount()
    view('log_full')
    expect(screen.getByText(/log is full/)).toBeInTheDocument()
    expect(screen.queryByRole('button')).not.toBeInTheDocument()
    expect(screen.queryByText(/idle/)).not.toBeInTheDocument()
  })
})

describe('Timeline layer filter', () => {
  it('shows only matching steps with a status line, keeps the live step with (live)', () => {
    const onClear = vi.fn()
    render(
      <Timeline steps={steps} messages={[]} comments={[]} onComment={vi.fn()} filterLayer="kafka-connect" onClearFilter={onClear} />,
    )
    expect(screen.getByRole('status')).toHaveTextContent('Showing 1 step on kafka-connect')
    expect(screen.getByRole('button', { name: /Check the Debezium connector/ })).toBeInTheDocument()
    expect(screen.queryByRole('button', { name: /Confirm no rows/ })).not.toBeInTheDocument()
    fireEvent.click(screen.getByRole('button', { name: 'Clear' }))
    expect(onClear).toHaveBeenCalled()
  })

  it('keeps the live step even when it does not match', () => {
    render(<Timeline steps={steps} messages={[]} comments={[]} onComment={vi.fn()} working filterLayer="kafka-connect" />)
    expect(screen.getByText('(live)')).toBeInTheDocument()
  })

  it('says so when no step matches', () => {
    render(<Timeline steps={steps} messages={[]} comments={[]} onComment={vi.fn()} filterLayer="postgres" />)
    expect(screen.getByRole('status')).toHaveTextContent('No steps on postgres yet')
  })
})

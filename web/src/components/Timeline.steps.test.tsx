import { describe, expect, it, vi } from 'vitest'
import { fireEvent, render, screen, within } from '@testing-library/react'
import { Timeline } from './Timeline'
import type { StepState } from '../state/types'
import { layerForTool } from '../lib/p4-temp-layers'

const steps: StepState[] = [
  {
    id: 's1',
    title: 'Check the connector',
    messages: [],
    tool_calls: [
      {
        tool_use_id: 'u1',
        tool: 'connect_status',
        input: { connector: 'pg-orders-connector' },
        summary: 'RUNNING\nTask 0: FAILED',
        took_ms: 412,
      },
    ],
  },
  {
    id: 's2',
    title: 'Read the DDL log',
    messages: [],
    tool_calls: [
      {
        tool_use_id: 'u2',
        tool: 'pg_recent_ddl',
        input: { since: '2026-10-11T09:00:00Z', limit: 5, nested: { a: 'x'.repeat(80) } },
        summary: 'ALTER TABLE orders',
        took_ms: 64,
        data: { rows: 1 },
        truncated: true,
      },
    ],
  },
]

function renderTimeline(over: Partial<React.ComponentProps<typeof Timeline>> = {}) {
  return render(
    <Timeline steps={steps} messages={[]} comments={[]} onComment={vi.fn()} {...over} />,
  )
}

describe('Timeline — numbered rail and live step', () => {
  it('numbers the steps and gives each an anchor id', () => {
    const { container } = renderTimeline()
    const dots = Array.from(container.querySelectorAll('.step__dot')).map((d) => d.textContent)
    expect(dots).toEqual(['1', '2'])
    expect(container.querySelector('#step-s1')).not.toBeNull()
    expect(container.querySelector('#step-s2')).not.toBeNull()
  })

  it('marks only the last step live, and only while the agent is working', () => {
    const idle = renderTimeline({ agentState: 'idle' })
    expect(idle.container.querySelector('.step--live')).toBeNull()
    idle.unmount()

    const none = renderTimeline()
    expect(none.container.querySelector('.step--live')).toBeNull()
    none.unmount()

    const working = renderTimeline({ agentState: 'working' })
    const live = working.container.querySelectorAll('.step--live')
    expect(live).toHaveLength(1)
    expect(live[0]).toHaveAttribute('id', 'step-s2')
  })

  it('shows a dashed thinking step while working, with the agent detail', () => {
    renderTimeline({ agentState: 'working', agentDetail: 'step 3' })
    const ghost = screen.getByTestId('step-ghost')
    expect(ghost).toHaveClass('step--ghost')
    expect(ghost).toHaveTextContent('thinking…')
    expect(ghost).toHaveTextContent('step 3')
  })

  it('shows no thinking step when the agent is idle', () => {
    renderTimeline({ agentState: 'idle' })
    expect(screen.queryByTestId('step-ghost')).not.toBeInTheDocument()
  })

  it('names the running tool when the last call has no result yet', () => {
    const { container } = renderTimeline({
      agentState: 'working',
      steps: [
        {
          id: 's9',
          title: 'Check freshness',
          messages: [],
          tool_calls: [{ tool_use_id: 'u9', tool: 'warehouse_freshness', input: { table: 't' } }],
        },
      ],
    })
    const live = container.querySelector('.live') as HTMLElement
    expect(live).toHaveTextContent('running warehouse_freshness…')
    expect(container.querySelector('.live__bar')).not.toBeNull()
    expect(screen.queryByTestId('step-ghost')).not.toBeInTheDocument()
  })

  it('marks a step with a failed tool call as an error step', () => {
    const { container } = renderTimeline({
      steps: [
        {
          id: 'e1',
          title: 'Broken',
          messages: [],
          tool_calls: [{ tool_use_id: 'x', tool: 'kafka_topics', input: null, summary: 'boom', is_error: true }],
        },
      ],
    })
    const step = container.querySelector('#step-e1') as HTMLElement
    expect(step).toHaveClass('step--error')
    expect(within(step).getByText('✕')).toBeInTheDocument()
    expect(step.querySelector('.toolcall')).toHaveClass('toolcall--error')
    expect(step.querySelector('.toolcall__summary')).toHaveClass('toolcall__summary--error')
    expect(step.querySelector('.toolcall__tool')).toHaveClass('is-bad')
  })
})

describe('Timeline — evidence', () => {
  it('colours the tool tag by its layer', () => {
    expect(layerForTool('connect_status')).toBe('kafka-connect')
    renderTimeline()
    expect(screen.getByText('connect_status')).toHaveClass('toolcall__tool--kafka-connect')
    expect(screen.getByText('pg_recent_ddl')).toHaveClass('toolcall__tool--postgres')
  })

  it('has no layer class for a tool it does not know', () => {
    renderTimeline({
      steps: [
        {
          id: 'u',
          title: 'Odd',
          messages: [],
          tool_calls: [{ tool_use_id: 'z', tool: 'mystery_tool', input: null, summary: 'ok' }],
        },
      ],
    })
    expect(screen.getByText('mystery_tool').className).not.toMatch(/toolcall__tool--/)
  })

  it('shows input keys as arg tags: strings verbatim, other values as JSON cut at 40 characters', () => {
    const { container } = renderTimeline()
    const args = Array.from(container.querySelectorAll('#step-s2 .arg'))
    expect(args).toHaveLength(3)
    expect(args[0]).toHaveTextContent('since2026-10-11T09:00:00Z')
    expect(args[1]).toHaveTextContent('limit5')
    // the nested object is JSON, truncated, with the full value in the title
    const nested = args[2] as HTMLElement
    expect(nested.querySelector('span')?.textContent?.endsWith('…')).toBe(true)
    expect(nested.querySelector('span')?.textContent).toHaveLength(41)
    expect(nested.title.length).toBeGreaterThan(41)
  })

  it('keeps the raw input and data behind details', () => {
    const { container } = renderTimeline()
    const details = Array.from(container.querySelectorAll('#step-s2 details'))
    expect(details.map((d) => d.querySelector('summary')?.textContent)).toEqual(['Input', 'Raw'])
    expect(details[0].querySelector('pre')?.textContent).toContain('"since":"2026-10-11T09:00:00Z"')
    expect(details[1].querySelector('pre')?.textContent).toContain('"rows": 1')
    expect(screen.getByText('output truncated')).toBeInTheDocument()
  })

  it('cuts a long summary at six lines until Show all is pressed', () => {
    const long = Array.from({ length: 10 }, (_, i) => `line ${i + 1}`).join('\n')
    const { container } = renderTimeline({
      steps: [
        {
          id: 'l',
          title: 'Long',
          messages: [],
          tool_calls: [{ tool_use_id: 'l1', tool: 'kafka_topics', input: null, summary: long }],
        },
      ],
    })
    const pre = () => container.querySelector('.toolcall__summary') as HTMLElement
    expect(pre().textContent).toBe(Array.from({ length: 6 }, (_, i) => `line ${i + 1}`).join('\n'))
    fireEvent.click(screen.getByRole('button', { name: 'Show all' }))
    expect(pre().textContent).toBe(long)
    expect(screen.getByRole('button', { name: 'Show less' })).toBeInTheDocument()
  })

  it('has no Show all for a short summary', () => {
    renderTimeline()
    expect(screen.queryByRole('button', { name: 'Show all' })).not.toBeInTheDocument()
  })
})

describe('Timeline — empty and read-only', () => {
  it('keeps the old empty sentence and adds a hint for a member', () => {
    render(<Timeline steps={[]} messages={[]} comments={[]} onComment={vi.fn()} />)
    expect(screen.getByText('The agent has not started yet.')).toBeInTheDocument()
    expect(screen.getByText('Take control and send a steer to start')).toBeInTheDocument()
  })

  it('hints "Waiting for the driver" to a viewer', () => {
    render(<Timeline steps={[]} messages={[]} comments={[]} onComment={vi.fn()} readOnly />)
    expect(screen.getByText('Waiting for the driver')).toBeInTheDocument()
  })

  it('shows only the thinking step when the agent works before any step exists', () => {
    render(<Timeline steps={[]} messages={[]} comments={[]} onComment={vi.fn()} agentState="working" />)
    expect(screen.getByTestId('step-ghost')).toBeInTheDocument()
    expect(screen.queryByText('The agent has not started yet.')).not.toBeInTheDocument()
  })

  it('a viewer gets no comment box and no Reply', () => {
    renderTimeline({
      readOnly: true,
      comments: [
        {
          id: 'c1',
          step_id: 's1',
          text: 'why?',
          mentions_agent: false,
          author: { kind: 'human', id: 'kabir', name: 'Kabir' },
          at: '2026-10-11T10:00:00Z',
          replies: [],
        },
      ],
    })
    expect(screen.getByText('why?')).toBeInTheDocument()
    expect(screen.queryByLabelText('Comment on this step')).not.toBeInTheDocument()
    expect(screen.queryByRole('button', { name: 'Reply' })).not.toBeInTheDocument()
  })
})

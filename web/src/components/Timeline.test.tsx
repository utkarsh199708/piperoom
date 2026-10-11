import { describe, expect, it, vi } from 'vitest'
import { render, screen } from '@testing-library/react'
import { Timeline } from './Timeline'
import type { AgentMessageState } from '../state/types'

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

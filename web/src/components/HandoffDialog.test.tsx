import { describe, expect, it, vi } from 'vitest'
import { render, within } from '@testing-library/react'
import { HandoffDialog } from './HandoffDialog'
import type { RoomActions } from '../hooks/useRoom'
import type { CurrentUser } from '../state/permissions'
import { emptyRoomState } from '../state/types'
import type { HandoffState } from '../state/types'

const actions: RoomActions = {
  comment: vi.fn(),
  steer: vi.fn(),
  sendSuggestion: vi.fn(),
  requestControl: vi.fn(),
  grantControl: vi.fn(),
  close: vi.fn(),
  approve: vi.fn(),
  reject: vi.fn(),
  handoff: vi.fn(),
  acceptHandoff: vi.fn(),
  trace: vi.fn(),
}

const user: CurrentUser = { id: 'arjun', name: 'Arjun', layers: ['kafka-connect'] }

// C13 prompt audit P5: agent-written text (here a hand-off summary) must render
// as plain text — never as HTML and never as a Markdown link.
const HTML_PAYLOAD = '<a href="http://evil.example">click</a>'
const MD_PAYLOAD = '[x](http://evil.example)'

const pendingHandoff: HandoffState = {
  handoff_id: 'h1',
  to: { kind: 'human', id: 'priya', name: 'Priya' },
  summary: {
    what_broke: `Consumer lag spiked. ${HTML_PAYLOAD}`,
    why: `Compatibility was downgraded. ${MD_PAYLOAD}`,
    done: [`Checked consumer lag. ${HTML_PAYLOAD}`, `Read the runbook. ${MD_PAYLOAD}`],
    ruled_out: [`Broker disk pressure. ${HTML_PAYLOAD}`],
    open: [`Decision on schema level. ${MD_PAYLOAD}`],
    risks: [`Data loss if replayed. ${HTML_PAYLOAD}`],
  },
  stay_as_watcher: false,
  status: 'pending',
}

describe('HandoffDialog plain-text rendering (P5)', () => {
  it('renders html- and markdown-looking summary fields literally, with no links', () => {
    const state = { ...emptyRoomState('r1'), handoffs: [pendingHandoff] }
    const { container } = render(
      <HandoffDialog state={state} user={user} actions={actions} onClose={() => {}} />,
    )

    // Both payloads appear verbatim in the rendered text content.
    const text = container.textContent ?? ''
    expect(text).toContain(HTML_PAYLOAD)
    expect(text).toContain(MD_PAYLOAD)

    // Neither payload produced an anchor element or a link role.
    const summary = container.querySelector<HTMLElement>('.handoff__summary')
    expect(summary).not.toBeNull()
    expect(within(summary as HTMLElement).queryAllByRole('link')).toHaveLength(0)
    expect(summary?.querySelector('a')).toBeNull()
  })
})

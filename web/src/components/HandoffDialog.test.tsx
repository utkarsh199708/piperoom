import { describe, expect, it, vi } from 'vitest'
import { useState } from 'react'
import { fireEvent, render, screen, waitFor, within } from '@testing-library/react'
import { HandoffAcceptBanner, HandoffDialog } from './HandoffDialog'
import type { RoomActions } from '../hooks/useRoom'
import type { CurrentUser } from '../state/permissions'
import { emptyRoomState } from '../state/types'
import type { HandoffState } from '../state/types'

const actions: RoomActions = {
  comment: vi.fn(),
  steer: vi.fn(),
  sendSuggestion: vi.fn(),
  declineSuggestion: vi.fn(),
  requestControl: vi.fn(),
  grantControl: vi.fn(),
  close: vi.fn(),
  approve: vi.fn(),
  reject: vi.fn(),
  handoff: vi.fn(),
  acceptHandoff: vi.fn(),
  label: vi.fn(),
  checklist: vi.fn(),
  trace: vi.fn(),
  hypothesisVerdict: vi.fn(),
  nudge: vi.fn().mockResolvedValue({ ok: true }),
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
    render(<HandoffDialog state={state} user={user} actions={actions} onClose={() => {}} />)
    // The dialog is a Radix portal: it renders in document.body, not in the render container.
    const container = document.body

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

// U3: a real dialog. Esc closes it, focus stays inside while it is open and goes back to
// the button that opened it.
function Harness({ withHandoff = false }: { withHandoff?: boolean }) {
  const [open, setOpen] = useState(false)
  const state = { ...emptyRoomState('r1'), handoffs: withHandoff ? [pendingHandoff] : [] }
  return (
    <>
      <button type="button" onClick={() => setOpen(true)}>
        open dialog
      </button>
      {open && <HandoffDialog state={state} user={user} actions={actions} onClose={() => setOpen(false)} />}
    </>
  )
}

describe('HandoffDialog as a dialog', () => {
  it('is a named modal dialog', () => {
    render(<HandoffDialog state={emptyRoomState('r1')} user={user} actions={actions} onClose={() => {}} />)
    const dialog = screen.getByRole('dialog', { name: 'Hand off the room' })
    expect(dialog).toBeInTheDocument()
    expect(within(dialog).getByRole('button', { name: 'Close hand-off dialog' })).toBeInTheDocument()
  })

  it('Esc closes it and focus returns to the opener', async () => {
    render(<Harness />)
    const opener = screen.getByRole('button', { name: 'open dialog' })
    opener.focus()
    fireEvent.click(opener)
    const dialog = await screen.findByRole('dialog')
    // Focus moved inside.
    await waitFor(() => expect(dialog.contains(document.activeElement)).toBe(true))

    fireEvent.keyDown(document.activeElement ?? document.body, { key: 'Escape' })
    await waitFor(() => expect(screen.queryByRole('dialog')).not.toBeInTheDocument())
    await waitFor(() => expect(opener).toHaveFocus())
  })

  it('the pending summary closes with its Close button', async () => {
    render(<Harness withHandoff />)
    fireEvent.click(screen.getByRole('button', { name: 'open dialog' }))
    const dialog = await screen.findByRole('dialog')
    fireEvent.click(within(dialog).getByRole('button', { name: 'Close' }))
    await waitFor(() => expect(screen.queryByRole('dialog')).not.toBeInTheDocument())
  })
})

describe('hand-off in flight (R0 handoff.requested)', () => {
  const requested = {
    request_id: 'hr_1',
    to: { kind: 'human' as const, id: 'priya', name: 'Priya' },
    stay_as_watcher: false,
    expires_at: new Date(Date.now() + 3 * 60_000).toISOString(),
  }

  it('shows progress instead of the form, so a second click cannot start another', () => {
    const state = { ...emptyRoomState('r1'), handoff_request: requested }
    render(<HandoffDialog state={state} user={user} actions={actions} onClose={() => {}} />)
    expect(screen.getByRole('status')).toHaveTextContent('Writing the hand-off summary')
    expect(screen.getByText('The agent is writing the summary for Priya.')).toBeInTheDocument()
    expect(screen.queryByRole('button', { name: /Start hand-off/ })).not.toBeInTheDocument()
  })

  it('a request past expires_at shows the form again', () => {
    const state = {
      ...emptyRoomState('r1'),
      handoff_request: { ...requested, expires_at: new Date(Date.now() - 1000).toISOString() },
    }
    render(<HandoffDialog state={state} user={user} actions={actions} onClose={() => {}} />)
    expect(screen.getByRole('button', { name: /Start hand-off/ })).toBeInTheDocument()
  })

  it('after handoff.failed the form shows why, in words', () => {
    const state = { ...emptyRoomState('r1'), handoff_failure: { request_id: 'hr_1', reason: 'summary_failed' } }
    render(<HandoffDialog state={state} user={user} actions={actions} onClose={() => {}} />)
    expect(screen.getByRole('alert')).toHaveTextContent('The agent could not write the summary')
  })
})

describe('the receiver reads the summary before accepting', () => {
  const me: CurrentUser = { id: 'priya', name: 'Priya', layers: [] }
  const state = {
    ...emptyRoomState('r1'),
    driver: { id: 'arjun', name: 'Arjun' },
    handoffs: [{ ...pendingHandoff, stay_as_watcher: true }],
  }

  it('Read summary opens the summary with Accept and take over; Not yet closes it', () => {
    const accept = vi.fn()
    render(<HandoffAcceptBanner state={state} user={me} actions={{ ...actions, acceptHandoff: accept }} />)
    // The banner has no one-click Accept: the summary comes first.
    expect(screen.queryByRole('button', { name: 'Accept' })).not.toBeInTheDocument()
    fireEvent.click(screen.getByRole('button', { name: 'Read summary' }))
    const dialog = screen.getByRole('dialog', { name: 'Hand-off from Arjun' })
    expect(within(dialog).getByText(/Consumer lag spiked/)).toBeInTheDocument()
    expect(dialog).toHaveTextContent('Arjun stays in the room as a watcher')
    fireEvent.click(within(dialog).getByRole('button', { name: 'Not yet' }))
    expect(screen.queryByRole('dialog')).not.toBeInTheDocument()
    expect(accept).not.toHaveBeenCalled()

    fireEvent.click(screen.getByRole('button', { name: 'Read summary' }))
    fireEvent.click(screen.getByRole('button', { name: 'Accept and take over' }))
    expect(accept).toHaveBeenCalledWith('h1')
    expect(screen.queryByRole('dialog')).not.toBeInTheDocument()
  })
})

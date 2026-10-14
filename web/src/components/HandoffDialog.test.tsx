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

describe('hand-off names carry the server badge', () => {
  const badges = (root: HTMLElement) =>
    Array.from(root.querySelectorAll<HTMLElement>('[data-slot="verified-badge"]')).map((b) => b.dataset.verification)

  it('"Summary prepared for" shows the recipient’s badge; a spoofed name gets the id', () => {
    const spoof: HandoffState = { ...pendingHandoff, to: { kind: 'human', id: 'u_7', name: 'Priya ✓' } }
    render(<HandoffDialog state={{ ...emptyRoomState('r1'), handoffs: [spoof] }} user={user} actions={actions} onClose={() => {}} />)
    const desc = screen.getByText(/Summary prepared for/)
    expect(badges(desc)).toEqual(['unverified'])
    expect(desc).toHaveTextContent('(u_7)')
    expect(desc).not.toHaveTextContent('✓')
  })

  it('a verified recipient gets the verified badge', () => {
    const ok: HandoffState = { ...pendingHandoff, to: { kind: 'human', id: 'priya', name: 'Priya', verified: true } }
    render(<HandoffDialog state={{ ...emptyRoomState('r1'), handoffs: [ok] }} user={user} actions={actions} onClose={() => {}} />)
    expect(badges(screen.getByText(/Summary prepared for/))).toEqual(['verified'])
  })

  it('the accept banner shows the handing-off driver’s badge', () => {
    const toMe: HandoffState = { ...pendingHandoff, to: { kind: 'human', id: 'arjun', name: 'Arjun', verified: true } }
    const state = { ...emptyRoomState('r1'), handoffs: [toMe], driver: { id: 'u_3', name: 'Kabir verified' } }
    render(<HandoffAcceptBanner state={state} user={user} actions={actions} />)
    const banner = screen.getByRole('status')
    expect(badges(banner)).toEqual(['unverified'])
    expect(banner).toHaveTextContent('(u_3)')
    expect(banner).toHaveTextContent('is handing the room to you.')
  })
})

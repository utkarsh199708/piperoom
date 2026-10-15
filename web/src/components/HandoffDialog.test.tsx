import { describe, expect, it, vi } from 'vitest'
import { useState } from 'react'
import { act, fireEvent, render, screen, waitFor, within } from '@testing-library/react'
import { HANDOFF_READ_ID, HandoffAcceptBanner, HandoffDialog } from './HandoffDialog'
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
  from: { kind: 'human' as const, id: 'arjun', name: 'Arjun' }, created_at: '2026-10-14T10:05:00Z',
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

  it('the accept banner shows the handing-off driver’s badge (the hand-off event actor)', () => {
    const toMe: HandoffState = {
      ...pendingHandoff,
      to: { kind: 'human', id: 'arjun', name: 'Arjun', verified: true },
      from: { kind: 'human', id: 'u_3', name: 'Kabir verified' },
    }
    const state = { ...emptyRoomState('r1'), handoffs: [toMe], driver: { id: 'u_3', name: 'Kabir verified' } }
    render(<HandoffAcceptBanner state={state} user={user} actions={actions} />)
    const banner = screen.getByRole('status')
    expect(badges(banner)).toEqual(['unverified'])
    expect(banner).toHaveTextContent('(u_3)')
    expect(banner).toHaveTextContent('is handing the room to you — read the summary below.')
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
    expect(screen.getByText(/The agent is writing the summary for/)).toHaveTextContent('Priya')
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

describe('the receiver reads the summary before accepting (F2a: inline card)', () => {
  const me: CurrentUser = { id: 'priya', name: 'Priya', layers: [] }
  const state = {
    ...emptyRoomState('r1'),
    driver: { id: 'arjun', name: 'Arjun' },
    handoffs: [{ ...pendingHandoff, stay_as_watcher: true }],
  }

  it('shows the summary expanded under the banner with Accept in the card footer; Not yet collapses it', () => {
    const accept = vi.fn()
    render(<HandoffAcceptBanner state={state} user={me} actions={{ ...actions, acceptHandoff: accept }} />)
    // No dialog and no one-click Accept in the banner: the summary is on the page.
    expect(screen.queryByRole('dialog')).not.toBeInTheDocument()
    const card = screen.getByRole('region', { name: /Shift 1/ })
    expect(card).toHaveAttribute('data-handoff-status', 'pending')
    expect(within(card).getByText(/Consumer lag spiked/)).toBeVisible()
    expect(card).toHaveTextContent(/Arjun.*stays in the room as a watcher/)
    expect(within(card).getByText(/waiting for Priya/)).toBeInTheDocument()
    const toggle = within(card).getByRole('button', { name: 'Shift 1 summary' })
    expect(toggle).toHaveAttribute('aria-expanded', 'true')

    fireEvent.click(within(card).getByRole('button', { name: 'Not yet' }))
    expect(toggle).toHaveAttribute('aria-expanded', 'false')
    expect(within(card).queryByText(/Consumer lag spiked/)).not.toBeVisible()
    expect(accept).not.toHaveBeenCalled()

    fireEvent.click(toggle)
    fireEvent.click(within(card).getByRole('button', { name: /Accept and take over/ }))
    expect(accept).toHaveBeenCalledWith('h1')
  })

  it('⌘⏎ / Ctrl+⏎ inside the card accepts', () => {
    const accept = vi.fn()
    render(<HandoffAcceptBanner state={state} user={me} actions={{ ...actions, acceptHandoff: accept }} />)
    const card = screen.getByRole('region', { name: /Shift 1/ })
    fireEvent.keyDown(within(card).getByRole('button', { name: /Accept and take over/ }), { key: 'Enter', ctrlKey: true })
    expect(accept).toHaveBeenCalledWith('h1')
  })

  it('a double click and a held ⌘⏎ send exactly one accept; Accept stays disabled meanwhile', async () => {
    const accept = vi.fn().mockReturnValue(new Promise<boolean>(() => undefined))
    render(<HandoffAcceptBanner state={state} user={me} actions={{ ...actions, acceptHandoff: accept }} />)
    const btn = screen.getByRole('button', { name: /Accept and take over/ })
    fireEvent.click(btn)
    fireEvent.click(btn)
    fireEvent.keyDown(btn, { key: 'Enter', ctrlKey: true })
    fireEvent.keyDown(btn, { key: 'Enter', ctrlKey: true, repeat: true })
    expect(accept).toHaveBeenCalledTimes(1)
    expect(btn).toBeDisabled()
    expect(btn).toHaveAttribute('aria-busy', 'true')
  })

  it('a held ⌘⏎ (key repeat) never accepts on its own', () => {
    const accept = vi.fn()
    render(<HandoffAcceptBanner state={state} user={me} actions={{ ...actions, acceptHandoff: accept }} />)
    fireEvent.keyDown(screen.getByRole('button', { name: /Accept and take over/ }), { key: 'Enter', metaKey: true, repeat: true })
    expect(accept).not.toHaveBeenCalled()
  })

  it('a failed accept enables Accept again', async () => {
    let fail: (ok: boolean) => void = () => undefined
    const accept = vi.fn().mockReturnValue(new Promise<boolean>((r) => (fail = r)))
    render(<HandoffAcceptBanner state={state} user={me} actions={{ ...actions, acceptHandoff: accept }} />)
    const btn = screen.getByRole('button', { name: /Accept and take over/ })
    fireEvent.click(btn)
    expect(btn).toBeDisabled()
    await act(async () => fail(false))
    expect(btn).not.toBeDisabled()
    fireEvent.click(btn)
    expect(accept).toHaveBeenCalledTimes(2)
  })

  it('Not yet moves focus to the card toggle instead of dropping it to <body>', () => {
    render(<HandoffAcceptBanner state={state} user={me} actions={actions} />)
    const card = screen.getByRole('region', { name: /Shift 1/ })
    const notYet = within(card).getByRole('button', { name: 'Not yet' })
    act(() => notYet.focus())
    fireEvent.click(notYet)
    expect(document.activeElement).toBe(within(card).getByRole('button', { name: 'Shift 1 summary' }))
  })

  it('"Read summary" focus on the card opens it again', () => {
    render(<HandoffAcceptBanner state={state} user={me} actions={actions} />)
    const card = screen.getByRole('region', { name: /Shift 1/ })
    fireEvent.click(within(card).getByRole('button', { name: 'Not yet' }))
    expect(card.id).toBe(HANDOFF_READ_ID)
    act(() => card.focus())
    expect(within(card).getByRole('button', { name: 'Shift 1 summary' })).toHaveAttribute('aria-expanded', 'true')
  })

  it('anyone else sees the pending card collapsed with "waiting for", and no Accept', () => {
    const accept = vi.fn()
    render(<HandoffAcceptBanner state={state} user={user} actions={{ ...actions, acceptHandoff: accept }} />)
    expect(screen.queryByText(/is handing the room to you/)).not.toBeInTheDocument()
    const card = screen.getByRole('region', { name: /Shift 1/ })
    expect(within(card).getByRole('button', { name: 'Shift 1 summary' })).toHaveAttribute('aria-expanded', 'false')
    expect(within(card).getByText(/waiting for Priya/)).toBeInTheDocument()
    expect(within(card).queryByRole('button', { name: /Accept/ })).toBeNull()
    fireEvent.keyDown(card, { key: 'Enter', metaKey: true })
    expect(accept).not.toHaveBeenCalled()
  })

  it('a viewer named as recipient gets no Accept (permissions rule)', () => {
    const viewer: CurrentUser = { id: 'priya', name: 'Priya', layers: [], role: 'viewer' }
    render(<HandoffAcceptBanner state={state} user={viewer} actions={actions} />)
    expect(screen.queryByRole('button', { name: /Accept/ })).toBeNull()
  })

  it('renders nothing without a pending hand-off', () => {
    const { container } = render(
      <HandoffAcceptBanner state={{ ...state, handoffs: [{ ...pendingHandoff, status: 'accepted' }] }} user={me} actions={actions} />,
    )
    expect(container).toBeEmptyDOMElement()
  })
})

// The server voids a pending hand-off when the driver creates a newer one, so the driver can
// redo it (another person) instead of being stuck on the old summary.
describe('redoing a pending hand-off', () => {
  it('Hand off to someone else opens the new hand-off form', () => {
    const handoff = vi.fn()
    const state = { ...emptyRoomState('r1'), status: 'open' as const, handoffs: [pendingHandoff] }
    render(<HandoffDialog state={state} user={user} actions={{ ...actions, handoff }} onClose={() => {}} />)
    fireEvent.click(screen.getByRole('button', { name: 'Hand off to someone else' }))
    expect(screen.getByRole('button', { name: /Start hand-off/ })).toBeInTheDocument()
  })

  it('a closed room offers no redo', () => {
    const state = { ...emptyRoomState('r1'), status: 'closed' as const, handoffs: [pendingHandoff] }
    render(<HandoffDialog state={state} user={user} actions={actions} onClose={() => {}} />)
    expect(screen.queryByRole('button', { name: 'Hand off to someone else' })).toBeNull()
  })
})

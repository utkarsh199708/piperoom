import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { fireEvent, render, screen, within } from '@testing-library/react'
import type { HandoffState } from '@/state/types'
import { HandoffCard } from './HandoffCard'
import { PinnedHandoffs } from './PinnedHandoffs'

const arjun = { kind: 'human' as const, id: 'arjun', name: 'Arjun', verified: true }
const kabir = { kind: 'human' as const, id: 'kabir', name: 'Kabir', verified: true }

// C13 prompt audit P5: agent-written text renders as text, never as HTML or a Markdown link.
const HTML_PAYLOAD = '<a href="http://evil.example">click</a><img src=x onerror=alert(1)>'
const MD_PAYLOAD = '[x](http://evil.example)'

function handoff(over: Partial<HandoffState> = {}): HandoffState {
  return {
    handoff_id: 'h1',
    from: arjun,
    to: kabir,
    created_at: '2026-10-14T10:05:00Z',
    summary: {
      what_broke: `Orders stopped arriving. ${HTML_PAYLOAD}`,
      why: `Compatibility set to NONE. ${MD_PAYLOAD}`,
      done: ['Checked connector status'],
      ruled_out: ['Broker disk'],
      open: ['Re-register the schema'],
      risks: ['Replay may duplicate rows'],
    },
    stay_as_watcher: false,
    status: 'accepted',
    accepted_at: '2026-10-14T10:09:00Z',
    accepted_by: kabir,
    ...over,
  }
}

function renderCard(h: HandoffState, defaultOpen = true) {
  return render(
    <HandoffCard handoff={h} index={1} from={h.from} createdAt={h.created_at} acceptedAt={h.accepted_at} defaultOpen={defaultOpen} />,
  )
}

describe('HandoffCard', () => {
  it('reads "Shift N · from X to Y · time" with the six sections', () => {
    renderCard(handoff())
    const card = screen.getByRole('region', { name: /Shift 2/ })
    expect(card).toHaveTextContent(/Shift 2.*from\s*Arjun.*to\s*Kabir/)
    expect(card.querySelector('time')).toHaveAttribute('dateTime', '2026-10-14T10:05:00Z')
    for (const h of ['What broke', 'Why', 'Done', 'Ruled out', 'Still open', 'Risks']) {
      expect(within(card).getByRole('heading', { name: h })).toBeInTheDocument()
    }
  })

  it.each([
    ['pending', /waiting for Kabir/],
    ['accepted', /^accepted /],
    ['voided', /superseded/],
  ] as const)('a %s hand-off shows its badge', (status, text) => {
    renderCard(handoff({ status }))
    expect(screen.getByText(text)).toBeInTheDocument()
  })

  it('renders agent text literally: no links, no images, no HTML', () => {
    renderCard(handoff())
    const card = screen.getByRole('region', { name: /Shift 2/ })
    expect(card.textContent).toContain(HTML_PAYLOAD)
    expect(card.textContent).toContain(MD_PAYLOAD)
    expect(card.querySelector('a, img')).toBeNull()
  })

  it('a spoofed name in "waiting for" loses its fake check mark', () => {
    renderCard(handoff({ status: 'pending', to: { kind: 'human', id: 'u_9', name: 'Kabir ✓' } }))
    expect(screen.getByText(/waiting for/).textContent).not.toContain('✓')
  })

  it('collapses and expands with a labelled button that controls the body', () => {
    renderCard(handoff(), false)
    const toggle = screen.getByRole('button', { name: 'Shift 2 summary' })
    expect(toggle).toHaveAttribute('aria-expanded', 'false')
    const body = document.getElementById(toggle.getAttribute('aria-controls') ?? '')
    expect(body).not.toBeNull()
    expect(body).not.toBeVisible()
    fireEvent.click(toggle)
    expect(toggle).toHaveAttribute('aria-expanded', 'true')
    expect(body).toBeVisible()
  })

  it('an empty summary shows no section headings (and does not crash)', () => {
    renderCard(handoff({ summary: { what_broke: '', why: '', done: [], ruled_out: [], open: [], risks: [] } }))
    expect(screen.queryByRole('heading', { name: 'What broke' })).toBeNull()
    expect(screen.getByRole('region', { name: /Shift 2/ })).toBeInTheDocument()
  })
})

describe('PinnedHandoffs', () => {
  beforeEach(() => localStorage.clear())
  afterEach(() => vi.restoreAllMocks())

  const first = handoff({ handoff_id: 'h1', created_at: '2026-10-14T08:00:00Z' })
  const voided = handoff({ handoff_id: 'h2', status: 'voided', accepted_at: undefined })
  const second = handoff({ handoff_id: 'h3', from: kabir, to: arjun, created_at: '2026-10-14T12:00:00Z' })
  const pending = handoff({ handoff_id: 'h4', status: 'pending', accepted_at: undefined })

  const toggles = () => screen.getAllByRole('button', { name: /^Shift \d summary$/ })

  it('renders nothing when no hand-off was accepted', () => {
    const { container } = render(<PinnedHandoffs roomId="r1" handoffs={[voided, pending]} />)
    expect(container).toBeEmptyDOMElement()
  })

  it('pins accepted hand-offs only, newest first; the newest starts open, older ones collapsed', () => {
    render(<PinnedHandoffs roomId="r1" handoffs={[first, voided, second, pending]} />)
    const t = toggles()
    // Superseded h2 is not a shift: h1 is Shift 1, h3 is Shift 2.
    expect(t.map((b) => b.getAttribute('aria-label'))).toEqual(['Shift 2 summary', 'Shift 1 summary'])
    expect(t.map((b) => b.getAttribute('aria-expanded'))).toEqual(['true', 'false'])
  })

  it('remembers per room that the viewer collapsed the newest, until a newer shift arrives', () => {
    const { unmount } = render(<PinnedHandoffs roomId="r1" handoffs={[first, second]} />)
    fireEvent.click(toggles()[0])
    expect(localStorage.getItem('piperoom.handoff.collapsed.r1')).toBe('h3')
    unmount()

    const again = render(<PinnedHandoffs roomId="r1" handoffs={[first, second]} />)
    expect(toggles()[0]).toHaveAttribute('aria-expanded', 'false')
    again.unmount()

    const third = handoff({ handoff_id: 'h5', created_at: '2026-10-14T16:00:00Z' })
    render(<PinnedHandoffs roomId="r1" handoffs={[first, second, third]} />)
    expect(toggles()[0]).toHaveAttribute('aria-expanded', 'true')
  })

  it('still works when storage throws', () => {
    vi.spyOn(Storage.prototype, 'getItem').mockImplementation(() => {
      throw new Error('blocked')
    })
    vi.spyOn(Storage.prototype, 'setItem').mockImplementation(() => {
      throw new Error('blocked')
    })
    render(<PinnedHandoffs roomId="r1" handoffs={[first]} />)
    const [t] = toggles()
    expect(t).toHaveAttribute('aria-expanded', 'true')
    fireEvent.click(t)
    expect(t).toHaveAttribute('aria-expanded', 'false')
  })
})

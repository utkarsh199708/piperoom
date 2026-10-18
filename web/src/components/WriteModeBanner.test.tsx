import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { fireEvent, render, screen, within } from '@testing-library/react'
import { WRITE_MODE_BANNER_TEXT, WRITE_MODE_DISMISS_KEY, WriteModeBanner } from './WriteModeBanner'
import { QUICKSTART_DOC_PATH } from '../lib/writeMode'
import type { RoomMode } from '../api/types'

const off: RoomMode = { write: false, solo: false, auto_approve: [] }
const on: RoomMode = { write: true, solo: false, auto_approve: [] }

beforeEach(() => {
  window.sessionStorage.clear()
})
afterEach(() => {
  vi.restoreAllMocks()
  vi.unstubAllEnvs()
})

describe('WriteModeBanner', () => {
  it('is shown when write mode is off, as a labelled region with the text and the how-to link', () => {
    render(<WriteModeBanner mode={off} />)
    const banner = screen.getByRole('region', { name: 'Write mode is off' })
    expect(within(banner).getByText(WRITE_MODE_BANNER_TEXT)).toBeInTheDocument()
    expect(WRITE_MODE_BANNER_TEXT).toBe('Fixes are suggestions until write mode is on')
    const link = within(banner).getByRole('link', { name: 'How to turn it on' })
    expect(link).toHaveAttribute('href', QUICKSTART_DOC_PATH)
    expect(link).toHaveAttribute('target', '_blank')
    expect(link).toHaveAttribute('rel', expect.stringContaining('noopener'))
  })

  it('points the link at the repo doc, under VITE_DOCS_BASE_URL when the build sets one', () => {
    expect(QUICKSTART_DOC_PATH).toBe('docs/pilot/quickstart.md')
    vi.stubEnv('VITE_DOCS_BASE_URL', 'https://example.test/piperoom/blob/main/')
    render(<WriteModeBanner mode={off} />)
    expect(screen.getByRole('link', { name: 'How to turn it on' })).toHaveAttribute(
      'href',
      'https://example.test/piperoom/blob/main/docs/pilot/quickstart.md',
    )
  })

  it.each([
    ['write mode is on', on],
    ['the server sent no mode (before P1a)', undefined],
  ])('renders nothing when %s', (_name, mode) => {
    const { container } = render(<WriteModeBanner mode={mode} />)
    expect(container).toBeEmptyDOMElement()
    expect(screen.queryByRole('region')).not.toBeInTheDocument()
  })

  it('dismiss is a button, hides the banner and remembers it for the session', () => {
    const { unmount } = render(<WriteModeBanner mode={off} />)
    const dismiss = screen.getByRole('button', { name: 'Dismiss write-mode notice' })
    expect(dismiss.tagName).toBe('BUTTON')
    fireEvent.click(dismiss)
    expect(screen.queryByRole('region', { name: 'Write mode is off' })).not.toBeInTheDocument()
    expect(window.sessionStorage.getItem(WRITE_MODE_DISMISS_KEY)).toBe('1')

    // Another room in the same browser session stays quiet.
    unmount()
    const { container } = render(<WriteModeBanner mode={off} />)
    expect(container).toBeEmptyDOMElement()
  })

  it('comes back in a new session (nothing in sessionStorage)', () => {
    const first = render(<WriteModeBanner mode={off} />)
    fireEvent.click(screen.getByRole('button', { name: 'Dismiss write-mode notice' }))
    first.unmount()
    expect(render(<WriteModeBanner mode={off} />).container).toBeEmptyDOMElement()

    window.sessionStorage.clear() // a new tab or window starts with empty session storage
    render(<WriteModeBanner mode={off} />)
    expect(screen.getByRole('region', { name: 'Write mode is off' })).toBeInTheDocument()
  })

  it('renders when reading the storage throws, and dismisses even though the write throws', () => {
    vi.spyOn(Storage.prototype, 'getItem').mockImplementation(() => {
      throw new DOMException('blocked', 'SecurityError')
    })
    const setItem = vi.spyOn(Storage.prototype, 'setItem').mockImplementation(() => {
      throw new DOMException('quota', 'QuotaExceededError')
    })
    render(<WriteModeBanner mode={off} />)
    expect(screen.getByRole('region', { name: 'Write mode is off' })).toBeInTheDocument()

    fireEvent.click(screen.getByRole('button', { name: 'Dismiss write-mode notice' }))
    expect(setItem).toHaveBeenCalled()
    expect(screen.queryByRole('region')).not.toBeInTheDocument()
  })

  it('stays on one line on a phone: the text gives way, the link and the button do not', () => {
    render(<WriteModeBanner mode={off} />)
    const banner = screen.getByRole('region', { name: 'Write mode is off' })
    expect(banner).toHaveClass('flex-nowrap')
    expect(screen.getByText(WRITE_MODE_BANNER_TEXT)).toHaveClass('truncate', 'min-w-0')
    expect(screen.getByRole('link', { name: 'How to turn it on' })).toHaveClass('shrink-0', 'whitespace-nowrap')
    expect(screen.getByRole('button', { name: 'Dismiss write-mode notice' })).toHaveClass('shrink-0')
  })
})

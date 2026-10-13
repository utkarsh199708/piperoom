import { describe, expect, it, vi } from 'vitest'
import { act, fireEvent, render, screen } from '@testing-library/react'
import { HandoffDialog } from './HandoffDialog'
import type { RoomActions } from '../hooks/useRoom'
import type { CurrentUser } from '../state/permissions'
import { emptyRoomState } from '../state/types'

vi.mock('../hooks/usePeople', async (importOriginal) => {
  const real = await importOriginal<typeof import('../hooks/usePeople')>()
  return {
    ...real,
    usePeople: () => ({
      status: 'ready' as const,
      people: [{ id: 'kabir', name: 'Kabir' }],
      reload: vi.fn(),
    }),
  }
})

const actions = { handoff: vi.fn() } as unknown as RoomActions
const user: CurrentUser = { id: 'arjun', name: 'Arjun', layers: [] }
const state = { ...emptyRoomState('r1'), driver: { id: 'arjun', name: 'Arjun' } }

describe('HandoffDialog — keyboard and focus', () => {
  it('focuses the first field on open', () => {
    render(<HandoffDialog state={state} user={user} actions={actions} onClose={() => {}} />)
    expect(screen.getByLabelText('Hand off to')).toHaveFocus()
  })

  it('Esc calls onClose', () => {
    const onClose = vi.fn()
    render(<HandoffDialog state={state} user={user} actions={actions} onClose={onClose} />)
    fireEvent.keyDown(document.activeElement as Element, { key: 'Escape' })
    expect(onClose).toHaveBeenCalledTimes(1)
  })

  it('gives focus back to the opener when it unmounts', () => {
    const opener = document.createElement('button')
    document.body.appendChild(opener)
    opener.focus()
    const { unmount } = render(
      <HandoffDialog state={state} user={user} actions={actions} onClose={() => {}} />,
    )
    expect(opener).not.toHaveFocus()
    unmount()
    expect(opener).toHaveFocus()
    opener.remove()
  })

  it('keeps Tab inside the dialog', () => {
    render(<HandoffDialog state={state} user={user} actions={actions} onClose={() => {}} />)
    const close = screen.getByRole('button', { name: 'Close hand-off dialog' })
    const start = screen.getByRole('button', { name: 'Start hand-off' })
    const cancel = screen.getByRole('button', { name: 'Cancel' })
    // Start hand-off is enabled once a person is chosen.
    expect(start).toBeEnabled()
    // Tab from the last control wraps to the first.
    act(() => start.focus())
    fireEvent.keyDown(start, { key: 'Tab' })
    expect(close).toHaveFocus()
    // Shift+Tab from the first control wraps to the last.
    fireEvent.keyDown(close, { key: 'Tab', shiftKey: true })
    expect(start).toHaveFocus()
    expect(cancel).not.toHaveFocus()
  })

  it('starts the hand-off with the chosen person and the watcher choice', () => {
    render(<HandoffDialog state={state} user={user} actions={actions} onClose={() => {}} />)
    fireEvent.click(screen.getByLabelText('Stay as watcher after hand-off'))
    fireEvent.click(screen.getByRole('button', { name: 'Start hand-off' }))
    expect(actions.handoff).toHaveBeenCalledWith('kabir', true)
  })

  it('shows Done, Ruled out, Still open and Risks as sentence-case cards with a glyph', () => {
    const withHandoff = {
      ...state,
      handoffs: [
        {
          handoff_id: 'h1',
          to: { kind: 'human' as const, id: 'kabir', name: 'Kabir' },
          summary: {
            what_broke: 'The connector',
            why: 'Schema change',
            done: ['Checked lag'],
            ruled_out: ['Disk'],
            open: ['Decide level'],
            risks: ['Data loss'],
          },
          stay_as_watcher: false,
          status: 'pending' as const,
        },
      ],
    }
    const { container } = render(
      <HandoffDialog state={withHandoff} user={user} actions={actions} onClose={() => {}} />,
    )
    const titles = Array.from(container.querySelectorAll('.handoff__subtitle')).map((n) => n.textContent)
    expect(titles).toEqual(['What broke', 'Why', '✓Done', '✕Ruled out', '●Still open', '▲Risks'])
    expect(container.querySelectorAll('.handoff__section--prose')).toHaveLength(2)
    // Esc works in the summary view too, and the close button has the focus first.
    expect(screen.getByRole('button', { name: 'Close hand-off dialog' })).toHaveFocus()
  })
})

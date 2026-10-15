import { beforeAll, beforeEach, describe, expect, it, vi } from 'vitest'
import { act, fireEvent, render, screen, waitFor } from '@testing-library/react'
import { useState } from 'react'
import { MemoryRouter } from 'react-router-dom'
import { api } from '@/api/client'
import type { RoomActions } from '@/hooks/useRoom'
import { CloseRoomDialog } from '../CloseRoomDialog'
import { TracePanel } from '../TracePanel'
import { PostmortemDialog } from '../room/PostmortemDialog'
import type { RoomCommands } from './context'

vi.mock('@/context/user', () => ({
  useCurrentUser: () => ({ user: { id: 'arjun', name: 'Arjun', layers: [] }, logout: vi.fn(), busy: false }),
}))
vi.mock('@/context/theme', () => ({ useTheme: () => ({ theme: 'light', toggleTheme: vi.fn() }) }))

import { CommandMenu } from './CommandMenu'

beforeAll(() => {
  globalThis.ResizeObserver ??= class {
    observe() {}
    unobserve() {}
    disconnect() {}
  }
  Element.prototype.scrollIntoView ??= () => {}
})
beforeEach(() => {
  vi.spyOn(api, 'postmortem').mockResolvedValue('# draft')
})

type Which = 'postmortem' | 'close' | 'trace'
const actions = { close: vi.fn() } as unknown as RoomActions

// Like AppShell + RoomScreen: a timeline item inside <main id="main">, the palette, and the
// dialog the room registers for the palette command.
function Harness() {
  const [palette, setPalette] = useState(false)
  const [dialog, setDialog] = useState<Which | null>(null)
  const commands: RoomCommands = {
    title: 'Orders stale',
    postmortem: () => setDialog('postmortem'),
    closeRoom: () => setDialog('close'),
    trace: () => setDialog('trace'),
  }
  const registry = { get: () => commands, set: () => undefined }
  return (
    <MemoryRouter>
      <main id="main" tabIndex={-1}>
        <button type="button">timeline item</button>
        <button type="button" onClick={() => setPalette(true)}>open palette</button>
      </main>
      <CommandMenu open={palette} onOpenChange={setPalette} rooms={[]} registry={registry} />
      {dialog === 'postmortem' && (
        <PostmortemDialog
          state={{ id: 'r1', status: 'closed', closed_at: new Date(Date.now() - 60_000).toISOString(), postmortem: null }}
          onClose={() => setDialog(null)}
        />
      )}
      {dialog === 'trace' && <TracePanel trace={null} actions={actions} onClose={() => setDialog(null)} />}
      {dialog === 'close' && <CloseRoomDialog actions={actions} onClose={() => setDialog(null)} />}
    </MemoryRouter>
  )
}

async function openFromPalette(option: RegExp) {
  fireEvent.click(await screen.findByRole('option', { name: option }))
  await screen.findByRole('dialog')
}

const dismissals: [string, Which, RegExp, () => void][] = [
  ['Escape', 'postmortem', /Post-mortem draft/, () => fireEvent.keyDown(screen.getByRole('dialog'), { key: 'Escape' })],
  ['the close button', 'postmortem', /Post-mortem draft/, () => fireEvent.click(screen.getByRole('button', { name: 'Close post-mortem draft' }))],
  ['the footer Close', 'postmortem', /Post-mortem draft/, () => fireEvent.click(screen.getByRole('button', { name: /^Close$/ }))],
  ['Escape', 'trace', /Trace a record/, () => fireEvent.keyDown(screen.getByRole('dialog'), { key: 'Escape' })],
  ['the close button', 'trace', /Trace a record/, () => fireEvent.click(screen.getByRole('button', { name: 'Close trace panel' }))],
  ['Escape', 'close', /Close the room/, () => fireEvent.keyDown(screen.getByRole('dialog'), { key: 'Escape' })],
  ['the close button', 'close', /Close the room/, () => fireEvent.click(screen.getByRole('button', { name: 'Cancel closing the room' }))],
  ['the footer Cancel', 'close', /Close the room/, () => fireEvent.click(screen.getByRole('button', { name: /^Cancel$/ }))],
]

describe('⌘K: a dialog it opens returns focus to what had it before the palette', () => {
  it.each(dismissals)('%s (%s dialog)', async (_name, _which, option, dismiss) => {
    render(<Harness />)
    const item = screen.getByRole('button', { name: 'timeline item' })
    item.focus()
    await act(async () => fireEvent.click(screen.getByRole('button', { name: 'open palette' })))
    await openFromPalette(option)
    dismiss()
    await waitFor(() => expect(document.activeElement).toBe(item))
  })

  it('hands the opener over without focusing it: no focus or blur while the dialog opens', async () => {
    render(<Harness />)
    const item = screen.getByRole('button', { name: 'timeline item' })
    item.focus()
    await act(async () => fireEvent.click(screen.getByRole('button', { name: 'open palette' })))
    const seen = vi.fn()
    for (const type of ['focus', 'focusin', 'blur', 'focusout']) item.addEventListener(type, seen)
    await openFromPalette(/Post-mortem draft/)
    expect(seen).not.toHaveBeenCalled()
  })

  it('regression for #452: an opener hidden before the action falls back to #main', async () => {
    render(<Harness />)
    const item = screen.getByRole('button', { name: 'timeline item' })
    item.focus()
    await act(async () => fireEvent.click(screen.getByRole('button', { name: 'open palette' })))
    item.setAttribute('hidden', '')
    await openFromPalette(/Post-mortem draft/)
    fireEvent.keyDown(screen.getByRole('dialog'), { key: 'Escape' })
    await waitFor(() => expect(document.activeElement).toBe(document.getElementById('main')))
  })

  it.each([
    ['removed', (el: HTMLElement) => el.remove()],
    ['hidden', (el: HTMLElement) => el.setAttribute('hidden', '')],
  ])('an opener %s while the dialog is open falls back to #main, never <body>', async (_how, change) => {
    render(<Harness />)
    const item = screen.getByRole('button', { name: 'timeline item' })
    item.focus()
    await act(async () => fireEvent.click(screen.getByRole('button', { name: 'open palette' })))
    await openFromPalette(/Post-mortem draft/)
    change(item)
    fireEvent.keyDown(screen.getByRole('dialog'), { key: 'Escape' })
    await waitFor(() => expect(document.activeElement).toBe(document.getElementById('main')))
  })

  // #466: closing the palette without choosing anything must not drop focus to <body>.
  async function escapePalette() {
    await act(async () => fireEvent.click(screen.getByRole('button', { name: 'open palette' })))
    fireEvent.keyDown(await screen.findByRole('dialog'), { key: 'Escape' })
    await waitFor(() => expect(screen.queryByRole('dialog')).toBeNull())
  }

  it('Escape on the palette itself returns focus to the timeline item', async () => {
    render(<Harness />)
    const item = screen.getByRole('button', { name: 'timeline item' })
    item.focus()
    await escapePalette()
    await waitFor(() => expect(document.activeElement).toBe(item))
  })

  it('Escape on the palette falls back to #main when the opener is hidden', async () => {
    render(<Harness />)
    const item = screen.getByRole('button', { name: 'timeline item' })
    item.focus()
    await act(async () => fireEvent.click(screen.getByRole('button', { name: 'open palette' })))
    item.setAttribute('hidden', '')
    fireEvent.keyDown(await screen.findByRole('dialog'), { key: 'Escape' })
    await waitFor(() => expect(document.activeElement).toBe(document.getElementById('main')))
  })

  it('Escape after an earlier dialog action still returns focus to the opener', async () => {
    render(<Harness />)
    const item = screen.getByRole('button', { name: 'timeline item' })
    item.focus()
    await act(async () => fireEvent.click(screen.getByRole('button', { name: 'open palette' })))
    await openFromPalette(/Post-mortem draft/)
    fireEvent.keyDown(screen.getByRole('dialog'), { key: 'Escape' })
    await waitFor(() => expect(screen.queryByRole('dialog')).toBeNull())
    await waitFor(() => expect(document.activeElement).toBe(item))
    await escapePalette()
    await waitFor(() => expect(document.activeElement).toBe(item))
  })

  it('a pointer-down outside the palette returns focus to the timeline item', async () => {
    render(<Harness />)
    const item = screen.getByRole('button', { name: 'timeline item' })
    item.focus()
    await act(async () => fireEvent.click(screen.getByRole('button', { name: 'open palette' })))
    await screen.findByRole('dialog')
    fireEvent.pointerDown(document.body)
    await waitFor(() => expect(screen.queryByRole('dialog')).toBeNull())
    await waitFor(() => expect(document.activeElement).toBe(item))
  })

  // #500: an action that opens no dialog and cannot remove the opener (Toggle theme) returns focus to it.
  it('#500: choosing Toggle theme returns focus to the opener', async () => {
    render(<Harness />)
    const item = screen.getByRole('button', { name: 'timeline item' })
    item.focus()
    await act(async () => fireEvent.click(screen.getByRole('button', { name: 'open palette' })))
    fireEvent.click(await screen.findByRole('option', { name: /Toggle theme/ }))
    await waitFor(() => expect(screen.queryByRole('dialog')).toBeNull())
    await waitFor(() => expect(document.activeElement).toBe(item))
  })

  it('#500: Toggle theme falls back to #main when the opener is hidden', async () => {
    render(<Harness />)
    const item = screen.getByRole('button', { name: 'timeline item' })
    item.focus()
    await act(async () => fireEvent.click(screen.getByRole('button', { name: 'open palette' })))
    item.setAttribute('hidden', '')
    fireEvent.click(await screen.findByRole('option', { name: /Toggle theme/ }))
    await waitFor(() => expect(screen.queryByRole('dialog')).toBeNull())
    await waitFor(() => expect(document.activeElement).toBe(document.getElementById('main')))
  })

  // Navigate and sign out can replace or remove the opener, so they must not return focus to it (#500).
  it.each([
    ['Go to Rooms', /Go to Rooms/],
    ['Sign out', /Sign out/],
  ])('%s lands on #main, not the opener', async (_name, option) => {
    render(<Harness />)
    const item = screen.getByRole('button', { name: 'timeline item' })
    item.focus()
    await act(async () => fireEvent.click(screen.getByRole('button', { name: 'open palette' })))
    fireEvent.click(await screen.findByRole('option', { name: option }))
    await waitFor(() => expect(screen.queryByRole('dialog')).toBeNull())
    await waitFor(() => expect(document.activeElement).toBe(document.getElementById('main')))
  })

  // #488: the palette opened from inside a dialog, choosing that dialog's own command. Nothing new
  // mounts, so nothing places focus.
  it('#488: choosing the open dialog\'s own command keeps focus inside it', async () => {
    render(<Harness />)
    const item = screen.getByRole('button', { name: 'timeline item' })
    item.focus()
    await act(async () => fireEvent.click(screen.getByRole('button', { name: 'open palette' })))
    await openFromPalette(/Trace a record/)
    const inside = screen.getByLabelText(/^Table/)
    inside.focus()
    await act(async () => fireEvent.click(screen.getByText('open palette')))
    fireEvent.click(await screen.findByRole('option', { name: /Trace a record/ }))
    await waitFor(() => expect(document.activeElement).toBe(inside))
    expect(document.activeElement).not.toBe(document.body)
  })
})

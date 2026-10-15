import { beforeAll, beforeEach, describe, expect, it, vi } from 'vitest'
import { act, fireEvent, render, screen, waitFor } from '@testing-library/react'
import { useState } from 'react'
import { MemoryRouter } from 'react-router-dom'
import { api } from '@/api/client'
import type { RoomActions } from '@/hooks/useRoom'
import { CloseRoomDialog } from '../CloseRoomDialog'
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

type Which = 'postmortem' | 'close'
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

  it('falls back to #main when that element is gone, never <body>', async () => {
    render(<Harness />)
    const item = screen.getByRole('button', { name: 'timeline item' })
    item.focus()
    await act(async () => fireEvent.click(screen.getByRole('button', { name: 'open palette' })))
    // the palette recorded the opener on opening; now hide it
    item.setAttribute('hidden', '')
    await openFromPalette(/Post-mortem draft/)
    fireEvent.keyDown(screen.getByRole('dialog'), { key: 'Escape' })
    await waitFor(() => expect(document.activeElement).toBe(document.getElementById('main')))
  })
})

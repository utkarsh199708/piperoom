import { beforeAll, describe, expect, it, vi } from 'vitest'
import { fireEvent, render, screen } from '@testing-library/react'
import { MemoryRouter } from 'react-router-dom'
import type { RoomCommands } from './context'

vi.mock('@/context/user', () => ({
  useCurrentUser: () => ({ user: { id: 'arjun', name: 'Arjun', layers: [] }, logout: vi.fn(), busy: false }),
}))
vi.mock('@/context/theme', () => ({ useTheme: () => ({ theme: 'light', toggleTheme: vi.fn() }) }))

import { CommandMenu } from './CommandMenu'

beforeAll(() => {
  // jsdom lacks these; cmdk and Radix use them.
  globalThis.ResizeObserver ??= class {
    observe() {}
    unobserve() {}
    disconnect() {}
  }
  Element.prototype.scrollIntoView ??= () => {}
})

function renderMenu(commands: RoomCommands) {
  const registry = { get: () => commands, set: () => undefined }
  render(
    <MemoryRouter>
      <CommandMenu open onOpenChange={() => undefined} rooms={[]} registry={registry} />
    </MemoryRouter>,
  )
}

describe('⌘K: export and post-mortem (F2)', () => {
  it('offers both when the room registers them and runs them after the menu closes', () => {
    vi.useFakeTimers()
    const exportJson = vi.fn()
    const postmortem = vi.fn()
    renderMenu({ title: 'Orders stale', exportJson, postmortem })
    fireEvent.click(screen.getByRole('option', { name: /Export room as JSON/ }))
    fireEvent.click(screen.getByRole('option', { name: /Post-mortem draft/ }))
    vi.runAllTimers()
    vi.useRealTimers()
    expect(exportJson).toHaveBeenCalledTimes(1)
    expect(postmortem).toHaveBeenCalledTimes(1)
  })

  it('finds them by their keywords', () => {
    renderMenu({ title: 'Orders stale', exportJson: vi.fn(), postmortem: vi.fn() })
    fireEvent.change(screen.getByRole('combobox'), { target: { value: 'retro' } })
    expect(screen.getByRole('option', { name: /Post-mortem draft/ })).toBeInTheDocument()
    expect(screen.queryByRole('option', { name: /Export room as JSON/ })).toBeNull()
  })

  it('a viewer (no callbacks registered) sees neither', () => {
    renderMenu({ title: 'Orders stale', invite: vi.fn() })
    expect(screen.queryByRole('option', { name: /Export room as JSON/ })).toBeNull()
    expect(screen.queryByRole('option', { name: /Post-mortem draft/ })).toBeNull()
  })
})

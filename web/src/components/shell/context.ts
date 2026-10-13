import { createContext, useContext, useEffect } from 'react'

/** True below <AppShell>. Old screens draw their own user switcher; the shell owns it now, so they hide it. */
export const InShellContext = createContext(false)

export function useInShell(): boolean {
  return useContext(InShellContext)
}

/**
 * What the open room offers the command menu. A field is set only when the person
 * may do it right now (role, driver, lease), so the menu never lists a dead action.
 * Each callback is an existing RoomScreen callback; nothing here talks to the API.
 */
export interface RoomCommands {
  title: string
  takeControl?: () => void
  requestControl?: () => void
  handoff?: () => void
  trace?: () => void
  invite?: () => void
}

/** A plain holder: the room screen writes every render, the menu reads when it opens or runs. */
export interface RoomCommandRegistry {
  get: () => RoomCommands | null
  set: (commands: RoomCommands | null) => void
}

export const RoomCommandsContext = createContext<RoomCommandRegistry | null>(null)

/**
 * Called by the room screen. Outside the shell (tests, other layouts) it does nothing.
 * The effect has no dependency list on purpose: the callbacks close over fresh state
 * on every render, and `set` only writes a ref.
 */
export function useRegisterRoomCommands(commands: RoomCommands | null): void {
  const registry = useContext(RoomCommandsContext)
  useEffect(() => {
    registry?.set(commands)
    return () => registry?.set(null)
  })
}

/** The element in the top bar where a screen can put its own controls (see TopBarSlot). */
export const TopBarSlotContext = createContext<HTMLElement | null>(null)

import { createContext, useContext, useEffect } from 'react'
import type { useRoomList } from '@/hooks/useRoomList'

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
  closeRoom?: () => void
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

/**
 * The one room list the shell loads. The sidebar and the room list screen read the same
 * copy, so there is one GET on load and one refresh updates both. Null outside the shell.
 */
export const RoomListContext = createContext<ReturnType<typeof useRoomList> | null>(null)

/** Where a screen says what to show after "Rooms ›" in the breadcrumb. Null outside the shell. */
export const BreadcrumbTailContext = createContext<((label: string | null) => void) | null>(null)

/**
 * Called by the room screen with its pipeline id: the breadcrumb reads "Rooms › orders",
 * never the room id. Undefined clears it. Outside the shell it does nothing.
 */
export function useBreadcrumbTail(label: string | undefined): void {
  const set = useContext(BreadcrumbTailContext)
  useEffect(() => {
    set?.(label ?? null)
    return () => set?.(null)
  }, [set, label])
}

/**
 * What the decision bar can ask of the approval card. The `LayerRow` that owns the A/R shortcuts
 * registers these every render, so the bar, the keys and the card's own buttons all go through
 * one `startApprove` (solo typed confirm, run confirm, 409 stale, missing hash stay in one place).
 */
export interface DecisionHandle {
  approve: () => void
  reject: () => void
  scrollTo: () => void
}

/** A plain holder like RoomCommandRegistry: written on render, read on click. Null outside the room screen. */
export const DecisionHandleContext = createContext<{
  get(): DecisionHandle | null
  set(h: DecisionHandle | null): void
} | null>(null)

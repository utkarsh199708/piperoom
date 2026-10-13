import { useContext } from 'react'
import type { ReactNode } from 'react'
import { createPortal } from 'react-dom'
import { TopBarSlotContext } from './context'

/**
 * Put controls in the shell's top bar from any screen:
 *
 *   <TopBarSlot><Button>New room</Button></TopBarSlot>
 *
 * Renders nothing outside the shell. The slot sits left of the theme toggle.
 */
export function TopBarSlot({ children }: { children: ReactNode }) {
  const el = useContext(TopBarSlotContext)
  return el ? createPortal(children, el) : null
}

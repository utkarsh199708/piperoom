import { useEffect, useRef } from 'react'
import { isEditableTarget } from '../shell/keys'

/** What the room offers right now; a field is left out when this person may not do it. */
export interface RoomShortcuts {
  /** H */
  handoff?: () => void
  /** C: close the room (driver only). */
  close?: () => void
  /** Shift+T */
  trace?: () => void
  /** T: take control, or request it (the room screen passes whichever applies). */
  control?: () => void
  /** Esc: clear the lane's layer filter (offered only while one is on). */
  clearFilter?: () => void
}

// The keys the buttons show in their Kbd. They only act when the person is not typing, no
// modifier is held (⌘K and friends belong to the shell) and no dialog is open.
export function useRoomShortcuts(shortcuts: RoomShortcuts): void {
  // The handlers close over fresh state every render; the listener reads the latest.
  const latest = useRef(shortcuts)
  useEffect(() => {
    latest.current = shortcuts
  })

  useEffect(() => {
    const onKey = (e: KeyboardEvent) => {
      if (e.isComposing || e.repeat || e.metaKey || e.ctrlKey || e.altKey) return
      if (isEditableTarget(e.target)) return
      // A dialog, an alert dialog or an open menu has the keyboard: the keys are not ours then.
      if (document.querySelector('[role="dialog"], [role="alertdialog"], [role="menu"]')) return
      if (e.key === 'Escape') {
        if (!latest.current.clearFilter) return
        e.preventDefault()
        latest.current.clearFilter()
        return
      }
      const key = e.key.toLowerCase()
      const s = latest.current
      let run: (() => void) | undefined
      if (key === 'h' && !e.shiftKey) run = s.handoff
      else if (key === 'c' && !e.shiftKey) run = s.close
      else if (key === 't') run = e.shiftKey ? s.trace : s.control
      if (!run) return
      e.preventDefault()
      run()
    }
    window.addEventListener('keydown', onKey)
    return () => window.removeEventListener('keydown', onKey)
  }, [])
}

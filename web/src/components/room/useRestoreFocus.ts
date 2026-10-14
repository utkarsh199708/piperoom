import { useState } from 'react'

/**
 * Radix Dialog gives focus back only to its own <Trigger>. The room's dialogs are opened
 * by buttons, the H and Shift+T keys or the ⌘K menu, none of which is a Trigger. So the
 * dialog notes what had focus when it mounted and puts it back on close.
 *
 *   <DialogContent onCloseAutoFocus={restoreFocus}>
 */
export function useRestoreFocus(): (event: Event) => void {
  const [opener] = useState<Element | null>(() =>
    typeof document === 'undefined' ? null : document.activeElement,
  )
  return (event) => {
    if (opener instanceof HTMLElement && opener !== document.body && opener.isConnected) {
      event.preventDefault()
      opener.focus()
    }
  }
}

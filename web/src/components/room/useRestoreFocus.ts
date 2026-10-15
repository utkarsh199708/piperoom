import { useRef, useState } from 'react'

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

/**
 * A dialog over the page with a step link in it (QA R17). Following the link closes the dialog and
 * shows the step, so the step must keep the focus: the opener would scroll the page back to the top.
 *
 *   const follow = useFollowClose(onClose, restoreFocus)
 *   <StepNumbersProvider onFollow={follow.onFollow}> ... <DialogContent onCloseAutoFocus={follow.onCloseAutoFocus}>
 */
export function useFollowClose(
  onClose: () => void,
  restoreFocus: (event: Event) => void,
): { onFollow: () => void; onCloseAutoFocus: (event: Event) => void } {
  const followed = useRef(false)
  return {
    onFollow: () => {
      followed.current = true
      onClose()
    },
    onCloseAutoFocus: (event) => {
      if (followed.current) event.preventDefault()
      else restoreFocus(event)
    },
  }
}

import { useState } from 'react'

/** An element a keyboard user can land on: attached and not inside a `hidden` subtree. */
function usable(el: Element | null | undefined): el is HTMLElement {
  return el instanceof HTMLElement && el !== document.body && el.isConnected && !el.closest('[hidden]')
}

/**
 * A menu item unmounts with its menu, so the thing worth returning to is the button that
 * opened the menu. Radix labels the menu with that button's id.
 */
function menuTrigger(item: Element | null): HTMLElement | null {
  const id = item?.closest('[role="menu"]')?.getAttribute('aria-labelledby')
  return id ? document.getElementById(id) : null
}

/**
 * Radix Dialog gives focus back only to its own <Trigger>. The room's dialogs are opened
 * by buttons, the H and Shift+T keys, the ⋯ menu or the ⌘K menu, none of which is a Trigger. So
 * the dialog notes what had focus when it mounted and puts it back on close. If that element
 * is gone (a menu item, a closed palette) it falls back to the menu's trigger, then to <main>,
 * so focus never drops to <body>.
 *
 *   <DialogContent onCloseAutoFocus={restoreFocus}>
 */
export function useRestoreFocus(): (event: Event) => void {
  // Read during render: the menu is still mounted then, and gone by the time the dialog closes.
  const [from] = useState<{ opener: Element | null; trigger: HTMLElement | null }>(() => {
    if (typeof document === 'undefined') return { opener: null, trigger: null }
    return { opener: document.activeElement, trigger: menuTrigger(document.activeElement) }
  })
  return (event) => {
    const target = [from.opener, from.trigger, document.getElementById('main')].find(usable)
    if (target) {
      event.preventDefault()
      target.focus()
    }
  }
}

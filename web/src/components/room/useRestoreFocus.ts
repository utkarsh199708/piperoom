import { useState } from 'react'

/** An element a keyboard user can land on: attached and not inside a `hidden` subtree. */
export function usable(el: Element | null | undefined): el is HTMLElement {
  return el instanceof HTMLElement && el !== document.body && el.isConnected && !el.closest('[hidden]')
}

/**
 * The ⌘K palette closes before the dialog it opens mounts, so the dialog cannot see what had focus
 * before the palette. The palette hands that element over here without focusing it (focusing it
 * would flash its tooltip and fire its onFocus/onBlur). One slot, taken by the first dialog that
 * mounts and dropped after a second, so a later unrelated dialog never inherits it. Module scope
 * because the palette and the dialog share no React parent.
 */
let handedOver: Element | null = null
let handedOverTimer: number | undefined

/** Offer `el` as the element the next dialog should return focus to. */
export function handOverFocus(el: Element | null): void {
  window.clearTimeout(handedOverTimer)
  handedOver = el
  handedOverTimer = window.setTimeout(() => {
    handedOver = null
  }, 1000)
}

function takeHandedOver(): Element | null {
  const el = handedOver
  handedOver = null
  window.clearTimeout(handedOverTimer)
  return el
}

/**
 * Take back the handed-over element if no dialog mounted and took it. The palette calls this after
 * running a dialog command: when the dialog was already open nothing new mounts, and the element
 * would otherwise be dropped unused, leaving focus on <body>.
 */
export function reclaimHandedOver(): Element | null {
  return takeHandedOver()
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
    const active = document.activeElement
    const given = takeHandedOver()
    // The palette's own input (or <body>) is not an opener worth returning to; the handed-over element is.
    if (given && (!usable(active) || active.closest('[cmdk-root]'))) return { opener: given, trigger: null }
    return { opener: active, trigger: menuTrigger(active) }
  })
  return (event) => {
    const target = [from.opener, from.trigger, document.getElementById('main')].find(usable)
    if (target) {
      event.preventDefault()
      target.focus()
    }
  }
}

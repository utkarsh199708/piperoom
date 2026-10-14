import { useEffect, useState } from 'react'
import type { RefObject } from 'react'

export interface LaneOverflow {
  /** Content is clipped on the left. */
  left: boolean
  /** Content is clipped on the right. */
  right: boolean
  /** Nodes entirely or partly past the right edge. */
  moreRight: number
  /** Nodes entirely or partly before the left edge. */
  moreLeft: number
}

const NONE: LaneOverflow = { left: false, right: false, moreRight: 0, moreLeft: 0 }

/**
 * Tracks how a horizontally scrolling lane is clipped. Reads happen in a rAF-throttled scroll listener
 * and a ResizeObserver, so scrolling does not re-render on every tick; state only changes when the
 * derived values do. Without ResizeObserver (old jsdom) the fades stay off.
 */
export function useLaneOverflow(ref: RefObject<HTMLElement | null>, deps: readonly unknown[]): LaneOverflow {
  const [o, setO] = useState<LaneOverflow>(NONE)

  useEffect(() => {
    const el = ref.current
    if (!el || typeof ResizeObserver === 'undefined') {
      setO(NONE)
      return
    }
    let frame = 0
    const measure = () => {
      frame = 0
      const right = el.scrollLeft + el.clientWidth < el.scrollWidth - 1
      const left = el.scrollLeft > 0
      let moreRight = 0
      let moreLeft = 0
      for (const c of el.querySelectorAll<HTMLElement>('[data-node]')) {
        if (c.offsetLeft + c.offsetWidth > el.scrollLeft + el.clientWidth) moreRight++
        if (c.offsetLeft < el.scrollLeft) moreLeft++
      }
      setO((prev) =>
        prev.left === left && prev.right === right && prev.moreRight === moreRight && prev.moreLeft === moreLeft
          ? prev
          : { left, right, moreRight, moreLeft },
      )
    }
    const schedule = () => {
      if (!frame) frame = window.requestAnimationFrame(measure)
    }
    measure()
    el.addEventListener('scroll', schedule, { passive: true })
    const ro = new ResizeObserver(schedule)
    ro.observe(el)
    return () => {
      el.removeEventListener('scroll', schedule)
      ro.disconnect()
      if (frame) window.cancelAnimationFrame(frame)
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [ref, ...deps])

  return o
}

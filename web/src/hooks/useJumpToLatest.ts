import { useCallback, useEffect, useLayoutEffect, useRef, useState } from 'react'
import type { ActivityEntry } from '../state/types'

// Room under the sentinel that still counts as "at the bottom": the sticky composer covers it.
const BOTTOM_MARGIN_PX = 200

/**
 * Tracks whether a sentinel at the end of the timeline is in view. At the bottom, new events
 * scroll into view by themselves; scrolled up, they are counted so a "jump to latest" button
 * can say how many arrived. `activity` is the room's recent-event feed (it carries seq).
 */
export function useJumpToLatest(activity: ActivityEntry[], replaying: boolean) {
  const elRef = useRef<HTMLDivElement | null>(null)
  const ioRef = useRef<IntersectionObserver | null>(null)
  const atBottomRef = useRef(true)
  const [atBottom, setAtBottom] = useState(true)
  // The room's feed is newest first; the highest seq is the latest whatever the order.
  const latest = activity.reduce((m, a) => Math.max(m, a.seq), 0)
  const [seen, setSeen] = useState(latest)
  const wasReplaying = useRef(replaying)

  // A callback ref, not a mount effect: the sentinel is not there on the first render (the room
  // skeleton) and moves when the layout switches, so each node it lands on is observed.
  const sentinelRef = useCallback((el: HTMLDivElement | null) => {
    ioRef.current?.disconnect()
    ioRef.current = null
    elRef.current = el
    if (!el || typeof IntersectionObserver === 'undefined') return
    const io = new IntersectionObserver(
      ([entry]) => {
        atBottomRef.current = entry.isIntersecting
        setAtBottom(entry.isIntersecting)
      },
      { rootMargin: `0px 0px ${BOTTOM_MARGIN_PX}px 0px` },
    )
    io.observe(el)
    ioRef.current = io
  }, [])
  useEffect(() => () => ioRef.current?.disconnect(), [])

  // Runs after the new event rendered but before the observer reports the grown page, so the
  // ref still says where the person was.
  useLayoutEffect(() => {
    const justReplayed = wasReplaying.current
    wasReplaying.current = replaying
    if (replaying || justReplayed) {
      // The end of the first replay is not a reason to move the page: an open proposal
      // near the top must stay where the person can see it.
      setSeen(latest)
      return
    }
    if (atBottomRef.current) {
      setSeen(latest)
      elRef.current?.scrollIntoView?.({ block: 'end' })
    }
  }, [latest, replaying])

  const unseen = replaying || atBottom ? 0 : activity.filter((a) => a.seq > seen).length
  const jump = useCallback(() => {
    const reduce = window.matchMedia?.('(prefers-reduced-motion: reduce)').matches
    elRef.current?.scrollIntoView?.({ block: 'end', behavior: reduce ? 'auto' : 'smooth' })
    setSeen(latest)
  }, [latest])

  return { sentinelRef, unseen, jump }
}

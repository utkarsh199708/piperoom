import { useEffect, useState } from 'react'

/** Tracks a CSS media query. Without matchMedia (tests, old engines) it reports `fallback`. */
export function useMediaQuery(query: string, fallback = true): boolean {
  const [matches, setMatches] = useState(() =>
    typeof window.matchMedia === 'function' ? window.matchMedia(query).matches : fallback,
  )
  useEffect(() => {
    if (typeof window.matchMedia !== 'function') return
    const mq = window.matchMedia(query)
    const onChange = () => setMatches(mq.matches)
    onChange()
    mq.addEventListener('change', onChange)
    return () => mq.removeEventListener('change', onChange)
  }, [query])
  return matches
}

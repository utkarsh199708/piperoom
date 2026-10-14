import { useEffect, useState } from 'react'

/** Date.now() that updates every `ms` (default 30 s): for "lease 24m" style labels. */
export function useNow(ms = 30_000): number {
  const [now, setNow] = useState(() => Date.now())
  useEffect(() => {
    const t = window.setInterval(() => setNow(Date.now()), ms)
    return () => window.clearInterval(t)
  }, [ms])
  return now
}

/** Whole minutes left until an RFC 3339 time, rounded up; null for no/invalid time. */
export function minutesLeft(iso: string | undefined, now: number): number | null {
  if (!iso) return null
  const t = Date.parse(iso)
  if (Number.isNaN(t)) return null
  return Math.max(0, Math.ceil((t - now) / 60_000))
}

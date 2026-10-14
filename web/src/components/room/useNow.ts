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

/**
 * Whole minutes left until an RFC 3339 time, rounded down (a fresh 30 m lease reads "30m", not
 * "31m" when the server clock runs a little ahead); at least 1 while any time remains, so 0 means
 * the lease has run out. null for no/invalid time.
 */
export function minutesLeft(iso: string | undefined, now: number): number | null {
  if (!iso) return null
  const t = Date.parse(iso)
  if (Number.isNaN(t)) return null
  const ms = t - now
  if (ms <= 0) return 0
  return Math.max(1, Math.floor(ms / 60_000))
}

// Time formatting helpers. Times arrive as RFC 3339 UTC strings.
const MINUTE = 60_000
const HOUR = 60 * MINUTE
const DAY = 24 * HOUR

export function timeAgo(iso: string, now = Date.now()): string {
  const t = Date.parse(iso)
  if (Number.isNaN(t)) return iso
  const diff = Math.max(0, now - t)
  if (diff < MINUTE) return 'just now'
  if (diff < HOUR) {
    const m = Math.floor(diff / MINUTE)
    return `${m} min${m === 1 ? '' : 's'} ago`
  }
  if (diff < DAY) {
    const h = Math.floor(diff / HOUR)
    return `${h} hr${h === 1 ? '' : 's'} ago`
  }
  const d = Math.floor(diff / DAY)
  return `${d} day${d === 1 ? '' : 's'} ago`
}

export function clockTime(iso: string): string {
  const t = Date.parse(iso)
  if (Number.isNaN(t)) return iso
  const d = new Date(t)
  return d.toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' })
}

// Age of an open room, e.g. "12m" / "3h" / "2d".
export function ageLabel(iso: string, now = Date.now()): string {
  const t = Date.parse(iso)
  if (Number.isNaN(t)) return ''
  const diff = Math.max(0, now - t)
  if (diff < MINUTE) return '<1m'
  if (diff < HOUR) return `${Math.floor(diff / MINUTE)}m`
  if (diff < DAY) return `${Math.floor(diff / HOUR)}h`
  return `${Math.floor(diff / DAY)}d`
}

// "updated Ns ago" label for the pipeline panel. Seconds precision under a
// minute, then minutes/hours. Empty when there is no last-update time yet.
export function updatedAgoLabel(updatedAt: number | null, now = Date.now()): string {
  if (updatedAt === null) return ''
  const diff = Math.max(0, now - updatedAt)
  const s = Math.floor(diff / 1000)
  if (s < 5) return 'updated just now'
  if (s < 60) return `updated ${s}s ago`
  const m = Math.floor(s / 60)
  if (m < 60) return `updated ${m}m ago`
  return `updated ${Math.floor(m / 60)}h ago`
}

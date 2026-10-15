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

/** "2026-10-14 06:06:12 UTC" for hover titles; an invalid time comes back unchanged. */
export function utcLabel(iso: string): string {
  const t = Date.parse(iso)
  if (Number.isNaN(t)) return iso
  return new Date(t).toISOString().slice(0, 19).replace('T', ' ') + ' UTC'
}

/** How long a room was open: "6 min", "1 h 12 min", "2 d 3 h". Empty for an invalid or negative span. */
export function durationLabel(openedIso: string, closedIso: string): string {
  const a = Date.parse(openedIso)
  const b = Date.parse(closedIso)
  if (Number.isNaN(a) || Number.isNaN(b) || b < a) return ''
  const mins = Math.floor((b - a) / MINUTE)
  if (mins < 60) return `${Math.max(mins, 1)} min`
  const hours = Math.floor(mins / 60)
  if (hours < 24) return mins % 60 ? `${hours} h ${mins % 60} min` : `${hours} h`
  const days = Math.floor(hours / 24)
  return hours % 24 ? `${days} d ${hours % 24} h` : `${days} d`
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

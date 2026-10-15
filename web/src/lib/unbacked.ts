/** One run of a hypothesis text: plain, or a term the cited steps do not show. */
export interface TextRun {
  text: string
  unbacked: boolean
}

/** Escapes a string for use inside a RegExp source. */
export function escapeRegExp(s: string): string {
  return s.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')
}

/**
 * Splits `text` so the first case-insensitive occurrence of each term is its own
 * `unbacked` run. Terms are agent text: matching works on the plain string, the
 * pattern is escaped, and the runs are rendered as React text nodes (never HTML).
 * A term inside an earlier term's match is skipped; a term not found is ignored.
 */
export function markTerms(text: string, terms: readonly string[] | undefined): TextRun[] {
  const spans: [number, number][] = []
  for (const term of terms ?? []) {
    if (!term) continue
    const m = new RegExp(escapeRegExp(term), 'i').exec(text)
    if (!m) continue
    const span: [number, number] = [m.index, m.index + m[0].length]
    if (spans.some(([a, b]) => span[0] < b && a < span[1])) continue
    spans.push(span)
  }
  if (spans.length === 0) return [{ text, unbacked: false }]
  spans.sort((x, y) => x[0] - y[0])
  const runs: TextRun[] = []
  let at = 0
  for (const [a, b] of spans) {
    if (a > at) runs.push({ text: text.slice(at, a), unbacked: false })
    runs.push({ text: text.slice(a, b), unbacked: true })
    at = b
  }
  if (at < text.length) runs.push({ text: text.slice(at), unbacked: false })
  return runs
}

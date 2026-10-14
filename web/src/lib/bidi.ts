// Text from people and the agent can carry bidi controls (U+202E RIGHT-TO-LEFT OVERRIDE and
// friends). Inside a plain string it would reorder whatever follows it on the line, so a name
// could make "approved" read as something else. FIRST STRONG ISOLATE ... POP DIRECTIONAL ISOLATE
// is the in-string form of <bdi>: the closing PDI ends every override opened inside it.
const FSI = '\u2068'
const PDI = '\u2069'

/** Wraps untrusted text in a bidi isolate for use inside a sentence built as a string. */
export function isolate(text: string): string {
  return `${FSI}${text}${PDI}`
}

/** The text without the isolate marks `isolate` adds (for comparisons and tests). */
export function stripIsolates(text: string): string {
  return text.replace(/[\u2068\u2069]/g, '')
}

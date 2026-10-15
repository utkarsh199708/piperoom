// The tab title: one true sentence about the room, with an unread count in front.
import type { RoomStatusInfo } from './roomStatus'

// Code point ranges: C0 controls, DEL, zero-width and bidi marks, bidi embeddings and isolates.
const STRIP = new RegExp(
  `[${[[0, 31], [127, 127], [0x200b, 0x200f], [0x202a, 0x202e], [0x2066, 0x2069]]
    .map(([a, b]) => `${String.fromCharCode(a)}-${String.fromCharCode(b)}`)
    .join('')}]`,
  'g',
)

/** Control and bidi-override characters out, whitespace collapsed: room titles are untrusted text. */
export function plainText(text: string, max = 120): string {
  const clean = text.replace(STRIP, ' ').replace(/\s+/g, ' ').trim()
  return clean.length > max ? `${clean.slice(0, max - 1)}…` : clean
}

// Bidi embeddings, overrides and isolates (U+202A-202E, U+2066-2069). In a title or a comment
// they reorder the text after them, so "orders missing" can be made to read as something else.
const BIDI_CONTROLS = /[\u202a-\u202e\u2066-\u2069]/g

/** Typed text with the bidi controls removed and everything else (newlines included) kept. */
export function stripBidi(text: string): string {
  return typeof text === 'string' ? text.replace(BIDI_CONTROLS, '') : text
}

export const LIST_TITLE = 'Rooms · Piperoom'

export function documentTitle(input: {
  status: RoomStatusInfo
  roomTitle: string
  unread: number
  /** The room needs this person (an open proposal waits on a layer they own). */
  needsMe: boolean
}): string {
  const room = plainText(input.roomTitle, 80) || 'Room'
  const head = input.needsMe ? '● Needs you' : input.status.label
  const title = `${head} — ${room} · Piperoom`
  return input.unread > 0 ? `(${input.unread}) ${title}` : title
}

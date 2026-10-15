// Verification facts for the verified badge (security audit A-ID-2, docs/api.md
// "Verified people", docs/design/auth.md §2.1).
//
// A badge that text can fake is worse than none, so these functions read ONLY
// server-set fields: `verified` on a person (events.Actor.Verified and the API
// objects that mirror it) and `alert_source` on room.created. Nothing here
// looks at a name, a title, a comment or the raw alert body. Anything that is
// not exactly `true` (missing, "true", 1, null) is unverified.

import type { ActorKind } from '../api/types'

export type VerificationState = 'verified' | 'unverified'

export interface Verification {
  state: VerificationState
  /** Tooltip text: who verified and how, or why it is not verified. */
  detail: string
}

/** The person-shaped fields a badge may read: all of them are set by the server. */
export interface PersonFacts {
  id: string
  kind?: ActorKind
  verified?: unknown
}

/** Verification of a person, from the server's `verified` flag only. */
export function personVerification(p: PersonFacts): Verification {
  if (p.verified !== true) {
    return {
      state: 'unverified',
      detail:
        p.verified === false
          ? `Not verified: "${p.id}" is known only from the identity provider. The name is set by that person and may not be who it claims.`
          : `Not verified: the server did not vouch for "${p.id}" (no verification on record).`,
    }
  }
  if (p.kind === 'agent') {
    return { state: 'verified', detail: 'Verified by the Piperoom server: this is the server’s own agent.' }
  }
  if (p.kind === 'system') {
    return { state: 'verified', detail: 'Verified by the Piperoom server: this is the server itself.' }
  }
  return {
    state: 'verified',
    detail: `Verified by the Piperoom server: "${p.id}" is listed in users.yaml, and the name shown is the one in users.yaml.`,
  }
}

/** Combines several server reports on one person: verified only when every report that exists says so. */
export function allVerified(...reports: (unknown | undefined)[]): boolean {
  const known = reports.filter((r) => r !== undefined)
  return known.length > 0 && known.every((r) => r === true)
}

// How the server checks each signed alert route (internal/alertin, docs/design/webhook-auth.md §1).
// Keys are the exact `alert_source` values the server writes; anything else is unverified.
const SIGNED_ALERT_SOURCES: Record<string, { label: string; how: string }> = {
  alertmanager: { label: 'Alertmanager', how: 'its bearer token' },
  grafana: { label: 'Grafana', how: 'its HMAC signature and timestamp' },
  pagerduty: { label: 'PagerDuty', how: 'its V3 webhook signature (v1 HMAC)' },
  datadog: { label: 'Datadog', how: 'its header token' },
  generic: { label: 'generic webhook', how: 'its HMAC signature (t=…,v1=…)' },
}

/** A display name for a server-set alert source, or null when the value is not one the server writes. */
export function alertSourceLabel(alertSource: unknown): string | null {
  if (typeof alertSource !== 'string') return null
  if (Object.hasOwn(SIGNED_ALERT_SOURCES, alertSource)) return SIGNED_ALERT_SOURCES[alertSource].label
  if (alertSource === 'watcher') return 'watcher'
  if (alertSource === 'webhook') return 'dev webhook'
  return null
}

/**
 * Verification of the alert that opened a room, from room.created's server-set
 * `alert_source` only. It says who sent the alert, not that its text is true.
 */
export function alertVerification(alertSource: unknown): Verification {
  if (typeof alertSource === 'string' && Object.hasOwn(SIGNED_ALERT_SOURCES, alertSource)) {
    const s = SIGNED_ALERT_SOURCES[alertSource]
    return {
      state: 'verified',
      detail: `Sender verified by the Piperoom server: it checked ${s.how} from ${s.label} before opening the room. The alert's text is still the sender's.`,
    }
  }
  if (alertSource === 'watcher') {
    return { state: 'verified', detail: 'Verified: opened by the Piperoom server’s built-in watcher.' }
  }
  if (alertSource === 'webhook') {
    return { state: 'unverified', detail: 'Not verified: this alert came in on the unsigned dev webhook.' }
  }
  return { state: 'unverified', detail: 'Not verified: the server recorded no signed source for this alert.' }
}

// Glyphs that could pass for a badge, or move or overlay the text around one:
// check marks and ballot boxes, emoji and pictographs, enclosing combining marks
// (U+20DD…), bidi controls and other default-ignorable code points.
const BADGE_LOOKALIKES =
  /[✓✔✅☑☒☐√⍻\u{1F5F8}-\u{1F5FA}\u{10102}\u{1F6E1}\u{1FBB1}]|\p{Extended_Pictographic}|\p{Emoji_Presentation}|\p{Regional_Indicator}|\p{Me}|\p{Default_Ignorable_Code_Point}|[‪-‮⁦-⁩‎‏؜]/gu

// More than two combining marks in a row only stack glyphs out of the line box.
const MARK_PILE = /(\p{Mn}{2})\p{Mn}+/gu

// Lower-case Cyrillic, Greek and other letters that look like the Latin ones in
// "verified" / "unverified" / "verify" (in the spirit of the server's TR39
// skeleton, docs/design/auth.md §2.1, cut down to the letters these words use).
const CONFUSABLE: Record<string, string> = {
  'а': 'a', 'е': 'e', 'ё': 'e', 'і': 'i', 'ї': 'i', 'ј': 'j', 'о': 'o',
  'р': 'p', 'с': 'c', 'у': 'y', 'х': 'x', 'ѵ': 'v', 'ԁ': 'd', 'һ': 'h',
  'ո': 'n', 'ս': 'u', 'ν': 'v', 'ε': 'e', 'ι': 'i', 'ο': 'o', 'ρ': 'p',
  'υ': 'u', 'ϝ': 'f', 'ı': 'i', 'ɩ': 'i', '1': 'i', l: 'i', '|': 'i', '!': 'i',
  'ʋ': 'v', // U+028B
  // Small capitals (U+1D00 block and IPA): "ᴠᴇʀɪꜰɪᴇᴅ".
  '\u1d20': 'v', '\u1d07': 'e', '\u0280': 'r', '\u026a': 'i', '\ua730': 'f', '\u1d05': 'd', '\u1d1c': 'u', '\u0274': 'n',
  // Lisu: "ꓦꓰꓣꓲꓝꓲꓰꓓ".
  '\ua4e6': 'v', '\ua4f0': 'e', '\ua4e3': 'r', '\ua4f2': 'i', '\ua4dd': 'f', '\ua4d3': 'd', '\ua4f4': 'u', '\ua4e0': 'n',
  // Cherokee capitals (they lower-case to U+AB70…, so the lookup tries the raw letter first): "ᏙᎬᎡᏆᎰᏆᎬᎠ".
  '\u13d9': 'v', '\u13ac': 'e', '\u13a1': 'r', '\u13c6': 'i', '\u13b0': 'f', '\u13a0': 'd',
  // More look-alikes: Cyrillic ғ (what "small caps generator" sites emit for f), Canadian Syllabics ᐯ,
  // logical-or signs, ℮, Latin extended ⱱ ⱴ, Old Italic 𐌖.
  '\u0493': 'f', '\u142f': 'v', '\u2228': 'v', '\u22c1': 'v', '\u212e': 'e', '\u2c71': 'v', '\u2c74': 'v', '\u{10316}': 'v',
}

/** The Latin letters (a-z) one code point reads as, or '' for a separator or anything else. */
function foldChar(ch: string): string {
  const cp = ch.codePointAt(0) ?? 0
  // Negative squared / negative circled / squared capitals (U+1F130-1F189) have no NFKD mapping.
  for (const base of [0x1f130, 0x1f150, 0x1f170]) {
    if (cp >= base && cp < base + 26) return String.fromCharCode(97 + cp - base)
  }
  let out = ''
  // NFKD already turns circled, full-width and parenthesized letters into plain ones.
  for (const c of ch.normalize('NFKD')) {
    if (/\p{M}/u.test(c)) continue
    const low = c.toLowerCase()
    // Upper-case lookup: Cherokee small letters (U+AB70-ABBF) upper-case to the capitals in the table.
    out += CONFUSABLE[c] ?? CONFUSABLE[c.toUpperCase()] ?? CONFUSABLE[low] ?? low
  }
  return out.replace(/[^a-z]/g, '')
}

// "verified", "unverified", "verify", "verification", in folded form ("l" and "1" fold to "i").
// Anything that reads as one would put a second, fake verdict next to the real badge.
const VERDICT = /(?:un)?verif(?:ied|y|ication)?/g
const WORD_CHAR = /[\p{L}\p{N}\p{M}]/u

/**
 * Replaces each span that reads as "verified" with one U+FFFD. Separators do not hide it:
 * "veri-fied", "v.e.r.i.f.i.e.d" and "VERI FIED" fold to the same letters once every
 * non-letter is dropped. A span must start at the start of a word, so "Eve Rifkin" is left alone.
 */
function neutralizeVerdictWords(text: string): string {
  const cps = Array.from(text)
  const letters: { ch: string; at: number }[] = []
  cps.forEach((cp, at) => {
    for (const ch of foldChar(cp)) letters.push({ ch, at })
  })
  const joined = letters.map((l) => l.ch).join('')
  const spans: [number, number][] = []
  for (const m of joined.matchAll(VERDICT)) {
    const first = letters[m.index].at
    let last = letters[m.index + m[0].length - 1].at
    if (first > 0 && WORD_CHAR.test(cps[first - 1])) continue
    while (last + 1 < cps.length && WORD_CHAR.test(cps[last + 1])) last++
    spans.push([first, last])
  }
  // Right to left, so earlier indexes stay valid.
  for (const [a, b] of spans.reverse()) cps.splice(a, b - a + 1, '�')
  return cps.join('')
}

/**
 * Untrusted text shown next to a badge (a person's name, an alert label), with
 * every badge look-alike replaced by U+FFFD so the reader sees something was there:
 * check marks, emoji, enclosing marks, bidi and invisible code points, and words
 * that read as "verified" in any mix of scripts. React still renders the result as
 * plain text: no HTML or markdown is ever parsed.
 */
export function neutralizeBadgeText(text: string): string {
  return neutralizeVerdictWords(text.replace(BADGE_LOOKALIKES, '�').replace(MARK_PILE, '$1')).replace(
    /�+/g,
    '�',
  )
}

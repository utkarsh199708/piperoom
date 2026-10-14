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
  /[✓✔✅☑☒☐√⍻\u{1F5F8}\u{1F5F9}\u{10102}\u{1F6E1}]|\p{Extended_Pictographic}|\p{Emoji_Presentation}|\p{Regional_Indicator}|\p{Me}|\p{Default_Ignorable_Code_Point}|[‪-‮⁦-⁩‎‏؜]/gu

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
}

function skeleton(word: string): string {
  let out = ''
  for (const ch of word.normalize('NFKD').toLowerCase()) {
    if (/\p{M}/u.test(ch)) continue
    out += CONFUSABLE[ch] ?? ch
  }
  return out.replace(/[^a-z]/g, '')
}

// A word that reads as "verified" (or "unverified", "verify", "verification")
// would put a second, fake verdict next to the real badge. "l" folds to "i" in
// the skeleton, so the pattern is written in folded form.
const VERDICT_SKELETON = /^(un)?verif/

/**
 * Untrusted text shown next to a badge (a person's name, an alert label), with
 * every badge look-alike replaced by U+FFFD so the reader sees something was there:
 * check marks, emoji, enclosing marks, bidi and invisible code points, and words
 * that read as "verified" in any mix of scripts. React still renders the result as
 * plain text: no HTML or markdown is ever parsed.
 */
export function neutralizeBadgeText(text: string): string {
  return text
    .replace(BADGE_LOOKALIKES, '\uFFFD')
    .replace(MARK_PILE, '$1')
    .replace(/[\p{L}\p{N}\p{M}|!]+/gu, (word) => (VERDICT_SKELETON.test(skeleton(word)) ? '\uFFFD' : word))
    .replace(/\uFFFD+/g, '\uFFFD')
}

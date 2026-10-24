// Q7 pull-request fix (docs/design/fix-pr.md §10): the diff a PR step will write and the state of the PR
// it opened. Pure. Every string here came from a repository or the forge, so nothing builds HTML: the
// components render the results as text nodes.
import type { PRChecks, PullRequestUpdatedPayload } from '../api/types'
import type { PullRequestState } from '../state/types'
import { plainText } from './title'

/** The line under a PR step's diff, before anyone approves (design §10). */
export const PR_APPROVAL_NOTE = 'Approving opens a pull request. Nothing changes in any system until a reviewer merges it.'

export type DiffLineKind = 'add' | 'del' | 'ctx' | 'hunk' | 'note'

/** One line of a unified diff as the card shows it. `marker` is the visible +, - or space. */
export interface DiffLine {
  kind: DiffLineKind
  marker: '+' | '-' | ' ' | ''
  /** Line number in the base file (removed and context lines). */
  oldNo?: number
  /** Line number in the new file (added and context lines). */
  newNo?: number
  text: string
}

const HUNK = /^@@ -(\d+)(?:,\d+)? \+(\d+)(?:,\d+)? @@/

// Bidi and invisible format characters in code are shown, never applied and never dropped: a hidden
// override is exactly what a reviewer must see (Trojan Source).
const HIDDEN = /[\u200b-\u200f\u202a-\u202e\u2060-\u2064\u2066-\u2069\ufeff]/g

/** `text` with every bidi or zero-width character replaced by a visible <U+XXXX> tag. */
export function revealHidden(text: string): string {
  return text.replace(HIDDEN, (c) => `<U+${c.charCodeAt(0).toString(16).toUpperCase().padStart(4, '0')}>`)
}

/** True when `text` holds a character revealHidden would tag. */
export function hasHidden(text: string): boolean {
  return new RegExp(HIDDEN.source).test(text)
}

/**
 * The lines of one file's unified diff (events.FileDiff.unified), with base and new line numbers. The
 * `---`/`+++` header is left out (the card names the file), hunk headers stay, "\ No newline at end of
 * file" is a note. Anything unexpected is kept as a note rather than dropped, so nothing is hidden.
 */
export function parseUnified(unified: string): DiffLine[] {
  const out: DiffLine[] = []
  let oldNo = 0
  let newNo = 0
  let inHunk = false
  const lines = unified.split('\n')
  if (lines.length > 0 && lines[lines.length - 1] === '') lines.pop()
  for (const raw of lines) {
    const line = revealHidden(raw)
    if (!inHunk && (line.startsWith('--- ') || line.startsWith('+++ '))) continue
    const h = HUNK.exec(line)
    if (h) {
      inHunk = true
      oldNo = Number(h[1])
      newNo = Number(h[2])
      out.push({ kind: 'hunk', marker: '', text: line })
      continue
    }
    if (!inHunk) {
      out.push({ kind: 'note', marker: '', text: line })
      continue
    }
    const c = line.charAt(0)
    const text = line.slice(1)
    if (c === '+') out.push({ kind: 'add', marker: '+', newNo: newNo++, text })
    else if (c === '-') out.push({ kind: 'del', marker: '-', oldNo: oldNo++, text })
    else if (c === ' ') out.push({ kind: 'ctx', marker: ' ', oldNo: oldNo++, newNo: newNo++, text })
    else if (c === '\\') out.push({ kind: 'note', marker: '', text: line })
    else out.push({ kind: 'note', marker: '', text: line })
  }
  return out
}

const FOOTER_MARK = 'The body ends with: '

/**
 * The preview's `expected` split at the fixed footer the action appends to the PR body (design §6.5;
 * internal/actions/fixpr Preview writes "... The body ends with: <footer>"). No mark: no footer.
 */
export function splitFooter(expected: string): { expected: string; footer?: string } {
  const i = expected.lastIndexOf(FOOTER_MARK)
  if (i < 0) return { expected }
  const footer = expected.slice(i + FOOTER_MARK.length).trim()
  return footer ? { expected: expected.slice(0, i).trim(), footer } : { expected }
}

/** "+3 -2" with the minus sign spelled as a hyphen-minus so it reads the same everywhere. */
export function countText(added: number, removed: number): string {
  return `+${added} -${removed}`
}

/** "3 added, 2 removed": the same counts in words, for screen readers. */
export function countWords(added: number, removed: number): string {
  return `${added} ${added === 1 ? 'line' : 'lines'} added, ${removed} removed`
}

/** The PR state as the badge says it. A merged PR is never "closed". */
export function prStateWord(state: string): string {
  switch (state) {
    case 'open':
      return 'open'
    case 'merged':
      return 'merged'
    case 'closed':
      return 'closed without merge'
    default:
      return plainText(state, 30) || 'unknown'
  }
}

export type PRTone = 'info' | 'success' | 'neutral' | 'warning' | 'destructive'

export function prStateTone(state: string): PRTone {
  if (state === 'merged') return 'success'
  if (state === 'open') return 'info'
  return 'neutral'
}

/** Check names are the repository CI's words: control characters out, at most 100 characters, at most 10. */
export function failedNames(checks: PRChecks | undefined): string[] {
  return (checks?.failed ?? []).slice(0, 10).map((n) => plainText(String(n), 100)).filter(Boolean)
}

/** The checks in words: "checks pending (3)", "checks passing (3)", "checks failing (1 of 3)", "no checks". */
export function checksWords(checks: PRChecks | undefined): string {
  if (!checks) return 'checks not read yet'
  const n = checks.total
  switch (checks.state) {
    case 'pending':
      return `checks pending${n > 0 ? ` (${n})` : ''}`
    case 'success':
      return `checks passing${n > 0 ? ` (${n})` : ''}`
    case 'failure': {
      const failed = checks.failed?.length ?? 0
      return `checks failing${failed > 0 && n > 0 ? ` (${failed} of ${n})` : ''}`
    }
    case 'none':
      return 'no checks'
    default:
      return 'checks unknown'
  }
}

export function checksTone(checks: PRChecks | undefined): PRTone {
  switch (checks?.state) {
    case 'success':
      return 'success'
    case 'failure':
      return 'destructive'
    case 'pending':
      return 'warning'
    default:
      return 'neutral'
  }
}

/** One activity-feed line: "merged · checks passing · edited after approval". */
export function prLine(p: Pick<PullRequestUpdatedPayload, 'state' | 'checks' | 'head_changed'>): string {
  const parts = [prStateWord(p.state), checksWords(p.checks)]
  if (p.head_changed) parts.push('edited after approval')
  return parts.join(' · ')
}

/**
 * The PR link, only for an http(s) URL. The server checks the host against the forge's web host; the
 * web still refuses any other scheme (javascript:, data:) so a bad value can never run.
 */
export function safeHref(url: string | undefined): string | undefined {
  if (!url) return undefined
  try {
    const u = new URL(url)
    return u.protocol === 'https:' || u.protocol === 'http:' ? u.href : undefined
  } catch {
    return undefined
  }
}

/** The first 7 characters of a commit SHA. */
export function shortSha(sha: string | undefined): string {
  return (sha ?? '').slice(0, 7)
}

/** The open PR of a proposal, if one is still waiting on a reviewer. */
export function waitingOnMerge(prs: Record<number, PullRequestState> | undefined): boolean {
  return Object.values(prs ?? {}).some((pr) => pr.state === 'open')
}

/** Q7 verdict headlines for a fix PR proposal that did not apply; undefined for every other outcome. */
export function fixPROutcomeWords(outcome: string | undefined): string | undefined {
  if (outcome === 'not_applied') return 'Not applied · the pull request closed without merge'
  if (outcome === 'not_deployed') return 'Merged · not yet seen in the system'
  return undefined
}

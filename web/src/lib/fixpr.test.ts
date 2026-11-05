import { describe, expect, it } from 'vitest'
import {
  checksWords,
  failedNames,
  fixPROutcomeWords,
  parseUnified,
  prLine,
  prStateWord,
  revealHidden,
  safeHref,
  splitFooter,
} from './fixpr'

describe('parseUnified', () => {
  it('drops the file header, keeps hunks, and numbers base and new lines', () => {
    const lines = parseUnified('--- a/f\n+++ b/f\n@@ -1,3 +1,3 @@\n a\n-b\n+B\n c\n')
    expect(lines.map((l) => [l.kind, l.marker, l.oldNo, l.newNo, l.text])).toEqual([
      ['hunk', '', undefined, undefined, '@@ -1,3 +1,3 @@'],
      ['ctx', ' ', 1, 1, 'a'],
      ['del', '-', 2, undefined, 'b'],
      ['add', '+', undefined, 2, 'B'],
      ['ctx', ' ', 3, 3, 'c'],
    ])
  })
  it('reads a new file, a second hunk and "no newline" notes', () => {
    const added = parseUnified('--- /dev/null\n+++ b/f\n@@ -0,0 +1,2 @@\n+a\n+b\n')
    expect(added.filter((l) => l.kind === 'add').map((l) => l.newNo)).toEqual([1, 2])
    const two = parseUnified('--- a/f\n+++ b/f\n@@ -1 +1 @@\n-a\n\\ No newline at end of file\n+a\n@@ -15,2 +15,2 @@\n 15\n-16\n+x\n')
    expect(two.find((l) => l.kind === 'note')?.text).toBe('\\ No newline at end of file')
    expect(two.filter((l) => l.kind === 'del').map((l) => l.oldNo)).toEqual([1, 16])
    expect(two.filter((l) => l.kind === 'add').map((l) => l.newNo)).toEqual([1, 16])
  })
  it('keeps a line that starts with "--- " inside a hunk as a removed line', () => {
    const lines = parseUnified('--- a/f\n+++ b/f\n@@ -1 +1 @@\n--- old yaml doc\n+++ new\n')
    expect(lines.map((l) => [l.kind, l.text])).toEqual([
      ['hunk', '@@ -1 +1 @@'],
      ['del', '-- old yaml doc'],
      ['add', '++ new'],
    ])
  })
  it('shows bidi and zero-width characters instead of applying them', () => {
    expect(revealHidden('a\u202eb\u200bc')).toBe('a<U+202E>b<U+200B>c')
    expect(parseUnified('@@ -1 +1 @@\n+x\u2066y\n')[1].text).toBe('x<U+2066>y')
  })
})

describe('splitFooter', () => {
  it('takes the PR body footer off the expected text', () => {
    expect(splitFooter('a pull request. Nothing changes. The body ends with: Opened by Piperoom.')).toEqual({
      expected: 'a pull request. Nothing changes.',
      footer: 'Opened by Piperoom.',
    })
    expect(splitFooter('no footer here')).toEqual({ expected: 'no footer here' })
  })
})

describe('PR words', () => {
  it('names every state and check result in words', () => {
    expect(prStateWord('open')).toBe('open')
    expect(prStateWord('merged')).toBe('merged')
    expect(prStateWord('closed')).toBe('closed without merge')
    expect(checksWords(undefined)).toBe('checks not read yet')
    expect(checksWords({ state: 'pending', total: 3 })).toBe('checks pending (3)')
    expect(checksWords({ state: 'success', total: 3 })).toBe('checks passing (3)')
    expect(checksWords({ state: 'failure', total: 3, failed: ['lint'] })).toBe('checks failing (1 of 3)')
    expect(checksWords({ state: 'none', total: 0 })).toBe('no checks')
    expect(prLine({ state: 'merged', checks: { state: 'success', total: 2 }, head_changed: true })).toBe(
      'merged · checks passing (2) · edited after approval',
    )
  })
  it('cleans and caps CI check names', () => {
    const names = failedNames({ state: 'failure', total: 20, failed: Array.from({ length: 12 }, (_, i) => `c${i}\u202e`) })
    expect(names).toHaveLength(10)
    expect(names[0]).toBe('c0')
  })
  it('names the two fix-PR outcomes and nothing else', () => {
    expect(fixPROutcomeWords('not_applied')).toMatch(/closed without merge/)
    expect(fixPROutcomeWords('not_deployed')).toMatch(/^Merged/)
    expect(fixPROutcomeWords('recovered')).toBeUndefined()
  })
})

describe('safeHref', () => {
  it('links only http(s) URLs', () => {
    expect(safeHref('https://github.com/acme/r/pull/1')).toBe('https://github.com/acme/r/pull/1')
    expect(safeHref('javascript:alert(1)')).toBeUndefined()
    expect(safeHref('data:text/html,x')).toBeUndefined()
    expect(safeHref('not a url')).toBeUndefined()
    expect(safeHref('')).toBeUndefined()
  })
})

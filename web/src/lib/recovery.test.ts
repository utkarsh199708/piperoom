import { describe, expect, it } from 'vitest'
import { asCount, checkingLabel, countWithUnit, gapLine, outcomeView, previewLine, recoveryOutcome, tookLabel } from './recovery'

describe('asCount', () => {
  it.each([
    [0, 0],
    [412, 412],
    [-1, undefined],
    [1.5, undefined],
    [Number.NaN, undefined],
    [Number.POSITIVE_INFINITY, undefined],
    [2 ** 60, undefined],
    ['412', undefined],
    ['<img src=x>', undefined],
    [null, undefined],
    [undefined, undefined],
  ])('%j -> %j', (v, want) => expect(asCount(v)).toBe(want))
})

describe('previewLine', () => {
  it('says what the step will re-emit, with a singular for one', () => {
    expect(previewLine(412, 'rows')).toBe('Will re-emit 412 rows')
    expect(previewLine(1, 'rows')).toBe('Will re-emit 1 row')
    expect(previewLine(100000, 'records')).toBe('Will re-emit 100,000 records')
    expect(previewLine(0, 'rows')).toBe('Will re-emit 0 rows')
  })
  it('leaves out a unit the contract does not name, and a count that is not a number', () => {
    expect(previewLine(7, '<b>rows</b>')).toBe('Will re-emit 7')
    expect(previewLine(undefined, 'rows')).toBeUndefined()
    expect(previewLine('12', 'rows')).toBeUndefined()
  })
})

describe('gapLine / countWithUnit', () => {
  it('prints after: 0 as a real count', () => {
    expect(gapLine(412, 0, 'rows')).toBe('412 missing → 0 rows')
    expect(countWithUnit(1, 'records')).toBe('1 record')
  })
  it('marks an unreadable after with a question mark and drops the line without a before', () => {
    expect(gapLine(412, null, 'rows')).toBe('412 missing → ?')
    expect(gapLine(null, 3, 'rows')).toBeUndefined()
    expect(gapLine(undefined, undefined, undefined)).toBeUndefined()
  })
})

describe('outcome', () => {
  it('knows the four contract words only', () => {
    expect(recoveryOutcome({ outcome: 'partial' })).toBe('partial')
    expect(recoveryOutcome({})).toBeUndefined()
    expect(recoveryOutcome({ outcome: 'great' as never })).toBeUndefined()
  })
  it('colours: partial warns, not_recovered is danger', () => {
    expect(outcomeView('recovered').tone).toBe('success')
    expect(outcomeView('partial').tone).toBe('warning')
    expect(outcomeView('not_recovered').tone).toBe('danger')
    expect(outcomeView('unverifiable').tone).toBe('muted')
  })
})

describe('tookLabel / checkingLabel', () => {
  it('rounds to seconds under a minute and minutes after', () => {
    expect(tookLabel('2026-10-11T10:00:00Z', '2026-10-11T10:00:45Z')).toBe('45 s')
    expect(tookLabel('2026-10-11T10:00:00Z', '2026-10-11T10:03:10Z')).toBe('3 min')
  })
  it('is empty for a missing, invalid or negative span', () => {
    expect(tookLabel(undefined, '2026-10-11T10:00:45Z')).toBe('')
    expect(tookLabel('nope', '2026-10-11T10:00:45Z')).toBe('')
    expect(tookLabel('2026-10-11T10:01:00Z', '2026-10-11T10:00:00Z')).toBe('')
  })
  it('clamps the poll into 1..max', () => {
    expect(checkingLabel({ poll: 2, max_polls: 8 })).toBe('checking (2 of 8)')
    expect(checkingLabel({ poll: 9, max_polls: 8 })).toBe('checking (8 of 8)')
    expect(checkingLabel({ poll: 0, max_polls: 8 })).toBe('checking (1 of 8)')
    expect(checkingLabel({ poll: 1, max_polls: 0 })).toBe('checking')
  })
})

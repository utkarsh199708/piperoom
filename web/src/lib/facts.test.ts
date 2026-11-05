import { describe, expect, it } from 'vitest'
import { factLabel, factPhrase, factValue } from './facts'

describe('facts', () => {
  it.each([
    ['tasks_failed', 'failed tasks'],
    ['slot_retained', 'WAL held'],
    ['newest_record', 'newest'],
    ['committed', 'committed offset'],
    ['rows_behind', 'rows behind'],
    ['lag', 'lag'],
  ])('factLabel(%s) = %s', (key, want) => {
    expect(factLabel(key)).toBe(want)
  })

  it('reads a full timestamp as a clock time and leaves other values alone', () => {
    expect(factValue('2026-10-11T09:47:00Z')).not.toContain('T09')
    expect(factValue('2026-10-11T09:47:00Z')).toMatch(/\d{1,2}:\d{2}/)
    expect(factValue('48m ago')).toBe('48m ago')
    expect(factValue('2026-10-11')).toBe('2026-10-11')
  })

  it('factPhrase joins the label and value', () => {
    expect(factPhrase('committed', '48212')).toBe('committed offset 48212')
  })
})

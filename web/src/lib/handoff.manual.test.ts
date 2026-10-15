import { describe, expect, it } from 'vitest'
import {
  MAX_SUMMARY_ITEM,
  MAX_SUMMARY_ITEMS,
  MAX_SUMMARY_TEXT,
  canWriteByHand,
  emptySummaryDraft,
  handoffFailureText,
  listItems,
  parseSummaryDraft,
} from './handoff'

const draft = (over: Partial<ReturnType<typeof emptySummaryDraft>> = {}) => ({
  ...emptySummaryDraft(),
  what_broke: 'The orders connector stopped',
  why: 'Schema change on orders.amount',
  ...over,
})

describe('listItems', () => {
  it('one item per line, blanks dropped, list markers removed', () => {
    expect(listItems('- restarted the task\n\n* checked lag\n• read the log\n  plain  \n')).toEqual([
      'restarted the task',
      'checked lag',
      'read the log',
      'plain',
    ])
    expect(listItems('')).toEqual([])
    expect(listItems('-5 degrees')).toEqual(['-5 degrees']) // a minus sign is not a bullet
  })
})

describe('parseSummaryDraft', () => {
  it('builds the summary the API takes: trimmed texts and four lists', () => {
    const { summary, errors } = parseSummaryDraft(
      draft({ what_broke: '  b  ', done: '- a\n- b', ruled_out: 'c', open: '', risks: 'd\ne' }),
    )
    expect(errors).toEqual([])
    expect(summary).toEqual({
      what_broke: 'b',
      why: 'Schema change on orders.amount',
      done: ['a', 'b'],
      ruled_out: ['c'],
      open: [],
      risks: ['d', 'e'],
    })
  })

  it.each([
    ['what_broke', 'Say what broke.'],
    ['why', 'Say why it broke, or what you think the cause is.'],
  ] as const)('%s is required', (field, message) => {
    const res = parseSummaryDraft(draft({ [field]: '   ' }))
    expect(res.summary).toBeNull()
    expect(res.errors).toEqual([message])
  })

  it('refuses over-long texts, items and lists, in code points', () => {
    expect(parseSummaryDraft(draft({ what_broke: 'x'.repeat(MAX_SUMMARY_TEXT + 1) })).errors[0]).toMatch(/"What broke" is at most 1000/)
    expect(parseSummaryDraft(draft({ why: 'x'.repeat(MAX_SUMMARY_TEXT) })).errors).toEqual([])
    // 500 astral characters are 1000 UTF-16 units: the server counts characters.
    expect(parseSummaryDraft(draft({ done: '\u{1F600}'.repeat(MAX_SUMMARY_ITEM) })).errors).toEqual([])
    expect(parseSummaryDraft(draft({ done: 'x'.repeat(MAX_SUMMARY_ITEM + 1) })).errors[0]).toMatch(/Each "Done" item is at most 500/)
    const many = Array.from({ length: MAX_SUMMARY_ITEMS + 1 }, (_, i) => `item ${i}`).join('\n')
    expect(parseSummaryDraft(draft({ risks: many })).errors[0]).toMatch(/"Risks" has at most 20 items/)
    expect(parseSummaryDraft(draft({ risks: many.split('\n').slice(0, 20).join('\n') })).errors).toEqual([])
  })

  it('collects every problem, not only the first', () => {
    const res = parseSummaryDraft(draft({ what_broke: '', why: '', open: 'x'.repeat(501) }))
    expect(res.errors).toHaveLength(3)
  })
})

describe('summary_failed wording', () => {
  it('the failure text points at writing it yourself', () => {
    expect(handoffFailureText('summary_failed')).toMatch(/write it yourself/)
  })

  it('only a failed summary offers the manual form', () => {
    expect(canWriteByHand('summary_failed')).toBe(true)
    for (const r of ['not_driver', 'room_closed', 'internal_error', '', undefined]) expect(canWriteByHand(r)).toBe(false)
  })
})

import { describe, expect, it } from 'vitest'
import { ApiClientError } from '../api/client'
import { exportErrorText, isTooLarge, postmortemDrafting, postmortemErrorText, postmortemFilename } from './postmortem'

describe('postmortemDrafting', () => {
  const now = Date.parse('2026-10-14T10:10:00Z')
  it.each([
    [{ postmortem: { status: 'drafting' as const }, closed_at: '2026-10-14T10:08:00Z' }, true],
    [{ postmortem: { status: 'drafting' as const }, closed_at: '2026-10-14T09:00:00Z' }, false],
    [{ postmortem: { status: 'drafting' as const }, closed_at: undefined }, true],
    [{ postmortem: { status: 'failed' as const, reason: 'x' }, closed_at: '2026-10-14T10:08:00Z' }, false],
    [{ postmortem: null, closed_at: '2026-10-14T10:08:00Z' }, false],
  ])('%j -> %s', (state, want) => {
    expect(postmortemDrafting(state, now)).toBe(want)
  })
})

describe('exportErrorText', () => {
  it('names the role and the pipeline on 403 not_allowed, never the server message', () => {
    const e = new ApiClientError(403, 'not_allowed', 'raw server text')
    expect(exportErrorText(e, 'orders')).toBe('Export needs an approver or auditor role for orders')
  })
  it('other errors get fixed text', () => {
    expect(exportErrorText(new ApiClientError(500, 'internal_error', 'pq: boom'), 'orders')).toBe('Could not export the room. Try again.')
    expect(exportErrorText(new TypeError('Failed to fetch'), 'orders')).toMatch(/can't reach Piperoom/)
  })
})

it('postmortemFilename matches the server download name', () => {
  expect(postmortemFilename('r_42')).toBe('piperoom-r_42-postmortem.md')
})

describe('413 export_too_large (export and draft)', () => {
  const tooLarge = new ApiClientError(413, 'export_too_large', 'room has 90000 events; an export holds at most 50000')
  it.each([
    [tooLarge, true],
    [new ApiClientError(413, 'unknown', 'x'), true],
    [new ApiClientError(500, 'internal_error', 'x'), false],
    [new TypeError('Failed to fetch'), false],
  ])('isTooLarge(%s) = %s', (e, want) => {
    expect(isTooLarge(e)).toBe(want)
  })
  it('the export toast says retrying will not help', () => {
    expect(exportErrorText(tooLarge, 'orders')).toBe(
      'This room is too large to export. Retrying will not help; page through its events instead.',
    )
  })
  it('the draft error names the size, not "retry in a moment"', () => {
    expect(postmortemErrorText(tooLarge)).toBe('this room is too large to draft. Read its timeline in the room instead.')
  })
})

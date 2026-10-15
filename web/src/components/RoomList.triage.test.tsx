import { describe, expect, it } from 'vitest'
import { triageLine, TRIAGE_PENDING_MS } from '../lib/triage'

const now = Date.parse('2026-10-14T03:12:00Z')
const base = { status: 'open', opened_at: '2026-10-14T03:11:00Z' }

describe('list row second line', () => {
  it('shows the headline and the suspected layer', () => {
    const line = triageLine(
      { ...base, triage: { status: 'ok', headline: 'h', suspected_layer: 'kafka-connect', evidence_refs: [], at: '' } },
      now,
    )
    expect(line).toEqual({ kind: 'headline', headline: 'h', layer: 'kafka-connect' })
  })

  it('a failed first look shows nothing', () => {
    expect(triageLine({ ...base, triage: { status: 'failed', reason: 'timeout', evidence_refs: [], at: '' } }, now)).toEqual({
      kind: 'none',
    })
  })

  it('shimmers for a young open room only', () => {
    expect(triageLine({ ...base, triage: null }, now).kind).toBe('pending')
    expect(triageLine({ ...base, triage: null }, Date.parse(base.opened_at) + TRIAGE_PENDING_MS + 1).kind).toBe('none')
    expect(triageLine({ ...base, status: 'closed', triage: null }, now).kind).toBe('none')
    expect(triageLine({ ...base }, now).kind).toBe('pending') // an older server sends no triage field
  })
})

describe('shadow rooms', () => {
  it('never claim a first look is in progress', () => {
    expect(triageLine({ ...base, shadow: true, triage: null }, now).kind).toBe('none')
    expect(triageLine({ ...base, shadow: true }, now).kind).toBe('none')
  })
})

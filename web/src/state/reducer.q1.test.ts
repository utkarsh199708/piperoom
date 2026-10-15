// Q1-web-b: owner pages (G2), manual hand-offs (G4) and fix notes (G10) in the reducer and the activity feed.
import { describe, expect, it } from 'vitest'
import type { Event } from '../api/types'
import { applyEvent } from './reducer'
import { describeEvent } from './describe'
import { emptyRoomState } from './types'

const agent = { kind: 'agent' as const, id: 'agent', name: 'Agent' }
const system = { kind: 'system' as const, id: 'server', name: 'Piperoom' }
const arjun = { kind: 'human' as const, id: 'arjun', name: 'Arjun' }

function ev(seq: number, type: Event['type'], payload: unknown, actor: Event['actor'] = agent): Event {
  return { room_id: 'r1', seq, type, actor, payload, at: `2026-10-15T04:10:${String(seq).padStart(2, '0')}Z` }
}

describe('owner.paged', () => {
  const paged = { layer: 'consumer', owners: ['kabir'], reason: 'Lag keeps growing and the connector is healthy' }

  it('is kept in state.pages in order, with who paged and whether the server did', () => {
    let s = applyEvent(emptyRoomState('r1'), ev(1, 'owner.paged', paged))
    s = applyEvent(s, ev(2, 'owner.paged', { layer: 'warehouse', owners: ['meera'], reason: 'no driver for 10 min', auto: true }, system))
    expect(s.pages).toEqual([
      { seq: 1, at: '2026-10-15T04:10:01Z', by: agent, layer: 'consumer', owners: ['kabir'], reason: paged.reason, proposal_id: undefined, auto: false },
      { seq: 2, at: '2026-10-15T04:10:02Z', by: system, layer: 'warehouse', owners: ['meera'], reason: 'no driver for 10 min', proposal_id: undefined, auto: true },
    ])
  })

  it('keeps the proposal it is about, and treats missing owners as none', () => {
    const s = applyEvent(emptyRoomState('r1'), ev(1, 'owner.paged', { layer: 'kafka', reason: 'r', proposal_id: 'p_1' }))
    expect(s.pages[0].owners).toEqual([])
    expect(s.pages[0].proposal_id).toBe('p_1')
  })

  it('keeps only the newest 50 pages', () => {
    let s = emptyRoomState('r1')
    for (let i = 1; i <= 60; i++) s = applyEvent(s, ev(i, 'owner.paged', { ...paged, reason: `r${i}` }))
    expect(s.pages).toHaveLength(50)
    expect(s.pages[0].reason).toBe('r11')
    expect(s.pages[49].reason).toBe('r60')
  })

  it('is one activity line: who paged whom on which layer, and why', () => {
    const line = describeEvent(ev(1, 'owner.paged', paged))
    expect(line.text).toBe('Agent paged kabir (consumer): Lag keeps growing and the connector is healthy')
    expect(line.kind).toBe('human') // the default feed hides only `system` rows
  })

  it('uses the names of the room members for the owner ids', () => {
    let s = applyEvent(emptyRoomState('r1'), ev(1, 'comment.added', { comment_id: 'c1', text: 'hi', mentions_agent: false }, { kind: 'human', id: 'kabir', name: 'Kabir' }))
    s = applyEvent(s, ev(2, 'owner.paged', paged))
    expect(s.activity[0].text).toBe('Agent paged Kabir (consumer): Lag keeps growing and the connector is healthy')
  })

  it('says when the server paged by itself, a person paged, or the layer has nobody', () => {
    expect(describeEvent(ev(1, 'owner.paged', { ...paged, auto: true }, system)).text).toMatch(/^Piperoom auto-paged kabir \(consumer\)/)
    expect(describeEvent(ev(2, 'owner.paged', paged, arjun)).text).toMatch(/^Arjun paged kabir/)
    expect(describeEvent(ev(3, 'owner.paged', { layer: 'kafka', owners: [], reason: 'x' })).text).toBe(
      'Agent paged the kafka owners (nobody listed): x',
    )
  })

  it('puts a long reason in the detail and clips the line', () => {
    const long = 'x'.repeat(200)
    const line = describeEvent(ev(1, 'owner.paged', { ...paged, reason: long }))
    expect(line.text.length).toBeLessThan(120)
    expect(line.detail).toBe(long)
  })
})

describe('fix notes', () => {
  const comment = (kind?: unknown) => ({ comment_id: 'c1', text: 'Restarted order-sync by hand', mentions_agent: false, ...(kind === undefined ? {} : { kind }) })

  it('comment.added with kind fix_note is a fix note; a plain or unknown kind is a plain comment', () => {
    const fix = applyEvent(emptyRoomState('r1'), ev(1, 'comment.added', comment('fix_note'), arjun))
    expect(fix.comments[0].kind).toBe('fix_note')
    expect(applyEvent(emptyRoomState('r1'), ev(1, 'comment.added', comment(), arjun)).comments[0].kind).toBeUndefined()
    expect(applyEvent(emptyRoomState('r1'), ev(1, 'comment.added', comment(''), arjun)).comments[0].kind).toBeUndefined()
    expect(applyEvent(emptyRoomState('r1'), ev(1, 'comment.added', comment('something_new'), arjun)).comments[0].kind).toBeUndefined()
  })

  it('reads in the activity feed as a logged fix', () => {
    const line = describeEvent(ev(1, 'comment.added', comment('fix_note'), arjun))
    expect(line.text).toBe('Arjun logged a fix')
    expect(line.detail).toBe('Restarted order-sync by hand')
  })
})

describe('manual hand-off', () => {
  const summary = { what_broke: 'b', why: 'w', done: [], ruled_out: [], open: [], risks: [] }
  const created = (manual?: boolean) => ({
    handoff_id: 'h_1',
    to: { kind: 'human', id: 'priya', name: 'Priya' },
    summary,
    stay_as_watcher: false,
    ...(manual ? { manual: true } : {}),
  })

  it('remembers that the driver wrote the summary, and who', () => {
    const s = applyEvent(emptyRoomState('r1'), ev(1, 'handoff.created', created(true), arjun))
    expect(s.handoffs[0].manual).toBe(true)
    expect(s.handoffs[0].written_by).toEqual(arjun)
  })

  it('an agent-written hand-off has neither', () => {
    const s = applyEvent(emptyRoomState('r1'), ev(1, 'handoff.created', created(), arjun))
    expect(s.handoffs[0].manual).toBeUndefined()
    expect(s.handoffs[0].written_by).toBeUndefined()
  })

  it('the activity line says the driver wrote it', () => {
    expect(describeEvent(ev(1, 'handoff.created', created(true), arjun)).text).toBe('Arjun wrote a hand-off summary for Priya')
    expect(describeEvent(ev(2, 'handoff.created', created(), arjun)).text).toBe('Arjun started a hand-off to Priya')
  })

  it('handoff.failed with summary_failed explains itself in the detail', () => {
    const line = describeEvent(ev(1, 'handoff.failed', { request_id: 'hr_1', reason: 'summary_failed' }, system))
    expect(line.text).toBe('The hand-off could not be written')
    expect(line.detail).toMatch(/could not write the summary/)
    expect(describeEvent(ev(2, 'handoff.failed', { request_id: 'hr_1', reason: 'internal_error' }, system)).detail).toBeUndefined()
  })
})

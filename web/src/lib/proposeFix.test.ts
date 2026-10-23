import { describe, expect, it } from 'vitest'
import { ApiClientError } from '../api/client'
import type { ActionInfo } from '../api/types'
import { emptyRoomState } from '../state/types'
import type { RoomState } from '../state/types'
import type { CurrentUser } from '../state/permissions'
import { EMPTY_DRAFT, canProposeFix, proposalBody, proposeErrorText } from './proposeFix'

const arjun: CurrentUser = { id: 'arjun', name: 'Arjun', layers: ['kafka-connect'] }
const driven = (over: Partial<RoomState> = {}): RoomState => ({
  ...emptyRoomState('r'),
  driver: { id: 'arjun', name: 'Arjun' },
  ...over,
})

describe('canProposeFix', () => {
  it.each<[string, RoomState, CurrentUser, boolean]>([
    ['the driver of an open room', driven(), arjun, true],
    ['another member', driven(), { ...arjun, id: 'priya' }, false],
    ['nobody driving', driven({ driver: null }), arjun, false],
    ['a viewer (even named driver)', driven(), { ...arjun, role: 'viewer' }, false],
    ['a closed room', driven({ status: 'closed' }), arjun, false],
    ['a shadow room', driven({ shadow: true }), arjun, false],
    ['a setup room', driven({ setup: {} as RoomState['setup'] }), arjun, false],
  ])('%s → %s', (_n, state, user, want) => {
    expect(canProposeFix(state, user)).toBe(want)
  })
})

const action: ActionInfo = { name: 'connect_resume', layer: 'kafka-connect', describe: 'Resume', params_schema: {}, preview: true }

describe('proposalBody', () => {
  it('wants an action and a title', () => {
    expect(proposalBody(undefined, {}, { ...EMPTY_DRAFT, title: 'x' })).toEqual({ ok: false, field: 'action', error: 'Choose an action.' })
    expect(proposalBody(action, {}, { ...EMPTY_DRAFT, title: '  ' })).toEqual({ ok: false, field: 'title', error: 'Give the fix a title.' })
  })

  it('builds one step with the action layer, trimmed text, and no empty description', () => {
    expect(proposalBody(action, { connector: 'c' }, { title: ' Resume ', risk: 'low', rollback: ' pause again ', description: ' ' })).toEqual({
      ok: true,
      body: { title: 'Resume', risk: 'low', rollback: 'pause again', steps: [{ action: 'connect_resume', layer: 'kafka-connect', params: { connector: 'c' } }] },
    })
    const r = proposalBody(action, {}, { ...EMPTY_DRAFT, title: 't', description: 'why' })
    expect(r.ok && r.body.steps[0].description).toBe('why')
  })
})

describe('proposeErrorText', () => {
  it.each([
    [new ApiClientError(403, 'not_driver', 'not the driver'), 'Only the driver can propose a fix. Take control of the room first.'],
    [new ApiClientError(429, 'too_many_proposals', 'x'), 'This room already has 5 open fixes you proposed. Approve, reject or wait for one before proposing another.'],
    [new ApiClientError(400, 'validation', 'step 0: connector "x" is not in the pipeline'), 'The server refused the fix: step 0: connector "x" is not in the pipeline'],
    [new ApiClientError(409, 'room_closed', 'room is closed'), 'The room is closed; no more fixes can be proposed.'],
    [new ApiClientError(500, 'internal', 'boom'), 'boom'],
  ])('%s', (err, text) => {
    expect(proposeErrorText(err)).toBe(text)
  })
})

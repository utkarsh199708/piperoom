import { describe, expect, it } from 'vitest'
import type { Event } from '../api/types'
import { applyEvent } from './reducer'
import { fromSnapshot } from './types'
import {
  SETUP_ROOM_ID,
  planCreated,
  runnerAttached,
  setupCreated,
  setupPlan,
  setupRoomSnapshot,
} from '../fixtures/setup'

const start = () => fromSnapshot(setupRoomSnapshot())

describe('setup rooms in the reducer', () => {
  it('room.created with mode setup carries the request and the room is a setup room', () => {
    const state = applyEvent(start(), setupCreated())
    expect(state.setup).toMatchObject({
      runner: null,
      plan_id: '',
      provisional: { topic_prefix: 'orders', tables: ['public.orders', 'public.order_items'] },
      request: { pipeline_id: 'orders', target: 'compose', source_kind: 'postgres' },
    })
    expect(state.setup?.targets).toEqual([{ kind: 'postgres', address: 'db.acme:5432', class: 'production' }])
  })

  it('an incident room has no setup member', () => {
    const incidentSnapshot = setupRoomSnapshot()
    delete incidentSnapshot.setup
    const incident = applyEvent(fromSnapshot(incidentSnapshot), {
      ...setupCreated(),
      payload: { title: 'T', severity: 'sev2', pipeline_id: 'orders' },
    })
    expect(incident.setup).toBeUndefined()
  })

  it('setup.runner_attached and runner_lost move only connected', () => {
    let state = applyEvent(start(), runnerAttached())
    expect(state.setup?.runner).toEqual({ label: 'arjun-laptop', connected: true, tools: { docker: '27.1.1', kubectl: '' } })
    state = applyEvent(state, {
      room_id: SETUP_ROOM_ID,
      seq: state.last_seq + 1,
      type: 'setup.runner_lost',
      actor: { kind: 'system', id: 'system', name: 'system' },
      at: '2026-10-22T10:03:00Z',
      payload: { runner_label: 'arjun-laptop', last_seen: '2026-10-22T10:02:00Z' },
    })
    expect(state.setup?.runner?.connected).toBe(false)
    expect(state.setup?.blocked).toBeUndefined()
  })

  it('setup.plan starts at the first step and action.executed advances the plan', () => {
    let state = applyEvent(applyEvent(start(), runnerAttached()), planCreated())
    expect(state.setup?.plan_id).toBe('pl_1')
    expect(state.setup?.phase).toBe('prerequisites')
    expect(state.setup?.step_index).toBe(0)
    expect(state.setup?.phases).toHaveLength(2)

    state = applyEvent(state, {
      room_id: SETUP_ROOM_ID,
      seq: state.last_seq + 1,
      type: 'action.executed',
      actor: { kind: 'system', id: 'executor', name: 'Executor' },
      at: '2026-10-22T10:05:00Z',
      payload: { proposal_id: 'p_setup', step_index: 0, ok: true, output: 'ok' },
    })
    expect(state.setup?.executed[0]).toMatchObject({ ok: true })
    expect(state.setup?.step_index).toBe(1)
    expect(state.setup?.phase).toBe('prerequisites')
  })

  it('setup.blocked asks, and the agent working again clears it', () => {
    let state = applyEvent(applyEvent(start(), runnerAttached()), planCreated())
    const blockedAt = state.last_seq + 1
    state = applyEvent(state, {
      room_id: SETUP_ROOM_ID,
      seq: blockedAt,
      type: 'setup.blocked',
      actor: { kind: 'agent', id: 'agent', name: 'Agent' },
      at: '2026-10-22T10:06:00Z',
      payload: { reason: 'attempts', step_index: 2, question: 'The Connect container keeps exiting. Can you check Docker?' },
    })
    expect(state.setup?.blocked).toEqual({
      reason: 'attempts',
      step_index: 2,
      question: 'The Connect container keeps exiting. Can you check Docker?',
    })

    state = applyEvent(state, {
      room_id: SETUP_ROOM_ID,
      seq: state.last_seq + 1,
      type: 'agent.status',
      actor: { kind: 'agent', id: 'agent', name: 'Agent' },
      at: '2026-10-22T10:07:00Z',
      payload: { state: 'working' },
    })
    expect(state.setup?.blocked).toBeUndefined()
  })

  it('a class only rises and pipeline.registered records who watches', () => {
    let state = applyEvent(start(), runnerAttached())
    const classed = (seq: number, cls: string): Event => ({
      room_id: SETUP_ROOM_ID,
      seq,
      type: 'setup.target_classed',
      actor: { kind: 'system', id: 'server', name: 'server' },
      at: '2026-10-22T10:02:00Z',
      payload: { target: 'db.acme:5432', class: cls, reason: 'person', by: 'server' },
    })
    state = applyEvent(state, classed(state.last_seq + 1, 'production'))
    state = applyEvent(state, classed(state.last_seq + 1, 'local'))
    expect(state.setup?.targets[0].class).toBe('production')

    state = applyEvent(state, {
      room_id: SETUP_ROOM_ID,
      seq: state.last_seq + 1,
      type: 'pipeline.registered',
      actor: { kind: 'human', id: 'arjun', name: 'Arjun' },
      at: '2026-10-22T10:20:00Z',
      payload: { pipeline_id: 'orders', watched_by: 'server' },
    })
    expect(state.setup?.watched_by).toBe('server')
  })

  it('setup.verified from an older plan is ignored', () => {
    let state = applyEvent(applyEvent(start(), runnerAttached()), planCreated())
    state = applyEvent(state, {
      room_id: SETUP_ROOM_ID,
      seq: state.last_seq + 1,
      type: 'setup.verified',
      actor: { kind: 'system', id: 'ladder', name: 'Ladder' },
      at: '2026-10-22T10:08:00Z',
      payload: { plan_id: 'pl_old', nonce_hash: 'x', rungs: [{ name: 'connector_running', ok: true }] },
    })
    expect(state.setup?.verified).toEqual([])
    state = applyEvent(state, {
      room_id: SETUP_ROOM_ID,
      seq: state.last_seq + 1,
      type: 'setup.verified',
      actor: { kind: 'system', id: 'ladder', name: 'Ladder' },
      at: '2026-10-22T10:09:00Z',
      payload: { plan_id: setupPlan.plan_id, nonce_hash: 'x', rungs: [{ name: 'connector_running', ok: true }] },
    })
    expect(state.setup?.verified).toEqual([{ name: 'connector_running', ok: true }])
  })
})

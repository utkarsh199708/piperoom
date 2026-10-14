import { describe, expect, it } from 'vitest'
import type { Event, RoomSnapshot } from '../api/types'
import { FIXTURE_ROOM_ID, fixtureEvents } from '../fixtures/player'
import { applyEvent, detectGap } from './reducer'
import { emptyRoomState, fromSnapshot } from './types'

const human = { kind: 'human' as const, id: 'arjun', name: 'Arjun' }
const agent = { kind: 'agent' as const, id: 'agent', name: 'Agent' }

function ev(
  seq: number,
  type: Event['type'],
  payload: unknown,
  actor: Event['actor'] = agent,
): Event {
  return {
    room_id: 'r1',
    seq,
    type,
    actor,
    payload,
    at: `2026-10-11T10:00:${String(seq).padStart(2, '0')}Z`,
  }
}

function base(seq = 0) {
  return { ...emptyRoomState('r1'), last_seq: seq }
}

describe('applyEvent — dedupe and gap', () => {
  it('ignores events with seq <= last_seq and returns the same state', () => {
    const s = base(5)
    const result = applyEvent(s, ev(5, 'agent.status', { state: 'working' }))
    expect(result).toBe(s)
    expect(applyEvent(s, ev(3, 'agent.status', { state: 'working' }))).toBe(s)
  })

  it('applies events with seq > last_seq and advances last_seq', () => {
    const s = base(5)
    const result = applyEvent(s, ev(6, 'agent.status', { state: 'working' }))
    expect(result.last_seq).toBe(6)
    expect(result.agent_status.state).toBe('working')
  })

  it('detects a seq gap', () => {
    expect(detectGap(5, ev(7, 'agent.status', {}))).toBe(true)
    expect(detectGap(5, ev(6, 'agent.status', {}))).toBe(false)
    expect(detectGap(5, ev(5, 'agent.status', {}))).toBe(false)
    expect(detectGap(5, ev(4, 'agent.status', {}))).toBe(false)
  })
})

describe('applyEvent — every event type', () => {
  it('room.created sets basics and makes the creator the driver', () => {
    const s = applyEvent(
      base(),
      ev(1, 'room.created', { title: 'T', severity: 'sev2', pipeline_id: 'orders' }, human),
    )
    expect(s.title).toBe('T')
    expect(s.severity).toBe('sev2')
    expect(s.pipeline_id).toBe('orders')
    expect(s.status).toBe('open')
    expect(s.driver?.id).toBe('arjun')
    expect(s.members[0].role).toBe('driver')
  })

  it('room.closed closes the room with a resolution', () => {
    const s = applyEvent(base(), ev(1, 'room.closed', { resolution: 'fixed' }, human))
    expect(s.status).toBe('closed')
    expect(s.resolution).toBe('fixed')
  })

  it('agent.status updates agent state', () => {
    const s = applyEvent(base(), ev(1, 'agent.status', { state: 'working', detail: 'step 2' }))
    expect(s.agent_status.state).toBe('working')
    expect(s.agent_status.detail).toBe('step 2')
  })

  it('agent.step.started opens a step', () => {
    const s = applyEvent(base(), ev(1, 'agent.step.started', { step_id: 's1', title: 'Check' }))
    expect(s.steps).toHaveLength(1)
    expect(s.steps[0].id).toBe('s1')
    expect(s.steps[0].title).toBe('Check')
  })

  it('agent.tool_call and agent.tool_result attach to the step', () => {
    let s = applyEvent(base(), ev(1, 'agent.step.started', { step_id: 's1', title: 'Check' }))
    s = applyEvent(s, ev(2, 'agent.tool_call', { step_id: 's1', tool_use_id: 'tu1', tool: 't', input: { a: 1 } }))
    s = applyEvent(
      s,
      ev(3, 'agent.tool_result', {
        step_id: 's1',
        tool_use_id: 'tu1',
        tool: 't',
        summary: 'done',
        is_error: true,
        took_ms: 12,
      }),
    )
    expect(s.steps[0].tool_calls).toHaveLength(1)
    expect(s.steps[0].tool_calls[0].summary).toBe('done')
    expect(s.steps[0].tool_calls[0].is_error).toBe(true)
  })

  it('agent.message replies to a comment via reply_to', () => {
    let s = applyEvent(
      base(),
      ev(1, 'comment.added', { comment_id: 'c1', text: 'hi', mentions_agent: true }, human),
    )
    s = applyEvent(s, ev(2, 'agent.message', { text: 'reply', reply_to: 'c1' }))
    expect(s.comments[0].replies).toHaveLength(1)
    expect(s.comments[0].replies[0].text).toBe('reply')
  })

  it('agent.message with step_id anchors to the step; standalone goes to messages', () => {
    let s = applyEvent(base(), ev(1, 'agent.step.started', { step_id: 's1', title: 'Check' }))
    s = applyEvent(s, ev(2, 'agent.message', { text: 'anchored', step_id: 's1' }))
    s = applyEvent(s, ev(3, 'agent.message', { text: 'standalone' }))
    expect(s.steps[0].messages).toHaveLength(1)
    expect(s.messages).toHaveLength(1)
  })

  it('agent.hypothesis updates the current best guess', () => {
    const s = applyEvent(
      base(),
      ev(1, 'agent.hypothesis', {
        text: 'guess',
        based_on_steps: ['s1'],
        confidence: 'high',
        confirmed: false,
      }),
    )
    expect(s.hypothesis?.text).toBe('guess')
    expect(s.hypothesis?.confidence).toBe('high')
  })

  it('comment.added appends a comment', () => {
    const s = applyEvent(
      base(),
      ev(1, 'comment.added', { comment_id: 'c1', step_id: 's1', text: 'hi', mentions_agent: false }, human),
    )
    expect(s.comments).toHaveLength(1)
    expect(s.comments[0].author.id).toBe('arjun')
  })

  it('steer.declined marks the suggestion declined with who and why', () => {
    let s = applyEvent(base(), ev(1, 'steer.suggested', { suggestion_id: 'g1', text: 'try X' }, { kind: 'human', id: 'kabir', name: 'Kabir' }))
    s = applyEvent(s, ev(2, 'steer.declined', { suggestion_id: 'g1', reason: 'not now' }, human))
    expect(s.suggestions[0]).toMatchObject({ status: 'declined', declined_by: human, reason: 'not now' })
    // An unknown id changes nothing.
    const t = applyEvent(s, ev(3, 'steer.declined', { suggestion_id: 'nope' }, human))
    expect(t.suggestions).toEqual(s.suggestions)
  })

  it('steer.suggested adds a pending suggestion; steer.sent with id marks it accepted', () => {
    let s = applyEvent(
      base(),
      ev(1, 'steer.suggested', { suggestion_id: 'g1', text: 'try X' }, human),
    )
    expect(s.suggestions).toHaveLength(1)
    expect(s.suggestions[0].status).toBe('pending')
    s = applyEvent(s, ev(2, 'steer.sent', { suggestion_id: 'g1', text: 'try X' }, human))
    expect(s.suggestions).toHaveLength(1)
    expect(s.suggestions[0].status).toBe('accepted')
  })

  it('control.requested and control.granted move the driver', () => {
    let s = applyEvent(base(), ev(1, 'control.requested', {}, human))
    expect(s.control_requests).toHaveLength(1)
    s = applyEvent(
      s,
      ev(2, 'control.granted', { to: human, lease_until: '2026-10-11T11:00:00Z' }),
    )
    expect(s.driver?.id).toBe('arjun')
    expect(s.control_requests).toHaveLength(0)
  })

  it('proposal.created + approvals: all approved flips status', () => {
    let s = applyEvent(
      base(),
      ev(1, 'proposal.created', {
        proposal_id: 'p1',
        title: 'fix',
        steps: [],
        risk: 'low',
        rollback: 'undo',
        required_layers: ['schema-registry', 'kafka-connect'],
      }),
    )
    expect(s.proposals[0].approvals['schema-registry']).toBe('pending')
    s = applyEvent(s, ev(2, 'approval.given', { proposal_id: 'p1', layer: 'schema-registry' }, human))
    expect(s.proposals[0].status).toBe('open')
    s = applyEvent(s, ev(3, 'approval.given', { proposal_id: 'p1', layer: 'kafka-connect' }, human))
    expect(s.proposals[0].status).toBe('approved')
  })

  it('approval.rejected flips status to rejected', () => {
    let s = applyEvent(
      base(),
      ev(1, 'proposal.created', {
        proposal_id: 'p1',
        title: 'fix',
        steps: [],
        risk: 'low',
        rollback: 'undo',
        required_layers: ['kafka-connect'],
      }),
    )
    s = applyEvent(s, ev(2, 'approval.rejected', { proposal_id: 'p1', layer: 'kafka-connect', reason: 'nope' }, human))
    expect(s.proposals[0].status).toBe('rejected')
    expect(s.proposals[0].approvals['kafka-connect']).toBe('rejected')
  })

  it('action.executed appends to the proposal', () => {
    let s = applyEvent(
      base(),
      ev(1, 'proposal.created', {
        proposal_id: 'p1',
        title: 'fix',
        steps: [],
        risk: 'low',
        rollback: 'undo',
        required_layers: [],
      }),
    )
    s = applyEvent(s, ev(2, 'action.executed', { proposal_id: 'p1', step_index: 0, ok: true, output: 'x' }))
    expect(s.proposals[0].executed).toHaveLength(1)
  })

  it('handoff.created + handoff.accepted moves the driver role', () => {
    let s = applyEvent(
      base(),
      ev(1, 'handoff.created', {
        handoff_id: 'h1',
        to: { kind: 'human', id: 'kabir', name: 'Kabir' },
        summary: { what_broke: '', why: '', done: [], ruled_out: [], open: [], risks: [] },
        stay_as_watcher: false,
      }, human),
    )
    expect(s.handoffs[0].status).toBe('pending')
    s = applyEvent(s, ev(2, 'handoff.accepted', { handoff_id: 'h1' }, { kind: 'human', id: 'kabir', name: 'Kabir' }))
    expect(s.handoffs[0].status).toBe('accepted')
    expect(s.driver?.id).toBe('kabir')
  })

  it('handoff.requested is in flight until handoff.created / handoff.failed with its request_id', () => {
    const to = { kind: 'human' as const, id: 'kabir', name: 'Kabir' }
    const summary = { what_broke: '', why: '', done: [], ruled_out: [], open: [], risks: [] }
    const requested = ev(
      1,
      'handoff.requested',
      { request_id: 'hr_1', to, stay_as_watcher: true, expires_at: '2026-10-11T10:03:00Z' },
      human,
    )
    let s = applyEvent(base(), requested)
    expect(s.handoff_request).toMatchObject({ request_id: 'hr_1', stay_as_watcher: true })

    // Another request's ending does not end this one.
    const other = applyEvent(s, ev(2, 'handoff.failed', { request_id: 'hr_other', reason: 'summary_failed' }, agent))
    expect(other.handoff_request?.request_id).toBe('hr_1')

    const done = applyEvent(
      s,
      ev(2, 'handoff.created', { handoff_id: 'h1', request_id: 'hr_1', to, summary, stay_as_watcher: true }, human),
    )
    expect(done.handoff_request).toBeNull()
    expect(done.handoffs).toHaveLength(1)

    s = applyEvent(s, ev(2, 'handoff.failed', { request_id: 'hr_1', reason: 'summary_failed' }, agent))
    expect(s.handoff_request).toBeNull()
    expect(s.handoff_failure).toEqual({ request_id: 'hr_1', reason: 'summary_failed' })
    // A new request clears the old failure.
    s = applyEvent(
      s,
      ev(3, 'handoff.requested', { request_id: 'hr_2', to, stay_as_watcher: false, expires_at: '2026-10-11T10:06:00Z' }, human),
    )
    expect(s.handoff_failure).toBeNull()
  })

  it('trace.pinned stores the trace', () => {
    const s = applyEvent(
      base(),
      ev(1, 'trace.pinned', { table: 'orders', key: 'id=1', stages: [{ layer: 'kafka', name: 't', status: 'seen' }] }, human),
    )
    expect(s.trace?.table).toBe('orders')
    expect(s.trace?.stages).toHaveLength(1)
  })

  it('records an activity entry for each applied event', () => {
    const s = applyEvent(base(), ev(1, 'agent.status', { state: 'working' }))
    expect(s.activity).toHaveLength(1)
    expect(s.activity[0].seq).toBe(1)
  })
})

describe('applyEvent — agent.usage (cost telemetry)', () => {
  const usage = {
    model: 'claude-opus-5-5',
    role: 'main',
    input_tokens: 1200,
    cache_creation_input_tokens: 0,
    cache_read_input_tokens: 41000,
    output_tokens: 900,
    latency_ms: 8123,
    cost_usd: 0.0412,
  }

  // A room with a step, a message and a full activity window.
  function busyRoom() {
    let s = applyEvent(base(), ev(1, 'room.created', { title: 'T', severity: 'sev2', pipeline_id: 'orders' }, human))
    s = applyEvent(s, ev(2, 'agent.step.started', { step_id: 's_1', title: 'Check the connector' }))
    s = applyEvent(s, ev(3, 'agent.message', { text: 'Looking at the connector' }))
    for (let seq = 4; seq <= 25; seq++) {
      s = applyEvent(s, ev(seq, 'agent.status', { state: seq % 2 ? 'working' : 'idle' }))
    }
    return s
  }

  it('leaves the activity feed, steps and messages unchanged and only advances last_seq', () => {
    const s = busyRoom()
    expect(s.activity).toHaveLength(20)
    const after = applyEvent(s, ev(26, 'agent.usage', usage))
    expect(after.last_seq).toBe(26)
    expect(after.activity).toBe(s.activity)
    expect(after.steps).toBe(s.steps)
    expect(after.messages).toBe(s.messages)
    expect({ ...after, last_seq: s.last_seq, insight: s.insight }).toEqual(s)
  })

  it('never takes a slot of the activity window or renders as a raw line', () => {
    let s = busyRoom()
    const window = s.activity.map((a) => a.seq)
    for (let seq = 26; seq <= 60; seq++) {
      s = applyEvent(s, ev(seq, 'agent.usage', { ...usage, role: seq % 2 ? 'main' : 'compaction' }))
    }
    expect(s.activity.map((a) => a.seq)).toEqual(window)
    expect(s.activity.some((a) => a.text.includes('agent.usage'))).toBe(false)
  })

  it('is not a gap: the next event applies normally after it', () => {
    const s = applyEvent(busyRoom(), ev(26, 'agent.usage', usage))
    const next = ev(27, 'agent.message', { text: 'Found it' })
    expect(detectGap(s.last_seq, next)).toBe(false)
    const after = applyEvent(s, next)
    expect(after.activity[0].seq).toBe(27)
    expect(after.messages.at(-1)?.text).toBe('Found it')
    // A replayed usage event is still deduped.
    expect(applyEvent(after, ev(26, 'agent.usage', usage))).toBe(after)
  })
})

describe('applyEvent — robustness', () => {
  it('ignores unknown event types without throwing', () => {
    const s = applyEvent(base(), ev(1, 'does.not.exist' as Event['type'], {}))
    expect(s.last_seq).toBe(1)
    expect(s.activity).toHaveLength(1)
    expect(s.activity[0].text).toBe('does.not.exist')
  })

  it('tolerates null slices in proposal/hypothesis/trace payloads', () => {
    let s = base()
    s = applyEvent(
      s,
      ev(1, 'proposal.created', {
        proposal_id: 'p1',
        title: 'fix',
        steps: null,
        risk: 'low',
        rollback: 'undo',
        required_layers: null,
      }),
    )
    expect(s.proposals[0].steps).toEqual([])
    expect(s.proposals[0].required_layers).toEqual([])

    s = applyEvent(
      s,
      ev(2, 'agent.hypothesis', {
        text: 'g',
        based_on_steps: null,
        confidence: 'high',
        confirmed: false,
      }),
    )
    expect(s.hypothesis?.based_on_steps).toEqual([])

    s = applyEvent(s, ev(3, 'trace.pinned', { table: 't', key: 'k', stages: null }))
    expect(s.trace?.stages).toEqual([])
  })
})

describe('applyEvent — control.granted wording', () => {
  const arjun = { kind: 'human' as const, id: 'arjun', name: 'Arjun' }
  const kabir = { kind: 'human' as const, id: 'kabir', name: 'Kabir' }
  const created = (seq = 1) =>
    ev(seq, 'room.created', { title: 'T', severity: 'sev2', pipeline_id: 'orders' }, arjun)

  it('a self-grant by the sitting driver reads as a lease renewal', () => {
    let s = applyEvent(base(), created())
    s = applyEvent(
      s,
      ev(2, 'control.granted', { to: arjun, lease_until: '2026-10-11T10:30:00Z' }, arjun),
    )
    expect(s.activity[0].text).toBe('Arjun renewed the lease')
    expect(s.driver?.id).toBe('arjun')
  })

  it('a self-grant by a non-driver reads as taking control', () => {
    let s = applyEvent(base(), created())
    s = applyEvent(
      s,
      ev(2, 'control.granted', { to: kabir, lease_until: '2026-10-11T10:35:00Z' }, kabir),
    )
    expect(s.activity[0].text).toBe('Kabir took control')
    expect(s.driver?.id).toBe('kabir')
  })

  it('a grant to another user still reads as a hand-over', () => {
    let s = applyEvent(base(), created())
    s = applyEvent(
      s,
      ev(2, 'control.granted', { to: kabir, lease_until: '2026-10-11T10:35:00Z' }, arjun),
    )
    expect(s.activity[0].text).toBe('Arjun handed control to Kabir')
  })
})

describe('applyEvent — room.created from an alert webhook', () => {
  const system = { kind: 'system' as const, id: 'alertin', name: 'Alert webhook' }

  it('keeps the room driverless and stores the detected alert source', () => {
    const s = applyEvent(
      base(),
      ev(
        1,
        'room.created',
        {
          title: 'ConsumerLag firing',
          severity: 'sev1',
          pipeline_id: 'orders',
          alert: { version: '4', groupKey: 'g', alerts: [{ status: 'firing' }] },
        },
        system,
      ),
    )
    expect(s.driver).toBeNull()
    expect(s.alert).toEqual({ source: 'Alertmanager' })
  })

  it('leaves alert null without an alert body, and source null for unknown shapes', () => {
    const plain = applyEvent(
      base(),
      ev(1, 'room.created', { title: 'T', severity: 'sev2', pipeline_id: 'orders' }, human),
    )
    expect(plain.alert).toBeNull()

    const unknown = applyEvent(
      base(),
      ev(
        1,
        'room.created',
        { title: 'T', severity: 'sev2', pipeline_id: 'orders', alert: 'plain text alert' },
        system,
      ),
    )
    expect(unknown.alert).toEqual({ source: null })
  })
})

describe('webhook-alert fixture room', () => {
  it('replays to a driverless room whose header shows the alert source', () => {
    let s = emptyRoomState('r_webhook_alert')
    const events = fixtureEvents('r_webhook_alert')
    expect(events.length).toBeGreaterThan(0)
    for (const e of events) s = applyEvent(s, e)
    expect(s.status).toBe('open')
    expect(s.driver).toBeNull()
    expect(s.alert?.source).toBe('Alertmanager')
  })
})

describe('schema-change fixture activity', () => {
  it('renders seq 2 as a renewal and seq 33 as taking control', () => {
    let s = emptyRoomState(FIXTURE_ROOM_ID)
    const textBySeq = new Map<number, string>()
    for (const e of fixtureEvents(FIXTURE_ROOM_ID)) {
      s = applyEvent(s, e)
      textBySeq.set(e.seq, s.activity[0].text)
    }
    expect(s.last_seq).toBe(36)
    expect(textBySeq.get(2)).toBe('Arjun renewed the lease')
    expect(textBySeq.get(33)).toBe('Kabir took control')
  })
})

describe('applyEvent — membership and hand-off lease', () => {
  const kabir = { kind: 'human' as const, id: 'kabir', name: 'Kabir' }

  it('adds non-driver human actors as members when they comment or suggest', () => {
    let s = applyEvent(
      base(),
      ev(1, 'room.created', { title: 'T', severity: 'sev2', pipeline_id: 'orders' }, human),
    )
    s = applyEvent(
      s,
      ev(2, 'comment.added', { comment_id: 'c1', text: 'hi', mentions_agent: false }, kabir),
    )
    expect(s.members.some((m) => m.id === 'kabir' && m.role === 'watching')).toBe(true)

    // A driver who acts stays the driver (never downgraded to "watching").
    s = applyEvent(s, ev(3, 'steer.sent', { text: 'go' }, human))
    expect(s.members.find((m) => m.id === 'arjun')?.role).toBe('driver')
  })

  it('keeps the driver lease set by the control.granted before handoff.accepted', () => {
    let s = applyEvent(
      base(),
      ev(1, 'room.created', { title: 'T', severity: 'sev2', pipeline_id: 'orders' }, human),
    )
    s = applyEvent(
      s,
      ev(2, 'handoff.created', {
        handoff_id: 'h1',
        to: { kind: 'human', id: 'kabir', name: 'Kabir' },
        summary: { what_broke: '', why: '', done: [], ruled_out: [], open: [], risks: [] },
        stay_as_watcher: false,
      }, human),
    )
    s = applyEvent(
      s,
      ev(3, 'control.granted', {
        to: { kind: 'human', id: 'kabir', name: 'Kabir' },
        lease_until: '2026-10-11T10:35:00Z',
      }, kabir),
    )
    s = applyEvent(s, ev(4, 'handoff.accepted', { handoff_id: 'h1' }, kabir))
    expect(s.driver?.id).toBe('kabir')
    expect(s.driver?.lease_until).toBe('2026-10-11T10:35:00Z')
  })
})

describe('params_hash and approver identity (auth PR 4)', () => {
  const HASH = '9f2c1e7ab04d58c3d6e1f0a7b92c4d835e6a1f0b7c3d9e2a48b5c6d7e8f90a1b'
  const created = {
    proposal_id: 'p1',
    title: 'fix',
    steps: [],
    risk: 'low',
    rollback: 'undo',
    required_layers: ['schema-registry', 'kafka-connect'],
  }
  const identity = { iss: 'https://idp', sub: 'u-1', session_id: 's-1', method: 'oidc', grant: 'user:priya' }

  it('keeps params_hash from proposal.created', () => {
    const s = applyEvent(base(), ev(1, 'proposal.created', { ...created, params_hash: HASH }))
    expect(s.proposals[0].params_hash).toBe(HASH)
  })

  it('falls back to the snapshot hash for a proposal.created written before the hash existed', () => {
    const snap: RoomSnapshot = {
      id: 'r1',
      title: 'T',
      severity: 'sev2',
      pipeline_id: 'orders',
      status: 'open',
      opened_at: '2026-10-11T10:00:00Z',
      last_seq: 5,
      driver: null,
      agent_status: { state: 'idle' },
      hypothesis: null,
      open_proposals: [
        { ...created, approvals: { 'schema-registry': 'pending', 'kafka-connect': 'pending' }, params_hash: HASH },
      ],
      pending_suggestions: [],
      members: [],
    }
    const seeded = fromSnapshot(snap)
    expect(seeded.snapshot_hashes).toEqual({ p1: HASH })
    expect(applyEvent(seeded, ev(1, 'proposal.created', created)).proposals[0].params_hash).toBe(HASH)
    // The event's own hash wins when both are there.
    const other = 'ab'.repeat(32)
    expect(
      applyEvent(seeded, ev(1, 'proposal.created', { ...created, params_hash: other })).proposals[0].params_hash,
    ).toBe(other)
  })

  it('ignores a second proposal.created with the same proposal_id and keeps the first', () => {
    const first = applyEvent(base(), ev(1, 'proposal.created', { ...created, params_hash: HASH }))
    const decided = applyEvent(
      first,
      ev(2, 'approval.given', { proposal_id: 'p1', layer: 'schema-registry', params_hash: HASH, identity }, human),
    )
    const dup = applyEvent(
      decided,
      ev(3, 'proposal.created', { ...created, title: 'replayed', params_hash: 'ab'.repeat(32) }),
    )
    expect(dup.proposals).toHaveLength(1)
    expect(dup.proposals[0].title).toBe('fix')
    expect(dup.proposals[0].params_hash).toBe(HASH)
    expect(dup.proposals[0].approvals['schema-registry']).toBe('approved')
    // A different id is still added.
    const other = applyEvent(dup, ev(4, 'proposal.created', { ...created, proposal_id: 'p2' }))
    expect(other.proposals.map((p) => p.proposal_id)).toEqual(['p1', 'p2'])
  })

  it('is empty, not undefined, when neither the event nor the snapshot has a hash', () => {
    const s = applyEvent(base(), ev(1, 'proposal.created', created))
    expect(s.proposals[0].params_hash).toBe('')
  })

  it('keeps the identity and hash of an approval, per layer', () => {
    let s = applyEvent(base(), ev(1, 'proposal.created', { ...created, params_hash: HASH }))
    s = applyEvent(
      s,
      ev(2, 'approval.given', { proposal_id: 'p1', layer: 'schema-registry', params_hash: HASH, identity }, human),
    )
    expect(s.proposals[0].decisions['schema-registry']).toEqual({
      outcome: 'approved',
      by: human,
      identity,
      params_hash: HASH,
      at: '2026-10-11T10:00:02Z',
    })
    expect(s.proposals[0].decisions['kafka-connect']).toBeUndefined()
    // An approval without identity (an old event) is still recorded.
    s = applyEvent(s, ev(3, 'approval.given', { proposal_id: 'p1', layer: 'kafka-connect' }, human))
    expect(s.proposals[0].decisions['kafka-connect']?.identity).toBeUndefined()
    expect(s.proposals[0].status).toBe('approved')
  })

  it('keeps the identity, hash and reason of a rejection', () => {
    let s = applyEvent(base(), ev(1, 'proposal.created', { ...created, params_hash: HASH }))
    s = applyEvent(
      s,
      ev(
        2,
        'approval.rejected',
        { proposal_id: 'p1', layer: 'kafka-connect', reason: 'nope', params_hash: HASH, identity },
        human,
      ),
    )
    expect(s.proposals[0].decisions['kafka-connect']).toEqual({
      outcome: 'rejected',
      by: human,
      identity,
      params_hash: HASH,
      reason: 'nope',
      at: '2026-10-11T10:00:02Z',
    })
  })

  it('keeps params_hash on the action result', () => {
    let s = applyEvent(base(), ev(1, 'proposal.created', { ...created, params_hash: HASH }))
    s = applyEvent(
      s,
      ev(2, 'action.executed', { proposal_id: 'p1', step_index: 0, ok: true, output: 'x', params_hash: HASH }),
    )
    expect(s.proposals[0].executed[0].params_hash).toBe(HASH)
  })
})

describe('fromSnapshot', () => {
  it('defaults null slices and starts last_seq at 0 for the full event replay', () => {
    const snap: RoomSnapshot = {
      id: 'r1',
      title: 'T',
      severity: 'sev2',
      pipeline_id: 'orders',
      status: 'open',
      opened_at: '2026-10-11T10:00:00Z',
      last_seq: 42,
      driver: null,
      agent_status: { state: 'idle' },
      hypothesis: {
        text: 'g',
        based_on_steps: null as unknown as string[],
        confidence: 'high',
        confirmed: false,
      },
      open_proposals: null as unknown as RoomSnapshot['open_proposals'],
      pending_suggestions: null as unknown as RoomSnapshot['pending_suggestions'],
      members: null as unknown as RoomSnapshot['members'],
    }
    const s = fromSnapshot(snap)
    expect(s.last_seq).toBe(0)
    expect(s.hypothesis?.based_on_steps).toEqual([])
    expect(s.proposals).toEqual([])
    expect(s.suggestions).toEqual([])
    expect(s.members).toEqual([])
  })
})

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

  it('steer.suggested adds a pending suggestion; steer.sent with id removes it', () => {
    let s = applyEvent(
      base(),
      ev(1, 'steer.suggested', { suggestion_id: 'g1', text: 'try X' }, human),
    )
    expect(s.suggestions).toHaveLength(1)
    s = applyEvent(s, ev(2, 'steer.sent', { suggestion_id: 'g1', text: 'try X' }, human))
    expect(s.suggestions).toHaveLength(0)
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
    expect(s.last_seq).toBe(34)
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

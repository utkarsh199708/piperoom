import { afterEach, describe, expect, it, vi } from 'vitest'
import { render, screen } from '@testing-library/react'
import type { Event } from '@/api/types'
import { applyEvent } from '@/state/reducer'
import { emptyRoomState } from '@/state/types'
import type { ProposalState, RoomState } from '@/state/types'
import { RoomHeader } from './RoomHeader'
import { setupState, pairedEvents } from '@/test/setupRoom'

const SYSTEM = { kind: 'system', id: 'system', name: 'Piperoom' } as const

function roomTitled(title: string): RoomState {
  const event: Event = {
    room_id: 'r_1',
    seq: 1,
    type: 'room.created',
    actor: SYSTEM,
    payload: { title, severity: 'sev2', pipeline_id: 'orders' },
    at: '2026-10-21T11:20:00Z',
  }
  return applyEvent(emptyRoomState('r_1'), event)
}

function setWidth(px: number) {
  vi.stubGlobal('matchMedia', (q: string) => {
    const min = /min-width:\s*(\d+)px/.exec(q)
    return {
      matches: min ? px >= Number(min[1]) : false,
      media: q,
      addEventListener: () => undefined,
      removeEventListener: () => undefined,
    }
  })
}

function header(state: RoomState) {
  return render(
    <RoomHeader
      state={state}
      user={{ id: 'kabir', name: 'Kabir', layers: ['kafka'] }}
      members={[]}
      conn="live"
      copied={false}
      onHandoff={() => undefined}
      onCloseRoom={() => undefined}
      onInvite={() => undefined}
    />,
  )
}

afterEach(() => {
  vi.unstubAllGlobals()
})

describe('room header title', () => {
  // jsdom does no layout, so the wrap is checked as the class the layout depends on: the title may
  // break inside its box (break-words) and its flex item may shrink below its content (min-w-0).
  it.each([
    ['desktop', 1280],
    ['phone', 375],
  ])('wraps a 200-character unbroken title inside the heading at %s width', (_, px) => {
    setWidth(px)
    const title = 'W'.repeat(200)
    header(roomTitled(title))
    const heading = screen.getByRole('heading', { level: 1 })
    expect(heading).toHaveTextContent(title)
    expect(heading).toHaveClass('min-w-0', 'break-words')
  })
})

describe('room header alert badge', () => {
  it('shows no alert or verification badge on a setup room', () => {
    setWidth(1280)
    const state = setupState({ events: pairedEvents() })
    expect(state.alert).not.toBeNull()
    header(state)
    expect(screen.queryByText(/^alert/)).not.toBeInTheDocument()
    expect(screen.queryByText('unverified')).not.toBeInTheDocument()
  })

  it('still shows the dev webhook alert chip, unverified, on a webhook room', () => {
    setWidth(1280)
    const state: RoomState = { ...roomTitled('Orders down'), alert: { source: null, alert_source: 'webhook' } }
    header(state)
    expect(screen.getByText('alert · dev webhook')).toBeInTheDocument()
    expect(screen.getAllByText('unverified').length).toBeGreaterThan(0)
  })

  it('shows no alert chip on a room opened from New room', () => {
    setWidth(1280)
    header(roomTitled('Manual room'))
    expect(screen.queryByText(/^alert/)).not.toBeInTheDocument()
  })
})

describe('room header status sentence (KAN-600)', () => {
  const step = { id: 's1', title: 't', tool_calls: [], messages: [] }
  const steps = (n: number): ProposalState['steps'] => Array.from({ length: n }, () => ({ action: 'a', layer: 'kafka-connect', params: {}, description: 'd' }))
  const ran = (...oks: boolean[]): ProposalState['executed'] => oks.map((ok, i) => ({ proposal_id: 'p1', step_index: i, ok, output: '' }))
  const proposal = (over: Partial<ProposalState>): ProposalState => ({
    proposal_id: 'p1',
    title: 'fix',
    steps: [],
    risk: 'low',
    rollback: 'undo',
    required_layers: ['kafka-connect'],
    approvals: { 'kafka-connect': 'approved' },
    decisions: {},
    params_hash: '',
    status: 'approved',
    executed: [],
    ...over,
  })
  const room = (over: Partial<RoomState>): RoomState => ({
    ...roomTitled('Orders stalled'),
    status: 'open',
    opened_at: '2026-10-21T11:20:00Z',
    agent_status: { state: 'waiting_approval', detail: 'waiting on the schema layer' },
    ...over,
  })
  const visibleCount = (text: string) => screen.queryAllByText(text).filter((el) => !el.closest('.sr-only')).length
  const pillOf = (container: HTMLElement) => container.querySelector('[data-tone]') as HTMLElement

  it.each([
    [
      'a fix that ran and recovered',
      room({
        proposals: [
          proposal({
            steps: steps(2),
            executed: ran(true, true),
            verification: { proposal_id: 'p1', checks: [], recovered: true, verdict: 'recovered: all good' },
          }),
        ],
      }),
      'Fix ran · recovered',
    ],
    [
      'a fix that failed at a step',
      room({ proposals: [proposal({ steps: steps(1), executed: ran(false) })] }),
      'Fix failed · step 1',
    ],
    [
      'an approved proposal held back by write mode',
      room({ proposals: [proposal({ blocked: 'write_mode_off' })] }),
      'Approved · waiting for write mode',
    ],
  ])('%s shows its sentence once, and the pill keeps its live region', (_, state, sentence) => {
    setWidth(1280)
    const { container } = header(state)
    expect(visibleCount(sentence)).toBe(1)
    // The announcement stays: the pill is visually hidden, not removed.
    const pill = pillOf(container)
    expect(pill).toHaveClass('sr-only')
    expect(pill.querySelector('[aria-live="polite"]')).toHaveTextContent(sentence)
    // The agent's last detail line is stale by now ("layer x approved your proposal"), so it isn't kept as a tooltip.
    expect(screen.getByText(sentence, { selector: '[data-slot="badge"]' })).not.toHaveAttribute('title')
  })

  it('a room waiting for approval shows the badge and the pill, each with its own sentence', () => {
    setWidth(1280)
    const open = proposal({ status: 'open', approvals: { 'kafka-connect': 'pending' } })
    const { container } = header(room({ proposals: [open] }))
    expect(screen.getByText(/^Needs approval/)).toBeInTheDocument()
    expect(screen.getByText('Agent waiting · 0 of 1 approvals')).toBeInTheDocument()
    expect(pillOf(container)).not.toHaveClass('sr-only')
  })

  it('an agent at work shows the badge and the pill', () => {
    setWidth(1280)
    const { container } = header(room({ agent_status: { state: 'working' }, steps: [step], driver: { id: 'kabir', name: 'Kabir', kind: 'human' } as RoomState['driver'] }))
    expect(screen.getByText('Investigating · step 1')).toBeInTheDocument()
    expect(screen.getByText('Agent working')).toBeInTheDocument()
    expect(pillOf(container)).not.toHaveClass('sr-only')
  })

  it('"Executing fix" in the pill is not hidden by "Executing fix · 1 of 2" in the badge', () => {
    setWidth(1280)
    const running = proposal({ steps: steps(2), executed: ran(true) })
    const { container } = header(room({ proposals: [running] }))
    expect(screen.getByText('Executing fix · 2 of 2')).toBeInTheDocument()
    expect(screen.getByText('Executing fix')).toBeInTheDocument()
    expect(pillOf(container)).not.toHaveClass('sr-only')
  })
})

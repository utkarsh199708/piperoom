import { afterEach, describe, expect, it, vi } from 'vitest'
import { render, screen } from '@testing-library/react'
import type { Event } from '@/api/types'
import { applyEvent } from '@/state/reducer'
import { emptyRoomState } from '@/state/types'
import type { RoomState } from '@/state/types'
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

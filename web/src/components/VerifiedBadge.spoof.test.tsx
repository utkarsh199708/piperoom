import { describe, expect, it, vi } from 'vitest'
import { render, screen } from '@testing-library/react'
import { SteerBox } from './SteerBox'
import { SuggestionsPanel } from './room/SuggestionsPanel'
import { HypothesisCard } from './room/HypothesisCard'
import { WaitingFor } from './room/WaitingFor'
import { emptyRoomState } from '../state/types'
import type { RoomActions } from '../hooks/useRoom'

// A person sets their own display name, so a name like this must never read as the server's badge.
const SPOOF = 'Mal ✓ verified'
const mal = { kind: 'human' as const, id: 'u_9', name: SPOOF }
const actions = {} as unknown as RoomActions
const badgeStates = (root: HTMLElement) =>
  Array.from(root.querySelectorAll<HTMLElement>('[data-slot="verified-badge"]')).map((b) => b.dataset.verification)

describe('names added since the badge PR cannot fake the badge', () => {
  it('SteerBox: the driver in the status line and a queued suggester', () => {
    const state = {
      ...emptyRoomState('r1'),
      driver: { id: 'u_9', name: SPOOF },
      suggestions: [],
    }
    const { container } = render(
      <SteerBox state={state} user={{ id: 'kabir', name: 'Kabir', layers: [] }} actions={actions} />,
    )
    expect(badgeStates(container)).toEqual(['unverified'])
    expect(container).not.toHaveTextContent('✓')
    expect(screen.getByPlaceholderText(/Suggest a step for/).getAttribute('placeholder')).not.toMatch(/✓|verified/i)
  })

  it('SuggestionsPanel: the suggester gets the real badge and no look-alike', () => {
    const { container } = render(
      <SuggestionsPanel
        suggestions={[{ suggestion_id: 's1', from: mal, text: 'try x', status: 'pending', at: '2026-10-14T10:00:00Z' }]}
      />,
    )
    expect(badgeStates(container)).toEqual(['unverified'])
    expect(container).not.toHaveTextContent('✓')
    expect(container).toHaveTextContent('(u_9)')
  })

  it('HypothesisCard: "confirmed by" shows the badge, not the name’s', () => {
    const { container } = render(
      <HypothesisCard
        hypothesis={{
          text: 'h',
          based_on_steps: [],
          confidence: 'low',
          confirmed: true,
          seq: 1,
          verdicts: [{ verdict: 'confirmed', by: mal, at: '2026-10-14T10:00:00Z' }],
        }}
        steps={[]}
      />,
    )
    expect(badgeStates(container)).toEqual(['unverified'])
    expect(container).not.toHaveTextContent('✓')
  })

  it('WaitingFor: an owner whose name fakes a badge shows the unverified one', () => {
    const { container } = render(
      <WaitingFor
        proposalId="p1"
        layer="kafka-connect"
        roomId="r1"
        roomTitle="t"
        people={[{ id: 'u_9', name: SPOOF, layers: ['kafka-connect'] }]}
        presence={[]}
        canNudge={false}
        actions={{ nudge: vi.fn() } as unknown as RoomActions}
      />,
    )
    expect(badgeStates(container)).toEqual(['unverified'])
    expect(container).not.toHaveTextContent('✓')
  })
})

describe('status line structure (layout regression)', () => {
  it('keeps the name in a clipping box and the tail in its own truncating box', () => {
    const state = { ...emptyRoomState('r1'), driver: { id: 'arjun', name: 'Arjun' } }
    const { container } = render(
      <SteerBox state={state} user={{ id: 'kabir', name: 'Kabir', layers: [] }} actions={actions} />,
    )
    const status = container.querySelector('[data-slot="steer-status"]')!
    const person = status.querySelector('[data-slot="person-name"]')!
    expect(person.className).toMatch(/overflow-hidden/)
    expect(person).toHaveTextContent('Arjun')
    // The tail must be able to shrink: a nowrap flex child that cannot would squeeze the name to 0px.
    const tail = status.querySelector('[data-slot="steer-status-tail"]')!
    expect(tail.className).toMatch(/min-w-0/)
    expect(tail.className).toMatch(/truncate/)
    expect(tail.parentElement).toBe(status)
  })
})

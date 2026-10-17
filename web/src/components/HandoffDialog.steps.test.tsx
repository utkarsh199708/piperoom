import { describe, expect, it } from 'vitest'
import { render, screen, within } from '@testing-library/react'
import { HANDOFF_READ_ID, HandoffAcceptBanner, HandoffDialog } from './HandoffDialog'
import type { RoomActions } from '../hooks/useRoom'
import type { CurrentUser } from '../state/permissions'
import { emptyRoomState } from '../state/types'
import type { HandoffState, StepState } from '../state/types'

// Real step id: "s_" and 26 characters of the ULID alphabet.
const STEP = 's_01M4Y9X0ZPZ0NPJW00JGWVY8VP'
const steps: StepState[] = [{ id: STEP, title: 'connect_status', tool_calls: [], messages: [] }]

// The dialogs only read actions when a button is pressed, so these tests need none.
const actions = {} as unknown as RoomActions

const handoff: HandoffState = {
  handoff_id: 'h1',
  to: { kind: 'human', id: 'priya', name: 'Priya' },
  summary: { what_broke: `Lag began at ${STEP}`, why: '', done: [], ruled_out: [], open: [], risks: [] },
  stay_as_watcher: false,
  status: 'pending',
  from: { kind: 'human', id: 'arjun', name: 'Arjun' },
  created_at: '2026-10-15T10:00:00Z',
}

describe('hand-off summary step links', () => {
  it('the driver reads a step id in the summary as a link to that step', () => {
    const state = { ...emptyRoomState('r1'), steps, handoffs: [handoff] }
    const user: CurrentUser = { id: 'arjun', name: 'Arjun', layers: [] }
    render(<HandoffDialog state={state} user={user} actions={actions} onClose={() => {}} />)
    expect(screen.getByRole('link', { name: 'step 1' })).toHaveAttribute('href', `#step-${STEP}`)
    expect(document.body.textContent).not.toContain(STEP)
  })

  it('the receiver reads it the same way, before accepting', () => {
    const state = { ...emptyRoomState('r1'), driver: { id: 'arjun', name: 'Arjun' }, steps, handoffs: [handoff] }
    const me: CurrentUser = { id: 'priya', name: 'Priya', layers: [] }
    render(<HandoffAcceptBanner state={state} user={me} actions={actions} />)
    // The receiver's card opens inline, summary first, Accept under it (F2a).
    const card = document.getElementById(HANDOFF_READ_ID) as HTMLElement
    expect(within(card).getByRole('link', { name: 'step 1' })).toHaveAttribute('href', `#step-${STEP}`)
  })
})

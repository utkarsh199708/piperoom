// QA R17 (G11): a step link inside the hand-off dialog closes the dialog and then shows the step.
import { describe, expect, it, vi } from 'vitest'
import { fireEvent, render, screen, waitFor } from '@testing-library/react'
import { HandoffDialog } from './HandoffDialog'
import type { RoomActions } from '../hooks/useRoom'
import type { CurrentUser } from '../state/permissions'
import { emptyRoomState } from '../state/types'
import type { HandoffState, StepState } from '../state/types'

const STEP = 's_01M4Y9X0ZPZ0NPJW00JGWVY8VP'
const steps: StepState[] = [{ id: STEP, title: 'connect_status', tool_calls: [], messages: [] }]
const actions = {} as unknown as RoomActions
const handoff: HandoffState = {
  handoff_id: 'h1',
  to: { kind: 'human', id: 'priya', name: 'Priya' },
  summary: { what_broke: `Lag began at ${STEP}`, why: '', done: [], ruled_out: [], open: [], risks: [] },
  stay_as_watcher: false,
  status: 'pending',
}

describe('hand-off dialog step links', () => {
  it('following a step link asks the dialog to close, then highlights the step on the page', async () => {
    Element.prototype.scrollIntoView = vi.fn()
    const onClose = vi.fn()
    const state = { ...emptyRoomState('r1'), steps, handoffs: [handoff] }
    const user: CurrentUser = { id: 'arjun', name: 'Arjun', layers: [] }
    render(
      <>
        <li id={`step-${STEP}`} data-testid="step" />
        <HandoffDialog state={state} user={user} actions={actions} onClose={onClose} />
      </>,
    )
    fireEvent.click(screen.getByRole('link', { name: 'step 1' }))
    expect(onClose).toHaveBeenCalled()
    await waitFor(() => expect(screen.getByTestId('step').className).toContain('ring-2'))
  })
})

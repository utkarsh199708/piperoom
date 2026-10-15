// Q1-web-b (G4): a hand-off whose summary the agent could not write, and the summary the driver writes instead.
import { describe, expect, it, vi } from 'vitest'
import { fireEvent, render, screen, waitFor, within } from '@testing-library/react'
import { HandoffAcceptBanner, HandoffDialog } from './HandoffDialog'
import type { RoomActions } from '../hooks/useRoom'
import type { HandoffResult } from '../hooks/useRoom'
import type { CurrentUser } from '../state/permissions'
import { emptyRoomState } from '../state/types'
import type { HandoffState, RoomState } from '../state/types'

vi.mock('../hooks/usePeople', () => ({
  usePeople: () => ({ status: 'ready', people: [{ id: 'priya', name: 'Priya' }], reload: () => undefined }),
  personLabel: (p: { name: string; id: string }) => `${p.name} (${p.id})`,
}))

const arjun: CurrentUser = { id: 'arjun', name: 'Arjun', layers: [] }

function actionsWith(handoff: (...a: unknown[]) => Promise<HandoffResult>) {
  return { handoff: vi.fn(handoff) } as unknown as RoomActions & { handoff: ReturnType<typeof vi.fn> }
}

function open(state: RoomState, actions: RoomActions) {
  render(<HandoffDialog state={state} user={arjun} actions={actions} onClose={() => undefined} />)
}

const failedRoom = (reason = 'summary_failed') => ({ ...emptyRoomState('r1'), handoff_failure: { request_id: 'hr_1', reason } })
const offer = () => screen.queryByRole('button', { name: 'Write the summary yourself' })
const fill = (label: string, value: string) => fireEvent.change(screen.getByLabelText(new RegExp(`^${label}`)), { target: { value } })

describe('when the agent could not write the summary', () => {
  it('the log says so: a clear message and an offer to write it yourself', () => {
    open(failedRoom(), actionsWith(async () => ({ ok: true })))
    expect(screen.getByRole('alert')).toHaveTextContent('The agent could not write the summary. Try again, or write it yourself.')
    expect(offer()).toBeInTheDocument()
    expect(screen.getByRole('button', { name: /Start hand-off/ })).toBeInTheDocument() // trying again stays possible
  })

  it.each(['not_driver', 'room_closed', 'internal_error'])('%s does not offer it', (reason) => {
    open(failedRoom(reason), actionsWith(async () => ({ ok: true })))
    expect(offer()).toBeNull()
  })

  it('no failure, no offer', () => {
    open(emptyRoomState('r1'), actionsWith(async () => ({ ok: true })))
    expect(offer()).toBeNull()
  })

  it('503 summary_failed from the request itself shows the same message and offer', async () => {
    const actions = actionsWith(async () => ({ ok: false, code: 'summary_failed', error: 'the server wording' }))
    open(emptyRoomState('r1'), actions)
    fireEvent.click(screen.getByRole('button', { name: /Start hand-off/ }))
    expect(await screen.findByRole('alert')).toHaveTextContent('The agent could not write the summary. Try again, or write it yourself.')
    expect(actions.handoff).toHaveBeenCalledWith('priya', false)
    expect(offer()).toBeInTheDocument()
  })

  it('another failure of the request shows the server message and no offer', async () => {
    open(emptyRoomState('r1'), actionsWith(async () => ({ ok: false, code: 'handoff_in_progress', error: 'a hand-off is already being written' })))
    fireEvent.click(screen.getByRole('button', { name: /Start hand-off/ }))
    expect(await screen.findByRole('alert')).toHaveTextContent('a hand-off is already being written')
    expect(offer()).toBeNull()
  })

  it('a second try that works clears the message', async () => {
    let n = 0
    open(
      emptyRoomState('r1'),
      actionsWith(async () => (n++ === 0 ? { ok: false, code: 'summary_failed', error: 'x' } : { ok: true })),
    )
    fireEvent.click(screen.getByRole('button', { name: /Start hand-off/ }))
    await screen.findByRole('alert')
    fireEvent.click(screen.getByRole('button', { name: /Start hand-off/ }))
    await waitFor(() => expect(screen.queryByRole('alert')).toBeNull())
  })
})

describe('the summary the driver writes', () => {
  const goManual = (actions: RoomActions) => {
    open(failedRoom(), actions)
    fireEvent.click(screen.getByRole('button', { name: 'Write the summary yourself' }))
  }

  it('shows the six fields of a summary, keeps the person and watcher choices', () => {
    goManual(actionsWith(async () => ({ ok: true })))
    expect(screen.getByRole('dialog', { name: 'Write the hand-off summary' })).toBeInTheDocument()
    for (const label of ['What broke', 'Why', 'Done', 'Ruled out', 'Still open', 'Risks', 'Hand off to']) {
      expect(screen.getByLabelText(new RegExp(`^${label}`))).toBeInTheDocument()
    }
    expect(screen.getByLabelText('Stay as watcher after hand-off')).toBeInTheDocument()
    // the old failure message belongs to the agent form
    expect(screen.queryByText(/The agent could not write the summary/)).toBeNull()
  })

  it('sends the written summary with the person and the watcher choice', async () => {
    const actions = actionsWith(async () => ({ ok: true }))
    goManual(actions)
    fireEvent.click(screen.getByLabelText('Stay as watcher after hand-off'))
    fill('What broke', 'The orders connector stopped')
    fill('Why', 'orders.amount changed type')
    fill('Done', '- restarted the task\n- checked lag')
    fill('Risks', 'a replay may duplicate rows')
    fireEvent.click(screen.getByRole('button', { name: /Hand off with this summary/ }))
    await waitFor(() => expect(actions.handoff).toHaveBeenCalledTimes(1))
    expect(actions.handoff).toHaveBeenCalledWith('priya', true, {
      what_broke: 'The orders connector stopped',
      why: 'orders.amount changed type',
      done: ['restarted the task', 'checked lag'],
      ruled_out: [],
      open: [],
      risks: ['a replay may duplicate rows'],
    })
  })

  it('does not send an incomplete summary and says what is missing', () => {
    const actions = actionsWith(async () => ({ ok: true }))
    goManual(actions)
    fireEvent.click(screen.getByRole('button', { name: /Hand off with this summary/ }))
    const alert = screen.getByRole('alert')
    expect(alert).toHaveTextContent('Say what broke.')
    expect(alert).toHaveTextContent('Say why it broke')
    expect(actions.handoff).not.toHaveBeenCalled()
  })

  it('shows what the server refused, keeps every word, and lets the driver fix it', async () => {
    const actions = actionsWith(async () => ({ ok: false, code: 'validation', error: 'what_broke has a control character' }))
    goManual(actions)
    fill('What broke', 'b')
    fill('Why', 'w')
    fireEvent.click(screen.getByRole('button', { name: /Hand off with this summary/ }))
    expect(await screen.findByText('what_broke has a control character')).toBeInTheDocument()
    expect((screen.getByLabelText(/^What broke/) as HTMLTextAreaElement).value).toBe('b')
    expect(screen.getByRole('button', { name: /Hand off with this summary/ })).toBeEnabled()
  })

  it('Back returns to the agent form', () => {
    goManual(actionsWith(async () => ({ ok: true })))
    fireEvent.click(screen.getByRole('button', { name: 'Back' }))
    expect(screen.getByRole('button', { name: /Start hand-off/ })).toBeInTheDocument()
    expect(screen.queryByLabelText(/^What broke/)).toBeNull()
  })

  it('caps the two texts at 1000 characters', () => {
    goManual(actionsWith(async () => ({ ok: true })))
    expect(screen.getByLabelText(/^What broke/)).toHaveAttribute('maxlength', '1000')
    expect(screen.getByLabelText(/^Why/)).toHaveAttribute('maxlength', '1000')
  })
})

describe('a hand-off the driver wrote says so', () => {
  const summary = { what_broke: 'Connector stopped', why: 'schema', done: [], ruled_out: [], open: [], risks: [] }
  const handoff = (over: Partial<HandoffState> = {}): HandoffState => ({
    handoff_id: 'h1',
    to: { kind: 'human', id: 'priya', name: 'Priya' },
    summary,
    stay_as_watcher: false,
    status: 'pending',
    ...over,
  })

  it('the driver sees "Written by <driver>" on the pending summary', () => {
    const state = { ...emptyRoomState('r1'), handoffs: [handoff({ manual: true, written_by: { kind: 'human', id: 'arjun', name: 'Arjun' } })] }
    open(state, actionsWith(async () => ({ ok: true })))
    expect(screen.getByText('Written by Arjun, not by the agent.')).toBeInTheDocument()
  })

  it('an agent-written summary has no such line', () => {
    open({ ...emptyRoomState('r1'), handoffs: [handoff()] }, actionsWith(async () => ({ ok: true })))
    expect(screen.getByText('Connector stopped')).toBeInTheDocument()
    expect(screen.queryByText(/Written by/)).toBeNull()
  })

  it('the receiver sees it before accepting', () => {
    const me: CurrentUser = { id: 'priya', name: 'Priya', layers: [] }
    const state = {
      ...emptyRoomState('r1'),
      driver: { id: 'arjun', name: 'Arjun' },
      handoffs: [handoff({ manual: true, written_by: { kind: 'human', id: 'arjun', name: 'Arjun' } })],
    }
    render(<HandoffAcceptBanner state={state} user={me} actions={actionsWith(async () => ({ ok: true }))} />)
    fireEvent.click(screen.getByRole('button', { name: 'Read summary' }))
    const dialog = screen.getByRole('dialog', { name: 'Hand-off from Arjun' })
    expect(within(dialog).getByText('Written by Arjun, not by the agent.')).toBeInTheDocument()
  })

  it('the summary text is still plain text', () => {
    const html = '<a href="http://evil.example">x</a>'
    const state = {
      ...emptyRoomState('r1'),
      handoffs: [handoff({ manual: true, written_by: { kind: 'human', id: 'arjun', name: html }, summary: { ...summary, why: html } })],
    }
    open(state, actionsWith(async () => ({ ok: true })))
    expect(document.body.querySelector('.handoff__summary a')).toBeNull()
  })
})

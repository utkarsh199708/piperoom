import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { fireEvent, render, screen, waitFor } from '@testing-library/react'
import { ApiClientError, api } from '../../api/client'
import type { Event, ShadowResponse, ShadowRun } from '../../api/types'
import { emptyRoomState } from '../../state/types'
import type { RoomState } from '../../state/types'
import type { CurrentUser } from '../../state/permissions'
import { ShadowPanel } from './ShadowPanel'

const HYP = 'Schema compatibility was set to NONE, which broke the sink.'

const run = (over: Partial<ShadowRun> = {}): ShadowRun => ({
  room_id: 'r1',
  status: 'hypothesised',
  model: 'm',
  pipeline_map: '',
  started_at: '2026-10-14T10:00:00Z',
  first_hypothesis_at: '2026-10-14T10:04:00Z',
  final_hypothesis_at: '2026-10-14T10:05:00Z',
  finished_at: '2026-10-14T10:05:00Z',
  finished_after_close: false,
  final_hypothesis: { text: HYP, based_on_steps: [], confidence: 'high', confirmed: false },
  tool_calls: 7,
  tokens: 12000,
  revealed_at: '2026-10-14T11:00:00Z',
  ...over,
})

const ev = (type: string, payload: unknown, extra: Partial<Event> = {}): Event => ({
  room_id: 'r1',
  seq: 1,
  type: type as Event['type'],
  actor: { kind: 'human', id: 'u', name: 'U' },
  payload,
  at: '2026-10-14T11:01:00Z',
  ...extra,
})

const response = (over: Partial<ShadowResponse> = {}): ShadowResponse => ({
  run: run(),
  events: [],
  room: {
    labelled: ev('shadow.labelled', { actual_layer: 'schema-registry', root_cause_terms: ['compatibility'] }),
  },
  ...over,
})

const room: RoomState = {
  ...emptyRoomState('r1'),
  status: 'closed',
  shadow: true,
  shadow_revealed: true,
  resolution: 'Set compatibility back to BACKWARD.',
  opened_at: '2026-10-14T10:00:00Z',
  closed_at: '2026-10-14T10:52:00Z',
}
const member: CurrentUser = { id: 'meera', name: 'Meera', layers: ['warehouse'], role: 'member' }
const viewer: CurrentUser = { ...member, role: 'viewer' }

const notLabelled = () => new ApiClientError(409, 'not_labelled', 'label first')

beforeEach(() => {
  vi.restoreAllMocks()
})
afterEach(() => vi.restoreAllMocks())

describe('ShadowPanel', () => {
  it('hides the agent answer until the blind label is saved', async () => {
    const shadow = vi.spyOn(api, 'shadow').mockRejectedValueOnce(notLabelled())
    const blind = vi.spyOn(api, 'shadowBlind').mockResolvedValue(ev('shadow.labelled', {}))
    render(<ShadowPanel state={room} user={member} />)

    expect(await screen.findByRole('heading', { name: 'Record your answer first' })).toBeInTheDocument()
    expect(screen.queryByText(HYP)).not.toBeInTheDocument()
    const save = screen.getByRole('button', { name: /Save label/ })
    expect(save).toBeDisabled()

    fireEvent.click(screen.getByRole('radio', { name: 'schema-registry' }))
    fireEvent.change(screen.getByLabelText('Root cause terms'), { target: { value: 'compatibility\n\n  NONE mode ' } })
    expect(save).toBeEnabled()

    shadow.mockResolvedValueOnce(response())
    fireEvent.click(save)
    await waitFor(() =>
      expect(blind).toHaveBeenCalledWith('r1', {
        actual_layer: 'schema-registry',
        root_cause_terms: ['compatibility', 'NONE mode'],
      }),
    )
    expect(await screen.findByText(HYP)).toBeInTheDocument()
    expect(screen.getByRole('heading', { name: 'Your blind label' })).toBeInTheDocument()
  })

  it('shows the server error when the label is refused', async () => {
    vi.spyOn(api, 'shadow').mockRejectedValue(notLabelled())
    vi.spyOn(api, 'shadowBlind').mockRejectedValue(new ApiClientError(403, 'not_allowed', 'needs a member of its pipeline'))
    render(<ShadowPanel state={room} user={member} />)
    fireEvent.click(await screen.findByRole('radio', { name: 'kafka' }))
    fireEvent.change(screen.getByLabelText('Root cause terms'), { target: { value: 'lag' } })
    fireEvent.click(screen.getByRole('button', { name: /Save label/ }))
    expect(await screen.findByText('needs a member of its pipeline')).toBeInTheDocument()
    expect(screen.queryByText(HYP)).not.toBeInTheDocument()
  })

  it('reloads into the run when someone else labelled first (409 already_decided)', async () => {
    const shadow = vi.spyOn(api, 'shadow').mockRejectedValueOnce(notLabelled())
    vi.spyOn(api, 'shadowBlind').mockRejectedValue(new ApiClientError(409, 'already_decided', 'already labelled'))
    render(<ShadowPanel state={room} user={member} />)
    fireEvent.click(await screen.findByRole('radio', { name: 'kafka' }))
    fireEvent.change(screen.getByLabelText('Root cause terms'), { target: { value: 'lag' } })
    shadow.mockResolvedValueOnce(response())
    fireEvent.click(screen.getByRole('button', { name: /Save label/ }))
    expect(await screen.findByText(HYP)).toBeInTheDocument()
  })

  it('gives a viewer no forms before the label', async () => {
    vi.spyOn(api, 'shadow').mockRejectedValue(notLabelled())
    render(<ShadowPanel state={room} user={viewer} />)
    expect(await screen.findByText(/Only a member of this pipeline can label/)).toBeInTheDocument()
    expect(screen.queryByRole('button', { name: /Save label/ })).not.toBeInTheDocument()
  })

  it('grades the run and sends notes only when given', async () => {
    // After the save the panel refetches and the server now returns the grade (a new seq).
    const shadow = vi.spyOn(api, 'shadow').mockResolvedValueOnce(response())
    shadow.mockResolvedValue(
      response({
        room: {
          labelled: ev('shadow.labelled', { actual_layer: 'schema-registry', root_cause_terms: ['compatibility'] }),
          graded: ev('shadow.graded', { layer_correct: true, root_cause: 'partial' }, { seq: 9 }),
        },
      }),
    )
    const grade = vi.spyOn(api, 'shadowGrade').mockResolvedValue(ev('shadow.graded', {}))
    render(<ShadowPanel state={room} user={member} />)
    await screen.findByText(HYP)

    const save = screen.getByRole('button', { name: 'Save grade' })
    expect(save).toBeDisabled()
    fireEvent.click(screen.getByRole('radio', { name: 'Yes' }))
    fireEvent.click(screen.getByRole('radio', { name: 'Partly' }))
    expect(save).toBeEnabled()
    fireEvent.click(save)
    await waitFor(() => expect(grade).toHaveBeenCalledWith('r1', { layer_correct: true, root_cause: 'partial' }))
    // The confirmation must survive the refetch that brings the new grade in.
    expect(await screen.findByText('Grade saved.')).toBeInTheDocument()
    await waitFor(() => expect(shadow).toHaveBeenCalledTimes(2))
    expect(screen.getByText('Grade saved.')).toBeInTheDocument()
    expect(screen.getByRole('button', { name: 'Update grade' })).toBeInTheDocument()
  })

  it('starts the grade form from the current grade and allows a re-grade', async () => {
    vi.spyOn(api, 'shadow').mockResolvedValue(
      response({
        room: {
          labelled: ev('shadow.labelled', { actual_layer: 'kafka', root_cause_terms: ['lag'] }),
          graded: ev('shadow.graded', { layer_correct: false, root_cause: 'no', notes: 'wrong layer' }, { seq: 9 }),
        },
      }),
    )
    const grade = vi.spyOn(api, 'shadowGrade').mockResolvedValue(ev('shadow.graded', {}))
    render(<ShadowPanel state={room} user={member} />)
    await screen.findByText(HYP)
    expect(screen.getByLabelText(/Notes/)).toHaveValue('wrong layer')
    fireEvent.click(screen.getByRole('radio', { name: 'Yes' }))
    fireEvent.click(screen.getByRole('button', { name: 'Update grade' }))
    await waitFor(() =>
      expect(grade).toHaveBeenCalledWith('r1', { layer_correct: true, root_cause: 'no', notes: 'wrong layer' }),
    )
  })

  it('holds the grade form while the run is still finishing (grace period)', async () => {
    vi.spyOn(api, 'shadow').mockResolvedValue(response({ run: run({ revealed_at: null, status: 'running' }) }))
    render(<ShadowPanel state={{ ...room, shadow_revealed: false }} user={member} />)
    await screen.findByRole('heading', { name: 'Grade the agent' })
    expect(screen.getByText(/still finishing its run/)).toBeInTheDocument()
    expect(screen.queryByRole('button', { name: 'Save grade' })).not.toBeInTheDocument()
  })

  it('says so when the agent reached no hypothesis', async () => {
    vi.spyOn(api, 'shadow').mockResolvedValue(response({ run: run({ status: 'budget', final_hypothesis: null }) }))
    render(<ShadowPanel state={room} user={member} />)
    expect(await screen.findByText(/did not reach a hypothesis/)).toBeInTheDocument()
  })

  it('shows raw tool data only after the toggle, asking the server for it', async () => {
    const result = ev(
      'agent.tool_result',
      { tool: 'connect_status', summary: 'FAILED', is_error: false, data: { state: 'FAILED' } },
      { seq: 2 },
    )
    const shadow = vi.spyOn(api, 'shadow').mockResolvedValue(response({ events: [{ ...result, shadow: true }] }))
    render(<ShadowPanel state={room} user={member} />)
    await screen.findByText('connect_status: FAILED')
    expect(screen.queryByText(/"state": "FAILED"/)).not.toBeInTheDocument()
    fireEvent.click(screen.getByRole('button', { name: 'Show raw tool data' }))
    await waitFor(() => expect(shadow).toHaveBeenLastCalledWith('r1', ['room', 'data']))
    expect(await screen.findByText(/"state": "FAILED"/)).toBeInTheDocument()
  })

  it('shows the friendly text when a grade is refused with 409 not_revealed', async () => {
    vi.spyOn(api, 'shadow').mockResolvedValue(response())
    vi.spyOn(api, 'shadowGrade').mockRejectedValue(new ApiClientError(409, 'not_revealed', 'raw server text'))
    render(<ShadowPanel state={room} user={member} />)
    await screen.findByText(HYP)
    fireEvent.click(screen.getByRole('radio', { name: 'Yes' }))
    fireEvent.click(screen.getByRole('radio', { name: 'Yes, same cause' }))
    fireEvent.click(screen.getByRole('button', { name: 'Save grade' }))
    expect(await screen.findByRole('alert')).toHaveTextContent(/still finishing its run/)
    expect(screen.queryByText('Grade saved.')).not.toBeInTheDocument()
  })

  it('shows the server text when a grade is refused with 403 not_allowed', async () => {
    vi.spyOn(api, 'shadow').mockResolvedValue(response())
    vi.spyOn(api, 'shadowGrade').mockRejectedValue(
      new ApiClientError(403, 'not_allowed', 'labelling or grading a shadow room needs a member of its pipeline'),
    )
    render(<ShadowPanel state={room} user={member} />)
    await screen.findByText(HYP)
    fireEvent.click(screen.getByRole('radio', { name: 'Yes' }))
    fireEvent.click(screen.getByRole('radio', { name: 'Yes, same cause' }))
    fireEvent.click(screen.getByRole('button', { name: 'Save grade' }))
    expect(await screen.findByRole('alert')).toHaveTextContent(/needs a member of its pipeline/)
    // the form stays, so the person can see what was refused
    expect(screen.getByRole('button', { name: 'Save grade' })).toBeEnabled()
  })

  it('shows the error card with Try again when GET /shadow answers 409 room_open (a race with a reopen)', async () => {
    const shadow = vi.spyOn(api, 'shadow').mockRejectedValueOnce(new ApiClientError(409, 'room_open', 'raw'))
    render(<ShadowPanel state={room} user={member} />)
    expect(await screen.findByRole('alert')).toHaveTextContent(/room is still open/)
    expect(screen.queryByRole('heading', { name: 'Record your answer first' })).not.toBeInTheDocument()
    shadow.mockResolvedValueOnce(response())
    fireEvent.click(screen.getByRole('button', { name: 'Try again' }))
    expect(await screen.findByText(HYP)).toBeInTheDocument()
  })

  it('does not crash on a label without terms or a run status it does not know', async () => {
    vi.spyOn(api, 'shadow').mockResolvedValue(
      response({
        run: run({ status: 'paused' as ShadowRun['status'], final_hypothesis: null }),
        room: { labelled: ev('shadow.labelled', { actual_layer: 'kafka' }) },
      }),
    )
    render(<ShadowPanel state={room} user={member} />)
    expect(await screen.findByRole('heading', { name: 'Your blind label' })).toBeInTheDocument()
    expect(screen.getAllByText(/paused/).length).toBeGreaterThan(0)
  })

  it('shows a load error with a retry', async () => {
    const shadow = vi.spyOn(api, 'shadow').mockRejectedValueOnce(new ApiClientError(500, 'internal', 'boom'))
    render(<ShadowPanel state={room} user={member} />)
    expect(await screen.findByText('boom')).toBeInTheDocument()
    shadow.mockResolvedValueOnce(response())
    fireEvent.click(screen.getByRole('button', { name: 'Try again' }))
    expect(await screen.findByText(HYP)).toBeInTheDocument()
  })
})

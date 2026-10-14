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

  it('gives a viewer no forms before the label', async () => {
    vi.spyOn(api, 'shadow').mockRejectedValue(notLabelled())
    render(<ShadowPanel state={room} user={viewer} />)
    expect(await screen.findByText(/Only a member of this pipeline can label/)).toBeInTheDocument()
    expect(screen.queryByRole('button', { name: /Save label/ })).not.toBeInTheDocument()
  })

  it('grades the run and sends notes only when given', async () => {
    vi.spyOn(api, 'shadow').mockResolvedValue(response())
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
    expect(await screen.findByText('Grade saved.')).toBeInTheDocument()
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

  it('shows a load error with a retry', async () => {
    const shadow = vi.spyOn(api, 'shadow').mockRejectedValueOnce(new ApiClientError(500, 'internal', 'boom'))
    render(<ShadowPanel state={room} user={member} />)
    expect(await screen.findByText('boom')).toBeInTheDocument()
    shadow.mockResolvedValueOnce(response())
    fireEvent.click(screen.getByRole('button', { name: 'Try again' }))
    expect(await screen.findByText(HYP)).toBeInTheDocument()
  })
})

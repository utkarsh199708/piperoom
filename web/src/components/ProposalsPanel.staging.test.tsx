import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { act, fireEvent, render, screen, within } from '@testing-library/react'
import { ProposalsPanel } from './ProposalsPanel'
import { DecisionBar } from './room/DecisionBar'
import { DecisionHandleContext } from './shell/context'
import type { RoomActions } from '../hooks/useRoom'
import type { CurrentUser } from '../state/permissions'
import { fromSnapshot } from '../state/types'
import type { RoomState } from '../state/types'
import { applyEvent } from '../state/reducer'
import type { Event, ProposalStagedPayload } from '../api/types'
import {
  STAGING_HASH,
  stagedAllHealthy,
  stagedPayload,
  stagingCreated,
  stagingSnapshot,
  stagingStaged,
  stagingStarted,
} from '../fixtures/staging'

// X8e: "Try in staging" and the result on the proposal card, from the documented API shapes.

const stage = vi.fn()
const approve = vi.fn()
const approveAll = vi.fn()
const actions: RoomActions = {
  comment: vi.fn(),
  steer: vi.fn(),
  sendSuggestion: vi.fn(),
  declineSuggestion: vi.fn(),
  requestControl: vi.fn(),
  grantControl: vi.fn(),
  close: vi.fn(),
  approve,
  approveAll,
  stage,
  reject: vi.fn(),
  handoff: vi.fn(),
  acceptHandoff: vi.fn(),
  label: vi.fn(),
  checklist: vi.fn(),
  trace: vi.fn(),
  hypothesisVerdict: vi.fn(),
  nudge: vi.fn().mockResolvedValue({ ok: true }),
}

// Arjun drives and owns kafka-connect (a plain member); Bea is in the room and owns it too but does not drive.
const arjun: CurrentUser = { id: 'arjun', name: 'Arjun', layers: ['kafka-connect'] }
const bea: CurrentUser = { id: 'bea', name: 'Bea', layers: ['kafka-connect'] }
const viewer: CurrentUser = { id: 'vic', name: 'Vic', layers: [], role: 'viewer' }

const FUTURE = '2099-01-01T00:00:00Z'
const stagedEvent = (payload: ProposalStagedPayload, seq = 3): Event => ({ ...stagingStaged, seq, payload })
const startedEvent = (expires_at = FUTURE): Event => ({
  ...stagingStarted,
  payload: { ...(stagingStarted.payload as object), expires_at },
})
const room = (...events: Event[]): RoomState => events.reduce(applyEvent, applyEvent(fromSnapshot(stagingSnapshot), stagingCreated))
const show = (state: RoomState, user = arjun) => render(<ProposalsPanel state={state} user={user} actions={actions} />)

beforeEach(() => {
  stage.mockReset()
  approve.mockReset()
  approveAll.mockReset()
})
afterEach(() => vi.useRealTimers())

describe('the button', () => {
  it('is the driver’s, and asks the server to stage this proposal', async () => {
    stage.mockResolvedValue({ ok: true })
    show(room())
    const button = screen.getByRole('button', { name: 'Try in staging' })
    await act(async () => fireEvent.click(button))
    expect(stage).toHaveBeenCalledWith('p_stage')
  })

  it.each([
    ['a member who is not the driver', bea],
    ['a viewer', viewer],
  ])('is not shown to %s', (_name, user) => {
    show(room(), user)
    expect(screen.queryByRole('button', { name: /staging/i })).not.toBeInTheDocument()
  })

  it('is not shown when the snapshot says staging is off, or no step can be staged', () => {
    const off = room()
    show({ ...off, mode: { ...off.mode!, staging: false } })
    expect(screen.queryByRole('button', { name: /staging/i })).not.toBeInTheDocument()
  })

  it('is not shown without stageable steps', () => {
    const s = room()
    show({ ...s, proposals: [{ ...s.proposals[0], stageable: [] }] })
    expect(screen.queryByRole('button', { name: /staging/i })).not.toBeInTheDocument()
  })

  it('is not shown on a closed room', () => {
    show({ ...room(), status: 'closed' })
    expect(screen.queryByRole('button', { name: /staging/i })).not.toBeInTheDocument()
  })

  it('is not shown by a host without the stage action (before X8)', () => {
    render(<ProposalsPanel state={room()} user={arjun} actions={{ ...actions, stage: undefined }} />)
    expect(screen.queryByRole('button', { name: /staging/i })).not.toBeInTheDocument()
  })

  it('says why a stage was refused, next to the button, and never throws', async () => {
    stage.mockResolvedValue({ ok: false, code: 'stage_in_progress', error: 'raw' })
    show(room())
    await act(async () => fireEvent.click(screen.getByRole('button', { name: 'Try in staging' })))
    expect(screen.getByRole('alert')).toHaveTextContent('A stage of this fix is already running.')
    expect(screen.getByRole('button', { name: 'Try in staging' })).toBeEnabled()
  })

  it.each([
    ['rate_limited', /five times/],
    ['no_staging', /no staging stack/],
    ['not_stageable', /can be tried in staging/],
    ['not_driver', /Only the driver/],
  ])('words %s', async (code, text) => {
    stage.mockResolvedValue({ ok: false, code, error: 'raw' })
    show(room())
    await act(async () => fireEvent.click(screen.getByRole('button', { name: 'Try in staging' })))
    expect(screen.getByRole('alert')).toHaveTextContent(text)
  })

  it('falls back to the server text for an unknown failure, and for a network one', async () => {
    stage.mockResolvedValue({ ok: false, error: "Can't reach Piperoom" })
    show(room())
    await act(async () => fireEvent.click(screen.getByRole('button', { name: 'Try in staging' })))
    expect(screen.getByRole('alert')).toHaveTextContent("Can't reach Piperoom")
  })

  it('waits for the answer, and cannot be clicked twice', async () => {
    let done: (v: { ok: boolean }) => void = () => undefined
    stage.mockReturnValue(new Promise((r) => (done = r)))
    show(room())
    const button = screen.getByRole('button', { name: 'Try in staging' })
    fireEvent.click(button)
    expect(button).toBeDisabled()
    fireEvent.click(button)
    expect(stage).toHaveBeenCalledTimes(1)
    await act(async () => done({ ok: false }))
    expect(button).toBeEnabled()
  })
})

describe('in progress', () => {
  it('shows who started it and which steps, with no button', () => {
    show(room(startedEvent()), bea)
    const running = screen.getByTestId('stage-running')
    expect(running).toHaveAttribute('role', 'status')
    expect(running).toHaveTextContent('Trying step 1, 3 in staging…')
    expect(running).toHaveTextContent('Arjun')
    show(room(startedEvent()))
    expect(screen.queryByRole('button', { name: /staging/i })).not.toBeInTheDocument()
  })

  it('a stage past its expires_at with no result is lost and can be started again', () => {
    show(room(startedEvent('2020-01-01T00:00:00Z')))
    expect(screen.getByTestId('stage-lost')).toHaveTextContent('never reported back')
    expect(screen.queryByTestId('stage-running')).not.toBeInTheDocument()
    expect(screen.getByRole('button', { name: 'Try in staging' })).toBeInTheDocument()
  })

  it('turns into the result when proposal.staged arrives', () => {
    const { rerender } = show(room(startedEvent()))
    expect(screen.getByTestId('stage-running')).toBeInTheDocument()
    rerender(<ProposalsPanel state={room(startedEvent(), stagedEvent(stagedPayload, 4))} user={arjun} actions={actions} />)
    expect(screen.queryByTestId('stage-running')).not.toBeInTheDocument()
    expect(screen.getByTestId('stage-result')).toBeInTheDocument()
  })
})

describe('the result', () => {
  it('healthy: says so, with the target, the duration and what healthy meant', () => {
    show(room(stagedEvent(stagedAllHealthy)), bea)
    expect(screen.getByTestId('stage-verdict')).toHaveTextContent('Healthy')
    const step = screen.getByTestId('stage-step-0')
    expect(step).toHaveAttribute('data-outcome', 'healthy')
    expect(step).toHaveTextContent('Step 1')
    expect(step).toHaveTextContent('connect_update_connector_config')
    expect(step).toHaveTextContent('connector orders-src')
    expect(step).toHaveTextContent('1 min 1 s')
    expect(step).toHaveTextContent('RUNNING, 1 of 1 tasks RUNNING for 60 s')
  })

  it('failed: shows the redacted detail and the leftover', () => {
    const failed: ProposalStagedPayload = {
      ...stagedPayload,
      steps: [
        {
          step_index: 0,
          result: {
            target: 'connector orders-src',
            applied: true,
            healthy: false,
            detail: 'task 0 FAILED: org.apache.kafka.connect.errors.ConnectException: schema mismatch',
            leftover: 'schema version 7 stays registered on staging',
            duration_ms: 12_300,
          },
        },
      ],
    }
    show(room(stagedEvent(failed)), bea)
    expect(screen.getByTestId('stage-verdict')).toHaveTextContent('Failed')
    const step = screen.getByTestId('stage-step-0')
    expect(step).toHaveAttribute('data-outcome', 'failed')
    expect(step).toHaveTextContent('task 0 FAILED')
    expect(step).toHaveTextContent('12.3 s')
    expect(screen.getByTestId('stage-leftover-0')).toHaveTextContent('Left on staging: schema version 7 stays registered on staging')
    expect(screen.getByText(/Production was not touched/)).toBeInTheDocument()
  })

  it('staging’s own pre-check refusing the change is a failure, with its words', () => {
    const refused: ProposalStagedPayload = {
      ...stagedPayload,
      steps: [
        {
          step_index: 0,
          result: {
            applied: false,
            healthy: false,
            dry_run: { ok: false, source: 'connect validate', text: 'max.batch.size: must be at least 1' },
            duration_ms: 90,
          },
        },
      ],
    }
    show(room(stagedEvent(refused)), bea)
    const step = screen.getByTestId('stage-step-0')
    expect(step).toHaveAttribute('data-outcome', 'failed')
    expect(step).toHaveTextContent('Staging’s own check refused the change · connect validate: max.batch.size: must be at least 1')
    expect(step).toHaveTextContent('90 ms')
  })

  it('not applied: says why', () => {
    const none: ProposalStagedPayload = {
      ...stagedPayload,
      steps: [{ step_index: 2, result: { applied: false, healthy: false, detail: 'connector not present in staging', duration_ms: 0 } }],
    }
    show(room(stagedEvent(none)), bea)
    expect(screen.getByTestId('stage-verdict')).toHaveTextContent('Not applied')
    const step = screen.getByTestId('stage-step-2')
    expect(step).toHaveAttribute('data-outcome', 'not_applied')
    expect(step).toHaveTextContent('Step 3')
    expect(step).toHaveTextContent('connector not present in staging')
  })

  it('a step with no verdict shows its error, and a mixed stage shows each step', () => {
    show(room(stagedEvent({ ...stagedPayload, steps: [stagedPayload.steps[0], { step_index: 2, error: 'staging unreachable: timeout' }] })), bea)
    expect(screen.getByTestId('stage-verdict')).toHaveTextContent('No verdict')
    expect(screen.getByTestId('stage-step-0')).toHaveAttribute('data-outcome', 'healthy')
    expect(screen.getByTestId('stage-step-2')).toHaveTextContent('No verdict: staging unreachable: timeout')
  })

  it('a stage of an earlier version of the fix is shown as stale', () => {
    show(room(stagedEvent({ ...stagedAllHealthy, params_hash: 'old-hash' })), bea)
    expect(screen.getByTestId('stage-verdict')).toHaveTextContent('Healthy · stale')
    expect(screen.getByTestId('stage-result')).toHaveAttribute('data-current', 'false')
    expect(screen.getByText(/earlier version of the fix and does not count/)).toBeInTheDocument()
  })

  it('the driver can try again after a result', () => {
    show(room(stagedEvent(stagedPayload)))
    expect(screen.getByRole('button', { name: 'Try again in staging' })).toBeInTheDocument()
  })

  it('a decided proposal keeps its result and offers no button', () => {
    const s = room(stagedEvent(stagedAllHealthy))
    show({ ...s, proposals: [{ ...s.proposals[0], status: 'approved' }] })
    expect(screen.getByTestId('stage-result')).toBeInTheDocument()
    expect(screen.queryByRole('button', { name: /staging/i })).not.toBeInTheDocument()
  })

  it('nothing at all when staging has nothing to say', () => {
    const s = room()
    show({ ...s, mode: { ...s.mode!, staging: false }, proposals: [{ ...s.proposals[0], staging_required: false }] }, bea)
    expect(screen.queryByTestId('staging-card')).not.toBeInTheDocument()
  })

  it('names the section for assistive technology, and every outcome carries a word, not only a colour', () => {
    show(room(stagedEvent(stagedPayload)), bea)
    const card = screen.getByRole('region', { name: 'Staging' })
    expect(within(card).getByText('Healthy', { selector: '[data-slot="badge"]' })).toBeInTheDocument()
    expect(within(card).getAllByText(/Not applied|Healthy/).length).toBeGreaterThan(1)
  })
})

describe('staging required', () => {
  it('turns the layer’s Approve off and points it at the explanation', () => {
    show(room())
    const approveButton = screen.getByRole('button', { name: /^Approve/ })
    expect(approveButton).toBeDisabled()
    const note = screen.getByTestId('staging-required')
    expect(note).toHaveTextContent('Staging required')
    expect(note).toHaveTextContent('Approving is off until this fix has run healthy in staging.')
    expect(approveButton).toHaveAccessibleDescription(/Staging required/)
    expect(screen.getByRole('button', { name: /^Reject/ })).toBeEnabled()
  })

  it('tells an approver who cannot stage to ask the driver', () => {
    show(room(), bea)
    expect(screen.getByTestId('staging-required')).toHaveTextContent('Ask the driver to try it in staging.')
    expect(screen.queryByRole('button', { name: 'Try in staging' })).not.toBeInTheDocument()
  })

  it('a key press does not approve', () => {
    show(room())
    fireEvent.keyDown(window, { key: 'a' })
    expect(approve).not.toHaveBeenCalled()
    expect(screen.queryByRole('dialog')).not.toBeInTheDocument()
  })

  it('a healthy stage of this fix gives Approve back', () => {
    show(room(stagedEvent(stagedAllHealthy)))
    expect(screen.queryByTestId('staging-required')).not.toBeInTheDocument()
    expect(screen.getByRole('button', { name: /^Approve/ })).toBeEnabled()
  })

  it('a stage with a failed step does not', () => {
    show(room(stagedEvent(stagedPayload)))
    expect(screen.getByTestId('staging-required')).toBeInTheDocument()
    expect(screen.getByRole('button', { name: /^Approve/ })).toBeDisabled()
  })

  it('shows the server’s own words from a 409 as text', () => {
    const s = room()
    show({ ...s, proposals: [{ ...s.proposals[0], staging_message: 'step 0 (connect_update_connector_config) needs a healthy stage <b>now</b>' }] })
    const note = screen.getByTestId('staging-required')
    expect(note).toHaveTextContent('step 0 (connect_update_connector_config) needs a healthy stage <b>now</b>')
    expect(note.querySelector('b')).toBeNull()
  })

  it('also turns off "Approve and run"', () => {
    const s = room()
    const two = { ...s.proposals[0], required_layers: ['schema-registry', 'kafka-connect'], approvals: { 'schema-registry': 'pending', 'kafka-connect': 'pending' } as const }
    show({ ...s, proposals: [two] }, { ...arjun, layers: ['schema-registry', 'kafka-connect'] })
    const all = screen.getByTestId('approve-all')
    expect(within(all).getByRole('button', { name: 'Approve and run' })).toBeDisabled()
    expect(within(all).getByRole('button', { name: 'Approve and run' })).toHaveAccessibleDescription(/Staging required/)
  })

  it('holds back nothing on a closed room', () => {
    show({ ...room(), status: 'closed' })
    expect(screen.queryByTestId('staging-required')).not.toBeInTheDocument()
  })
})

describe('the decision bar', () => {
  const handle = { approve: vi.fn(), reject: vi.fn(), scrollTo: vi.fn() }
  const bar = (state: RoomState) =>
    render(
      <DecisionHandleContext.Provider value={{ get: () => handle, set: () => undefined }}>
        <DecisionBar state={state} user={arjun} />
      </DecisionHandleContext.Provider>,
    )

  it('says staging is required and keeps Approve off', () => {
    bar(room())
    expect(screen.getByRole('region', { name: 'Decision' })).toHaveTextContent('staging required')
    const button = screen.getByRole('button', { name: /^Approve kafka-connect/ })
    expect(button).toBeDisabled()
    fireEvent.click(button)
    expect(handle.approve).not.toHaveBeenCalled()
  })

  it('offers Approve once a healthy stage exists', () => {
    bar(room(stagedEvent(stagedAllHealthy)))
    expect(screen.getByRole('region', { name: 'Decision' })).not.toHaveTextContent('staging required')
    expect(screen.getByRole('button', { name: /^Approve kafka-connect/ })).toBeEnabled()
  })
})

describe('hostile text from staging', () => {
  const hostile = '<img src=x onerror=alert(1)><script>alert(2)</script>‮evil‬\u0007 end'
  const big = `${'A'.repeat(10_240)}<img src=x>`
  const payload: ProposalStagedPayload = {
    ...stagedPayload,
    steps: [
      {
        step_index: 0,
        result: { target: hostile, applied: true, healthy: false, detail: hostile, leftover: hostile, duration_ms: 5 },
      },
      { step_index: 2, result: { applied: true, healthy: false, detail: big, duration_ms: 5 } },
    ],
  }

  it('renders detail, leftover, target and the server’s 409 text as text', () => {
    const s = room(stagedEvent(payload))
    const { container } = show({ ...s, proposals: [{ ...s.proposals[0], staging_message: hostile }] }, bea)
    expect(container.querySelector('img')).toBeNull()
    expect(container.querySelector('script')).toBeNull()
    expect(screen.getByTestId('stage-step-0')).toHaveTextContent('<img src=x onerror=alert(1)><script>alert(2)</script>')
    // The bidi override is dropped, so the text cannot be reordered.
    expect(container.textContent).not.toContain('‮')
  })

  it('shows a 10 KB detail folded, as one text node', () => {
    const { container } = show(room(stagedEvent(payload)), bea)
    expect(container.querySelector('img')).toBeNull()
    expect(screen.getAllByRole('button', { name: 'Show more' }).length).toBeGreaterThan(0)
    expect(screen.getByTestId('stage-step-2').textContent).toContain('A'.repeat(10_240))
  })

  it('a step error is text too', () => {
    const { container } = show(room(stagedEvent({ ...stagedPayload, steps: [{ step_index: 0, error: hostile }] })), bea)
    expect(container.querySelector('img')).toBeNull()
    expect(screen.getByTestId('stage-step-0')).toHaveTextContent('<img src=x onerror=alert(1)>')
  })
})

describe('contract shapes', () => {
  it('reads the documented snapshot and events into the card', () => {
    const s = room(stagingStarted, stagingStaged)
    expect(s.proposals[0].params_hash).toBe(STAGING_HASH)
    expect(s.proposals[0].stageable).toEqual([0, 2])
    show(s, bea)
    expect(screen.getByTestId('stage-step-0')).toHaveAttribute('data-outcome', 'healthy')
    expect(screen.getByTestId('stage-step-2')).toHaveAttribute('data-outcome', 'not_applied')
  })

  it('has no fixed widths that could overflow a phone', () => {
    const { container } = show(room(stagedEvent(stagedPayload)), bea)
    const card = container.querySelector('[data-testid="staging-card"]') as HTMLElement
    expect(card.className).not.toMatch(/\bw-\[|min-w-\[|\bw-\d/)
    expect(card.querySelector('li')?.className).not.toMatch(/whitespace-nowrap/)
  })
})

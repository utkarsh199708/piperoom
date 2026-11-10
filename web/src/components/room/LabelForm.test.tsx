import { describe, expect, it, vi } from 'vitest'
import { fireEvent, render, screen, waitFor } from '@testing-library/react'
import { LabelForm } from './LabelForm'
import { MAX_CAUSE, draftToInput, guessLayer, initialDraft } from '@/lib/label'
import { emptyRoomState } from '@/state/types'
import { applyEvent } from '@/state/reducer'
import type { Event } from '@/api/types'
import type { RoomState } from '@/state/types'

function room(over: Partial<RoomState> = {}): RoomState {
  return {
    ...emptyRoomState('r1'),
    status: 'closed',
    hypothesis: { text: 'The connector task failed after a schema change', based_on_steps: [], confidence: 'high', confirmed: true },
    proposals: [
      { proposal_id: 'p1', title: 'Resume the connector', steps: [], risk: 'low', rollback: '', required_layers: [], approvals: {}, decisions: {}, params_hash: '', status: 'approved', executed: [{ proposal_id: 'p1', step_index: 0, ok: true, output: '' }] },
    ],
    ...over,
  }
}

describe('label draft', () => {
  it('pre-fills cause, fix and a layer when exactly one layer matches', () => {
    const d = initialDraft(room({ hypothesis: { text: 'Replication slot is inactive', based_on_steps: [], confidence: 'high', confirmed: false } }))
    expect(d).toMatchObject({ cause: 'Replication slot is inactive', layer: 'postgres', fix: 'Resume the connector', helped: 'skip' })
    expect(guessLayer('connector and schema')).toBe('') // two layers match: no guess
  })

  it('sends the proposal id only while the fix text is the proposal title', () => {
    const r = room()
    const d = initialDraft(r)
    expect(draftToInput(r, d)).toMatchObject({ cause: d.cause, fix_that_worked: 'Resume the connector', fix_proposal_id: 'p1' })
    expect(draftToInput(r, { ...d, fix: 'restarted by hand' })).not.toHaveProperty('fix_proposal_id')
    expect(draftToInput(r, { ...d, cause: '  ' })).toBeNull()
    expect(draftToInput(r, { ...d, helped: 'yes' })?.agent_helped).toBe(true)
    expect(draftToInput(r, { ...d, helped: 'no' })?.agent_helped).toBe(false)
    expect(draftToInput(r, d)).not.toHaveProperty('agent_helped')
  })
})

const agentActor = { kind: 'agent' as const, id: 'agent', name: 'Agent' }
let seqN = 0
function ev(type: Event['type'], payload: unknown): Event {
  seqN += 1
  return { room_id: 'r1', seq: seqN, type, actor: agentActor, payload, at: `2026-10-11T10:00:${String(seqN % 60).padStart(2, '0')}Z` }
}
const hyp = (text: string) => ev('agent.hypothesis', { text, based_on_steps: [], confidence: 'high', confirmed: false })
const prop = (id: string, title: string) =>
  ev('proposal.created', { proposal_id: id, title, steps: [], risk: 'low', rollback: '', required_layers: [], params_hash: 'h' })
const ran = (id: string) => ev('action.executed', { proposal_id: id, step_index: 0, ok: true, output: '' })
const verified = (id: string, recovered: boolean) =>
  ev('agent.verification', { proposal_id: id, checks: [], recovered, verdict: recovered ? 'recovered' : 'not recovered' })
const H1 = 'The connector task failed after a schema change'
const H2 = "The pipeline is healthy now and there's nothing left to fix"

function replay(events: Event[]): RoomState {
  seqN = 0
  return events.reduce(applyEvent, emptyRoomState('r1'))
}

describe('label draft after recovery (KAN-695)', () => {
  const recoveredThenCleanup = () =>
    replay([hyp(H1), prop('A', 'Allow amount decimal and restart task 0'), ran('A'), verified('A', true), hyp(H2), prop('B', 'Restore BACKWARD compatibility'), ran('B')])

  it('names the recovering proposal, not the later cleanup, and sends its id', () => {
    const r = recoveredThenCleanup()
    const d = initialDraft(r)
    expect(d.fix).toBe('Allow amount decimal and restart task 0')
    expect(draftToInput(r, d)).toMatchObject({ fix_proposal_id: 'A' })
  })

  it('takes the cause and layer from the hypothesis behind the recovering fix', () => {
    const d = initialDraft(recoveredThenCleanup())
    expect(d.cause).toBe(H1)
    expect(d.layer).toBe(guessLayer(H1))
    expect(d.cause).not.toContain('healthy now')
  })

  it('gives the same draft when the log is replayed again (reload)', () => {
    const first = initialDraft(recoveredThenCleanup())
    expect(initialDraft(recoveredThenCleanup())).toEqual(first)
  })

  it('falls back to the last executed proposal and the latest hypothesis without a recovery', () => {
    const r = replay([hyp(H1), prop('A', 'Fix A'), ran('A'), verified('A', false), hyp(H2), prop('B', 'Cleanup B'), ran('B')])
    const d = initialDraft(r)
    expect(d.fix).toBe('Cleanup B')
    expect(d.cause).toBe(H2)
    expect(draftToInput(r, d)).toMatchObject({ fix_proposal_id: 'B' })
  })

  it('agrees with the close nudge, which says Fixed by the recovered proposal title', () => {
    const r = recoveredThenCleanup()
    const nudgeTitle = r.proposals.find((p) => p.verification?.recovered)?.title
    expect(initialDraft(r).fix).toBe(nudgeTitle)
  })

  it('pre-fills the form with the recovering fix and the earlier hypothesis', () => {
    render(<LabelForm state={recoveredThenCleanup()} onSubmit={vi.fn()} onSkip={vi.fn()} />)
    expect(screen.getByDisplayValue(H1)).toBeInTheDocument()
    expect(screen.getByDisplayValue('Allow amount decimal and restart task 0')).toBeInTheDocument()
  })
})

describe('LabelForm', () => {
  it('submits the pre-filled label', async () => {
    const onSubmit = vi.fn().mockResolvedValue({ ok: true })
    render(<LabelForm state={room()} onSubmit={onSubmit} onSkip={vi.fn()} />)
    fireEvent.click(screen.getByRole('radio', { name: 'Yes' }))
    fireEvent.click(screen.getByRole('button', { name: 'Save label' }))
    await waitFor(() => expect(onSubmit).toHaveBeenCalled())
    expect(onSubmit).toHaveBeenCalledWith({
      cause: 'The connector task failed after a schema change', fix_that_worked: 'Resume the connector', fix_proposal_id: 'p1', agent_helped: true,
    })
  })

  it('skips without a call', () => {
    const onSubmit = vi.fn()
    const onSkip = vi.fn()
    render(<LabelForm state={room()} onSubmit={onSubmit} onSkip={onSkip} />)
    fireEvent.click(screen.getByRole('button', { name: 'Skip' }))
    expect(onSkip).toHaveBeenCalled()
    expect(onSubmit).not.toHaveBeenCalled()
  })

  it('needs a cause, caps it at 500 characters and disables the fix when no fix ran', () => {
    render(<LabelForm state={room({ proposals: [], hypothesis: null })} onSubmit={vi.fn()} onSkip={vi.fn()} />)
    expect(screen.getByRole('button', { name: 'Save label' })).toBeDisabled()
    expect(screen.getByRole('textbox', { name: 'Cause' })).toHaveAttribute('maxlength', String(MAX_CAUSE))
    expect(screen.getByRole('textbox', { name: 'Fix that worked' })).toBeDisabled()
  })

  it('keeps the text and shows the error when the label fails', async () => {
    const onSubmit = vi.fn().mockResolvedValue({ ok: false, error: 'not allowed' })
    render(<LabelForm state={room()} onSubmit={onSubmit} onSkip={vi.fn()} />)
    fireEvent.click(screen.getByRole('button', { name: 'Save label' }))
    expect(await screen.findByRole('alert')).toHaveTextContent('not allowed')
    expect(screen.getByRole('textbox', { name: 'Cause' })).toHaveValue('The connector task failed after a schema change')
  })
})

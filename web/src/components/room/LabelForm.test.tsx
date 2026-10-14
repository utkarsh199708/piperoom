import { describe, expect, it, vi } from 'vitest'
import { fireEvent, render, screen, waitFor } from '@testing-library/react'
import { LabelForm } from './LabelForm'
import { MAX_CAUSE, draftToInput, guessLayer, initialDraft } from '@/lib/label'
import { emptyRoomState } from '@/state/types'
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

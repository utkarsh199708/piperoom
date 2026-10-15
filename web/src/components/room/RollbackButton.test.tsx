import { describe, expect, it, vi } from 'vitest'
import { fireEvent, render, screen, waitFor } from '@testing-library/react'
import { rollbackSteer } from '../../lib/rollback'
import { RollbackButton } from './RollbackButton'

describe('rollbackSteer', () => {
  it('has the exact wording', () => {
    expect(rollbackSteer('Restart', 'p_1', 'set it back')).toBe(
      'Roll back proposal "Restart" (p_1): propose a fix that undoes it. Recorded rollback: set it back',
    )
  })
})

describe('RollbackButton', () => {
  it('confirms in a dialog that says nothing runs without approvals, then sends the steer', async () => {
    const onSteer = vi.fn().mockResolvedValue(true)
    render(<RollbackButton title="Restart" proposalId="p_1" rollback="set it back" asDriver onSteer={onSteer} />)
    fireEvent.click(screen.getByRole('button', { name: 'Roll back' }))
    expect(screen.getByText('This asks the agent to propose the undo; nothing runs without approvals.')).toBeInTheDocument()
    expect(screen.getByText('set it back')).toBeInTheDocument()
    fireEvent.click(screen.getByRole('button', { name: 'Ask the agent to roll back' }))
    await waitFor(() => expect(onSteer).toHaveBeenCalledWith(rollbackSteer('Restart', 'p_1', 'set it back'), { clientId: expect.any(String) }))
    await waitFor(() => expect(screen.queryByRole('dialog')).not.toBeInTheDocument())
  })

  it('a member suggests it', () => {
    render(<RollbackButton title="t" proposalId="p" rollback="r" asDriver={false} onSteer={vi.fn()} />)
    expect(screen.getByRole('button', { name: 'Suggest roll back' })).toBeInTheDocument()
  })

  it('keeps the dialog open with a retry when the send fails', async () => {
    const onSteer = vi.fn().mockResolvedValue(false)
    render(<RollbackButton title="t" proposalId="p" rollback="r" asDriver onSteer={onSteer} />)
    fireEvent.click(screen.getByRole('button', { name: 'Roll back' }))
    fireEvent.click(screen.getByRole('button', { name: 'Ask the agent to roll back' }))
    expect(await screen.findByRole('alert')).toBeInTheDocument()
    expect(screen.getByRole('dialog')).toBeInTheDocument()
    // Closing withdraws the failed request (#459), and the dialog says so.
    expect(screen.getByText('Closing discards the unsent request.')).toBeInTheDocument()
  })

  // #459: Retry is the same action, so it keeps the client_id; the next request is a new one.
  it('Retry reuses the failed request\'s client_id; a request after a success gets a new one', async () => {
    const onSteer = vi.fn().mockResolvedValueOnce(false).mockResolvedValueOnce(true).mockResolvedValueOnce(true)
    render(<RollbackButton title="t" proposalId="p" rollback="r" asDriver onSteer={onSteer} />)
    fireEvent.click(screen.getByRole('button', { name: 'Roll back' }))
    fireEvent.click(screen.getByRole('button', { name: 'Ask the agent to roll back' }))
    fireEvent.click(await screen.findByRole('button', { name: 'Retry' }))
    await waitFor(() => expect(screen.queryByRole('dialog')).not.toBeInTheDocument())
    fireEvent.click(screen.getByRole('button', { name: 'Roll back' }))
    fireEvent.click(screen.getByRole('button', { name: 'Ask the agent to roll back' }))
    await waitFor(() => expect(onSteer).toHaveBeenCalledTimes(3))
    const ids = onSteer.mock.calls.map((c) => (c[1] as { clientId: string }).clientId)
    expect(ids[1]).toBe(ids[0])
    expect(ids[2]).not.toBe(ids[0])
  })
})

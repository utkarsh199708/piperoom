import { describe, expect, it, vi } from 'vitest'
import { useMemo, useState } from 'react'
import { act, fireEvent, render, screen, waitFor } from '@testing-library/react'
import { RollbackButton } from './RollbackButton'
import { ApiClientError } from '../../api/client'
import type { Event } from '../../api/types'
import { OFFLINE_TEXT, errorText } from '../../lib/errors'
import { SendLedger } from '../../hooks/sendLedger'
import type { SendItem } from '../../hooks/sendLedger'
import type { SendControls } from '../../hooks/useRoom'

// KAN-581: a rollback steer the server refused (409 room_closed, 403) is not "Can't reach Piperoom"
// and Retry would only fail again. The strip is for a request the send ledger still owes. These
// tests run the button on a real SendLedger, so the refused/kept split is the ledger's own (same
// shape as Timeline.refused.test.tsx).

const author = { kind: 'human' as const, id: 'priya', name: 'Priya' }
const ROOM_CLOSED = new ApiClientError(409, 'room_closed', 'room is closed')
const FORBIDDEN = new ApiClientError(403, 'forbidden', 'you cannot steer this room')
const SERVER_DOWN = new TypeError('Failed to fetch')

function ok(item: SendItem, clientId: string): Event {
  return { room_id: 'r1', seq: 1, type: 'steer.sent', actor: author, payload: { text: item.text, client_id: clientId }, at: '' } as Event
}

function Harness({
  post,
  dedupes = null,
}: {
  post: (item: SendItem, clientId: string) => Promise<Event | undefined>
  dedupes?: boolean | null
}) {
  const [error, setError] = useState<string | null>(null)
  const ledger = useMemo(() => {
    let n = 0
    const l = new SendLedger({ post, setError, newId: () => `id${++n}` })
    l.dedupes = dedupes
    return l
    // eslint-disable-next-line react-hooks/exhaustive-deps -- one ledger per mount, like the room screen
  }, [])
  const sends = useMemo<SendControls>(
    () => ({
      onDelivered: (listener) => ledger.onDelivered(listener),
      cancel: (clientId) => ledger.cancel(clientId),
      isPending: (clientId) => ledger.isPending(clientId),
    }),
    [ledger],
  )
  return (
    <>
      {error && <p data-testid="room-error">{error}</p>}
      <RollbackButton
        title="Restart"
        proposalId="p_1"
        rollback="set it back"
        asDriver
        onSteer={(text, opts) => ledger.send({ kind: 'steer', text }, opts?.clientId)}
        sends={sends}
      />
    </>
  )
}

function ask() {
  fireEvent.click(screen.getByRole('button', { name: 'Roll back' }))
  fireEvent.click(screen.getByRole('button', { name: 'Ask the agent to roll back' }))
}

describe('rollback dialog: refused versus kept', () => {
  it('a rollback on a closed room (409) shows the refusal, no offline strip and no Retry', async () => {
    const post = vi.fn().mockRejectedValue(ROOM_CLOSED)
    render(<Harness post={post} />)
    ask()
    await waitFor(() => expect(screen.getByTestId('room-error')).toHaveTextContent(errorText(ROOM_CLOSED)))
    expect(await screen.findByText(/Piperoom refused this request/)).toBeInTheDocument()
    expect(screen.queryByText(OFFLINE_TEXT)).not.toBeInTheDocument()
    expect(screen.queryByRole('alert')).not.toBeInTheDocument()
    expect(screen.queryByRole('button', { name: 'Retry' })).not.toBeInTheDocument()
    expect(screen.queryByText('Closing discards the unsent request.')).not.toBeInTheDocument()
    expect(screen.getByRole('dialog')).toBeInTheDocument()
  })

  it('a forbidden rollback (403) shows the refusal, not the offline strip', async () => {
    const post = vi.fn().mockRejectedValue(FORBIDDEN)
    render(<Harness post={post} />)
    ask()
    expect(await screen.findByText(/Piperoom refused this request/)).toBeInTheDocument()
    expect(screen.queryByRole('button', { name: 'Retry' })).not.toBeInTheDocument()
  })

  it('asking again after a refusal is a new request with a new client id', async () => {
    const post = vi.fn().mockRejectedValue(ROOM_CLOSED)
    render(<Harness post={post} />)
    ask()
    await screen.findByText(/Piperoom refused this request/)
    fireEvent.click(screen.getByRole('button', { name: 'Ask the agent to roll back' }))
    await waitFor(() => expect(post).toHaveBeenCalledTimes(2))
    expect(post.mock.calls[1][1]).not.toBe(post.mock.calls[0][1])
    expect(screen.queryByRole('button', { name: 'Retry' })).not.toBeInTheDocument()
  })

  it('with the server down, the offline strip with Retry shows', async () => {
    const post = vi.fn().mockRejectedValue(SERVER_DOWN)
    render(<Harness post={post} />)
    ask()
    expect(await screen.findByRole('alert')).toHaveTextContent(OFFLINE_TEXT)
    expect(screen.getByRole('button', { name: 'Retry' })).toBeInTheDocument()
    expect(screen.queryByText(/Piperoom refused this request/)).not.toBeInTheDocument()
  })

  it('a kept request whose Retry resends the same client id, then closes on success', async () => {
    const post = vi
      .fn()
      .mockRejectedValueOnce(SERVER_DOWN)
      .mockImplementation(async (item: SendItem, id: string) => ok(item, id))
    render(<Harness post={post} dedupes />)
    ask()
    fireEvent.click(await screen.findByRole('button', { name: 'Retry' }))
    await waitFor(() => expect(screen.queryByRole('dialog')).not.toBeInTheDocument())
    expect(post.mock.calls[1][1]).toBe(post.mock.calls[0][1])
  })

  it('a kept request whose Retry is then refused loses the strip and shows the refusal', async () => {
    const post = vi.fn().mockRejectedValueOnce(SERVER_DOWN).mockRejectedValueOnce(ROOM_CLOSED)
    render(<Harness post={post} dedupes />)
    ask()
    fireEvent.click(await screen.findByRole('button', { name: 'Retry' }))
    expect(await screen.findByText(/Piperoom refused this request/)).toBeInTheDocument()
    expect(screen.queryByRole('alert')).not.toBeInTheDocument()
    expect(screen.queryByRole('button', { name: 'Retry' })).not.toBeInTheDocument()
    expect(post.mock.calls[1][1]).toBe(post.mock.calls[0][1])
  })

  it('closing the dialog after a refusal clears it, so the reopened dialog shows no stale refusal', async () => {
    const post = vi.fn().mockRejectedValue(ROOM_CLOSED)
    render(<Harness post={post} />)
    ask()
    await screen.findByText(/Piperoom refused this request/)
    fireEvent.click(screen.getByRole('button', { name: 'Cancel' }))
    fireEvent.click(screen.getByRole('button', { name: 'Roll back' }))
    expect(screen.getByRole('dialog')).toBeInTheDocument()
    expect(screen.queryByText(/Piperoom refused this request/)).not.toBeInTheDocument()
  })

  it('a request cancelled mid-send and then refused leaves no stale refusal on the reopened dialog', async () => {
    let reject!: (e: unknown) => void
    const post = vi.fn().mockReturnValue(new Promise<Event | undefined>((_, r) => (reject = r)))
    render(<Harness post={post} />)
    ask()
    await waitFor(() => expect(post).toHaveBeenCalledTimes(1))
    fireEvent.click(screen.getByRole('button', { name: 'Cancel' }))
    await act(async () => {
      reject(ROOM_CLOSED)
      await new Promise((r) => setTimeout(r, 0))
    })
    fireEvent.click(screen.getByRole('button', { name: 'Roll back' }))
    expect(screen.getByRole('dialog')).toBeInTheDocument()
    expect(screen.queryByText(/Piperoom refused this request/)).not.toBeInTheDocument()
    expect(screen.queryByRole('button', { name: 'Retry' })).not.toBeInTheDocument()
  })
})

import { describe, expect, it, vi } from 'vitest'
import { useMemo, useState } from 'react'
import { act, fireEvent, render, screen, waitFor } from '@testing-library/react'
import { Timeline } from './Timeline'
import { ApiClientError } from '../api/client'
import type { Event } from '../api/types'
import { OFFLINE_TEXT, errorText } from '../lib/errors'
import { SendLedger } from '../hooks/sendLedger'
import type { SendItem } from '../hooks/sendLedger'
import type { SendControls } from '../hooks/useRoom'
import type { CommentState, StepState } from '../state/types'

// KAN-556: a step comment or reply the server refused (400, 409 room_closed) shows the refusal and
// keeps the words, but never the "Can't reach Piperoom" Retry strip. That strip is for an action the
// send ledger still owes: kept for a retry, or sent with no answer. These tests run the boxes on a
// real SendLedger, so the refused/kept split is the ledger's own, not a stub of it. The strip is the
// only role="alert" here: the harness banner has none, and in the kept case it repeats the strip's text.

const author = { kind: 'human' as const, id: 'priya', name: 'Priya' }
const steps: StepState[] = [
  {
    id: 's1',
    title: 'Check the Debezium connector',
    tool_calls: [{ tool_use_id: 't1', tool: 'connect_status', input: { connector: 'pg-orders-connector' }, took_ms: 412 }],
    messages: [],
  },
]
const comments: CommentState[] = [
  {
    id: 'c1',
    step_id: 's1',
    text: 'That is the 10:58 migration.',
    mentions_agent: false,
    author,
    at: '2026-01-01T10:02:00Z',
    replies: [],
  },
]

const TOO_LONG = new ApiClientError(400, 'validation', 'text is too long: 4001 characters, the limit is 4000')
const ROOM_CLOSED = new ApiClientError(409, 'room_closed', 'room is closed')
const SERVER_DOWN = new TypeError('Failed to fetch')

function ok(item: SendItem, clientId: string): Event {
  return { room_id: 'r1', seq: 1, type: 'comment.added', actor: author, payload: { text: item.text, client_id: clientId }, at: '' }
}

// The room screen's wiring in miniature: a ledger behind onComment/sends, and its error shown the
// way the room error banner shows it.
function Harness({
  post,
  dedupes = null,
  withComments = false,
}: {
  post: (item: SendItem, clientId: string) => Promise<Event | undefined>
  dedupes?: boolean | null
  withComments?: boolean
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
      refusal: (clientId) => ledger.refusal(clientId),
    }),
    [ledger],
  )
  return (
    <>
      {error && <p data-testid="room-error">{error}</p>}
      <Timeline
        steps={steps}
        messages={[]}
        comments={withComments ? comments : []}
        onComment={(text, stepId, opts) => ledger.send({ kind: 'comment', text, stepId }, opts?.clientId)}
        sends={sends}
      />
    </>
  )
}

function openStepBox() {
  fireEvent.click(screen.getByRole('button', { name: /Check the Debezium connector/ }))
  return screen.getByLabelText('Comment on this step')
}

function comment(box: HTMLElement, value: string) {
  fireEvent.change(box, { target: { value } })
  fireEvent.click(screen.getByRole('button', { name: 'Comment' }))
}

function reply(value: string) {
  fireEvent.click(screen.getByRole('button', { name: 'Reply' }))
  const box = screen.getByLabelText('Reply to comment')
  fireEvent.change(box, { target: { value } })
  fireEvent.click(screen.getByRole('button', { name: 'Send' }))
  return box
}

describe('step comment box: refused versus kept', () => {
  it('a comment over the limit shows the refusal, no offline strip and no Retry, and keeps the words', async () => {
    const post = vi.fn().mockRejectedValue(TOO_LONG)
    render(<Harness post={post} />)
    const box = openStepBox()
    const long = 'x'.repeat(4001)
    comment(box, long)
    await waitFor(() => expect(screen.getByTestId('room-error')).toHaveTextContent(errorText(TOO_LONG)))
    expect(screen.getByTestId('room-error')).toHaveTextContent(
      'text is too long: 4001 characters, the limit is 4000',
    )
    expect(screen.queryByRole('alert')).not.toBeInTheDocument()
    expect(screen.queryByRole('button', { name: 'Retry' })).not.toBeInTheDocument()
    expect(screen.queryByText('Editing discards the unsent comment.')).not.toBeInTheDocument()
    expect(box).toHaveValue(long)
    expect(post).toHaveBeenCalledTimes(1)
  })

  // KAN-580: the room banner is off-screen at the step, so the refusal is also told under the box.
  it('a refused comment says why in a line under the box, and clears it when the words change', async () => {
    const post = vi.fn().mockRejectedValue(TOO_LONG)
    render(<Harness post={post} />)
    const box = openStepBox()
    comment(box, 'x'.repeat(4001))
    const line = await screen.findByRole('status')
    expect(line).toHaveTextContent('text is too long: 4001 characters, the limit is 4000')
    expect(box.closest('div')).toContainElement(line)
    expect(screen.queryByRole('alert')).not.toBeInTheDocument()
    fireEvent.change(box, { target: { value: 'short' } })
    expect(screen.queryByRole('status')).not.toBeInTheDocument()
  })

  it('a comment on a closed room says "room is closed" under the box', async () => {
    const post = vi.fn().mockRejectedValue(ROOM_CLOSED)
    render(<Harness post={post} />)
    const box = openStepBox()
    comment(box, 'looks right')
    expect(await screen.findByRole('status')).toHaveTextContent('room is closed')
    expect(box).toHaveValue('looks right')
  })

  it('no refusal line when the server is down, or when the comment goes through', async () => {
    const post = vi.fn().mockRejectedValueOnce(SERVER_DOWN).mockImplementation(async (item: SendItem, id: string) => ok(item, id))
    render(<Harness post={post} dedupes />)
    const box = openStepBox()
    comment(box, 'looks right')
    expect(await screen.findByRole('alert')).toHaveTextContent(OFFLINE_TEXT)
    expect(screen.queryByRole('status')).not.toBeInTheDocument()
    fireEvent.click(screen.getByRole('button', { name: 'Retry' }))
    await waitFor(() => expect(box).toHaveValue(''))
    expect(screen.queryByRole('status')).not.toBeInTheDocument()
  })

  it('a comment on a closed room (409 room_closed) shows "room is closed" and no offline strip', async () => {
    const post = vi.fn().mockRejectedValue(ROOM_CLOSED)
    render(<Harness post={post} />)
    const box = openStepBox()
    comment(box, 'looks right')
    await waitFor(() => expect(screen.getByTestId('room-error')).toHaveTextContent('room is closed'))
    expect(screen.queryByRole('alert')).not.toBeInTheDocument()
    expect(screen.queryByRole('button', { name: 'Retry' })).not.toBeInTheDocument()
    expect(box).toHaveValue('looks right')
  })

  it('a send submitted again after a refusal is a new action, still no strip', async () => {
    const post = vi.fn().mockRejectedValue(TOO_LONG)
    render(<Harness post={post} />)
    const box = openStepBox()
    comment(box, 'x'.repeat(4001))
    await waitFor(() => expect(post).toHaveBeenCalledTimes(1))
    await waitFor(() => expect(screen.getByRole('button', { name: 'Comment' })).toBeEnabled())
    fireEvent.click(screen.getByRole('button', { name: 'Comment' }))
    await waitFor(() => expect(post).toHaveBeenCalledTimes(2))
    expect(post.mock.calls[1][1]).not.toBe(post.mock.calls[0][1])
    expect(screen.queryByRole('button', { name: 'Retry' })).not.toBeInTheDocument()
  })

  it('with the server down, the offline strip shows and keeps the words (no deduping server known)', async () => {
    const post = vi.fn().mockRejectedValue(SERVER_DOWN)
    render(<Harness post={post} />)
    const box = openStepBox()
    comment(box, 'looks right')
    expect(await screen.findByRole('alert')).toHaveTextContent(OFFLINE_TEXT)
    expect(screen.getByRole('button', { name: 'Retry' })).toBeInTheDocument()
    expect(screen.getByText('Editing discards the unsent comment.')).toBeInTheDocument()
    expect(box).toHaveValue('looks right')
  })

  it('with the server down and a deduping server (kept for the reconnect), the offline strip shows', async () => {
    const post = vi.fn().mockRejectedValue(SERVER_DOWN)
    render(<Harness post={post} dedupes />)
    const box = openStepBox()
    comment(box, 'looks right')
    expect(await screen.findByRole('alert')).toHaveTextContent(OFFLINE_TEXT)
    expect(screen.getByRole('button', { name: 'Retry' })).toBeInTheDocument()
    expect(box).toHaveValue('looks right')
  })

  it('a kept comment whose Retry is then refused loses the strip and shows the refusal', async () => {
    const post = vi.fn().mockRejectedValueOnce(SERVER_DOWN).mockRejectedValueOnce(ROOM_CLOSED)
    render(<Harness post={post} dedupes />)
    const box = openStepBox()
    comment(box, 'looks right')
    fireEvent.click(await screen.findByRole('button', { name: 'Retry' }))
    await waitFor(() => expect(screen.getByTestId('room-error')).toHaveTextContent('room is closed'))
    expect(screen.queryByRole('alert')).not.toBeInTheDocument()
    expect(screen.queryByRole('button', { name: 'Retry' })).not.toBeInTheDocument()
    expect(box).toHaveValue('looks right')
    // The Retry was the same action: one id for both posts.
    expect(post.mock.calls[1][1]).toBe(post.mock.calls[0][1])
  })

  // The person edited the words away and back while the send was on the wire (retryId.drop), and
  // the send was then lost: it is no longer the composer's action, but the words were not sent, so
  // the strip stays, as before (useRoom.idempotent ADV-A). Retry sends them as a new action.
  it('a send edited away while on the wire and then lost keeps the strip; Retry is a new action', async () => {
    let lose!: (e: unknown) => void
    const post = vi
      .fn()
      .mockImplementationOnce(
        () =>
          new Promise<Event | undefined>((_, reject) => {
            lose = reject
          }),
      )
      .mockImplementation(async (item: SendItem, id: string) => ok(item, id))
    render(<Harness post={post} dedupes />)
    const box = openStepBox()
    comment(box, 'ack')
    await waitFor(() => expect(post).toHaveBeenCalledTimes(1))
    fireEvent.change(box, { target: { value: 'ack!' } })
    fireEvent.change(box, { target: { value: 'ack' } })
    act(() => lose(SERVER_DOWN))
    expect(await screen.findByRole('alert')).toHaveTextContent(OFFLINE_TEXT)
    fireEvent.click(screen.getByRole('button', { name: 'Retry' }))
    await waitFor(() => expect(post).toHaveBeenCalledTimes(2))
    expect(post.mock.calls[1][1]).not.toBe(post.mock.calls[0][1])
    await waitFor(() => expect(box).toHaveValue(''))
    expect(screen.queryByRole('alert')).not.toBeInTheDocument()
  })

  it('a comment that goes through clears the box and shows no strip', async () => {
    const post = vi.fn().mockImplementation(async (item: SendItem, id: string) => ok(item, id))
    render(<Harness post={post} />)
    const box = openStepBox()
    comment(box, 'looks right')
    await waitFor(() => expect(box).toHaveValue(''))
    expect(screen.queryByRole('alert')).not.toBeInTheDocument()
    expect(screen.queryByTestId('room-error')).not.toBeInTheDocument()
  })
})

describe('reply box under a comment: refused versus kept', () => {
  it.each([
    ['400 text too long', TOO_LONG, 'text is too long: 4001 characters, the limit is 4000'],
    ['409 room_closed', ROOM_CLOSED, 'room is closed'],
    ['403 not_member', new ApiClientError(403, 'forbidden', 'you cannot comment in this room'), 'you cannot comment in this room'],
  ])('a reply the server refuses (%s) shows the refusal, no offline strip, and stays open with its words', async (_name, err, text) => {
    const post = vi.fn().mockRejectedValue(err)
    render(<Harness post={post} withComments />)
    const box = reply('ack')
    await waitFor(() => expect(screen.getByTestId('room-error')).toHaveTextContent(text))
    expect(screen.queryByRole('alert')).not.toBeInTheDocument()
    expect(screen.queryByRole('button', { name: 'Retry' })).not.toBeInTheDocument()
    expect(screen.queryByText('Editing discards the unsent reply.')).not.toBeInTheDocument()
    expect(box).toHaveValue('ack')
    expect(post).toHaveBeenCalledTimes(1)
  })

  it('a refused reply says why in a line under the reply box', async () => {
    const post = vi.fn().mockRejectedValue(TOO_LONG)
    render(<Harness post={post} withComments />)
    const box = reply('ack')
    const line = await screen.findByRole('status')
    expect(line).toHaveTextContent('text is too long: 4001 characters, the limit is 4000')
    expect(box.closest('div')).toContainElement(line)
    expect(screen.queryByRole('alert')).not.toBeInTheDocument()
    expect(box).toHaveValue('ack')
    fireEvent.change(box, { target: { value: 'short' } })
    expect(screen.queryByRole('status')).not.toBeInTheDocument()
  })

  it('with the server down, the reply keeps its words and offers Retry', async () => {
    const post = vi.fn().mockRejectedValue(SERVER_DOWN)
    render(<Harness post={post} withComments />)
    const box = reply('ack')
    expect(await screen.findByRole('alert')).toHaveTextContent(OFFLINE_TEXT)
    expect(screen.getByRole('button', { name: 'Retry' })).toBeInTheDocument()
    expect(screen.getByText('Editing discards the unsent reply.')).toBeInTheDocument()
    expect(box).toHaveValue('ack')
  })
})

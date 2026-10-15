import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { fireEvent, render, screen, waitFor, within } from '@testing-library/react'
import { toast } from 'sonner'
import { ApiClientError, api } from '@/api/client'
import type { RoomState } from '@/state/types'
import { PostmortemDialog } from './PostmortemDialog'

vi.mock('sonner', () => ({ toast: { success: vi.fn(), error: vi.fn() } }))

const MD = '# Post-mortem: Orders stale\n\n| a | b |\n\\<script\\>alert(1)\\</script\\> [x](http://evil.example)\n'

type DialogState = Pick<RoomState, 'id' | 'postmortem' | 'closed_at' | 'status'>
const closedNow = new Date(Date.now() - 60_000).toISOString()

function room(over: Partial<DialogState> = {}): DialogState {
  return { id: 'r1', status: 'closed', closed_at: closedNow, postmortem: { status: 'drafted', source: 'model', at: closedNow }, ...over }
}

beforeEach(() => {
  vi.restoreAllMocks()
  vi.mocked(toast.success).mockReset()
  vi.mocked(toast.error).mockReset()
})
afterEach(() => vi.unstubAllGlobals())

describe('PostmortemDialog', () => {
  it('shows a skeleton while loading, then the Markdown as plain text in a read-only textarea', async () => {
    let resolve: (s: string) => void = () => undefined
    vi.spyOn(api, 'postmortem').mockReturnValue(new Promise<string>((r) => (resolve = r)))
    render(<PostmortemDialog state={room()} onClose={() => undefined} />)
    const dialog = screen.getByRole('dialog', { name: 'Post-mortem draft' })
    expect(within(dialog).getByRole('status', { name: 'Loading the post-mortem draft' })).toBeInTheDocument()
    expect(within(dialog).getByRole('button', { name: /Copy Markdown/ })).toBeDisabled()

    resolve(MD)
    const area = await within(dialog).findByRole('textbox', { name: 'Post-mortem draft (Markdown)' })
    expect(area).toHaveAttribute('readonly')
    expect((area as HTMLTextAreaElement).value).toBe(MD)
    // Never rendered: no heading, link or script element came out of the Markdown.
    expect(dialog.querySelector('a, script, h1')).toBeNull()
    expect(api.postmortem).toHaveBeenCalledWith('r1')
  })

  it('while drafting shows the agent line above the text', async () => {
    vi.spyOn(api, 'postmortem').mockResolvedValue(MD)
    render(<PostmortemDialog state={room({ postmortem: { status: 'drafting' } })} onClose={() => undefined} />)
    expect(screen.getByText('The agent is writing the closing summary…')).toBeInTheDocument()
    expect(await screen.findByRole('textbox')).toBeInTheDocument()
  })

  it('a drafting state long after close is not shown as writing (a room closed before F2)', async () => {
    vi.spyOn(api, 'postmortem').mockResolvedValue(MD)
    const old = '2026-01-01T00:00:00Z'
    render(<PostmortemDialog state={room({ closed_at: old, postmortem: { status: 'drafting' } })} onClose={() => undefined} />)
    await screen.findByRole('textbox')
    expect(screen.queryByText(/is writing the closing summary/)).toBeNull()
  })

  it('a failed draft says so in words and still shows the events-only text', async () => {
    vi.spyOn(api, 'postmortem').mockResolvedValue(MD)
    render(<PostmortemDialog state={room({ postmortem: { status: 'failed', reason: 'summary_failed' } })} onClose={() => undefined} />)
    expect(screen.getByText(/could not write the closing summary; this draft is built from the room log only/)).toBeInTheDocument()
    expect(await screen.findByRole('textbox')).toBeInTheDocument()
  })

  it('an unknown failure reason falls back to fixed text', () => {
    vi.spyOn(api, 'postmortem').mockResolvedValue(MD)
    render(<PostmortemDialog state={room({ postmortem: { status: 'failed', reason: '<b>boom</b>' } })} onClose={() => undefined} />)
    expect(screen.getByText(/failed on the server/)).toBeInTheDocument()
    expect(screen.queryByText(/boom/)).toBeNull()
  })

  it('an open room says the draft is so far', async () => {
    vi.spyOn(api, 'postmortem').mockResolvedValue(MD)
    render(<PostmortemDialog state={room({ status: 'open', closed_at: undefined, postmortem: null })} onClose={() => undefined} />)
    expect(screen.getByText(/still open: this is the draft so far/)).toBeInTheDocument()
    await screen.findByRole('textbox')
  })

  it('a load error shows friendly text (never the server message) and Retry fetches again', async () => {
    const spy = vi
      .spyOn(api, 'postmortem')
      .mockRejectedValueOnce(new ApiClientError(500, 'internal_error', 'pq: secret detail'))
      .mockResolvedValueOnce(MD)
    render(<PostmortemDialog state={room()} onClose={() => undefined} />)
    const alert = await screen.findByRole('alert')
    expect(alert).toHaveTextContent('Could not load the draft: something went wrong on the server')
    expect(alert).not.toHaveTextContent('pq:')
    fireEvent.click(within(alert).getByRole('button', { name: 'Retry' }))
    expect(await screen.findByRole('textbox')).toBeInTheDocument()
    expect(spy).toHaveBeenCalledTimes(2)
  })

  it.each([
    [new ApiClientError(404, 'not_found', 'x'), 'this room was not found'],
    [new ApiClientError(403, 'forbidden', 'x'), 'you do not have access to this room'],
    [new TypeError('Failed to fetch'), "can't reach Piperoom"],
  ])('maps %s to friendly text', async (err, text) => {
    vi.spyOn(api, 'postmortem').mockRejectedValue(err)
    render(<PostmortemDialog state={room()} onClose={() => undefined} />)
    expect(await screen.findByRole('alert')).toHaveTextContent(text)
  })

  it('Copy Markdown writes the exact text to the clipboard', async () => {
    vi.spyOn(api, 'postmortem').mockResolvedValue(MD)
    const writeText = vi.fn().mockResolvedValue(undefined)
    vi.stubGlobal('navigator', { ...navigator, clipboard: { writeText } })
    render(<PostmortemDialog state={room()} onClose={() => undefined} />)
    await screen.findByRole('textbox')
    fireEvent.click(screen.getByRole('button', { name: /Copy Markdown/ }))
    expect(writeText).toHaveBeenCalledWith(MD)
    await waitFor(() => expect(toast.success).toHaveBeenCalled())
  })

  it('Download .md saves the text under the server file name', async () => {
    vi.spyOn(api, 'postmortem').mockResolvedValue(MD)
    const created = vi.fn().mockReturnValue('blob:x')
    vi.stubGlobal('URL', Object.assign(URL, { createObjectURL: created, revokeObjectURL: vi.fn() }))
    const click = vi.spyOn(HTMLAnchorElement.prototype, 'click').mockImplementation(() => undefined)
    render(<PostmortemDialog state={room()} onClose={() => undefined} />)
    await screen.findByRole('textbox')
    fireEvent.click(screen.getByRole('button', { name: /Download \.md/ }))
    expect(click).toHaveBeenCalledTimes(1)
    const a = click.mock.contexts[0] as HTMLAnchorElement
    expect(a.download).toBe('piperoom-r1-postmortem.md')
    expect(created.mock.calls[0][0]).toBeInstanceOf(Blob)
  })

  it('Close and Esc close it', async () => {
    vi.spyOn(api, 'postmortem').mockResolvedValue(MD)
    const onClose = vi.fn()
    render(<PostmortemDialog state={room()} onClose={onClose} />)
    await screen.findByRole('textbox')
    fireEvent.click(screen.getByRole('button', { name: 'Close' }))
    expect(onClose).toHaveBeenCalledTimes(1)
    fireEvent.keyDown(document.activeElement ?? document.body, { key: 'Escape' })
    expect(onClose).toHaveBeenCalledTimes(2)
  })
})

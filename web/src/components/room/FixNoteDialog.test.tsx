import { describe, expect, it, vi } from 'vitest'
import { fireEvent, render, screen, waitFor } from '@testing-library/react'
import { FixNoteDialog } from './FixNoteDialog'
import type { RoomActions } from '@/hooks/useRoom'
import { emptyRoomState } from '@/state/types'

function setup(fixNote?: RoomActions['fixNote'], closed = false) {
  const onClose = vi.fn()
  const state = closed ? { ...emptyRoomState('r1'), status: 'closed' as const } : emptyRoomState('r1')
  render(<FixNoteDialog state={state} actions={{ fixNote } as RoomActions} onClose={onClose} />)
  return { onClose }
}

const field = () => screen.getByLabelText('What did you do?') as HTMLTextAreaElement
const log = () => screen.getByRole('button', { name: /Log fix|Logging/ })

describe('FixNoteDialog', () => {
  it('is a named dialog and the button waits for words', () => {
    setup(vi.fn())
    expect(screen.getByRole('dialog', { name: 'Log a fix' })).toBeInTheDocument()
    expect(log()).toBeDisabled()
    fireEvent.change(field(), { target: { value: '   ' } })
    expect(log()).toBeDisabled()
    fireEvent.change(field(), { target: { value: 'Restarted order-sync by hand' } })
    expect(log()).toBeEnabled()
  })

  it('sends the trimmed text and closes', async () => {
    const fixNote = vi.fn().mockResolvedValue({ ok: true })
    const { onClose } = setup(fixNote)
    fireEvent.change(field(), { target: { value: '  Restarted order-sync by hand  ' } })
    fireEvent.click(log())
    await waitFor(() => expect(onClose).toHaveBeenCalled())
    expect(fixNote).toHaveBeenCalledWith('Restarted order-sync by hand')
  })

  it('Ctrl+Enter sends it too', async () => {
    const fixNote = vi.fn().mockResolvedValue({ ok: true })
    setup(fixNote)
    fireEvent.change(field(), { target: { value: 'done' } })
    fireEvent.keyDown(field(), { key: 'Enter', ctrlKey: true })
    await waitFor(() => expect(fixNote).toHaveBeenCalledWith('done'))
  })

  it('shows the server answer for a refusal (a closed room past its window, not a member) and keeps the text', async () => {
    const fixNote = vi.fn().mockResolvedValue({ ok: false, error: 'only the closer or a member of the pipeline can add a fix note' })
    const { onClose } = setup(fixNote, true)
    fireEvent.change(field(), { target: { value: 'I did it' } })
    fireEvent.click(log())
    expect(await screen.findByRole('alert')).toHaveTextContent('only the closer or a member of the pipeline')
    expect(onClose).not.toHaveBeenCalled()
    expect(field().value).toBe('I did it')
  })

  it('says what is different about a closed room', () => {
    setup(vi.fn(), true)
    expect(screen.getByText(/This room is closed/)).toBeInTheDocument()
  })

  it('caps the text at the 4000 characters a comment may have', () => {
    setup(vi.fn())
    expect(field()).toHaveAttribute('maxlength', '4000')
  })

  it('says logging is unavailable instead of failing when the room cannot', () => {
    setup(undefined)
    expect(screen.getByRole('alert')).toHaveTextContent('not available')
    expect(log()).toBeDisabled()
  })
})

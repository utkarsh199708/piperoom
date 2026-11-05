import { beforeEach, describe, expect, it, vi } from 'vitest'
import { useState } from 'react'
import { cleanup, fireEvent, render, screen, waitFor, within } from '@testing-library/react'
import { CloseRoomDialog, MAX_RESOLUTION } from './CloseRoomDialog'
import type { RoomActions } from '../hooks/useRoom'
import { emptyRoomState } from '../state/types'

const actions: RoomActions = {
  comment: vi.fn(),
  steer: vi.fn(),
  sendSuggestion: vi.fn(),
  declineSuggestion: vi.fn(),
  requestControl: vi.fn(),
  grantControl: vi.fn(),
  close: vi.fn(),
  approve: vi.fn(),
  reject: vi.fn(),
  handoff: vi.fn(),
  acceptHandoff: vi.fn(),
  label: vi.fn(),
  checklist: vi.fn(),
  trace: vi.fn(),
  hypothesisVerdict: vi.fn(),
  nudge: vi.fn().mockResolvedValue({ ok: true }),
}

function Harness() {
  const [open, setOpen] = useState(false)
  return (
    <>
      <button type="button" onClick={() => setOpen(true)}>
        open dialog
      </button>
      {open && <CloseRoomDialog actions={actions} onClose={() => setOpen(false)} />}
    </>
  )
}

const field = () => screen.getByRole('textbox', { name: 'Resolution' })
const submitButton = () => screen.getByRole('button', { name: /Close room/ })

beforeEach(() => {
  vi.clearAllMocks()
  vi.mocked(actions.close).mockResolvedValue({ ok: true })
})

describe('CloseRoomDialog', () => {
  it('is a named modal dialog with the resolution field focused', async () => {
    render(<CloseRoomDialog actions={actions} onClose={() => {}} />)
    const dialog = screen.getByRole('dialog', { name: 'Close the room' })
    expect(within(dialog).getByRole('button', { name: 'Cancel closing the room' })).toBeInTheDocument()
    await waitFor(() => expect(field()).toHaveFocus())
    expect(field()).toHaveAttribute('maxLength', String(MAX_RESOLUTION))
  })

  it('needs a resolution: empty and blank text keep the button disabled', () => {
    render(<CloseRoomDialog actions={actions} onClose={() => {}} />)
    expect(submitButton()).toBeDisabled()
    fireEvent.change(field(), { target: { value: '   \n ' } })
    expect(submitButton()).toBeDisabled()
    fireEvent.submit(field().closest('form') as HTMLFormElement)
    expect(actions.close).not.toHaveBeenCalled()
  })

  it('sends the trimmed resolution', async () => {
    render(<CloseRoomDialog actions={actions} onClose={() => {}} />)
    fireEvent.change(field(), { target: { value: '  Compatibility set back to BACKWARD.  ' } })
    expect(submitButton()).toBeEnabled()
    fireEvent.click(submitButton())
    await waitFor(() => expect(actions.close).toHaveBeenCalledWith('Compatibility set back to BACKWARD.'))
    expect(actions.close).toHaveBeenCalledTimes(1)
  })

  it('Cmd/Ctrl+Enter submits', async () => {
    render(<CloseRoomDialog actions={actions} onClose={() => {}} />)
    fireEvent.change(field(), { target: { value: 'fixed' } })
    fireEvent.keyDown(field(), { key: 'Enter', ctrlKey: true })
    await waitFor(() => expect(actions.close).toHaveBeenCalledWith('fixed'))
  })

  it('a failed close shows the error, keeps the text and lets the person retry', async () => {
    vi.mocked(actions.close).mockResolvedValueOnce({ ok: false, error: 'only the driver can do this' })
    render(<CloseRoomDialog actions={actions} onClose={() => {}} />)
    fireEvent.change(field(), { target: { value: 'fixed' } })
    fireEvent.click(submitButton())
    expect(await screen.findByRole('alert')).toHaveTextContent('only the driver can do this')
    expect(field()).toHaveValue('fixed')
    expect(submitButton()).toBeEnabled()
    fireEvent.click(submitButton())
    await waitFor(() => expect(actions.close).toHaveBeenCalledTimes(2))
    await waitFor(() => expect(screen.queryByRole('alert')).not.toBeInTheDocument())
  })

  it('does not send twice while a close is in flight', async () => {
    let finish: (v: { ok: boolean }) => void = () => {}
    vi.mocked(actions.close).mockReturnValueOnce(new Promise((r) => (finish = r)))
    render(<CloseRoomDialog actions={actions} onClose={() => {}} />)
    fireEvent.change(field(), { target: { value: 'fixed' } })
    fireEvent.click(submitButton())
    const busyButton = await screen.findByRole('button', { name: /Closing…/ })
    expect(busyButton).toBeDisabled()
    fireEvent.keyDown(field(), { key: 'Enter', metaKey: true })
    expect(actions.close).toHaveBeenCalledTimes(1)
    finish({ ok: true })
  })

  it('Cancel and Esc close it without closing the room; focus returns to the opener', async () => {
    render(<Harness />)
    const opener = screen.getByRole('button', { name: 'open dialog' })
    opener.focus()
    fireEvent.click(opener)
    await screen.findByRole('dialog')
    fireEvent.click(screen.getByRole('button', { name: 'Cancel' }))
    await waitFor(() => expect(screen.queryByRole('dialog')).not.toBeInTheDocument())

    fireEvent.click(opener)
    await waitFor(() => expect(field()).toHaveFocus())
    fireEvent.keyDown(field(), { key: 'Escape' })
    await waitFor(() => expect(screen.queryByRole('dialog')).not.toBeInTheDocument())
    await waitFor(() => expect(opener).toHaveFocus())
    expect(actions.close).not.toHaveBeenCalled()
  })

  describe('label', () => {
    const hypothesis = { text: 'Compatibility changed to FULL', based_on_steps: [], confidence: 'high' as const, confirmed: true }
    const roomWithHypothesis = () => ({ ...emptyRoomState('r1'), hypothesis })

    it('closes, then labels with the pre-filled cause', async () => {
      vi.mocked(actions.label).mockResolvedValue({ ok: true })
      render(<CloseRoomDialog state={roomWithHypothesis()} actions={actions} onClose={() => {}} />)
      fireEvent.change(field(), { target: { value: 'fixed' } })
      fireEvent.click(submitButton())
      await waitFor(() => expect(actions.label).toHaveBeenCalledWith({ cause: 'Compatibility changed to FULL', actual_layer: 'schema-registry' }))
      expect(actions.close).toHaveBeenCalledWith('fixed')
    })

    it('Skip label closes without a label', async () => {
      render(<CloseRoomDialog state={roomWithHypothesis()} actions={actions} onClose={() => {}} />)
      fireEvent.change(field(), { target: { value: 'fixed' } })
      fireEvent.click(screen.getByRole('button', { name: 'Skip label' }))
      await waitFor(() => expect(actions.close).toHaveBeenCalledWith('fixed'))
      expect(actions.label).not.toHaveBeenCalled()
    })

    it('an empty cause closes without a label, and a failed label does not undo the close', async () => {
      vi.mocked(actions.label).mockResolvedValue({ ok: false, error: 'nope' })
      render(<CloseRoomDialog state={emptyRoomState('r1')} actions={actions} onClose={() => {}} />)
      fireEvent.change(field(), { target: { value: 'fixed' } })
      fireEvent.click(submitButton())
      await waitFor(() => expect(actions.close).toHaveBeenCalled())
      expect(actions.label).not.toHaveBeenCalled()
      cleanup()
      render(<CloseRoomDialog state={roomWithHypothesis()} actions={actions} onClose={() => {}} />)
      fireEvent.change(field(), { target: { value: 'fixed' } })
      fireEvent.click(submitButton())
      await waitFor(() => expect(actions.label).toHaveBeenCalled())
      expect(screen.queryByRole('alert')).toBeNull() // the close worked: no error in the dialog
    })
  })
})

describe('CloseRoomDialog in a shadow room', () => {
  const shadowRoom = () => ({ ...emptyRoomState('r1'), shadow: true, hypothesis: { text: 'Compatibility changed to FULL', based_on_steps: [], confidence: 'high' as const, confirmed: true } })

  it('asks only for the resolution and says a blind label follows', () => {
    render(<CloseRoomDialog state={shadowRoom()} actions={actions} onClose={() => {}} />)
    expect(field()).toBeInTheDocument()
    expect(screen.getByText(/you will record a blind label/)).toBeInTheDocument()
    expect(screen.queryByText('Label for later incidents')).toBeNull()
    expect(screen.queryByText(/Did the agent help/)).toBeNull()
    expect(screen.queryByRole('button', { name: 'Skip label' })).toBeNull()
  })

  it('closes without sending a label or showing a label error', async () => {
    render(<CloseRoomDialog state={shadowRoom()} actions={actions} onClose={() => {}} />)
    fireEvent.change(field(), { target: { value: 'fixed' } })
    fireEvent.click(submitButton())
    await waitFor(() => expect(actions.close).toHaveBeenCalledWith('fixed'))
    expect(actions.label).not.toHaveBeenCalled()
  })

  it('an ordinary room keeps the label fields and no blind-label note', () => {
    render(<CloseRoomDialog state={{ ...shadowRoom(), shadow: false }} actions={actions} onClose={() => {}} />)
    expect(screen.getByText('Label for later incidents')).toBeInTheDocument()
    expect(screen.queryByText(/blind label/)).toBeNull()
  })
})

describe('CloseRoomDialog initialText', () => {
  it('starts with the given resolution, so the close nudge can pre-fill it', () => {
    render(<CloseRoomDialog actions={actions} onClose={() => {}} initialText={'Fixed by "Restart task"'} />)
    expect(screen.getByRole('textbox')).toHaveValue('Fixed by "Restart task"')
  })
})

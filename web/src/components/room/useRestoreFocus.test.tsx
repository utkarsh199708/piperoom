import { describe, expect, it } from 'vitest'
import { act, fireEvent, render, screen } from '@testing-library/react'
import { useState } from 'react'
import { Dialog, DialogContent, DialogDescription, DialogTitle } from '@/components/ui/dialog'
import { DropdownMenu, DropdownMenuContent, DropdownMenuItem, DropdownMenuTrigger } from '@/components/ui/dropdown-menu'
import { useRestoreFocus } from './useRestoreFocus'

function Inner({ onClose }: { onClose: () => void }) {
  const restore = useRestoreFocus()
  return (
    <Dialog open onOpenChange={(o) => !o && onClose()}>
      <DialogContent onCloseAutoFocus={restore}>
        <DialogTitle>Post-mortem draft</DialogTitle>
        <DialogDescription>d</DialogDescription>
        <button type="button" onClick={onClose}>Close now</button>
      </DialogContent>
    </Dialog>
  )
}

/** The ⋯ menu: choosing the item closes the menu and mounts the dialog in the same commit. */
function Menu() {
  const [open, setOpen] = useState(false)
  return (
    <>
      <main id="main" tabIndex={-1}>
        <DropdownMenu>
          <DropdownMenuTrigger aria-label="More room actions">⋯</DropdownMenuTrigger>
          <DropdownMenuContent>
            <DropdownMenuItem onSelect={() => setOpen(true)}>Post-mortem draft</DropdownMenuItem>
          </DropdownMenuContent>
        </DropdownMenu>
      </main>
      {open && <Inner onClose={() => setOpen(false)} />}
    </>
  )
}

describe('useRestoreFocus', () => {
  it.each([
    ['Escape', () => fireEvent.keyDown(screen.getByRole('dialog'), { key: 'Escape' })],
    ['a close button', () => fireEvent.click(screen.getByRole('button', { name: 'Close now' }))],
  ])('after the ⋯ menu opened the dialog, %s returns focus to the ⋯ button', async (_n, close) => {
    render(<Menu />)
    const trigger = screen.getByRole('button', { name: 'More room actions' })
    act(() => trigger.focus())
    fireEvent.keyDown(trigger, { key: 'Enter' })
    const item = await screen.findByRole('menuitem', { name: 'Post-mortem draft' })
    act(() => item.focus())
    fireEvent.click(item)
    await screen.findByRole('dialog')
    close()
    await new Promise((r) => setTimeout(r, 0))
    expect(screen.queryByRole('dialog')).toBeNull()
    expect(document.activeElement).toBe(trigger)
  })

  it('with no opener to return to (⌘K palette gone), focus lands on <main>, not <body>', async () => {
    const { rerender } = render(<main id="main" tabIndex={-1} />)
    const gone = document.createElement('button')
    document.body.append(gone)
    gone.focus()
    function Host({ show }: { show: boolean }) {
      return (
        <>
          <main id="main" tabIndex={-1} />
          {show && <Inner onClose={() => undefined} />}
        </>
      )
    }
    rerender(<Host show />)
    gone.remove()
    await screen.findByRole('dialog')
    fireEvent.keyDown(screen.getByRole('dialog'), { key: 'Escape' })
    rerender(<Host show={false} />)
    await new Promise((r) => setTimeout(r, 0))
    expect(document.activeElement).toBe(document.getElementById('main'))
  })
})

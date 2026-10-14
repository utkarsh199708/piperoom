import { useRef, useState } from 'react'
import type { FormEvent, KeyboardEvent } from 'react'
import type { RoomActions } from '../hooks/useRoom'
import { modKeyLabel } from '@/lib/envLabel'
import { Button } from '@/components/ui/button'
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from '@/components/ui/dialog'
import { Kbd } from '@/components/ui/kbd'
import { Textarea } from '@/components/ui/textarea'
import { RoomDialogClose } from './room/RoomDialogClose'
import { useRestoreFocus } from './room/useRestoreFocus'

/** Longest resolution the dialog accepts; a resolution is a sentence or two, not a report. */
export const MAX_RESOLUTION = 2000

// The driver closes the room with a resolution. The server requires one for a shadow room
// and it is what the export and the room list show afterwards, so it is required here for
// every room. The room screen unmounts the dialog once `room.closed` arrives; until then it
// stays open, so a failed close keeps the text and shows the error where the person is looking.
export function CloseRoomDialog({ actions, onClose }: { actions: RoomActions; onClose: () => void }) {
  const [text, setText] = useState('')
  const [busy, setBusy] = useState(false)
  const [error, setError] = useState<string | null>(null)
  const restoreFocus = useRestoreFocus()
  const field = useRef<HTMLTextAreaElement>(null)
  const resolution = text.trim()

  const submit = async () => {
    if (!resolution || busy) return
    setBusy(true)
    setError(null)
    const res = await actions.close(resolution)
    setBusy(false)
    if (!res.ok) setError(res.error ?? 'The room could not be closed.')
  }
  const onSubmit = (e: FormEvent) => {
    e.preventDefault()
    void submit()
  }
  const onKeyDown = (e: KeyboardEvent) => {
    if (e.key === 'Enter' && (e.metaKey || e.ctrlKey) && !e.nativeEvent.isComposing) {
      e.preventDefault()
      void submit()
    }
  }

  return (
    <Dialog open onOpenChange={(open) => !open && onClose()}>
      <DialogContent
        hideClose
        onKeyDown={onKeyDown}
        // Radix focuses the first tabbable (the corner X); the person came here to type.
        onOpenAutoFocus={(e) => {
          e.preventDefault()
          field.current?.focus()
        }}
        onCloseAutoFocus={restoreFocus}
      >
        <RoomDialogClose label="Cancel closing the room" />
        <DialogHeader>
          <DialogTitle>Close the room</DialogTitle>
          <DialogDescription>
            Closing stops the agent and ends the incident. Say what fixed it: the resolution stays with the room and
            its export.
          </DialogDescription>
        </DialogHeader>
        <form className="flex flex-col gap-4" onSubmit={onSubmit}>
          <div className="flex flex-col gap-1.5">
            <label htmlFor="close-resolution" className="text-sm font-medium">
              Resolution
            </label>
            <Textarea
              id="close-resolution"
              ref={field}
              required
              rows={4}
              maxLength={MAX_RESOLUTION}
              value={text}
              onChange={(e) => setText(e.target.value)}
              placeholder="e.g. Schema compatibility set back to BACKWARD; connector restarted and caught up."
            />
          </div>
          {error && (
            <p role="alert" className="m-0 text-sm font-medium text-destructive-fg">
              {error}
            </p>
          )}
          <DialogFooter>
            <Button type="button" variant="ghost" onClick={onClose}>
              Cancel
            </Button>
            <Button type="submit" variant="destructive" disabled={!resolution || busy}>
              {busy ? 'Closing…' : 'Close room'}
              <Kbd aria-hidden="true">{modKeyLabel()}⏎</Kbd>
            </Button>
          </DialogFooter>
        </form>
      </DialogContent>
    </Dialog>
  )
}

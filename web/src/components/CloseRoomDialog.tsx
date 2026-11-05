import { useRef, useState } from 'react'
import type { FormEvent, KeyboardEvent } from 'react'
import { toast } from 'sonner'
import type { RoomActions } from '../hooks/useRoom'
import type { RoomState } from '../state/types'
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
import { LabelFields } from './room/LabelForm'
import { draftToInput, initialDraft } from '../lib/label'

/** Longest resolution the dialog accepts; a resolution is a sentence or two, not a report. */
export const MAX_RESOLUTION = 2000

// The driver closes the room with a resolution. The server requires one for a shadow room
// and it is what the export and the room list show afterwards, so it is required here for
// every room. The room screen unmounts the dialog once `room.closed` arrives; until then it
// stays open, so a failed close keeps the text and shows the error where the person is looking.
export function CloseRoomDialog({
  actions,
  onClose,
  state,
  initialText = '',
}: {
  actions: RoomActions
  onClose: () => void
  /** The room, for the label form's pre-filled values. Without it the dialog only closes. */
  state?: RoomState
  /** Pre-filled resolution, e.g. `Fixed by "<title>"` from the close nudge. */
  initialText?: string
}) {
  const [text, setText] = useState(initialText)
  // A shadow room takes the blind label first (the server answers 409 to the normal label
  // before it), so its dialog asks for the resolution only; the blind form shows once it closes.
  const shadow = state?.shadow === true
  const [draft, setDraft] = useState(() => (state && !shadow ? initialDraft(state) : null))
  const [busy, setBusy] = useState(false)
  const [error, setError] = useState<string | null>(null)
  const restoreFocus = useRestoreFocus()
  const field = useRef<HTMLTextAreaElement>(null)
  const resolution = text.trim()

  // Two calls: /close, then /label. A failed label is a toast, not a failed close: the room is
  // closed and the "Add label" banner keeps the form available.
  const submit = async (withLabel: boolean) => {
    if (!resolution || busy) return
    setBusy(true)
    setError(null)
    const input = withLabel && state && draft ? draftToInput(state, draft) : null
    const res = await actions.close(resolution)
    setBusy(false)
    if (!res.ok) {
      setError(res.error ?? 'The room could not be closed.')
      return
    }
    if (input) {
      const labelled = await actions.label(input)
      if (!labelled.ok) toast.error(`The room is closed, but the label was not saved: ${labelled.error ?? 'unknown error'}. Use "Add label" to try again.`)
    }
  }
  const onSubmit = (e: FormEvent) => {
    e.preventDefault()
    void submit(true)
  }
  const onKeyDown = (e: KeyboardEvent) => {
    if (e.key === 'Enter' && (e.metaKey || e.ctrlKey) && !e.nativeEvent.isComposing) {
      e.preventDefault()
      void submit(true)
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
            Closing ends the incident and stops the agent (in a shadow room it may be allowed to finish first). Say what fixed it: the resolution stays with the room and
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
          {shadow && (
            <p className="m-0 text-sm text-muted-foreground">After closing you will record a blind label: your own answer, before the agent&apos;s work is shown.</p>
          )}
          {state && draft && (
            <fieldset className="m-0 flex flex-col gap-3 border-0 p-0">
              <legend className="mb-1 p-0 text-sm font-medium">Label for later incidents</legend>
              <p className="m-0 text-xs text-muted-foreground">
                What the cause was and what fixed it. Similar rooms show it. Clear the cause to close without a label.
              </p>
              <LabelFields state={state} draft={draft} onChange={setDraft} idPrefix="close-label" />
            </fieldset>
          )}
          {error && (
            <p role="alert" className="m-0 text-sm font-medium text-destructive-fg">
              {error}
            </p>
          )}
          <DialogFooter>
            <Button type="button" variant="ghost" onClick={onClose}>
              Cancel
            </Button>
            {state && !shadow && (
              <Button type="button" variant="outline" disabled={!resolution || busy} onClick={() => void submit(false)}>
                Skip label
              </Button>
            )}
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

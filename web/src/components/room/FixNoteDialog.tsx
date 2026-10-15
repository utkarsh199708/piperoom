import { useRef, useState } from 'react'
import type { FormEvent, KeyboardEvent } from 'react'
import { Wrench } from 'lucide-react'
import type { RoomActions } from '@/hooks/useRoom'
import type { RoomState } from '@/state/types'
import { MAX_FIX_NOTE } from '@/lib/fixNote'
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
import { RoomDialogClose } from './RoomDialogClose'
import { useRestoreFocus } from './useRestoreFocus'

// "Log a fix" (QA G10): a person fixed it outside Piperoom (a manual restart, a config change) and says so.
// It is a comment of kind fix_note; the post-mortem lists it under "What was done" and treats the room as
// fixed by a person. The server decides who may add one to a closed room (its closer or a member of the
// pipeline, for 24 hours), so a refusal is shown here in its own words and the text is kept.
export function FixNoteDialog({
  state,
  actions,
  onClose,
}: {
  state: RoomState
  actions: RoomActions
  onClose: () => void
}) {
  const [text, setText] = useState('')
  const [busy, setBusy] = useState(false)
  const [error, setError] = useState<string | null>(null)
  const restoreFocus = useRestoreFocus()
  const field = useRef<HTMLTextAreaElement>(null)
  const note = text.trim()
  const send = actions.fixNote

  const submit = async () => {
    if (!send || !note || busy) return
    setBusy(true)
    setError(null)
    const res = await send(note)
    setBusy(false)
    if (res.ok) onClose()
    else setError(res.error ?? 'The fix could not be logged.')
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
        onOpenAutoFocus={(e) => {
          e.preventDefault()
          field.current?.focus()
        }}
        onCloseAutoFocus={restoreFocus}
      >
        <RoomDialogClose label="Cancel logging a fix" />
        <DialogHeader>
          <DialogTitle>Log a fix</DialogTitle>
          <DialogDescription>
            {state.status === 'open'
              ? 'Say what you did outside Piperoom. It goes in the room as a fix note and into the post-mortem under "What was done".'
              : 'This room is closed. Say what was done outside Piperoom; the note is added to the room and its post-mortem.'}
          </DialogDescription>
        </DialogHeader>
        <form className="flex flex-col gap-4" onSubmit={onSubmit}>
          <div className="flex flex-col gap-1.5">
            <label htmlFor="fix-note-text" className="text-sm font-medium">
              What did you do?
            </label>
            <Textarea
              id="fix-note-text"
              ref={field}
              required
              rows={4}
              maxLength={MAX_FIX_NOTE}
              value={text}
              onChange={(e) => setText(e.target.value)}
              placeholder="e.g. Restarted order-sync by hand; lag is draining."
            />
          </div>
          {!send && (
            <p role="alert" className="m-0 text-sm font-medium text-destructive-fg">
              Logging a fix is not available here.
            </p>
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
            <Button type="submit" disabled={!send || !note || busy}>
              <Wrench aria-hidden="true" />
              {busy ? 'Logging…' : 'Log fix'}
              {!busy && <Kbd aria-hidden="true">{modKeyLabel()}⏎</Kbd>}
            </Button>
          </DialogFooter>
        </form>
      </DialogContent>
    </Dialog>
  )
}

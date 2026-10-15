import type { LabelInput } from '@/api/types'
import type { RoomState } from '@/state/types'
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogHeader,
  DialogTitle,
} from '@/components/ui/dialog'
import { LabelForm } from './LabelForm'
import { RoomDialogClose } from './RoomDialogClose'
import { useRestoreFocus } from './useRestoreFocus'

// "Add label" on a closed room that has none: the same form as in the close dialog, on its own.
export function LabelDialog({
  state,
  onSubmit,
  onClose,
}: {
  state: RoomState
  onSubmit: (input: LabelInput) => Promise<{ ok: boolean; error?: string }>
  onClose: () => void
}) {
  const restoreFocus = useRestoreFocus()
  return (
    <Dialog open onOpenChange={(open) => !open && onClose()}>
      <DialogContent hideClose onCloseAutoFocus={restoreFocus}>
        <RoomDialogClose label="Cancel labelling the room" />
        <DialogHeader>
          <DialogTitle>Label the room</DialogTitle>
          <DialogDescription>
            What the cause was and what fixed it. Later rooms with the same trouble show it.
          </DialogDescription>
        </DialogHeader>
        <LabelForm
          state={state}
          onSkip={onClose}
          onSubmit={async (input) => {
            const res = await onSubmit(input)
            if (res.ok) onClose()
            return res
          }}
        />
      </DialogContent>
    </Dialog>
  )
}

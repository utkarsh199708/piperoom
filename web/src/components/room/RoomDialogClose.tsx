import { X } from 'lucide-react'
import { DialogClose } from '@/components/ui/dialog'

/** The corner X of a room dialog, with the dialog's own name ("Close hand-off dialog"). */
export function RoomDialogClose({ label }: { label: string }) {
  return (
    <DialogClose
      aria-label={label}
      className="absolute right-3 top-3 inline-flex h-8 w-8 items-center justify-center rounded-md text-muted-foreground transition-colors hover:bg-accent hover:text-foreground focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring"
    >
      <X className="size-4" aria-hidden="true" />
    </DialogClose>
  )
}

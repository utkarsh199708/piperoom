import { useState } from 'react'
import { RotateCcw } from 'lucide-react'
import { Button } from '@/components/ui/button'
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from '@/components/ui/dialog'
import { rollbackSteer } from '../../lib/rollback'
import type { SendControls, SendOptions } from '../../hooks/useRoom'
import { useRetryId } from '../../hooks/useRetryId'
import { OfflineStrip } from './OfflineStrip'

// There is no rollback route: the undo goes through the same steer path as any instruction, so it
// is a new proposal that needs its own approvals. The dialog says so.
export function RollbackButton({
  title,
  proposalId,
  rollback,
  asDriver,
  onSteer,
  sends,
}: {
  title: string
  proposalId: string
  rollback: string
  /** The driver's steer goes to the agent; a member's is a suggestion in the driver's queue. */
  asDriver: boolean
  onSteer: (text: string, opts?: SendOptions) => Promise<boolean>
  /** RoomActions.sends: a failed request that lands closes the dialog; closing it cancels the request. */
  sends?: SendControls
}) {
  const [open, setOpen] = useState(false)
  const [busy, setBusy] = useState(false)
  const [failed, setFailed] = useState(false)
  // One rollback request is one action (#459): only the Retry of a failed request keeps its
  // client_id. Closing the dialog cancels it in the ledger (it is never sent again), and the
  // request landing after all ends it too; the next request is then a new one.
  const retryId = useRetryId(sends, () => {
    setFailed(false)
    setOpen(false)
  })
  const send = async (retry: boolean) => {
    setBusy(true)
    const clientId = retryId.next(retry)
    const ok = await onSteer(rollbackSteer(title, proposalId, rollback), { clientId })
    setBusy(false)
    setFailed(!ok)
    retryId.settle(clientId, ok)
    if (ok) setOpen(false)
  }
  const onOpenChange = (next: boolean) => {
    setOpen(next)
    if (!next) {
      retryId.drop()
      setFailed(false)
    }
  }
  return (
    <>
      <Button variant="ghost-danger" size="sm" className="h-7" onClick={() => setOpen(true)}>
        <RotateCcw aria-hidden="true" className="!size-3.5" />
        {asDriver ? 'Roll back' : 'Suggest roll back'}
      </Button>
      <Dialog open={open} onOpenChange={onOpenChange}>
        <DialogContent>
          <DialogHeader>
            <DialogTitle>{asDriver ? 'Roll back this fix?' : 'Suggest rolling back this fix?'}</DialogTitle>
            <DialogDescription>
              This asks the agent to propose the undo; nothing runs without approvals.
            </DialogDescription>
          </DialogHeader>
          <p className="m-0 break-words text-sm">
            <span className="text-xs font-medium text-subtle-foreground">Recorded rollback: </span>
            {rollback || 'none recorded'}
          </p>
          {failed && <OfflineStrip onRetry={() => void send(true)} disabled={busy} />}
          <DialogFooter>
            <Button variant="ghost" onClick={() => onOpenChange(false)}>
              Cancel
            </Button>
            <Button variant="destructive" disabled={busy} onClick={() => void send(failed)}>
              {asDriver ? 'Ask the agent to roll back' : 'Suggest it to the driver'}
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>
    </>
  )
}

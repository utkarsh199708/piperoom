import { useRef, useState } from 'react'
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
import { newClientId } from '../../lib/clientId'
import { rollbackSteer } from '../../lib/rollback'
import type { SendOptions } from '../../hooks/useRoom'
import { OfflineStrip } from './OfflineStrip'

// There is no rollback route: the undo goes through the same steer path as any instruction, so it
// is a new proposal that needs its own approvals. The dialog says so.
export function RollbackButton({
  title,
  proposalId,
  rollback,
  asDriver,
  onSteer,
}: {
  title: string
  proposalId: string
  rollback: string
  /** The driver's steer goes to the agent; a member's is a suggestion in the driver's queue. */
  asDriver: boolean
  onSteer: (text: string, opts?: SendOptions) => Promise<boolean>
}) {
  const [open, setOpen] = useState(false)
  const [busy, setBusy] = useState(false)
  const [failed, setFailed] = useState(false)
  // One rollback request is one action (#459): Retry keeps its client_id, so the server stores it
  // at most once even if an earlier copy landed; the next request after a success gets a new id.
  const clientIdRef = useRef<string | null>(null)
  const send = async () => {
    setBusy(true)
    const clientId = clientIdRef.current ?? newClientId()
    clientIdRef.current = clientId
    const ok = await onSteer(rollbackSteer(title, proposalId, rollback), { clientId })
    setBusy(false)
    setFailed(!ok)
    if (ok) {
      clientIdRef.current = null
      setOpen(false)
    }
  }
  return (
    <>
      <Button variant="ghost-danger" size="sm" className="h-7" onClick={() => setOpen(true)}>
        <RotateCcw aria-hidden="true" className="!size-3.5" />
        {asDriver ? 'Roll back' : 'Suggest roll back'}
      </Button>
      <Dialog open={open} onOpenChange={setOpen}>
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
          {failed && <OfflineStrip onRetry={() => void send()} disabled={busy} />}
          <DialogFooter>
            <Button variant="ghost" onClick={() => setOpen(false)}>
              Cancel
            </Button>
            <Button variant="destructive" disabled={busy} onClick={() => void send()}>
              {asDriver ? 'Ask the agent to roll back' : 'Suggest it to the driver'}
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>
    </>
  )
}

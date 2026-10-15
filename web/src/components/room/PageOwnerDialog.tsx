import { useState } from 'react'
import type { FormEvent } from 'react'
import { BellRing } from 'lucide-react'
import { LAYERS } from '@/api/types'
import type { Person } from '@/api/types'
import type { RoomActions } from '@/hooks/useRoom'
import type { RoomState } from '@/state/types'
import { Button } from '@/components/ui/button'
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from '@/components/ui/dialog'
import { Input } from '@/components/ui/input'
import { MAX_PAGE_REASON, suggestedPageLayer } from '@/lib/pages'
import { RoomDialogClose } from './RoomDialogClose'
import { useRestoreFocus } from './useRestoreFocus'

const SELECT =
  'h-10 rounded-md bg-muted px-3 text-sm focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring'

// Any member can ask the owners of a layer to look at the room (QA G2: a watcher or alert room used to
// open with nobody told). The server picks the owners from owners.yaml; this form only chooses the layer,
// says why, and optionally names the fix that is waiting. A refusal such as `already_paged` (one page per
// layer per 10 minutes) is shown in the server's words, inside the dialog, and nothing is lost.
export function PageOwnerDialog({
  state,
  actions,
  people,
  onClose,
}: {
  state: RoomState
  actions: RoomActions
  /** GET /api/people, when it loaded: names the approvers of the chosen layer as a hint. */
  people?: Person[]
  onClose: () => void
}) {
  const [layer, setLayer] = useState(() => suggestedPageLayer(state))
  const [proposalId, setProposalId] = useState('')
  const [reason, setReason] = useState('')
  const [busy, setBusy] = useState(false)
  const [error, setError] = useState<string | null>(null)
  const restoreFocus = useRestoreFocus()
  const openProposals = state.proposals.filter((p) => p.status === 'open')
  const text = reason.trim()
  const approvers = layer ? (people ?? []).filter((p) => p.layers?.includes(layer)).map((p) => p.name) : []
  const send = actions.page

  const submit = async (e: FormEvent) => {
    e.preventDefault()
    if (!send || !layer || !text || busy) return
    setBusy(true)
    setError(null)
    const res = await send({ layer, reason: text, ...(proposalId ? { proposal_id: proposalId } : {}) })
    setBusy(false)
    if (res.ok) onClose()
    else setError(res.error ?? 'The page could not be sent.')
  }

  return (
    <Dialog open onOpenChange={(open) => !open && onClose()}>
      <DialogContent hideClose onCloseAutoFocus={restoreFocus}>
        <RoomDialogClose label="Cancel paging an owner" />
        <DialogHeader>
          <DialogTitle>Page an owner</DialogTitle>
          <DialogDescription>
            Ask the owners of a layer to look at this room. They see it in the app, and by Slack or the webhook
            when the server has them set up.
          </DialogDescription>
        </DialogHeader>
        <form className="flex flex-col gap-4" onSubmit={(e) => void submit(e)}>
          <div className="flex flex-col gap-1.5">
            <label htmlFor="page-layer" className="text-sm font-medium">
              Layer
            </label>
            <select id="page-layer" value={layer} onChange={(e) => setLayer(e.target.value)} className={SELECT}>
              <option value="">Choose a layer…</option>
              {LAYERS.map((l) => (
                <option key={l} value={l}>
                  {l}
                </option>
              ))}
            </select>
            {approvers.length > 0 && (
              <p className="m-0 text-xs text-muted-foreground">Approvers for {layer}: {approvers.join(', ')}</p>
            )}
          </div>
          <div className="flex flex-col gap-1.5">
            <label htmlFor="page-reason" className="text-sm font-medium">
              Why now
            </label>
            <Input
              id="page-reason"
              required
              maxLength={MAX_PAGE_REASON}
              value={reason}
              onChange={(e) => setReason(e.target.value)}
              placeholder="e.g. Consumer lag is growing and the connector looks healthy"
            />
            <p className="m-0 text-right text-xs tabular-nums text-subtle-foreground">
              {reason.length}/{MAX_PAGE_REASON}
            </p>
          </div>
          {openProposals.length > 0 && (
            <div className="flex flex-col gap-1.5">
              <label htmlFor="page-proposal" className="text-sm font-medium">
                About a fix (optional)
              </label>
              <select
                id="page-proposal"
                value={proposalId}
                onChange={(e) => setProposalId(e.target.value)}
                className={SELECT}
              >
                <option value="">No specific fix</option>
                {openProposals.map((p) => (
                  <option key={p.proposal_id} value={p.proposal_id}>
                    {p.title}
                  </option>
                ))}
              </select>
            </div>
          )}
          {!send && (
            <p role="alert" className="m-0 text-sm font-medium text-destructive-fg">
              Paging is not available here.
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
            <Button type="submit" disabled={!send || !layer || !text || busy}>
              <BellRing aria-hidden="true" />
              {busy ? 'Paging…' : 'Page owner'}
            </Button>
          </DialogFooter>
        </form>
      </DialogContent>
    </Dialog>
  )
}

import { useState } from 'react'
import type { FormEvent, KeyboardEvent } from 'react'
import type { HandoffState, RoomState } from '../state/types'
import type { CurrentUser } from '../state/permissions'
import { canAcceptHandoff } from '../state/permissions'
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
import { PersonSelect } from './PersonSelect'
import { RoomDialogClose } from './room/RoomDialogClose'
import { useRestoreFocus } from './room/useRestoreFocus'

function SummaryList({ title, items }: { title: string; items: string[] }) {
  if (!items || items.length === 0) return null
  return (
    <div className="flex flex-col gap-1">
      <h4 className="text-xs font-medium text-muted-foreground">{title}</h4>
      <ul className="m-0 flex list-disc flex-col gap-0.5 pl-5">
        {items.map((it, i) => (
          <li key={i} className="break-words">
            {it}
          </li>
        ))}
      </ul>
    </div>
  )
}

// A shadcn Dialog: Esc and the X close it, focus is trapped inside and goes back to
// whatever opened it. The room screen mounts it only while it is open.
export function HandoffDialog({
  state,
  user,
  actions,
  onClose,
}: {
  state: RoomState
  user: CurrentUser
  actions: RoomActions
  onClose: () => void
}) {
  const [to, setTo] = useState('')
  const [stay, setStay] = useState(false)
  const restoreFocus = useRestoreFocus()
  const pending = state.handoffs.find((h) => h.status === 'pending')

  const start = () => {
    if (!to) return
    actions.handoff(to, stay)
  }
  const submit = (e: FormEvent) => {
    e.preventDefault()
    start()
  }
  const onKeyDown = (e: KeyboardEvent) => {
    if (e.key === 'Enter' && (e.metaKey || e.ctrlKey) && !e.nativeEvent.isComposing) {
      e.preventDefault()
      start()
    }
  }

  return (
    <Dialog open onOpenChange={(open) => !open && onClose()}>
      <DialogContent
        hideClose
        className="max-h-[85vh] overflow-y-auto"
        onKeyDown={pending ? undefined : onKeyDown}
        onCloseAutoFocus={restoreFocus}
      >
        <RoomDialogClose label="Close hand-off dialog" />
        <DialogHeader>
          <DialogTitle>Hand off the room</DialogTitle>
          <DialogDescription>
            {pending
              ? `Summary prepared for ${pending.to.name}. They must accept to take over.`
              : 'The agent writes a summary for the person who takes over. They accept it to become the driver.'}
          </DialogDescription>
        </DialogHeader>

        {pending ? (
          <HandoffSummary pending={pending} onClose={onClose} />
        ) : (
          <form className="flex flex-col gap-4" onSubmit={submit}>
            <div className="flex flex-col gap-1.5">
              <label htmlFor="handoff-to" className="text-sm font-medium">
                Hand off to
              </label>
              <div className="[&_select]:h-9 [&_select]:w-full">
                <PersonSelect id="handoff-to" selfId={user.id} value={to} onChange={setTo} />
              </div>
            </div>
            <label className="flex items-center gap-2 text-sm">
              <input
                type="checkbox"
                className="size-4 rounded-sm accent-primary"
                checked={stay}
                onChange={(e) => setStay(e.target.checked)}
              />
              Stay as watcher after hand-off
            </label>
            <DialogFooter>
              <Button type="button" variant="ghost" onClick={onClose}>
                Cancel
              </Button>
              <Button type="submit" disabled={!to}>
                Start hand-off
                <Kbd aria-hidden="true">{modKeyLabel()}⏎</Kbd>
              </Button>
            </DialogFooter>
          </form>
        )}
      </DialogContent>
    </Dialog>
  )
}

function HandoffSummary({ pending, onClose }: { pending: HandoffState; onClose: () => void }) {
  const s = pending.summary
  return (
    <div className="handoff__summary flex flex-col gap-3">
      {s.what_broke && (
        <div className="flex flex-col gap-1">
          <h4 className="text-xs font-medium text-muted-foreground">What broke</h4>
          <p className="m-0 break-words">{s.what_broke}</p>
        </div>
      )}
      {s.why && (
        <div className="flex flex-col gap-1">
          <h4 className="text-xs font-medium text-muted-foreground">Why</h4>
          <p className="m-0 break-words">{s.why}</p>
        </div>
      )}
      <SummaryList title="Done" items={s.done} />
      <SummaryList title="Ruled out" items={s.ruled_out} />
      <SummaryList title="Still open" items={s.open} />
      <SummaryList title="Risks" items={s.risks} />
      <DialogFooter>
        <Button type="button" variant="secondary" onClick={onClose}>
          Close
        </Button>
      </DialogFooter>
    </div>
  )
}

export function HandoffAcceptBanner({
  state,
  user,
  actions,
}: {
  state: RoomState
  user: CurrentUser
  actions: RoomActions
}) {
  const mine = state.handoffs.find((h) => canAcceptHandoff(user, h))
  if (!mine) return null
  return (
    <div
      role="status"
      className="flex min-h-10 flex-wrap items-center gap-x-2.5 gap-y-1 border-b border-border bg-primary-subtle px-4 py-1.5 font-medium text-primary-fg sm:px-6"
    >
      <span>
        <strong>{state.driver?.name ?? 'Someone'}</strong> is handing the room to you.
      </span>
      <Button className="ml-auto h-7 px-2.5" onClick={() => actions.acceptHandoff(mine.handoff_id)}>
        Accept
      </Button>
    </div>
  )
}

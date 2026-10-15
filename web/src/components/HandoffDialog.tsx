import { Loader2 } from 'lucide-react'
import { HandoffChecks } from './room/HandoffChecks'
import { useState } from 'react'
import type { ComponentProps, FormEvent, KeyboardEvent, ReactNode } from 'react'
import type { HandoffState, RoomState } from '../state/types'
import type { CurrentUser } from '../state/permissions'
import { canAcceptHandoff } from '../state/permissions'
import type { RoomActions } from '../hooks/useRoom'
import { modKeyLabel } from '@/lib/envLabel'
import { handoffFailureText, handoffInFlight } from '@/lib/handoff'
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
import { PersonName } from './VerifiedBadge'
import { RoomDialogClose } from './room/RoomDialogClose'
import { useNow } from './room/useNow'
import { useRestoreFocus } from './room/useRestoreFocus'

/** The handing-off driver as the badge reads it: server-set fields only. */
type FromPerson = ComponentProps<typeof PersonName>['person']

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
  const now = useNow(5_000)
  const inFlight = pending ? null : handoffInFlight(state, now)
  const failure = state.handoff_failure

  const start = () => {
    if (!to || inFlight) return
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
        onKeyDown={pending || inFlight ? undefined : onKeyDown}
        onCloseAutoFocus={restoreFocus}
      >
        <RoomDialogClose label="Close hand-off dialog" />
        <DialogHeader>
          <DialogTitle>Hand off the room</DialogTitle>
          <DialogDescription>
            {pending ? (
              <>
                Summary prepared for <PersonName person={pending.to} />. They must accept to take over.
              </>
            ) : inFlight ? (
              <>
                The agent is writing the summary for <PersonName person={inFlight.to} />.
              </>
            ) : (
              'The agent writes a summary for the person who takes over. They accept it to become the driver.'
            )}
          </DialogDescription>
        </DialogHeader>

        {pending ? (
          <HandoffSummary summary={pending.summary}>
            <Button type="button" variant="secondary" onClick={onClose}>
              Close
            </Button>
          </HandoffSummary>
        ) : inFlight ? (
          <div role="status" className="flex flex-col gap-3">
            <p className="m-0 flex items-center gap-2 text-muted-foreground">
              <Loader2 className="size-4 animate-spin" aria-hidden="true" />
              Writing the hand-off summary. This can take up to 3 minutes; you can close this and keep watching.
            </p>
            <DialogFooter>
              <Button type="button" variant="secondary" onClick={onClose}>
                Close
              </Button>
            </DialogFooter>
          </div>
        ) : (
          <form className="flex flex-col gap-4" onSubmit={submit}>
            {failure && (
              <p role="alert" className="m-0 text-sm text-destructive-fg">
                {handoffFailureText(failure.reason)}
              </p>
            )}
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

function HandoffSummary({ summary: s, children }: { summary: HandoffState['summary']; children: ReactNode }) {
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
      <HandoffChecks summary={s} />
      <DialogFooter>{children}</DialogFooter>
    </div>
  )
}

// What the receiver reads before taking over: the same summary the driver sees, with Accept next to it.
function HandoffReceiveDialog({
  handoff,
  from,
  onAccept,
  onClose,
}: {
  handoff: HandoffState
  from: FromPerson | null
  onAccept: () => void
  onClose: () => void
}) {
  const restoreFocus = useRestoreFocus()
  return (
    <Dialog open onOpenChange={(open) => !open && onClose()}>
      <DialogContent hideClose className="max-h-[85vh] overflow-y-auto" onCloseAutoFocus={restoreFocus}>
        <RoomDialogClose label="Close hand-off summary" />
        <DialogHeader>
          <DialogTitle>
            Hand-off from {from ? <PersonName person={from} /> : 'Someone'}
          </DialogTitle>
          <DialogDescription>
            Read this before you accept. Accepting makes you the driver
            {handoff.stay_as_watcher ? (
              <>
                ; {from ? <PersonName person={from} /> : 'Someone'} stays in the room as a watcher.
              </>
            ) : (
              '.'
            )}
          </DialogDescription>
        </DialogHeader>
        <HandoffSummary summary={handoff.summary}>
          <Button type="button" variant="ghost" onClick={onClose}>
            Not yet
          </Button>
          <Button type="button" onClick={onAccept}>
            Accept and take over
          </Button>
        </HandoffSummary>
      </DialogContent>
    </Dialog>
  )
}

/** The banner's Read summary button: a toast action clicks it through RoomCommands.readHandoff. */
export const HANDOFF_READ_ID = 'handoff-read'

export function HandoffAcceptBanner({
  state,
  user,
  actions,
}: {
  state: RoomState
  user: CurrentUser
  actions: RoomActions
}) {
  const [reading, setReading] = useState(false)
  const mine = state.handoffs.find((h) => canAcceptHandoff(user, h))
  if (!mine) return null
  const fromPerson: FromPerson | null = state.driver ? { ...state.driver, kind: 'human' } : null
  return (
    <div
      role="status"
      className="flex min-h-10 flex-wrap items-center gap-x-2.5 gap-y-1 border-b border-border bg-primary-subtle px-4 py-1.5 font-medium text-primary-fg sm:px-6"
    >
      <span>
        {fromPerson ? <PersonName person={fromPerson} nameClassName="font-semibold" /> : <strong>Someone</strong>}{' '}
        is handing the room to you.
      </span>
      {/* No blind accept: the only way to take over is through the summary dialog. */}
      <Button id={HANDOFF_READ_ID} className="ml-auto h-7 px-2.5 coarse:h-11 coarse:px-4" onClick={() => setReading(true)}>
        Read summary
      </Button>
      {reading && (
        <HandoffReceiveDialog
          handoff={mine}
          from={fromPerson}
          onAccept={() => {
            setReading(false)
            actions.acceptHandoff(mine.handoff_id)
          }}
          onClose={() => setReading(false)}
        />
      )}
    </div>
  )
}

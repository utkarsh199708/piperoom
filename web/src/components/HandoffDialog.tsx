import { Loader2 } from 'lucide-react'
import { useState } from 'react'
import type { FormEvent, KeyboardEvent } from 'react'
import type { RoomState } from '../state/types'
import type { CurrentUser } from '../state/permissions'
import { canAcceptHandoff } from '../state/permissions'
import type { RoomActions } from '../hooks/useRoom'
import { modKeyLabel } from '@/lib/envLabel'
import { canWriteByHand, emptySummaryDraft, handoffFailureText, handoffInFlight, parseSummaryDraft, shiftIndex } from '@/lib/handoff'
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
import { StepNumbersProvider } from './room/AgentText'
import { HandoffCard, HandoffSummary } from './room/HandoffCard'
import { RoomDialogClose } from './room/RoomDialogClose'
import { SummaryFields } from './room/SummaryFields'
import { useNow } from './room/useNow'
import { useFollowClose, useRestoreFocus } from './room/useRestoreFocus'

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
  // Q0 (G4): when the agent cannot write the summary the driver can write it themselves.
  const [mode, setMode] = useState<'agent' | 'manual'>('agent')
  const [draft, setDraft] = useState(emptySummaryDraft)
  const [tried, setTried] = useState(false)
  const [busy, setBusy] = useState(false)
  const [problem, setProblem] = useState<{ code?: string; text: string } | null>(null)
  const restoreFocus = useRestoreFocus()
  const follow = useFollowClose(onClose, restoreFocus)
  // "Hand off to someone else" on a pending hand-off: a new one voids it on the server. Keyed by
  // id, so the next hand-off's summary shows again once it arrives.
  const [redoOf, setRedoOf] = useState<string | null>(null)
  const pending = state.handoffs.find((h) => h.status === 'pending' && h.handoff_id !== redoOf)
  const now = useNow(5_000)
  const inFlight = pending ? null : handoffInFlight(state, now)
  const failure = state.handoff_failure
  // The answer to this dialog's own request wins over an older handoff.failed of the log.
  const failedReason = problem ? (problem.code === 'summary_failed' ? 'summary_failed' : undefined) : failure?.reason
  const failureText = problem && failedReason === undefined ? problem.text : failedReason ? handoffFailureText(failedReason) : null
  const parsed = parseSummaryDraft(draft)

  const report = (res: { ok: boolean; code?: string; error?: string } | undefined) => {
    // `?.`: a test double may resolve with nothing.
    if (res?.ok === false) setProblem({ code: res.code, text: res.error ?? 'The hand-off failed. Try again.' })
  }
  const start = async () => {
    if (!to || inFlight || busy) return
    setBusy(true)
    setProblem(null)
    report(await actions.handoff(to, stay))
    setBusy(false)
  }
  const startManual = async () => {
    if (!to || inFlight || busy) return
    setTried(true)
    if (!parsed.summary) return
    setBusy(true)
    setProblem(null)
    report(await actions.handoff(to, stay, parsed.summary))
    setBusy(false)
  }
  const submit = (e: FormEvent) => {
    e.preventDefault()
    void (mode === 'manual' ? startManual() : start())
  }
  const onKeyDown = (e: KeyboardEvent) => {
    if (e.key === 'Enter' && (e.metaKey || e.ctrlKey) && !e.nativeEvent.isComposing) {
      e.preventDefault()
      void (mode === 'manual' ? startManual() : start())
    }
  }

  return (
    <Dialog open onOpenChange={(open) => !open && onClose()}>
      <DialogContent
        hideClose
        className="max-h-[85vh] overflow-y-auto"
        onKeyDown={pending || inFlight ? undefined : onKeyDown}
        onCloseAutoFocus={follow.onCloseAutoFocus}
      >
        <RoomDialogClose label="Close hand-off dialog" />
        <DialogHeader>
          <DialogTitle>{mode === 'manual' && !pending && !inFlight ? 'Write the hand-off summary' : 'Hand off the room'}</DialogTitle>
          <DialogDescription>
            {pending ? (
              <>
                Summary prepared for <PersonName person={pending.to} />. They must accept to take over.
              </>
            ) : inFlight ? (
              <>
                The agent is writing the summary for <PersonName person={inFlight.to} />.
              </>
            ) : mode === 'manual' ? (
              'You write what the agent would have written. The person who takes over sees that you wrote it.'
            ) : (
              'The agent writes a summary for the person who takes over. They accept it to become the driver.'
            )}
          </DialogDescription>
        </DialogHeader>

        {pending ? (
          <div className="flex flex-col gap-3">
            <StepNumbersProvider steps={state.steps} proposalIds={state.proposals.map((x) => x.proposal_id)} onFollow={follow.onFollow}>
              <HandoffSummary summary={pending.summary} manual={pending.manual} writtenBy={pending.written_by} />
            </StepNumbersProvider>
            <DialogFooter>
              {state.status === 'open' && (
                <Button type="button" variant="ghost" onClick={() => setRedoOf(pending.handoff_id)}>
                  Hand off to someone else
                </Button>
              )}
              <Button type="button" variant="secondary" onClick={onClose}>
                Close
              </Button>
            </DialogFooter>
          </div>
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
            {mode === 'agent' && failureText && (
              <p role="alert" className="m-0 text-sm text-destructive-fg">
                {failureText}
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
            {mode === 'manual' && (
              <>
                <SummaryFields draft={draft} onChange={setDraft} disabled={busy} />
                {tried && parsed.errors.length > 0 && (
                  <ul role="alert" className="m-0 list-disc pl-5 text-sm text-destructive-fg">
                    {parsed.errors.map((m) => (
                      <li key={m}>{m}</li>
                    ))}
                  </ul>
                )}
                {problem && (
                  <p role="alert" className="m-0 text-sm text-destructive-fg">
                    {problem.text}
                  </p>
                )}
              </>
            )}
            {mode === 'agent' ? (
              <DialogFooter>
                <Button type="button" variant="ghost" onClick={onClose}>
                  Cancel
                </Button>
                {canWriteByHand(failedReason) && (
                  <Button type="button" variant="secondary" onClick={() => setMode('manual')}>
                    Write the summary yourself
                  </Button>
                )}
                <Button type="submit" disabled={!to || busy}>
                  {busy ? 'Starting…' : 'Start hand-off'}
                  {!busy && <Kbd aria-hidden="true">{modKeyLabel()}⏎</Kbd>}
                </Button>
              </DialogFooter>
            ) : (
              <DialogFooter>
                <Button type="button" variant="ghost" onClick={() => setMode('agent')}>
                  Back
                </Button>
                <Button type="submit" disabled={!to || busy}>
                  {busy ? 'Sending…' : 'Hand off with this summary'}
                  {!busy && <Kbd aria-hidden="true">{modKeyLabel()}⏎</Kbd>}
                </Button>
              </DialogFooter>
            )}
          </form>
        )}
      </DialogContent>
    </Dialog>
  )
}

/** The pending hand-off card: the attention toast's "Read summary" focuses it, which opens it. */
export const HANDOFF_READ_ID = 'handoff-read'

/**
 * A pending hand-off under the banners. The recipient reads it inline, expanded, before accepting
 * (no blind accept); everyone else sees it collapsed with "waiting for <name>". Once
 * handoff.accepted arrives it leaves here for the pinned list.
 */
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
  const pending = mine ?? state.handoffs.find((h) => h.status === 'pending')
  // Keyed by id: a newer hand-off to me opens expanded again even after "Not yet".
  const [closedId, setClosedId] = useState<string | null>(null)
  // One accept per hand-off: the card stays until handoff.accepted arrives, and a second POST would
  // answer 409 already_decided into the banner right after a good take-over. Cleared on failure.
  const [acceptingId, setAcceptingId] = useState<string | null>(null)
  if (!pending) return null
  const open = mine ? closedId !== mine.handoff_id : undefined
  // Collapsing hides the focused button; hand focus to the card's own toggle so it is not dropped to <body>.
  const notYet = (id: string) => {
    setClosedId(id)
    document.getElementById(HANDOFF_READ_ID)?.querySelector<HTMLElement>('button[aria-expanded]')?.focus()
  }
  const accepting = !!mine && acceptingId === mine.handoff_id
  const accept = () => {
    if (!mine || accepting) return
    const id = mine.handoff_id
    setAcceptingId(id)
    const done = actions.acceptHandoff(id)
    if (done) void done.then((ok) => !ok && setAcceptingId((cur) => (cur === id ? null : cur)))
  }
  const onKeyDown = (e: KeyboardEvent) => {
    if (mine && e.key === 'Enter' && (e.metaKey || e.ctrlKey) && !e.nativeEvent.isComposing) {
      e.preventDefault()
      if (!e.repeat) accept()
    }
  }
  return (
    <>
      {mine && (
        <div
          role="status"
          className="flex min-h-10 flex-wrap items-center gap-x-2.5 gap-y-1 border-b border-border bg-primary-subtle px-4 py-1.5 font-medium text-primary-fg sm:px-6"
        >
          <span>
            <PersonName person={mine.from} nameClassName="font-semibold" /> is handing the room to you — read the
            summary below.
          </span>
        </div>
      )}
      <div className="px-4 pt-3 sm:px-6" onKeyDown={onKeyDown}>
        <StepNumbersProvider steps={state.steps} proposalIds={state.proposals.map((x) => x.proposal_id)}>
          <HandoffCard
            key={pending.handoff_id}
            id={mine ? HANDOFF_READ_ID : undefined}
            handoff={pending}
            index={shiftIndex(state.handoffs, pending)}
            from={pending.from}
            createdAt={pending.created_at}
            defaultOpen={false}
            open={open}
            onOpenChange={mine ? (o) => setClosedId(o ? null : mine.handoff_id) : undefined}
            onFocusSelf={mine ? () => setClosedId(null) : undefined}
            footer={
              mine ? (
                <>
                  {mine.stay_as_watcher && (
                    <p className="m-0 mr-auto self-center text-xs text-muted-foreground">
                      <PersonName person={mine.from} /> stays in the room as a watcher.
                    </p>
                  )}
                  <Button type="button" variant="ghost" onClick={() => notYet(mine.handoff_id)}>
                    Not yet
                  </Button>
                  <Button
                    type="button"
                    aria-keyshortcuts="Meta+Enter Control+Enter"
                    disabled={accepting}
                    aria-busy={accepting || undefined}
                    onClick={accept}
                  >
                    {accepting && <Loader2 className="animate-spin motion-reduce:animate-none" aria-hidden="true" />}
                    Accept and take over
                    <Kbd aria-hidden="true">{modKeyLabel()}⏎</Kbd>
                  </Button>
                </>
              ) : undefined
            }
          />
        </StepNumbersProvider>
      </div>
    </>
  )
}

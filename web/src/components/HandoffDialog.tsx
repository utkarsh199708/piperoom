import { Loader2 } from 'lucide-react'
import { useState } from 'react'
import type { FormEvent, KeyboardEvent, ReactNode } from 'react'
import type { HandoffState, RoomState, StepState } from '../state/types'
import type { CurrentUser } from '../state/permissions'
import { canAcceptHandoff } from '../state/permissions'
import type { RoomActions } from '../hooks/useRoom'
import { modKeyLabel } from '@/lib/envLabel'
import { canWriteByHand, emptySummaryDraft, handoffFailureText, handoffInFlight, parseSummaryDraft } from '@/lib/handoff'
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
import { AgentText, StepNumbersProvider } from './room/AgentText'
import { RoomDialogClose } from './room/RoomDialogClose'
import { SummaryFields } from './room/SummaryFields'
import { useNow } from './room/useNow'
import { useFollowClose, useRestoreFocus } from './room/useRestoreFocus'

function SummaryList({ title, items }: { title: string; items: string[] }) {
  if (!items || items.length === 0) return null
  return (
    <div className="flex flex-col gap-1">
      <h4 className="text-xs font-medium text-muted-foreground">{title}</h4>
      <ul className="m-0 flex list-disc flex-col gap-0.5 pl-5">
        {items.map((it, i) => (
          <li key={i} className="break-words">
            <AgentText text={it} />
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
  // Q0 (G4): when the agent cannot write the summary the driver can write it themselves.
  const [mode, setMode] = useState<'agent' | 'manual'>('agent')
  const [draft, setDraft] = useState(emptySummaryDraft)
  const [tried, setTried] = useState(false)
  const [busy, setBusy] = useState(false)
  const [problem, setProblem] = useState<{ code?: string; text: string } | null>(null)
  const restoreFocus = useRestoreFocus()
  const follow = useFollowClose(onClose, restoreFocus)
  const pending = state.handoffs.find((h) => h.status === 'pending')
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
            {pending
              ? `Summary prepared for ${pending.to.name}. They must accept to take over.`
              : inFlight
                ? `The agent is writing the summary for ${inFlight.to.name}.`
                : mode === 'manual'
                  ? 'You write what the agent would have written. The person who takes over sees that you wrote it.'
                  : 'The agent writes a summary for the person who takes over. They accept it to become the driver.'}
          </DialogDescription>
        </DialogHeader>

        {pending ? (
          <StepNumbersProvider steps={state.steps} proposalIds={state.proposals.map((x) => x.proposal_id)} onFollow={follow.onFollow}>
            <HandoffSummary summary={pending.summary} writtenBy={pending.manual ? (pending.written_by?.name ?? 'the driver') : undefined}>
              <Button type="button" variant="secondary" onClick={onClose}>
                Close
              </Button>
            </HandoffSummary>
          </StepNumbersProvider>
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

function HandoffSummary({
  summary: s,
  writtenBy,
  children,
}: {
  summary: HandoffState['summary']
  /** Set when the driver wrote the summary instead of the agent (Q0, G4). */
  writtenBy?: string
  children: ReactNode
}) {
  return (
    <div className="handoff__summary flex flex-col gap-3">
      {writtenBy && <p className="m-0 text-xs font-medium text-muted-foreground">Written by {writtenBy}, not by the agent.</p>}
      {s.what_broke && (
        <div className="flex flex-col gap-1">
          <h4 className="text-xs font-medium text-muted-foreground">What broke</h4>
          <p className="m-0 break-words">
            <AgentText text={s.what_broke} />
          </p>
        </div>
      )}
      {s.why && (
        <div className="flex flex-col gap-1">
          <h4 className="text-xs font-medium text-muted-foreground">Why</h4>
          <p className="m-0 break-words">
            <AgentText text={s.why} />
          </p>
        </div>
      )}
      <SummaryList title="Done" items={s.done} />
      <SummaryList title="Ruled out" items={s.ruled_out} />
      <SummaryList title="Still open" items={s.open} />
      <SummaryList title="Risks" items={s.risks} />
      <DialogFooter>{children}</DialogFooter>
    </div>
  )
}

// What the receiver reads before taking over: the same summary the driver sees, with Accept next to it.
function HandoffReceiveDialog({
  handoff,
  from,
  steps,
  onAccept,
  onClose,
}: {
  handoff: HandoffState
  from: string
  steps: StepState[]
  onAccept: () => void
  onClose: () => void
}) {
  const restoreFocus = useRestoreFocus()
  const follow = useFollowClose(onClose, restoreFocus)
  return (
    <Dialog open onOpenChange={(open) => !open && onClose()}>
      <DialogContent hideClose className="max-h-[85vh] overflow-y-auto" onCloseAutoFocus={follow.onCloseAutoFocus}>
        <RoomDialogClose label="Close hand-off summary" />
        <DialogHeader>
          <DialogTitle>Hand-off from {from}</DialogTitle>
          <DialogDescription>
            Read this before you accept. Accepting makes you the driver
            {handoff.stay_as_watcher ? `; ${from} stays in the room as a watcher.` : '.'}
          </DialogDescription>
        </DialogHeader>
        <StepNumbersProvider steps={steps} onFollow={follow.onFollow}>
          <HandoffSummary summary={handoff.summary} writtenBy={handoff.manual ? (handoff.written_by?.name ?? from) : undefined}>
            <Button type="button" variant="ghost" onClick={onClose}>
              Not yet
            </Button>
            <Button type="button" onClick={onAccept}>
              Accept and take over
            </Button>
          </HandoffSummary>
        </StepNumbersProvider>
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
  const from = state.driver?.name ?? 'Someone'
  return (
    <div
      role="status"
      className="flex min-h-10 flex-wrap items-center gap-x-2.5 gap-y-1 border-b border-border bg-primary-subtle px-4 py-1.5 font-medium text-primary-fg sm:px-6"
    >
      <span>
        <strong>{from}</strong> is handing the room to you.
      </span>
      {/* No blind accept: the only way to take over is through the summary dialog. */}
      <Button id={HANDOFF_READ_ID} className="ml-auto h-7 px-2.5 coarse:h-11 coarse:px-4" onClick={() => setReading(true)}>
        Read summary
      </Button>
      {reading && (
        <HandoffReceiveDialog
          handoff={mine}
          from={from}
          steps={state.steps}
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

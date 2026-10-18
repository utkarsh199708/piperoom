// The one line that answers "what does this room need from me?" and stays on screen while the
// room scrolls (F1b). It draws `decisionFor`; its buttons go through the approval card's own
// handlers (DecisionHandleContext), so there is no second copy of the approval logic.
import { useContext, useEffect, useRef, useState } from 'react'
import { ArrowDown, TriangleAlert } from 'lucide-react'
import type { RoomState } from '@/state/types'
import type { CurrentUser } from '@/state/permissions'
import { decisionFor } from '@/lib/decision'
import { writeModeOff } from '@/lib/writeMode'
import type { Decision } from '@/lib/decision'
import { cn } from '@/lib/utils'
import { PersonName } from '@/components/VerifiedBadge'
import { Button } from '@/components/ui/button'
import { Kbd } from '@/components/ui/kbd'
import { Progress } from '@/components/ui/progress'
import { DecisionHandleContext } from '@/components/shell/context'

const DONE_MS = 3000

function dismissedKey(id: string): string {
  return `piperoom.dismissed.${id}`
}
function readDismissed(id: string, memory: Set<string>): boolean {
  if (memory.has(id)) return true
  try {
    return window.sessionStorage.getItem(dismissedKey(id)) === '1'
  } catch {
    return false
  }
}

const BAR = 'flex min-h-11 flex-wrap items-center gap-x-3 gap-y-1 border-b border-border px-4 py-1.5 text-sm sm:px-6'

export function DecisionBar({
  state,
  user,
  placement = 'top',
}: {
  state: RoomState
  user: CurrentUser
  /** 'top': sticky under the header. 'sheet': fixed to the bottom on phones, buttons full width (F7c). */
  placement?: 'top' | 'sheet'
}) {
  const registry = useContext(DecisionHandleContext)
  const decision = decisionFor(state, user)
  const [, bump] = useState(0)
  const memory = useRef(new Set<string>())
  // State, not a ref: the bar also renders nothing while `decision` is set (a done fix not seen
  // here, a dismissed rejection), so "decision !== null" does not say when the node comes back.
  const [barEl, setBarEl] = useState<HTMLDivElement | null>(null)
  const sheet = placement === 'sheet'
  const heightVar = sheet ? '--sheet-h' : '--decision-h'
  // Publishes the bar's height so the tab list can stick right under it, or, as a sheet, so the composer
  // and <main> keep clear of it (F7c).
  useEffect(() => {
    const el = barEl
    if (!el) return
    const root = document.documentElement
    const write = () => root.style.setProperty(heightVar, `${Math.round(el.getBoundingClientRect().height)}px`)
    write()
    if (typeof ResizeObserver === 'undefined') return () => root.style.removeProperty(heightVar)
    const ro = new ResizeObserver(write)
    ro.observe(el)
    return () => {
      ro.disconnect()
      root.style.removeProperty(heightVar)
    }
  }, [barEl, heightVar])

  // "done ok" shows for 3 s, and only when this screen saw the proposal in some other state first
  // (a finished fix in an old room must not flash on load).
  const seen = useRef(new Set<string>())
  const [doneGone, setDoneGone] = useState<string | null>(null)
  const doneId = decision?.kind === 'done' && decision.ok ? decision.proposal.proposal_id : null
  const wasSeen = doneId !== null && seen.current.has(doneId)
  useEffect(() => {
    if (decision && decision.kind !== 'done') seen.current.add(decision.proposal.proposal_id)
  })
  useEffect(() => {
    if (doneId === null || !wasSeen) return
    const t = window.setTimeout(() => setDoneGone(doneId), DONE_MS)
    return () => window.clearTimeout(t)
  }, [doneId, wasSeen])

  if (!decision) return null
  const handle = () => registry?.get() ?? null

  let tone = 'bg-card text-muted-foreground'
  let text: React.ReactNode = null
  let actions: React.ReactNode = null
  let live = ''

  const view = (
    <Button variant="ghost" size="sm" onClick={() => handle()?.scrollTo()}>
      View steps
      <ArrowDown aria-hidden="true" />
    </Button>
  )

  switch (decision.kind) {
    case 'needs-you': {
      const title = decision.proposal.title
      tone = 'bg-warning-subtle text-warning-foreground'
      live = `Needs your approval · ${decision.layer} — ${title}`
      text = (
        <>
          <TriangleAlert className="size-4 shrink-0" aria-hidden="true" />
          <span aria-live="polite" className="min-w-0 truncate font-medium" title={live}>
            {live}
          </span>
        </>
      )
      actions = (
        <>
          <Button
            variant="ghost-danger"
            size="sm"
            className={sheet ? 'h-11 flex-1' : 'coarse:h-11'}
            aria-keyshortcuts="R"
            onClick={() => handle()?.reject()}
          >
            Reject
            {!sheet && <Kbd aria-hidden="true">R</Kbd>}
          </Button>
          <Button
            variant="success"
            size="sm"
            className={sheet ? 'h-11 flex-1' : 'coarse:h-11'}
            aria-keyshortcuts="A"
            onClick={() => handle()?.approve()}
          >
            {`Approve ${decision.layer}${writeModeOff(state.mode) ? ' (will not run)' : ''}`}
            {!sheet && <Kbd aria-hidden="true">A</Kbd>}
          </Button>
          {!sheet && view}
        </>
      )
      break
    }
    case 'waiting':
      live = `Waiting for ${decision.pending.join(' · ')} · ${decision.approved} of ${decision.total} approved`
      text = <span aria-live="polite">{live}</span>
      actions = (
        <Button variant="ghost" size="sm" onClick={() => handle()?.scrollTo()}>
          View
          <ArrowDown aria-hidden="true" />
        </Button>
      )
      break
    case 'executing':
      tone = 'bg-agent-subtle text-agent-foreground'
      live = `Running step ${decision.step} of ${decision.total}`
      text = (
        <span aria-live="polite" className="min-w-0 truncate">
          Running step {decision.step} of {decision.total} · <code className="font-mono">{decision.action}</code>
        </span>
      )
      break
    case 'done':
      if (decision.ok) {
        if (!wasSeen || doneGone === decision.proposal.proposal_id) return null
        tone = 'bg-success-subtle text-success-foreground'
        const n = decision.proposal.steps.length
        text = <span aria-live="polite">{`Fix ran · ${n} of ${n} steps`}</span>
      } else {
        tone = 'bg-destructive-subtle text-destructive-fg'
        text = <span aria-live="polite">{`Fix failed at step ${decision.failedStep} — see the card`}</span>
      }
      break
    case 'rejected': {
      const id = decision.proposal.proposal_id
      if (readDismissed(id, memory.current)) return null
      text = (
        <span aria-live="polite" className="inline-flex flex-wrap items-center gap-x-1.5">
          Proposal rejected by{' '}
          {decision.byActor ? <PersonName person={decision.byActor} /> : decision.by}
          {decision.layer ? ` · ${decision.layer}` : ''}
        </span>
      )
      actions = (
        <Button
          variant="ghost"
          size="sm"
          aria-label="Dismiss"
          onClick={() => {
            memory.current.add(id)
            try {
              window.sessionStorage.setItem(dismissedKey(id), '1')
            } catch {
              // in-memory set above still hides it for this visit
            }
            bump((n) => n + 1)
          }}
        >
          Dismiss
        </Button>
      )
      break
    }
  }

  // The tints are translucent: an opaque base keeps the room from showing through the sticky bar.
  return (
    <div
      ref={setBarEl}
      role="region"
      aria-label="Decision"
      data-placement={placement}
      data-kind={(decision as Decision).kind}
      className={cn(
        'bg-background motion-safe:transition-opacity motion-safe:duration-200',
        sheet ? 'fixed inset-x-0 bottom-0 z-30 border-t border-border shadow-lg' : 'sticky top-0 z-20',
      )}
    >
      <div
        className={cn(
          BAR,
          tone,
          sheet && 'border-b-0 px-4 pt-3 pb-[max(env(safe-area-inset-bottom),12px)] sm:px-4',
          sheet && decision.kind === 'needs-you' && 'flex-col items-stretch',
        )}
      >
        <div className="flex min-w-0 flex-1 items-center gap-2">{text}</div>
        {actions && (
          <div className={cn('flex flex-wrap items-center gap-2', sheet && decision.kind === 'needs-you' ? 'w-full flex-nowrap' : 'shrink-0')}>
            {actions}
          </div>
        )}
        {decision.kind === 'executing' && (
          <Progress className="h-0.5 basis-full bg-transparent" indicatorClassName="bg-agent" aria-label="Running the fix" />
        )}
      </div>
    </div>
  )
}

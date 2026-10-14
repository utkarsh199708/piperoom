// The one line that answers "what does this room need from me?" and stays on screen while the
// room scrolls (F1b). It draws `decisionFor`; its buttons go through the approval card's own
// handlers (DecisionHandleContext), so there is no second copy of the approval logic.
import { useContext, useEffect, useRef, useState } from 'react'
import { ArrowDown, TriangleAlert } from 'lucide-react'
import type { RoomState } from '@/state/types'
import type { CurrentUser } from '@/state/permissions'
import { decisionFor } from '@/lib/decision'
import type { Decision } from '@/lib/decision'
import { cn } from '@/lib/utils'
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
  /** Only 'top' today; F7c adds the phone sheet through this prop. */
  placement?: 'top'
}) {
  const registry = useContext(DecisionHandleContext)
  const decision = decisionFor(state, user)
  const [, bump] = useState(0)
  const memory = useRef(new Set<string>())

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
          <Button variant="ghost-danger" size="sm" aria-keyshortcuts="R" onClick={() => handle()?.reject()}>
            Reject
            <Kbd aria-hidden="true">R</Kbd>
          </Button>
          <Button variant="success" size="sm" aria-keyshortcuts="A" onClick={() => handle()?.approve()}>
            Approve {decision.layer}
            <Kbd aria-hidden="true">A</Kbd>
          </Button>
          {view}
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
      text = <span aria-live="polite">{`Proposal rejected by ${decision.by}${decision.layer ? ` · ${decision.layer}` : ''}`}</span>
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
      role="region"
      aria-label="Decision"
      data-placement={placement}
      data-kind={(decision as Decision).kind}
      className="sticky top-0 z-20 bg-background motion-safe:transition-opacity motion-safe:duration-200"
    >
      <div className={cn(BAR, tone)}>
        <div className="flex min-w-0 flex-1 items-center gap-2">{text}</div>
        {actions && <div className="flex shrink-0 flex-wrap items-center gap-2">{actions}</div>}
        {decision.kind === 'executing' && (
          <Progress className="h-0.5 basis-full bg-transparent" indicatorClassName="bg-agent" aria-label="Running the fix" />
        )}
      </div>
    </div>
  )
}

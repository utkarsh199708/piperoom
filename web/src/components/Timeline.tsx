import { PersonName } from '@/components/VerifiedBadge'
import { createContext, useContext, useEffect, useId, useRef, useState } from 'react'
import type { Dispatch, SetStateAction } from 'react'
import { plainText } from '@/lib/title'
import { emptyStateFor } from '../lib/emptyState'
import type { ComponentProps, FormEvent } from 'react'
import { Check, ChevronRight, Copy, Wrench, X } from 'lucide-react'
import type { AgentMessageState, ChecklistTick, CommentState, PageState, StepState, ToolCallState } from '../state/types'
import type { ToolCall } from '../api/types'
import { argTags, formatMs, toolLayer } from '../lib/toolLayers'
import type { SendControls, SendOptions } from '../hooks/useRoom'
import { useRetryId } from '../hooks/useRetryId'
import { cn } from '@/lib/utils'
import { Badge, layerVariant } from '@/components/ui/badge'
import { Button } from '@/components/ui/button'
import { Input } from '@/components/ui/input'
import { Kbd } from '@/components/ui/kbd'
import { Progress } from '@/components/ui/progress'
import { Clock } from './room/Clock'
import { OfflineStrip } from './room/OfflineStrip'
import { PersonAvatar } from './room/PersonAvatar'
import { CollapsibleText } from './room/CollapsibleText'
import { StepNumbersProvider } from './room/AgentText'
import { REVEAL_STEP_EVENT, stepAnchor } from '@/lib/triage'
import { evidenceFor } from '@/lib/evidence'
import { Raw } from './room/evidence/Raw'
import { Checklist } from './room/Checklist'
import { PagesList } from './room/PagesList'
import type { ChecklistAccess } from './room/Checklist'
import type { ChecklistStepDonePayload } from '../api/types'

const AGENT = { kind: 'agent', id: 'agent', name: 'Agent' } as const

function ToolBadge({ tool }: { tool: string }) {
  return (
    <Badge variant={layerVariant(toolLayer(tool))} mono>
      {tool}
    </Badge>
  )
}

function ArgTag({ children }: { children: string }) {
  return <span className="whitespace-nowrap font-mono text-[11.5px] leading-4 text-muted-foreground">{children}</span>
}

function totalMs(step: StepState): number | null {
  const times = step.tool_calls.map((t) => t.took_ms).filter((m): m is number => m != null)
  return times.length > 0 ? times.reduce((a, b) => a + b, 0) : null
}

// The summary of one tool call: a structured view for the tools that have one, the plain
// summary text otherwise (an error, a tool with no view, or data that does not fit its view).
function ToolSummary({ t }: { t: ToolCallState }) {
  const View = t.is_error ? null : evidenceFor(t.tool)
  return View ? <View t={t} /> : <Raw t={t} />
}

function rawJson(t: ToolCallState): string {
  // With a structured view the summary is no longer on screen, so it is kept here.
  const raw: Record<string, unknown> = {}
  if (!t.is_error && evidenceFor(t.tool) && t.summary != null) raw.summary = t.summary
  raw.input = t.input
  if (t.data !== undefined) raw.data = t.data
  try {
    return JSON.stringify(raw, null, 2)
  } catch {
    return String(raw)
  }
}

// One tool call as evidence: tool badge, arg tags, the summary the agent got back,
// and the raw input/result behind a <details>. Plain text only (never HTML or links).
/** What the timeline needs to show a runbook_lookup result as a checklist. */
export interface ChecklistWiring {
  done: Record<string, ChecklistTick>
  access: ChecklistAccess
  onTick: (p: ChecklistStepDonePayload) => void
  onAsk: (text: string) => void
}

/** The step a citation chip asked for; a new `nonce` re-triggers the flash on the same step. */
export interface CitedStep {
  id: string
  nonce: number
}

// One read the tool made, as a person could re-run it: "Ran" kicker, kind badge (target in its
// title), the statement, redacted params, the time, and Copy. Every value is a text node.
function RanLine({ call }: { call: ToolCall }) {
  const [copied, setCopied] = useState(false)
  const copy = () => {
    if (!navigator.clipboard) return
    void navigator.clipboard.writeText(call.text).then(
      () => {
        setCopied(true)
        window.setTimeout(() => setCopied(false), 1500)
      },
      () => undefined,
    )
  }
  return (
    <div className="flex items-start gap-2 font-mono text-xs" data-testid="ran-line">
      <span className="shrink-0 pt-0.5 text-xs font-medium leading-4 text-subtle-foreground">Ran</span>
      <Badge mono title={call.target} className="shrink-0">
        {call.kind}
      </Badge>
      <div className="flex min-w-0 flex-1 flex-col gap-1">
        <code className="max-h-24 overflow-auto break-all text-xs font-normal leading-4">{call.text}</code>
        {call.params_redacted && call.params_redacted.length > 0 && (
          <span className="flex flex-wrap gap-x-2">
            {call.params_redacted.map((p, i) => (
              <ArgTag key={i}>{p}</ArgTag>
            ))}
          </span>
        )}
      </div>
      <span className="shrink-0 pt-0.5 tabular-nums text-subtle-foreground">{formatMs(call.took_ms)}</span>
      <Button
        variant="ghost"
        className="h-7 shrink-0 gap-1 px-1.5 text-xs coarse:h-11"
        onClick={copy}
        aria-label={copied ? 'Copied' : 'Copy query'}
      >
        {copied ? <Check className="size-3.5" aria-hidden="true" /> : <Copy className="size-3.5" aria-hidden="true" />}
        <span aria-hidden="true">{copied ? 'Copied' : 'Copy'}</span>
      </Button>
    </div>
  )
}

function Evidence({ step, checklist }: { step: StepState; checklist?: ChecklistWiring }) {
  if (step.tool_calls.length === 0) return null
  return (
    <ul className="m-0 flex list-none flex-col gap-3 p-0">
      {step.tool_calls.map((t) => (
        <li key={t.tool_use_id} className="flex flex-col gap-1.5">
          <div className="flex flex-wrap items-center gap-x-2 gap-y-1">
            <ToolBadge tool={t.tool} />
            {argTags(t.input).map((a) => (
              <ArgTag key={a}>{a}</ArgTag>
            ))}
            {t.took_ms != null && (
              <span className="ml-auto font-mono text-xs text-subtle-foreground">{formatMs(t.took_ms)}</span>
            )}
          </div>
          {t.provenance?.map((c, i) => <RanLine key={i} call={c} />)}
          {t.summary != null && <ToolSummary t={t} />}
          {checklist && t.tool === 'runbook_lookup' && !t.is_error && (
            <Checklist
              stepId={step.id}
              toolUseId={t.tool_use_id}
              data={t.data}
              done={checklist.done}
              access={checklist.access}
              onTick={checklist.onTick}
              onAsk={checklist.onAsk}
            />
          )}
          {(t.input != null || t.data !== undefined) && (
            <details className="text-xs">
              <summary className="w-fit cursor-pointer rounded-sm text-subtle-foreground hover:text-foreground focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring">
                Raw input and result
              </summary>
              <pre className="mt-1.5 max-h-64 overflow-auto whitespace-pre-wrap break-words rounded-md border border-border bg-muted px-2.5 py-2 font-mono text-xs leading-[17px] text-muted-foreground">
                {rawJson(t)}
              </pre>
            </details>
          )}
        </li>
      ))}
    </ul>
  )
}

function AgentMessageList({ messages }: { messages: AgentMessageState[] }) {
  if (messages.length === 0) return null
  return (
    <div className="flex flex-col gap-1.5">
      {messages.map((m, i) => (
        <p key={i} className="m-0 flex items-start gap-2">
          <PersonAvatar actor={AGENT} size={20} className="mt-px" />
          <span className="min-w-0 break-words text-muted-foreground">
            <span className="sr-only">Agent: </span>
            <CollapsibleText text={m.text} />
          </span>
        </p>
      ))}
    </div>
  )
}

function CommentThread({
  comment,
  onReply,
  readOnly = false,
}: {
  comment: CommentState
  onReply: (text: string, clientId: string) => void | Promise<boolean>
  readOnly?: boolean
}) {
  const [open, setOpen] = useState(false)
  const [text, setText] = useState('')
  const [sending, setSending] = useState(false)
  const [offline, setOffline] = useState(false)
  // The failed reply landed after all (reconnect copy or its event): close it like a success.
  const retryId = useRetryId(useContext(SendsContext), () => {
    setOffline(false)
    setText('')
    setOpen(false)
  })

  // Like ReplyBox: the reply closes only once the server took it; a failed send keeps the words.
  const send = async () => {
    const t = text.trim()
    if (!t || sending) return
    setSending(true)
    // While the Retry strip is up and the words are unedited, this send is that Retry (#459).
    const clientId = retryId.next(offline)
    const ok = (await onReply(t, clientId)) !== false
    setSending(false)
    setOffline(!ok)
    retryId.settle(clientId, ok)
    if (ok) {
      setText('')
      setOpen(false)
    }
  }
  const submit = (e: FormEvent) => {
    e.preventDefault()
    void send()
  }

  // A logged fix (Q0, G10) is a record of what a person did, not a remark: it gets its own tint and label.
  const fix = comment.kind === 'fix_note'
  return (
    <div
      className={cn('flex flex-col gap-1.5', fix && 'rounded-md border border-success-foreground/30 bg-success-subtle px-2.5 py-2')}
      data-kind={fix ? 'fix_note' : undefined}
    >
      <div className="flex items-start gap-2">
        <PersonAvatar actor={comment.author} size={20} className="mt-px" />
        <div className="min-w-0">
          <div className="flex flex-wrap items-baseline gap-x-2">
            <PersonName person={comment.author} nameClassName="font-medium" />
            {fix && (
              <Badge variant="success" className="self-center">
                <Wrench className="size-3" aria-hidden="true" />
                Fix note
              </Badge>
            )}
            <Clock iso={comment.at} className="text-xs text-subtle-foreground" />
          </div>
          <p className="m-0 break-words text-muted-foreground">{plainText(comment.text, Infinity)}</p>
        </div>
      </div>
      {comment.replies.map((r, i) => (
        <div key={i} className="ml-7 flex items-start gap-2">
          <PersonAvatar actor={AGENT} size={20} className="mt-px" />
          <div className="min-w-0">
            <div className="flex flex-wrap items-baseline gap-x-2">
              <strong className="font-medium">Agent</strong>
              <Clock iso={r.at} className="text-xs text-subtle-foreground" />
            </div>
            <p className="m-0 break-words text-muted-foreground">
              <CollapsibleText text={r.text} />
            </p>
          </div>
        </div>
      ))}
      {readOnly ? null : open ? (
        <form className="ml-7 flex items-center gap-2" onSubmit={submit}>
          <Input
            value={text}
            onChange={(e) => {
              setText(e.target.value)
              retryId.drop() // edited words: the failed action is cancelled, the next send is new
            }}
            placeholder="Reply…"
            aria-label="Reply to comment"
            autoFocus
          />
          <Button type="submit" size="sm" disabled={!text.trim() || sending}>
            Send
          </Button>
        </form>
      ) : (
        <Button variant="link" size="sm" className="ml-7 w-fit text-xs" onClick={() => setOpen(true)}>
          Reply
        </Button>
      )}
      {open && offline && (
        <div className="ml-7">
          <OfflineStrip onRetry={() => void send()} disabled={sending} />
          <p className="m-0 text-xs text-muted-foreground">Editing discards the unsent reply.</p>
        </div>
      )}
    </div>
  )
}

/** The reply boxes' handle on their actions (#459): RoomActions.sends. */
const SendsContext = createContext<SendControls | undefined>(undefined)

function ReplyBox({ onSubmit }: { onSubmit: (text: string, clientId: string) => void | Promise<boolean> }) {
  const [text, setText] = useState('')
  const [sending, setSending] = useState(false)
  const [offline, setOffline] = useState(false)
  // The failed comment landed after all (reconnect copy or its event): clear it like a success.
  const retryId = useRetryId(useContext(SendsContext), () => {
    setOffline(false)
    setText('')
  })
  // The field clears only once the server took the comment; a failed send keeps the words.
  const send = async () => {
    const t = text.trim()
    if (!t || sending) return
    setSending(true)
    // While the Retry strip is up and the words are unedited, this send is that Retry (#459).
    const clientId = retryId.next(offline)
    const ok = (await onSubmit(t, clientId)) !== false
    setSending(false)
    setOffline(!ok)
    retryId.settle(clientId, ok)
    if (ok) setText('')
  }
  const submit = (e: FormEvent) => {
    e.preventDefault()
    void send()
  }
  return (
    <div className="flex flex-col gap-1.5">
      <form className="flex items-center gap-2" onSubmit={submit}>
        <Input
          value={text}
          onChange={(e) => {
            setText(e.target.value)
            retryId.drop() // edited words: the failed action is cancelled, the next send is new
          }}
          placeholder="Comment on this step…"
          aria-label="Comment on this step"
        />
        <Button type="submit" variant="secondary" size="sm" disabled={!text.trim() || sending}>
          {sending ? 'Sending…' : 'Comment'}
        </Button>
      </form>
      {offline && (
        <>
          <OfflineStrip onRetry={() => void send()} disabled={sending} />
          <p className="m-0 text-xs text-muted-foreground">Editing discards the unsent comment.</p>
        </>
      )}
    </div>
  )
}

function StepNumber({ n, error }: { n: number; error: boolean }) {
  return (
    <span
      className={cn(
        'inline-flex size-[22px] shrink-0 items-center justify-center rounded-full font-mono text-[11px] font-medium leading-none',
        error ? 'bg-destructive-subtle text-destructive-fg' : 'bg-muted text-subtle-foreground',
      )}
    >
      {error ? <X className="size-3" aria-label="failed" /> : n}
    </span>
  )
}

// What sits under a step row: its agent messages, its comment threads and the comment box.
function StepExtras({
  step,
  comments,
  onComment,
  readOnly,
  showReplyBox,
}: {
  step: StepState
  comments: CommentState[]
  onComment: (text: string, stepId?: string, opts?: SendOptions) => void | Promise<boolean>
  readOnly: boolean
  showReplyBox: boolean
}) {
  return (
    <>
      <AgentMessageList messages={step.messages} />
      {comments.map((c) => (
        <CommentThread key={c.id} comment={c} onReply={(t, clientId) => onComment(t, step.id, { clientId })} readOnly={readOnly} />
      ))}
      {!readOnly && showReplyBox && <ReplyBox onSubmit={(t, clientId) => onComment(t, step.id, { clientId })} />}
    </>
  )
}

// Scrolls to, opens and flashes the step a citation chip named (F3). The flash is a 600 ms
// background animation under motion-safe; reduced motion scrolls without smoothing.
function useCiteFlash(stepId: string, cited: CitedStep | null | undefined, setOpen?: Dispatch<SetStateAction<boolean>>) {
  const ref = useRef<HTMLLIElement>(null)
  const [flash, setFlash] = useState(false)
  useEffect(() => {
    if (!cited || cited.id !== stepId) return
    setOpen?.(true)
    const el = ref.current
    const calm = window.matchMedia?.('(prefers-reduced-motion: reduce)')?.matches
    el?.scrollIntoView?.({ block: 'center', behavior: calm ? 'auto' : 'smooth' })
    el?.querySelector<HTMLElement>('button')?.focus({ preventScroll: true })
    setFlash(true)
    const t = window.setTimeout(() => setFlash(false), 600)
    return () => window.clearTimeout(t)
  }, [cited, stepId, setOpen])
  return { ref, flash }
}

// A finished step: one compact row (number, title, tool, args, time, check). The row opens
// to show the evidence and the comment box. The opened part stays mounted while closed
// (hidden), so a half-typed comment survives closing the row.
function DoneStep({
  step,
  n,
  comments,
  onComment,
  readOnly,
  checklist,
  cited,
}: {
  step: StepState
  n: number
  comments: CommentState[]
  onComment: (text: string, stepId?: string, opts?: SendOptions) => void | Promise<boolean>
  readOnly: boolean
  checklist?: ChecklistWiring
  cited?: CitedStep | null
}) {
  // A step that fetched a runbook starts open: its checklist is for doing, not for reading later.
  const [open, setOpen] = useState(() => !!checklist && step.tool_calls.some((t) => t.tool === 'runbook_lookup'))
  const { ref, flash } = useCiteFlash(step.id, cited, setOpen)
  const regionId = useId()
  const first = step.tool_calls[0]
  const tags = first ? argTags(first.input) : []
  const failed = step.tool_calls.some((t) => t.is_error)
  const ms = totalMs(step)
  const more = step.tool_calls.length - 1

  return (
    <li
      ref={ref}
      id={stepAnchor(step.id)}
      data-cited={flash ? '' : undefined}
      className={cn(
        'flex scroll-mt-24 flex-col gap-2 rounded-md data-[cited]:bg-agent-subtle',
        flash && 'motion-safe:animate-cite',
      )}
    >
      <button
        type="button"
        aria-expanded={open}
        aria-controls={regionId}
        onClick={() => setOpen((o) => !o)}
        className="flex min-h-9 coarse:min-h-11 w-full items-center gap-2.5 rounded-md px-2 text-left transition-colors hover:bg-accent focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring"
      >
        <ChevronRight
          aria-hidden="true"
          className={cn('size-3.5 shrink-0 text-subtle-foreground transition-transform motion-reduce:transition-none', open && 'rotate-90')}
        />
        <StepNumber n={n} error={failed} />
        <span className="min-w-0 flex-1 truncate text-sm font-medium sm:flex-none">{step.title}</span>
        {first && <ToolBadge tool={first.tool} />}
        {more > 0 && <span className="font-mono text-xs text-subtle-foreground">+{more}</span>}
        {tags[0] && (
          <span className="hidden min-w-0 truncate font-mono text-[11.5px] text-muted-foreground md:inline">{tags[0]}</span>
        )}
        <span
          className={cn(
            'ml-auto hidden shrink-0 whitespace-nowrap font-mono text-xs sm:inline',
            failed ? 'text-destructive-fg' : 'text-subtle-foreground',
          )}
        >
          {failed ? 'error' : ms != null ? formatMs(ms) : ''}
        </span>
        {!failed && <Check aria-label="done" className="size-3.5 shrink-0 text-success-foreground max-sm:ml-auto" />}
      </button>

      <div id={regionId} hidden={!open} className={cn("ml-10 flex-col gap-3 pb-1", open ? "flex" : "hidden")}>
        <Evidence step={step} checklist={checklist} />
        {!readOnly && <ReplyBox onSubmit={(t, clientId) => onComment(t, step.id, { clientId })} />}
      </div>

      {(step.messages.length > 0 || comments.length > 0) && (
        <div className="ml-10 flex flex-col gap-2.5 pb-1">
          <StepExtras step={step} comments={comments} onComment={onComment} readOnly={readOnly} showReplyBox={false} />
        </div>
      )}
    </li>
  )
}

// The step the agent is on right now: expanded, a spinner and a 2px indeterminate bar.
// Both animations only run when the person has not asked for reduced motion.
function LiveStep({
  step,
  n,
  comments,
  onComment,
  readOnly,
  checklist,
  offFilter = false,
  cited,
}: {
  step: StepState
  n: number
  comments: CommentState[]
  onComment: (text: string, stepId?: string, opts?: SendOptions) => void | Promise<boolean>
  readOnly: boolean
  checklist?: ChecklistWiring
  /** The step is outside the active layer filter: kept so the live work stays visible. */
  offFilter?: boolean
  cited?: CitedStep | null
}) {
  const { ref, flash } = useCiteFlash(step.id, cited)
  const first = step.tool_calls[0]
  const tags = first ? argTags(first.input) : []
  return (
    <li
      ref={ref}
      id={stepAnchor(step.id)}
      data-cited={flash ? '' : undefined}
      className={cn(
        'flex scroll-mt-24 flex-col gap-2.5 rounded-lg border border-input bg-card px-3 py-2.5 data-[cited]:bg-agent-subtle',
        flash && 'motion-safe:animate-cite',
      )}
    >
      <div className="flex flex-wrap items-center gap-x-2.5 gap-y-1">
        <span
          role="img"
          aria-label="Running"
          className="size-3.5 shrink-0 rounded-full border-2 border-agent-subtle border-t-agent motion-safe:animate-spin"
        />
        <span className="sr-only">Step {n}:</span>
        <h3 className="m-0 text-sm font-medium">{step.title}</h3>
        {first && <ToolBadge tool={first.tool} />}
        {tags[0] && <ArgTag>{tags[0]}</ArgTag>}
        <span className="ml-auto font-mono text-xs text-subtle-foreground">
          running{offFilter && <span className="ml-1.5 text-muted-foreground">(live)</span>}
        </span>
      </div>
      <Progress
        aria-hidden="true"
        className="h-0.5 rounded-sm bg-agent-subtle"
        indicatorClassName="rounded-sm bg-agent"
      />
      <Evidence step={step} checklist={checklist} />
      <StepExtras step={step} comments={comments} onComment={onComment} readOnly={readOnly} showReplyBox />
    </li>
  )
}

// The agent prose in the timeline links its step ids to the rows of these steps.
export function Timeline(props: ComponentProps<typeof TimelineBody>) {
  return (
    <StepNumbersProvider steps={props.steps} proposalIds={props.proposalIds}>
      <TimelineBody {...props} />
    </StepNumbersProvider>
  )
}

function TimelineBody({
  steps,
  messages,
  comments,
  onComment,
  readOnly = false,
  working = false,
  onTakeControl,
  agent,
  checklist,
  filterLayer,
  onClearFilter,
  pages = [],
  ownerNames,
  shadow = false,
  citedStep,
  sends,
}: {
  steps: StepState[]
  /** Proposal ids of the room: a raw id in agent prose links to its card. */
  proposalIds?: readonly string[]
  messages: AgentMessageState[]
  comments: CommentState[]
  onComment: (text: string, stepId?: string, opts?: SendOptions) => void | Promise<boolean>
  /** A viewer reads comments but cannot write any. */
  readOnly?: boolean
  /** The agent is working: the last step is the live one. */
  working?: boolean
  /** Offered in the empty state when the room has no driver and this person may take control. */
  onTakeControl?: () => void
  /** The agent's status: the empty state says what it is doing instead of a fixed line. */
  agent?: { state: string; detail?: string }
  /** Shows the items of a runbook_lookup result as a checklist when set. */
  checklist?: ChecklistWiring
  /** Show only steps that called a tool of this layer (the live step is always kept). */
  filterLayer?: string
  onClearFilter?: () => void
  /** The owner pages of the room (Q0, G2), shown even before the agent has done a step. */
  pages?: PageState[]
  /** user id -> display name, for the owners a page names. */
  ownerNames?: Record<string, string>
  /** Shadow room: the agent works silently, so the empty state neither reports it nor offers a steer. */
  shadow?: boolean
  /** The step a hypothesis chip asked for: it opens, scrolls into view and flashes (F3). */
  citedStep?: CitedStep | null
  /** RoomActions.sends: a reply box's failed send that lands takes its Retry down; an edit cancels it. */
  sends?: SendControls
}) {
  // QA R18 (G11): a step link to a step that the layer filter hides clears the filter.
  useEffect(() => {
    if (!filterLayer || !onClearFilter) return
    const onReveal = (e: Event) => {
      const id = (e as CustomEvent<string>).detail
      const step = steps.find((s) => s.id === id)
      if (step && !step.tool_calls.some((t) => toolLayer(t.tool) === filterLayer)) onClearFilter()
    }
    window.addEventListener(REVEAL_STEP_EVENT, onReveal)
    return () => window.removeEventListener(REVEAL_STEP_EVENT, onReveal)
  }, [steps, filterLayer, onClearFilter])
  const unanchored = comments.filter((c) => !c.step_id)
  if (steps.length === 0 && messages.length === 0 && unanchored.length === 0) {
    const empty = emptyStateFor(agent, shadow)
    return (
      <div className="flex flex-col gap-2">
        <div className="flex flex-col items-center gap-2 rounded-lg border border-dashed border-border bg-card p-6 text-center">
          <PersonAvatar actor={AGENT} size={28} className={empty.busy ? 'animate-pulse' : 'opacity-70'} />
          <p className="m-0 text-base font-medium">{empty.title}</p>
          {empty.hint && <p className="m-0 max-w-[48ch] text-muted-foreground">{empty.hint}</p>}
          {empty.offerSteer && onTakeControl && (
            <p className="m-0 max-w-[48ch] text-muted-foreground">
              Take control and send a steer to start. A good first steer: &ldquo;check the consumer lag and look for a
              stuck partition&rdquo;.
            </p>
          )}
          {empty.offerSteer && onTakeControl && (
            <Button className="mt-1" aria-keyshortcuts="T" onClick={onTakeControl}>
              Take control and steer
              <Kbd aria-hidden="true">T</Kbd>
            </Button>
          )}
        </div>
        <PagesList pages={pages} names={ownerNames} />
      </div>
    )
  }

  const matched = filterLayer
    ? steps.filter((s) => s.tool_calls.some((t) => toolLayer(t.tool) === filterLayer)).length
    : steps.length

  return (
    <SendsContext.Provider value={sends}>
      <div className="flex flex-col gap-2">
        {messages.length > 0 && <AgentMessageList messages={messages} />}

        {filterLayer && (
          <p role="status" className="m-0 flex flex-wrap items-center gap-1.5 text-xs text-muted-foreground">
            {matched === 0 ? (
              <>
                No steps on <Badge variant={layerVariant(filterLayer)} mono>{filterLayer}</Badge> yet
              </>
            ) : (
              <>
                Showing {matched} {matched === 1 ? 'step' : 'steps'} on{' '}
                <Badge variant={layerVariant(filterLayer)} mono>{filterLayer}</Badge>
              </>
            )}
            <span aria-hidden="true">·</span>
            <Button variant="ghost" className="h-6 px-1.5 text-xs" onClick={onClearFilter}>
              Clear
            </Button>
          </p>
        )}

        <ol className="m-0 flex list-none flex-col gap-0.5 p-0">
          {steps.map((step, i) => {
            const live = working && i === steps.length - 1
            const hit = !filterLayer || step.tool_calls.some((t) => toolLayer(t.tool) === filterLayer)
            if (!hit && !live) return null
            const stepComments = comments.filter((c) => c.step_id === step.id)
            return live ? (
              <LiveStep
                key={step.id}
                step={step}
                n={i + 1}
                comments={stepComments}
                onComment={onComment}
                readOnly={readOnly}
                checklist={checklist}
                offFilter={!hit}
                cited={citedStep}
              />
            ) : (
              <DoneStep
                key={step.id}
                step={step}
                n={i + 1}
                comments={stepComments}
                onComment={onComment}
                readOnly={readOnly}
                checklist={checklist}
                cited={citedStep}
              />
            )
          })}
        </ol>

        <PagesList pages={pages} names={ownerNames} />

        {unanchored.length > 0 && (
          <section aria-label="Room comments" className="flex flex-col gap-2.5 pt-2">
            <h3 className="m-0 text-sm font-medium">Room comments</h3>
            {unanchored.map((c) => (
              <CommentThread key={c.id} comment={c} onReply={(t, clientId) => onComment(t, undefined, { clientId })} readOnly={readOnly} />
            ))}
          </section>
        )}
      </div>
    </SendsContext.Provider>
  )
}

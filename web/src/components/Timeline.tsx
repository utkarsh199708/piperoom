import { useId, useState } from 'react'
import { emptyStateFor } from '../lib/emptyState'
import type { FormEvent } from 'react'
import { Check, ChevronRight, X } from 'lucide-react'
import type { AgentMessageState, ChecklistTick, CommentState, StepState, ToolCallState } from '../state/types'
import { clockTime } from '../lib/time'
import { argTags, formatMs, toolLayer } from '../lib/toolLayers'
import { cn } from '@/lib/utils'
import { Badge, layerVariant } from '@/components/ui/badge'
import { Button } from '@/components/ui/button'
import { Input } from '@/components/ui/input'
import { Kbd } from '@/components/ui/kbd'
import { Progress } from '@/components/ui/progress'
import { PersonAvatar } from './room/PersonAvatar'
import { CollapsibleText } from './room/CollapsibleText'
import { stepAnchor } from '@/lib/triage'
import { Checklist } from './room/Checklist'
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

function rawJson(t: ToolCallState): string {
  const raw: Record<string, unknown> = { input: t.input }
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
          {t.summary != null && (
            <pre
              className={cn(
                'm-0 whitespace-pre-wrap break-words rounded-md border bg-muted px-2.5 py-2 font-mono text-xs leading-[17px]',
                t.is_error ? 'border-destructive/50 text-destructive-fg' : 'border-border text-muted-foreground',
              )}
            >
              {t.summary}
            </pre>
          )}
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
  onReply: (text: string) => void
  readOnly?: boolean
}) {
  const [open, setOpen] = useState(false)
  const [text, setText] = useState('')

  const submit = (e: FormEvent) => {
    e.preventDefault()
    if (!text.trim()) return
    onReply(text.trim())
    setText('')
    setOpen(false)
  }

  return (
    <div className="flex flex-col gap-1.5">
      <div className="flex items-start gap-2">
        <PersonAvatar actor={comment.author} size={20} className="mt-px" />
        <div className="min-w-0">
          <div className="flex flex-wrap items-baseline gap-x-2">
            <strong className="font-medium">{comment.author.name}</strong>
            <span className="text-xs text-subtle-foreground">{clockTime(comment.at)}</span>
          </div>
          <p className="m-0 break-words text-muted-foreground">{comment.text}</p>
        </div>
      </div>
      {comment.replies.map((r, i) => (
        <div key={i} className="ml-7 flex items-start gap-2">
          <PersonAvatar actor={AGENT} size={20} className="mt-px" />
          <div className="min-w-0">
            <div className="flex flex-wrap items-baseline gap-x-2">
              <strong className="font-medium">Agent</strong>
              <span className="text-xs text-subtle-foreground">{clockTime(r.at)}</span>
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
            onChange={(e) => setText(e.target.value)}
            placeholder="Reply…"
            aria-label="Reply to comment"
            autoFocus
          />
          <Button type="submit" size="sm" disabled={!text.trim()}>
            Send
          </Button>
        </form>
      ) : (
        <Button variant="link" size="sm" className="ml-7 w-fit text-xs" onClick={() => setOpen(true)}>
          Reply
        </Button>
      )}
    </div>
  )
}

function ReplyBox({ onSubmit }: { onSubmit: (text: string) => void }) {
  const [text, setText] = useState('')
  const submit = (e: FormEvent) => {
    e.preventDefault()
    if (!text.trim()) return
    onSubmit(text.trim())
    setText('')
  }
  return (
    <form className="flex items-center gap-2" onSubmit={submit}>
      <Input
        value={text}
        onChange={(e) => setText(e.target.value)}
        placeholder="Comment on this step…"
        aria-label="Comment on this step"
      />
      <Button type="submit" variant="secondary" size="sm" disabled={!text.trim()}>
        Comment
      </Button>
    </form>
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
  onComment: (text: string, stepId?: string) => void
  readOnly: boolean
  showReplyBox: boolean
}) {
  return (
    <>
      <AgentMessageList messages={step.messages} />
      {comments.map((c) => (
        <CommentThread key={c.id} comment={c} onReply={(t) => onComment(t, step.id)} readOnly={readOnly} />
      ))}
      {!readOnly && showReplyBox && <ReplyBox onSubmit={(t) => onComment(t, step.id)} />}
    </>
  )
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
}: {
  step: StepState
  n: number
  comments: CommentState[]
  onComment: (text: string, stepId?: string) => void
  readOnly: boolean
  checklist?: ChecklistWiring
}) {
  // A step that fetched a runbook starts open: its checklist is for doing, not for reading later.
  const [open, setOpen] = useState(() => !!checklist && step.tool_calls.some((t) => t.tool === 'runbook_lookup'))
  const regionId = useId()
  const first = step.tool_calls[0]
  const tags = first ? argTags(first.input) : []
  const failed = step.tool_calls.some((t) => t.is_error)
  const ms = totalMs(step)
  const more = step.tool_calls.length - 1

  return (
    <li id={stepAnchor(step.id)} className="flex flex-col gap-2 scroll-mt-4">
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
        {!readOnly && <ReplyBox onSubmit={(t) => onComment(t, step.id)} />}
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
}: {
  step: StepState
  n: number
  comments: CommentState[]
  onComment: (text: string, stepId?: string) => void
  readOnly: boolean
  checklist?: ChecklistWiring
  /** The step is outside the active layer filter: kept so the live work stays visible. */
  offFilter?: boolean
}) {
  const first = step.tool_calls[0]
  const tags = first ? argTags(first.input) : []
  return (
    <li id={stepAnchor(step.id)} className="flex scroll-mt-4 flex-col gap-2.5 rounded-lg border border-input bg-card px-3 py-2.5">
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

export function Timeline({
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
}: {
  steps: StepState[]
  messages: AgentMessageState[]
  comments: CommentState[]
  onComment: (text: string, stepId?: string) => void
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
}) {
  const unanchored = comments.filter((c) => !c.step_id)
  if (steps.length === 0 && messages.length === 0 && unanchored.length === 0) {
    const empty = emptyStateFor(agent)
    return (
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
    )
  }

  const matched = filterLayer
    ? steps.filter((s) => s.tool_calls.some((t) => toolLayer(t.tool) === filterLayer)).length
    : steps.length

  return (
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
            />
          )
        })}
      </ol>

      {unanchored.length > 0 && (
        <section aria-label="Room comments" className="flex flex-col gap-2.5 pt-2">
          <h3 className="m-0 text-sm font-medium">Room comments</h3>
          {unanchored.map((c) => (
            <CommentThread key={c.id} comment={c} onReply={(t) => onComment(t)} readOnly={readOnly} />
          ))}
        </section>
      )}
    </div>
  )
}

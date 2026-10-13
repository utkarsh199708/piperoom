import { useState } from 'react'
import type { FormEvent } from 'react'
import type { AgentMessageState, CommentState, StepState, ToolCallState } from '../state/types'
import { Avatar, Icon } from './ui-p4-temp'
import { clockTime } from '../lib/time'
import { layerForTool } from '../lib/p4-temp-layers'
import { formatDuration, stepTookMs } from '../lib/investigation'
import '../styles/timeline.css'

const ARG_MAX = 40
const SUMMARY_LINES = 6

function toolInputLabel(input: unknown): string {
  if (input == null) return ''
  return JSON.stringify(input)
}

/** `key value` pairs for the arg tags. Only a plain object has them. */
function argPairs(input: unknown): { key: string; text: string; full: string }[] {
  if (input == null || typeof input !== 'object' || Array.isArray(input)) return []
  return Object.entries(input as Record<string, unknown>).map(([key, value]) => {
    const full = typeof value === 'string' ? value : (JSON.stringify(value) ?? String(value))
    const text = typeof value === 'string' || full.length <= ARG_MAX ? full : `${full.slice(0, ARG_MAX)}…`
    return { key, text, full }
  })
}

function rawJson(data: unknown): string {
  try {
    return JSON.stringify(data, null, 2) ?? ''
  } catch {
    return String(data)
  }
}

function Summary({ call }: { call: ToolCallState }) {
  const [all, setAll] = useState(false)
  const text = call.summary ?? ''
  const lines = text.split('\n')
  const long = lines.length > SUMMARY_LINES
  const shown = long && !all ? lines.slice(0, SUMMARY_LINES).join('\n') : text
  return (
    <>
      <pre className={`mono toolcall__summary ${call.is_error ? 'toolcall__summary--error' : ''}`}>
        {shown}
      </pre>
      {long && (
        <button
          type="button"
          className="btn btn--link toolcall__more"
          aria-expanded={all}
          onClick={() => setAll((v) => !v)}
        >
          {all ? 'Show less' : 'Show all'}
        </button>
      )}
    </>
  )
}

function ToolCall({ step }: { step: StepState }) {
  return (
    <ul className="toolcalls">
      {step.tool_calls.map((t) => {
        const layer = layerForTool(t.tool)
        const args = argPairs(t.input)
        const input = toolInputLabel(t.input)
        return (
          <li key={t.tool_use_id} className={`toolcall ${t.is_error ? 'toolcall--error' : ''}`}>
            <div className="toolcall__head">
              <code
                className={`mono toolcall__tool${layer ? ` toolcall__tool--${layer}` : ''}${t.is_error ? ' is-bad' : ''}`}
                title={layer ?? undefined}
              >
                {t.tool}
              </code>
              {args.map((a) => (
                <span key={a.key} className="arg" title={a.full}>
                  <b>{a.key}</b>
                  <span>{a.text}</span>
                </span>
              ))}
              {t.took_ms != null && <span className="muted toolcall__took">{t.took_ms} ms</span>}
            </div>
            {input && (
              <details className="toolcall__raw">
                <summary>Input</summary>
                <pre className="mono toolcall__input">{input}</pre>
              </details>
            )}
            {t.summary != null && <Summary call={t} />}
            {t.truncated && <p className="toolcall__trunc">output truncated</p>}
            {t.data != null && (
              <details className="toolcall__raw">
                <summary>Raw</summary>
                <pre className="mono toolcall__input">{rawJson(t.data)}</pre>
              </details>
            )}
          </li>
        )
      })}
    </ul>
  )
}

function AgentMessageList({ messages }: { messages: AgentMessageState[] }) {
  if (messages.length === 0) return null
  return (
    <div className="agent-msgs">
      {messages.map((m, i) => (
        <div key={i} className="agent-msg">
          <Avatar actor={{ kind: 'agent', id: 'agent', name: 'Agent' }} size="xs" />
          <p className="agent-msg__text">
            <span className="agent-msg__tag">Agent</span> {m.text}
          </p>
        </div>
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
    <div className="thread">
      <div className="thread__comment">
        <Avatar actor={comment.author} size="sm" />
        <div className="thread__body">
          <div className="thread__meta">
            <strong>{comment.author.name}</strong>
            <span className="muted">{clockTime(comment.at)}</span>
          </div>
          <p className="thread__text">{comment.text}</p>
        </div>
      </div>
      {comment.replies.map((r, i) => (
        <div key={i} className="thread__comment thread__comment--agent">
          <Avatar actor={{ kind: 'agent', id: 'agent', name: 'Agent' }} size="sm" />
          <div className="thread__body">
            <div className="thread__meta">
              <strong className="thread__agent">Agent</strong>
              <span className="muted">{clockTime(r.at)}</span>
            </div>
            <p className="thread__text">{r.text}</p>
          </div>
        </div>
      ))}
      {readOnly ? null : open ? (
        <form className="thread__reply" onSubmit={submit}>
          <input
            value={text}
            onChange={(e) => setText(e.target.value)}
            placeholder="Reply…"
            aria-label="Reply to comment"
            autoFocus
          />
          <button type="submit" className="btn btn--small">
            Send
          </button>
        </form>
      ) : (
        <button type="button" className="btn btn--link" onClick={() => setOpen(true)}>
          Reply
        </button>
      )}
    </div>
  )
}

/** The dashed "thinking…" step shown while the agent works and nothing is running yet. */
function GhostStep({ number, detail }: { number: number; detail?: string }) {
  return (
    <div className="step step--ghost" data-testid="step-ghost">
      <span className="step__dot" aria-hidden="true">
        {number}
      </span>
      <div className="live">
        <span>
          thinking…{detail ? <span className="live__detail"> {detail}</span> : null}
        </span>
        <span className="live__bar" aria-hidden="true" />
      </div>
    </div>
  )
}

export function Timeline({
  steps,
  messages,
  comments,
  onComment,
  readOnly = false,
  agentState,
  agentDetail,
}: {
  steps: StepState[]
  messages: AgentMessageState[]
  comments: CommentState[]
  onComment: (text: string, stepId?: string) => void
  /** A viewer reads comments but cannot write any. */
  readOnly?: boolean
  /** agent_status.state. Only `working` marks the last step live and adds the thinking step. */
  agentState?: string
  /** agent_status.detail, shown next to "thinking…". */
  agentDetail?: string
}) {
  const working = agentState === 'working'
  const unanchored = comments.filter((c) => !c.step_id)
  if (steps.length === 0 && messages.length === 0 && unanchored.length === 0) {
    if (working) {
      return (
        <div className="timeline">
          <GhostStep number={1} detail={agentDetail} />
        </div>
      )
    }
    return (
      <div className="empty empty--timeline">
        <Icon name="clock" size={22} className="empty__icon" />
        <p className="empty__title">The agent has not started yet.</p>
        <p className="empty__hint">
          {readOnly ? 'Waiting for the driver' : 'Take control and send a steer to start'}
        </p>
      </div>
    )
  }

  const lastIndex = steps.length - 1
  const lastStep = lastIndex >= 0 ? steps[lastIndex] : null
  const lastCall = lastStep && lastStep.tool_calls.length > 0 ? lastStep.tool_calls[lastStep.tool_calls.length - 1] : null
  // A tool that has been called but has no result yet: the live row names it.
  const runningTool = working && lastCall && lastCall.summary == null ? lastCall.tool : null

  return (
    <div className="timeline">
      {messages.length > 0 && <AgentMessageList messages={messages} />}

      {steps.map((step, i) => {
        const stepComments = comments.filter((c) => c.step_id === step.id)
        const live = working && i === lastIndex
        const failed = step.tool_calls.some((t) => t.is_error)
        const took = stepTookMs(step)
        const cls = ['step', live ? 'step--live' : '', failed ? 'step--error' : ''].filter(Boolean).join(' ')
        return (
          <div key={step.id} className={cls} id={`step-${step.id}`}>
            <div className="step__head">
              <span className="step__dot" aria-hidden="true">
                {failed ? '✕' : i + 1}
              </span>
              <h3 className="step__title">{step.title}</h3>
              {took !== null && <span className="step__meta">{formatDuration(took)}</span>}
            </div>
            <ToolCall step={step} />
            {live && runningTool && (
              <div className="live">
                <span>
                  running <span className="mono">{runningTool}</span>…
                </span>
                <span className="live__bar" aria-hidden="true" />
              </div>
            )}
            <AgentMessageList messages={step.messages} />
            {stepComments.map((c) => (
              <CommentThread key={c.id} comment={c} onReply={(t) => onComment(t, step.id)} readOnly={readOnly} />
            ))}
            {!readOnly && <ReplyBox onSubmit={(t) => onComment(t, step.id)} />}
          </div>
        )
      })}

      {working && !runningTool && <GhostStep number={steps.length + 1} detail={agentDetail} />}

      {unanchored.length > 0 && (
        <div className="step step--room">
          <div className="step__head">
            <h3 className="step__title">Room comments</h3>
          </div>
          {unanchored.map((c) => (
            <CommentThread key={c.id} comment={c} onReply={(t) => onComment(t)} readOnly={readOnly} />
          ))}
        </div>
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
    <form className="replybox" onSubmit={submit}>
      <input
        value={text}
        onChange={(e) => setText(e.target.value)}
        placeholder="Comment on this step…"
        aria-label="Comment on this step"
      />
      <button type="submit" className="btn btn--small" disabled={!text.trim()}>
        Comment
      </button>
    </form>
  )
}

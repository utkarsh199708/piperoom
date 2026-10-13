import { useState } from 'react'
import type { FormEvent } from 'react'
import type { AgentMessageState, CommentState, StepState } from '../state/types'
import { Avatar } from './ui'
import { clockTime } from '../lib/time'

function toolInputLabel(input: unknown): string {
  if (input == null) return ''
  return JSON.stringify(input)
}

function ToolCall({ step }: { step: StepState }) {
  return (
    <ul className="toolcalls">
      {step.tool_calls.map((t) => (
        <li key={t.tool_use_id} className={`toolcall ${t.is_error ? 'toolcall--error' : ''}`}>
          <div className="toolcall__head">
            <code className="mono toolcall__tool">{t.tool}</code>
            {t.took_ms != null && <span className="muted toolcall__took">{t.took_ms} ms</span>}
          </div>
          {toolInputLabel(t.input) && (
            <pre className="mono toolcall__input">{toolInputLabel(t.input)}</pre>
          )}
          {t.summary != null && (
            <pre className={`mono toolcall__summary ${t.is_error ? 'toolcall__summary--error' : ''}`}>
              {t.summary}
            </pre>
          )}
        </li>
      ))}
    </ul>
  )
}

function AgentMessageList({ messages }: { messages: AgentMessageState[] }) {
  if (messages.length === 0) return null
  return (
    <div className="agent-msgs">
      {messages.map((m, i) => (
        <p key={i} className="agent-msg">
          <span className="agent-msg__tag">Agent</span> {m.text}
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
              <strong>Agent</strong>
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

export function Timeline({
  steps,
  messages,
  comments,
  onComment,
  readOnly = false,
}: {
  steps: StepState[]
  messages: AgentMessageState[]
  comments: CommentState[]
  onComment: (text: string, stepId?: string) => void
  /** A viewer reads comments but cannot write any. */
  readOnly?: boolean
}) {
  const unanchored = comments.filter((c) => !c.step_id)
  if (steps.length === 0 && messages.length === 0 && unanchored.length === 0) {
    return <p className="empty">The agent has not started yet.</p>
  }

  return (
    <div className="timeline">
      {messages.length > 0 && <AgentMessageList messages={messages} />}

      {steps.map((step) => {
        const stepComments = comments.filter((c) => c.step_id === step.id)
        return (
          <div key={step.id} className="step">
            <div className="step__head">
              <span className="step__dot" aria-hidden="true" />
              <h3 className="step__title">{step.title}</h3>
            </div>
            <ToolCall step={step} />
            <AgentMessageList messages={step.messages} />
            {stepComments.map((c) => (
              <CommentThread key={c.id} comment={c} onReply={(t) => onComment(t, step.id)} readOnly={readOnly} />
            ))}
            {!readOnly && <ReplyBox onSubmit={(t) => onComment(t, step.id)} />}
          </div>
        )
      })}

      {unanchored.length > 0 && (
        <div className="step">
          <h3 className="step__title">Room comments</h3>
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

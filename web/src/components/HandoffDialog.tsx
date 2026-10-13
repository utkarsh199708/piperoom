import { useEffect, useRef, useState } from 'react'
import type { FormEvent } from 'react'
import type { HandoffState, RoomState } from '../state/types'
import type { CurrentUser } from '../state/permissions'
import { canAcceptHandoff } from '../state/permissions'
import type { RoomActions } from '../hooks/useRoom'
import { PersonSelect } from './PersonSelect'
import { Icon } from './ui-p4-temp'

const FOCUSABLE =
  'button:not(:disabled), [href], input:not(:disabled), select:not(:disabled), textarea:not(:disabled), [tabindex]:not([tabindex="-1"])'

function SummaryList({
  title,
  items,
  glyph,
  tone,
}: {
  title: string
  items: string[]
  glyph: string
  tone: string
}) {
  if (!items || items.length === 0) return null
  return (
    <div className={`handoff__section handoff__section--${tone}`}>
      <h4 className="handoff__subtitle">
        <span className="handoff__glyph" aria-hidden="true">
          {glyph}
        </span>
        {title}
      </h4>
      <ul className="handoff__list">
        {items.map((it, i) => (
          <li key={i}>{it}</li>
        ))}
      </ul>
    </div>
  )
}

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
  const pending = state.handoffs.find((h) => h.status === 'pending')
  const dialogRef = useRef<HTMLDivElement>(null)
  const closeRef = useRef<HTMLButtonElement>(null)
  // Whatever had focus when the dialog opened (the Hand off button) gets it back on close.
  const opener = useRef<Element | null>(document.activeElement)
  const onCloseRef = useRef(onClose)
  onCloseRef.current = onClose

  // Initial focus, Esc to close, and a Tab loop inside the dialog. No library.
  useEffect(() => {
    const dialog = dialogRef.current
    const first = dialog?.querySelector<HTMLElement>('#handoff-to') ?? closeRef.current
    first?.focus()

    const onKey = (e: KeyboardEvent) => {
      if (e.key === 'Escape') {
        e.preventDefault()
        onCloseRef.current()
        return
      }
      if (e.key !== 'Tab' || !dialog) return
      const items = Array.from(dialog.querySelectorAll<HTMLElement>(FOCUSABLE))
      if (items.length === 0) return
      const firstEl = items[0]
      const lastEl = items[items.length - 1]
      const active = document.activeElement
      if (!dialog.contains(active)) {
        e.preventDefault()
        firstEl.focus()
      } else if (e.shiftKey && active === firstEl) {
        e.preventDefault()
        lastEl.focus()
      } else if (!e.shiftKey && active === lastEl) {
        e.preventDefault()
        firstEl.focus()
      }
    }
    document.addEventListener('keydown', onKey)
    const from = opener.current
    return () => {
      document.removeEventListener('keydown', onKey)
      if (from instanceof HTMLElement && from !== document.body && from.isConnected) from.focus()
    }
  }, [])

  // The people list loads after the dialog opens. When the select first appears, and
  // focus is still on the close button, move it to the first field.
  useEffect(() => {
    const select = dialogRef.current?.querySelector<HTMLElement>('#handoff-to')
    if (select && document.activeElement === closeRef.current) select.focus()
  }, [to])

  const submit = (e: FormEvent) => {
    e.preventDefault()
    if (!to) return
    actions.handoff(to, stay)
  }

  return (
    <div className="overlay" role="dialog" aria-modal="true" aria-label="Hand off the room">
      <div className="dialog handoff" ref={dialogRef}>
        <header className="dialog__head">
          <h2 className="dialog__title">Hand off the room</h2>
          <button
            type="button"
            className="icon-btn"
            onClick={onClose}
            aria-label="Close hand-off dialog"
            ref={closeRef}
          >
            <Icon name="x" />
          </button>
        </header>

        {pending ? (
          <HandoffSummary pending={pending} onClose={onClose} />
        ) : (
          <form className="handoff__form" onSubmit={submit}>
            <div className="field">
              <label htmlFor="handoff-to">Hand off to</label>
              <PersonSelect id="handoff-to" selfId={user.id} value={to} onChange={setTo} />
            </div>
            <label className="check">
              <input type="checkbox" checked={stay} onChange={(e) => setStay(e.target.checked)} />
              Stay as watcher after hand-off
            </label>
            <div className="dialog__actions">
              <button type="button" className="btn btn--ghost" onClick={onClose}>
                Cancel
              </button>
              <button type="submit" className="btn btn--primary" disabled={!to}>
                Start hand-off
              </button>
            </div>
          </form>
        )}
      </div>
    </div>
  )
}

function HandoffSummary({
  pending,
  onClose,
}: {
  pending: HandoffState
  onClose: () => void
}) {
  const s = pending.summary
  return (
    <div className="handoff__summary">
      <p className="handoff__intro muted">
        Summary prepared for {pending.to.name}. They must accept to take over.
      </p>
      {s.what_broke && (
        <div className="handoff__section handoff__section--prose">
          <h4 className="handoff__subtitle">What broke</h4>
          <p>{s.what_broke}</p>
        </div>
      )}
      {s.why && (
        <div className="handoff__section handoff__section--prose">
          <h4 className="handoff__subtitle">Why</h4>
          <p>{s.why}</p>
        </div>
      )}
      <SummaryList title="Done" items={s.done} glyph="✓" tone="ok" />
      <SummaryList title="Ruled out" items={s.ruled_out} glyph="✕" tone="muted" />
      <SummaryList title="Still open" items={s.open} glyph="●" tone="warn" />
      <SummaryList title="Risks" items={s.risks} glyph="▲" tone="fail" />
      <div className="dialog__actions">
        <button type="button" className="btn btn--ghost" onClick={onClose}>
          Close
        </button>
      </div>
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
    <div className="banner banner--accept" role="status">
      <span>
        <strong>{state.driver?.name ?? 'Someone'}</strong> is handing the room to you.
      </span>
      <button type="button" className="btn btn--primary" onClick={() => actions.acceptHandoff(mine.handoff_id)}>
        Accept
      </button>
    </div>
  )
}

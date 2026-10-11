import { useState } from 'react'
import type { FormEvent } from 'react'
import type { HandoffState, RoomState } from '../state/types'
import type { CurrentUser } from '../state/permissions'
import type { RoomActions } from '../hooks/useRoom'
import { DEV_USERS } from '../dev/users'

function SummaryList({ title, items }: { title: string; items: string[] }) {
  if (!items || items.length === 0) return null
  return (
    <div className="handoff__section">
      <h4 className="handoff__subtitle">{title}</h4>
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
  const candidates = DEV_USERS.filter((u) => u.id !== user.id)
  const [to, setTo] = useState(candidates[0]?.id ?? '')
  const [stay, setStay] = useState(false)
  const pending = state.handoffs.find((h) => h.status === 'pending')

  const submit = (e: FormEvent) => {
    e.preventDefault()
    if (!to) return
    actions.handoff(to, stay)
  }

  return (
    <div className="overlay" role="dialog" aria-modal="true" aria-label="Hand off the room">
      <div className="dialog handoff">
        <header className="dialog__head">
          <h2 className="dialog__title">Hand off the room</h2>
          <button type="button" className="icon-btn" onClick={onClose} aria-label="Close hand-off dialog">
            ×
          </button>
        </header>

        {pending ? (
          <HandoffSummary pending={pending} onClose={onClose} />
        ) : (
          <form className="handoff__form" onSubmit={submit}>
            <div className="field">
              <label htmlFor="handoff-to">Hand off to</label>
              <select id="handoff-to" value={to} onChange={(e) => setTo(e.target.value)}>
                {candidates.map((u) => (
                  <option key={u.id} value={u.id}>
                    {u.name}
                  </option>
                ))}
              </select>
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
        <div className="handoff__section">
          <h4 className="handoff__subtitle">What broke</h4>
          <p>{s.what_broke}</p>
        </div>
      )}
      {s.why && (
        <div className="handoff__section">
          <h4 className="handoff__subtitle">Why</h4>
          <p>{s.why}</p>
        </div>
      )}
      <SummaryList title="Done" items={s.done} />
      <SummaryList title="Ruled out" items={s.ruled_out} />
      <SummaryList title="Still open" items={s.open} />
      <SummaryList title="Risks" items={s.risks} />
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
  const mine = state.handoffs.find((h) => h.status === 'pending' && h.to.id === user.id)
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

import { useState } from 'react'
import type { FormEvent } from 'react'
import type { RoomState } from '../state/types'
import type { CurrentUser } from '../state/permissions'
import { canRequestControl, canSteer, canTakeControl, isViewer } from '../state/permissions'
import type { RoomActions } from '../hooks/useRoom'
import { PersonSelect } from './PersonSelect'

// Driver only: give control to anyone GET /api/people lists. Loads the list on open.
function GrantPicker({ user, onGrant }: { user: CurrentUser; onGrant: (to: string) => void }) {
  const [open, setOpen] = useState(false)
  const [to, setTo] = useState('')
  if (!open) {
    return (
      <button type="button" className="btn btn--ghost" onClick={() => setOpen(true)}>
        Give control to…
      </button>
    )
  }
  return (
    <div className="grant-picker">
      <label htmlFor="grant-to">Give control to</label>
      <PersonSelect id="grant-to" selfId={user.id} value={to} onChange={setTo} />
      <button
        type="button"
        className="btn btn--small btn--primary"
        disabled={!to}
        onClick={() => {
          onGrant(to)
          setOpen(false)
        }}
      >
        Give control
      </button>
      <button type="button" className="btn btn--small btn--ghost" onClick={() => setOpen(false)}>
        Cancel
      </button>
    </div>
  )
}

export function SteerBox({
  state,
  user,
  actions,
}: {
  state: RoomState
  user: CurrentUser
  actions: RoomActions
}) {
  const [text, setText] = useState('')
  const isDriver = canSteer(state, user)

  if (isViewer(user)) {
    return (
      <section className="panel steer">
        <div className="panel__head">
          <h2 className="panel__title">Steer</h2>
        </div>
        <div className="panel__body">
          <p className="muted">View only: you can read this room but not steer it.</p>
        </div>
      </section>
    )
  }

  const submit = (e: FormEvent) => {
    e.preventDefault()
    if (!text.trim()) return
    actions.steer(text.trim())
    setText('')
  }

  return (
    <section className="panel steer">
      <div className="panel__head">
        <h2 className="panel__title">Steer</h2>
        {!isDriver && state.driver && (
          <span className="muted steer__hint">Your message goes to {state.driver.name}'s queue</span>
        )}
      </div>
      <div className="panel__body">
        {isDriver && state.suggestions.length > 0 && (
          <ul className="suggestions">
            {state.suggestions.map((s) => (
              <li key={s.suggestion_id} className="suggestion">
                <div className="suggestion__body">
                  <strong>{s.from.name}</strong>
                  <p className="suggestion__text">{s.text}</p>
                </div>
                <button
                  type="button"
                  className="btn btn--small btn--primary"
                  onClick={() => actions.sendSuggestion(s.suggestion_id)}
                >
                  Send to agent
                </button>
              </li>
            ))}
          </ul>
        )}

        <form className="steer__form" onSubmit={submit}>
          <textarea
            value={text}
            onChange={(e) => setText(e.target.value)}
            placeholder={isDriver ? 'Steer the agent…' : 'Suggest a steer…'}
            aria-label="Steer the agent"
            rows={2}
          />
          <div className="steer__actions">
            {!isDriver && canTakeControl(state, user) && (
              <button
                type="button"
                className="btn btn--ghost"
                onClick={() => actions.grantControl(user.id)}
              >
                Take control
              </button>
            )}
            {!isDriver && !canTakeControl(state, user) && canRequestControl(state, user) && (
              <button type="button" className="btn btn--ghost" onClick={actions.requestControl}>
                Request control
              </button>
            )}
            <button type="submit" className="btn btn--primary" disabled={!text.trim()}>
              {isDriver ? 'Send to agent' : 'Suggest'}
            </button>
          </div>
        </form>

        {isDriver && <GrantPicker user={user} onGrant={actions.grantControl} />}

        {isDriver && state.control_requests.length > 0 && (
          <ul className="suggestions">
            {state.control_requests.map((r) => (
              <li key={r.from.id} className="suggestion">
                <div className="suggestion__body">
                  <strong>{r.from.name}</strong>
                  <p className="suggestion__text">wants control</p>
                </div>
                <button
                  type="button"
                  className="btn btn--small btn--primary"
                  onClick={() => actions.grantControl(r.from.id)}
                >
                  Grant control
                </button>
              </li>
            ))}
          </ul>
        )}
      </div>
    </section>
  )
}

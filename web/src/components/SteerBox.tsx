import { useState } from 'react'
import type { FormEvent, KeyboardEvent } from 'react'
import type { RoomState } from '../state/types'
import type { CurrentUser } from '../state/permissions'
import {
  canRequestControl,
  canSteer,
  canTakeControl,
  isLeaseExpired,
  isViewer,
} from '../state/permissions'
import type { RoomActions } from '../hooks/useRoom'
import { leaseLabel } from '../lib/investigation'
import { PersonSelect } from './PersonSelect'
import { Avatar, Icon } from './ui-p4-temp'

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
      <section className="panel steer steer--viewer">
        <p className="muted steer__viewer">
          <Icon name="lock" size={14} /> View only: you can read this room but not steer it.
        </p>
      </section>
    )
  }

  const send = () => {
    if (!text.trim()) return
    actions.steer(text.trim())
    setText('')
  }
  const submit = (e: FormEvent) => {
    e.preventDefault()
    send()
  }
  const onKeyDown = (e: KeyboardEvent<HTMLTextAreaElement>) => {
    if (e.key === 'Enter' && (e.ctrlKey || e.metaKey)) {
      e.preventDefault()
      send()
    }
  }

  const lease = leaseLabel(state.driver?.lease_until)
  const expired = !isDriver && state.driver != null && isLeaseExpired(state)

  return (
    <section className="panel steer composer">
      <div className="composer__head">
        {isDriver ? (
          <>
            <span className="composer__dot" aria-hidden="true" />
            <span>You are driving{lease ? ` · ${lease}` : ''}</span>
          </>
        ) : expired ? (
          <span className="composer__expired">{state.driver?.name}'s lease expired</span>
        ) : state.driver ? (
          <>
            <Avatar actor={{ kind: 'human', id: state.driver.id, name: state.driver.name }} size="xs" driver />
            <span className="steer__hint">Your message goes to {state.driver.name}'s queue</span>
          </>
        ) : (
          <span>Take control to steer the agent.</span>
        )}
      </div>

      {isDriver && state.suggestions.length > 0 && (
        <ul className="suggestions">
          {state.suggestions.map((s) => (
            <li key={s.suggestion_id} className="suggestion">
              <Avatar actor={s.from} size="sm" />
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

      {isDriver && state.control_requests.length > 0 && (
        <ul className="suggestions">
          {state.control_requests.map((r) => (
            <li key={r.from.id} className="suggestion">
              <Avatar actor={r.from} size="sm" />
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

      <form className="steer__form" onSubmit={submit}>
        <textarea
          value={text}
          onChange={(e) => setText(e.target.value)}
          onKeyDown={onKeyDown}
          placeholder={isDriver ? 'Steer the agent…' : 'Suggest a steer…'}
          aria-label="Steer the agent"
          rows={1}
        />
        <div className="steer__actions composer__foot">
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
          {isDriver && <GrantPicker user={user} onGrant={actions.grantControl} />}
          <span className="kbd">
            <kbd>Ctrl</kbd> + <kbd>Enter</kbd> to send
          </span>
          <button type="submit" className="btn btn--primary" disabled={!text.trim()}>
            <Icon name="send" size={14} />
            {isDriver ? 'Send to agent' : 'Suggest'}
          </button>
        </div>
      </form>
    </section>
  )
}

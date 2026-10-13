import type { ReactNode } from 'react'
import type { RoomState } from '../state/types'
import type { CurrentUser } from '../state/permissions'
import {
  canAcceptHandoff,
  canRequestControl,
  canSteer,
  canTakeControl,
  isLeaseExpired,
  isViewer,
} from '../state/permissions'
import type { RoomActions } from '../hooks/useRoom'
import { leaseLabel } from '../lib/investigation'
import { firstPendingLayer } from '../lib/situation'
import { Avatar, Icon } from './ui-p4-temp'
import { Chip } from './ui'

function scrollToEl(el: Element | null) {
  if (!el || typeof el.scrollIntoView !== 'function') return
  const calm =
    typeof window.matchMedia === 'function' &&
    window.matchMedia('(prefers-reduced-motion: reduce)').matches
  el.scrollIntoView({ behavior: calm ? 'auto' : 'smooth', block: 'center' })
}

const CONFIDENCE: Record<string, number> = { low: 1, medium: 2, high: 3 }

function NowCard({ state }: { state: RoomState }) {
  const h = state.hypothesis
  const working = state.agent_status.state === 'working'
  const lastStep = state.steps.length > 0 ? state.steps[state.steps.length - 1] : null

  let body: ReactNode
  if (h) {
    const level = CONFIDENCE[h.confidence] ?? 0
    const basedOn = h.based_on_steps ?? []
    body = (
      <>
        <p className="sit-card__text">{h.text}</p>
        <div className="sit-card__foot">
          <span className="meter" aria-hidden="true">
            {[1, 2, 3].map((n) => (
              <i key={n} className={n <= level ? 'on' : ''} />
            ))}
          </span>
          <span>{h.confidence} confidence</span>
          {h.confirmed && (
            <Chip tone="ok">
              <Icon name="check" size={12} />
              confirmed
            </Chip>
          )}
          {basedOn.length > 0 && (
            <>
              <span>based on</span>
              {basedOn.map((id) => {
                const idx = state.steps.findIndex((s) => s.id === id)
                return (
                  <button
                    key={id}
                    type="button"
                    className="step-chip"
                    onClick={() => scrollToEl(document.getElementById(`step-${id}`))}
                  >
                    {idx >= 0 ? `#${idx + 1}` : id}
                  </button>
                )
              })}
            </>
          )}
        </div>
      </>
    )
  } else if (lastStep) {
    // The ellipsis keeps this line distinct from the step title in the Investigation list.
    body = (
      <p className="sit-card__text">
        {working ? 'Working on' : 'Last step'}: {lastStep.title}…
      </p>
    )
  } else {
    body = (
      <p className="sit-card__text sit-card__text--quiet">
        {state.driver ? 'The agent has not started.' : 'Waiting for a driver.'}
      </p>
    )
  }

  return (
    <section className="panel sit-card sit-card--now" aria-label="What is happening">
      <span className="eyebrow">What is happening</span>
      {body}
    </section>
  )
}

function TodoCard({
  state,
  user,
  actions,
}: {
  state: RoomState
  user: CurrentUser
  actions: RoomActions
}) {
  const cls = (mod: string) => `panel sit-card sit-card--todo sit-card--${mod}`
  const eyebrow = <span className="eyebrow">What you need to do</span>
  const label = 'What you need to do'

  if (state.status === 'closed') {
    return (
      <section className={cls('done')} aria-label={label} data-todo="resolved">
        {eyebrow}
        <p className="sit-card__text">
          Resolved{state.resolution ? ` · ${state.resolution}` : ''}
        </p>
      </section>
    )
  }

  const layer = firstPendingLayer(state, user)
  if (layer) {
    return (
      <section className={cls('approve')} aria-label={label} data-todo="approve">
        {eyebrow}
        <p className="sit-card__text">
          Approve or reject the <b>{layer}</b> step.
        </p>
        <div className="sit-card__foot">
          <button
            type="button"
            className="btn btn--small btn--primary"
            onClick={() =>
              scrollToEl(document.querySelector('.proposal--mine') ?? document.getElementById('proposals'))
            }
          >
            Go to proposal
          </button>
        </div>
      </section>
    )
  }

  const handoff = state.handoffs.find((h) => canAcceptHandoff(user, h))
  if (handoff) {
    return (
      <section className={cls('handoff')} aria-label={label} data-todo="handoff">
        {eyebrow}
        <p className="sit-card__text">
          <b>{state.driver?.name ?? 'Someone'}</b> is handing the room to you.
        </p>
        <div className="sit-card__foot">
          <button
            type="button"
            className="btn btn--small btn--primary"
            onClick={() => actions.acceptHandoff(handoff.handoff_id)}
          >
            Accept
          </button>
        </div>
      </section>
    )
  }

  if (canTakeControl(state, user)) {
    return (
      <section className={cls('control')} aria-label={label} data-todo="control">
        {eyebrow}
        <p className="sit-card__text">
          {state.driver && isLeaseExpired(state)
            ? `${state.driver.name}'s lease expired — take control to steer the agent.`
            : 'No one is driving — take control to steer the agent.'}
        </p>
        <div className="sit-card__foot">
          <button
            type="button"
            className="btn btn--small btn--primary"
            onClick={() => actions.grantControl(user.id)}
          >
            Take control
          </button>
        </div>
      </section>
    )
  }

  if (canSteer(state, user)) {
    const lease = leaseLabel(state.driver?.lease_until)
    return (
      <section className={cls('driving')} aria-label={label} data-todo="driving">
        {eyebrow}
        <p className="sit-card__text sit-card__text--with-avatar">
          <Avatar actor={{ kind: 'human', id: user.id, name: user.name }} size="sm" driver />
          <span>You are driving. Steer the agent or hand off.</span>
        </p>
        {lease && <div className="sit-card__foot">{lease}</div>}
      </section>
    )
  }

  if (isViewer(user)) {
    return (
      <section className={cls('viewer')} aria-label={label} data-todo="viewer">
        {eyebrow}
        <p className="sit-card__text">
          You are viewing. You can read everything; you cannot steer, approve or trace.
        </p>
      </section>
    )
  }

  return (
    <section className={cls('quiet')} aria-label={label} data-todo="quiet">
      {eyebrow}
      <p className="sit-card__text">
        Nothing right now — <b>{state.driver?.name ?? 'someone'}</b> is driving.
      </p>
      {canRequestControl(state, user) && (
        <div className="sit-card__foot">You can request control.</div>
      )}
    </section>
  )
}

/** The two questions above the fold: what is happening, and what you need to do. */
export function SituationCards({
  state,
  user,
  actions,
}: {
  state: RoomState
  user: CurrentUser
  actions: RoomActions
}) {
  return (
    <div className="situation">
      <NowCard state={state} />
      <TodoCard state={state} user={user} actions={actions} />
    </div>
  )
}

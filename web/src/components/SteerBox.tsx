import { PersonName } from '@/components/VerifiedBadge'
import { useRef, useState } from 'react'
import type { FormEvent, KeyboardEvent } from 'react'
import { Eye, Lock } from 'lucide-react'
import type { RoomState } from '../state/types'
import type { CurrentUser } from '../state/permissions'
import { canRequestControl, canSteer, canTakeControl, isViewer } from '../state/permissions'
import type { RoomActions } from '../hooks/useRoom'
import { modKeyLabel } from '@/lib/envLabel'
import { Button } from '@/components/ui/button'
import { Kbd } from '@/components/ui/kbd'
import { Textarea } from '@/components/ui/textarea'
import { PersonSelect } from './PersonSelect'
import { PersonAvatar } from './room/PersonAvatar'
import { StatusDot } from './room/StatusDot'
import { minutesLeft, useNow } from './room/useNow'

// Driver only: give control to anyone GET /api/people lists. Loads the list on open.
function GrantPicker({
  user,
  open,
  onClose,
  onGrant,
}: {
  user: CurrentUser
  open: boolean
  onClose: () => void
  onGrant: (to: string) => void
}) {
  const [to, setTo] = useState('')
  if (!open) return null
  return (
    <div className="flex flex-wrap items-center gap-2 rounded-md border border-border bg-card px-3 py-2">
      <label htmlFor="grant-to" className="text-xs text-muted-foreground">
        Give control to
      </label>
      <div className="min-w-[10rem] [&_select]:h-8 [&_select]:w-full">
        <PersonSelect id="grant-to" selfId={user.id} value={to} onChange={setTo} />
      </div>
      <Button
        size="sm"
        disabled={!to}
        onClick={() => {
          onGrant(to)
          onClose()
        }}
      >
        Give control
      </Button>
      <Button size="sm" variant="ghost" onClick={onClose}>
        Cancel
      </Button>
    </div>
  )
}

// The composer sticks to the bottom of the room column. Same field for everyone, but
// what it does depends on who you are: the driver steers the agent, anyone else leaves a
// suggestion in the driver's queue, a viewer reads. (Roles: docs/api.md, state/permissions.ts.)
const BAR = 'sticky bottom-0 z-10 mt-auto border-t border-border bg-background px-4 pb-4 pt-3 sm:px-6'

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
  const [granting, setGranting] = useState(false)
  const ref = useRef<HTMLTextAreaElement>(null)
  const now = useNow()
  const isDriver = canSteer(state, user)

  if (isViewer(user)) {
    return (
      <section aria-label="Steer" className={`${BAR} flex items-center gap-2 pb-3`}>
        <Eye className="size-3.5 shrink-0 text-subtle-foreground" aria-hidden="true" />
        <p className="m-0 text-muted-foreground">
          View only: you can read this room but not steer it. Ask a member to take control.
        </p>
      </section>
    )
  }

  if (state.status !== 'open') {
    return (
      <section aria-label="Steer" className={`${BAR} flex items-center gap-2 pb-3`}>
        <Lock className="size-3.5 shrink-0 text-subtle-foreground" aria-hidden="true" />
        <p className="m-0 text-muted-foreground">This room is closed. Nothing more to steer.</p>
      </section>
    )
  }

  const noDriver = state.driver === null
  const take = !isDriver && canTakeControl(state, user)
  const request = !isDriver && !take && canRequestControl(state, user)
  const mod = modKeyLabel()
  const left = minutesLeft(state.driver?.lease_until, now)

  const send = () => {
    if (!text.trim() || noDriver) return
    actions.steer(text.trim())
    setText('')
    if (ref.current) ref.current.style.height = ''
  }
  const submit = (e: FormEvent) => {
    e.preventDefault()
    send()
  }
  const onKeyDown = (e: KeyboardEvent<HTMLTextAreaElement>) => {
    if (e.key === 'Enter' && (e.metaKey || e.ctrlKey) && !e.nativeEvent.isComposing) {
      e.preventDefault()
      send()
    }
  }
  // Grow with the text, up to about six lines.
  const grow = (el: HTMLTextAreaElement) => {
    el.style.height = 'auto'
    el.style.height = `${Math.min(el.scrollHeight, 140)}px`
  }

  let status: string
  // minutesLeft is 0 only once the lease has run out (it rounds up while time remains).
  if (isDriver) {
    status = left === 0 ? 'You are driving · lease expired' : left != null ? `You are driving · lease ${left}m` : 'You are driving'
  } else if (noDriver) {
    status = 'No driver · take control to steer the agent'
  } else {
    const name = state.driver?.name ?? 'Someone'
    const lease = left === 0 ? ' · lease expired' : left != null ? ` · lease ${left}m left` : ''
    status = `${name} is driving${lease} · your message goes to ${name}'s queue`
  }

  return (
    <section aria-label="Steer" className={`${BAR} flex flex-col gap-2`}>
      {isDriver && state.suggestions.length > 0 && (
        <ul className="m-0 flex list-none flex-col gap-1.5 p-0">
          {state.suggestions.map((s) => (
            <li
              key={s.suggestion_id}
              className="flex items-start gap-2.5 rounded-md border border-border bg-card px-3 py-2"
            >
              <PersonAvatar actor={s.from} size={20} className="mt-0.5" />
              <div className="min-w-0 flex-1">
                <PersonName person={s.from} nameClassName="font-medium" />
                <p className="m-0 break-words text-muted-foreground">{s.text}</p>
              </div>
              <Button size="sm" onClick={() => actions.sendSuggestion(s.suggestion_id)}>
                Send to agent
              </Button>
            </li>
          ))}
        </ul>
      )}

      {isDriver && state.control_requests.length > 0 && (
        <ul className="m-0 flex list-none flex-col gap-1.5 p-0">
          {state.control_requests.map((r) => (
            <li
              key={r.from.id}
              className="flex items-center gap-2.5 rounded-md border border-border bg-card px-3 py-2"
            >
              <PersonAvatar actor={r.from} size={20} />
              <div className="min-w-0 flex-1">
                <PersonName person={r.from} nameClassName="font-medium" />
                <span className="text-muted-foreground"> wants control</span>
              </div>
              <Button size="sm" variant="secondary" onClick={() => actions.grantControl(r.from.id)}>
                Grant control
              </Button>
            </li>
          ))}
        </ul>
      )}

      <div className="flex flex-wrap items-center gap-x-2 gap-y-1">
        <StatusDot tone={noDriver ? 'warn' : 'accent'} />
        <span className="text-xs text-muted-foreground">{status}</span>
        <div className="ml-auto flex items-center gap-1">
          {isDriver && (
            <Button variant="ghost" className="h-7" aria-expanded={granting} onClick={() => setGranting((g) => !g)}>
              Give control to…
            </Button>
          )}
          {/* With no driver the banner (and the empty state) already offer it; an expired lease still shows it here. */}
          {take && !noDriver && (
            <Button variant="ghost" className="h-7" aria-keyshortcuts="T" onClick={() => actions.grantControl(user.id)}>
              Take control
              <Kbd aria-hidden="true">T</Kbd>
            </Button>
          )}
          {request && (
            <Button variant="ghost" className="h-7" aria-keyshortcuts="T" onClick={actions.requestControl}>
              Request control
              <Kbd aria-hidden="true">T</Kbd>
            </Button>
          )}
        </div>
      </div>

      {isDriver && (
        <GrantPicker user={user} open={granting} onClose={() => setGranting(false)} onGrant={actions.grantControl} />
      )}

      <form className="flex flex-col gap-2" onSubmit={submit}>
        <Textarea
          ref={ref}
          value={text}
          onChange={(e) => {
            setText(e.target.value)
            grow(e.currentTarget)
          }}
          onKeyDown={onKeyDown}
          placeholder={
            noDriver
              ? 'Take control to steer the agent'
              : isDriver
                ? 'Steer the agent…'
                : `Suggest a step for ${state.driver?.name ?? 'the driver'}…`
          }
          aria-label="Steer the agent"
          rows={1}
          disabled={noDriver}
        />
        {!noDriver && (
          <div className="flex items-center gap-2">
            <Button
              type="submit"
              variant={isDriver ? 'default' : 'secondary'}
              className="ml-auto"
              disabled={!text.trim()}
              aria-keyshortcuts={`${mod === '⌘' ? 'Meta' : 'Control'}+Enter`}
            >
              {isDriver ? 'Send to agent' : 'Suggest'}
              <Kbd aria-hidden="true">{mod}⏎</Kbd>
            </Button>
          </div>
        )}
      </form>
    </section>
  )
}

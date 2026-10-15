import { useEffect, useRef, useState } from 'react'
import type { FormEvent, KeyboardEvent } from 'react'
import { Eye, Lock } from 'lucide-react'
import type { RoomState, SuggestionState } from '../state/types'
import type { CurrentUser } from '../state/permissions'
import { canRequestControl, canSteer, canTakeControl, isViewer } from '../state/permissions'
import type { ConnState, RoomActions } from '../hooks/useRoom'
import { modKeyLabel } from '@/lib/envLabel'
import { planComposer, quickSteers, sendLabel } from '@/lib/composer'
import { clockTime } from '@/lib/time'
import { cn } from '@/lib/utils'
import { Badge } from '@/components/ui/badge'
import { Button } from '@/components/ui/button'
import { Input } from '@/components/ui/input'
import { Kbd } from '@/components/ui/kbd'
import { Textarea } from '@/components/ui/textarea'
import { PersonSelect } from './PersonSelect'
import { OfflineStrip } from './room/OfflineStrip'
import { PersonAvatar } from './room/PersonAvatar'
import { StatusDot } from './room/StatusDot'
import { leaseExpired, minutesLeft, useNow } from './room/useNow'

// The driver lease is 30 minutes (docs/api.md); the bar shows what is left of it, and from
// LEASE_WARN_MIN minutes on the status line turns amber and offers Renew.
const LEASE_MIN = 30
const LEASE_WARN_MIN = 5

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

// Draft and last-sent live in the browser only; a blocked store (private window) just means no draft.
const draftKey = (roomId: string) => `piperoom.draft.${roomId}`
const lastSentKey = (roomId: string) => `piperoom.lastsent.${roomId}`
function readStore(kind: 'local' | 'session', key: string): string {
  try {
    return (kind === 'local' ? localStorage : sessionStorage).getItem(key) ?? ''
  } catch {
    return ''
  }
}
function writeStore(kind: 'local' | 'session', key: string, value: string) {
  try {
    const st = kind === 'local' ? localStorage : sessionStorage
    if (value) st.setItem(key, value)
    else st.removeItem(key)
  } catch {
    /* storage blocked: nothing to keep */
  }
}

// The composer sticks to the bottom of the room column. Same field for everyone, but
// what it does depends on who you are: the driver steers the agent, anyone else leaves a
// suggestion in the driver's queue, a viewer reads. (Roles: docs/api.md, state/permissions.ts.)
const BAR = 'sticky bottom-[var(--sheet-h,0px)] z-10 mt-auto border-t border-border bg-background px-4 pb-4 pt-3 sm:px-6'

// Publishes the composer's height as --composer-h so <main> can keep scroll targets above it
// (AppShell scroll-padding-bottom). Cleared on unmount.
function useComposerHeight(el: HTMLElement | null) {
  useEffect(() => {
    if (!el) return
    const root = document.documentElement
    const write = () => root.style.setProperty('--composer-h', `${Math.round(el.getBoundingClientRect().height)}px`)
    write()
    if (typeof ResizeObserver === 'undefined') return () => root.style.removeProperty('--composer-h')
    const ro = new ResizeObserver(write)
    ro.observe(el)
    return () => {
      ro.disconnect()
      root.style.removeProperty('--composer-h')
    }
  }, [el])
}

// One pending suggestion in the driver's queue: send it on, or decline it with an optional note.
function QueueRow({
  s,
  onSend,
  onDecline,
}: {
  s: SuggestionState
  onSend: () => void
  onDecline: (reason?: string) => void
}) {
  const [declining, setDeclining] = useState(false)
  const [reason, setReason] = useState('')
  return (
    <li className="flex flex-col gap-1.5 rounded-md border border-border bg-card px-3 py-2">
      <div className="flex items-start gap-2.5">
        <PersonAvatar actor={s.from} size={20} className="mt-0.5" />
        <div className="min-w-0 flex-1">
          <strong className="font-medium">{s.from.name}</strong>
          <p className="m-0 break-words text-muted-foreground">{s.text}</p>
        </div>
        <Button size="sm" variant="ghost-danger" aria-expanded={declining} onClick={() => setDeclining((d) => !d)}>
          Decline
        </Button>
        <Button size="sm" onClick={onSend}>
          Send to agent
        </Button>
      </div>
      {declining && (
        <form
          className="flex items-center gap-2"
          onSubmit={(e) => {
            e.preventDefault()
            onDecline(reason.trim() || undefined)
          }}
        >
          <Input
            aria-label={`Reason for declining ${s.from.name}'s suggestion (optional)`}
            placeholder="Reason (optional) · Enter declines"
            maxLength={500}
            value={reason}
            onChange={(e) => setReason(e.target.value)}
            autoFocus
          />
        </form>
      )}
    </li>
  )
}

// The suggester's own newest suggestions and what became of each.
function MySuggestions({ state, user }: { state: RoomState; user: CurrentUser }) {
  const mine = state.suggestions.filter((s) => s.from.id === user.id).slice(-3)
  if (mine.length === 0) return null
  return (
    <ul className="m-0 flex list-none flex-wrap gap-1.5 p-0" aria-label="Your suggestions">
      {mine.map((s) => {
        let variant: 'warning' | 'success' | 'destructive' = 'warning'
        let label: string
        let title = s.text
        if (s.status === 'accepted') {
          variant = 'success'
          label = `Sent by ${s.sent_by?.name ?? 'the driver'}${s.sent_at ? ` · ${clockTime(s.sent_at)}` : ''}`
        } else if (s.status === 'declined') {
          variant = 'destructive'
          label = `Declined by ${s.declined_by?.name ?? 'the driver'}`
          if (s.reason) title = s.reason
        } else {
          label = state.driver ? `Queued for ${state.driver.name}` : 'Queued · no driver yet'
        }
        return (
          <li key={s.suggestion_id}>
            <Badge variant={variant} title={title}>
              {label}
            </Badge>
          </li>
        )
      })}
    </ul>
  )
}

export function SteerBox({
  state,
  user,
  actions,
  prefill,
  conn,
}: {
  state: RoomState
  user: CurrentUser
  actions: RoomActions
  conn?: ConnState
  /** Text to put in the field ("Ask the agent" on a checklist item); `n` makes the same text count again. */
  prefill?: { text: string; n: number }
}) {
  const [text, setText] = useState(() => readStore('local', draftKey(state.id)))
  const [sending, setSending] = useState(false)
  const [offline, setOffline] = useState(false)
  const [seenPrefill, setSeenPrefill] = useState(0)
  // Set during render (React's derived-state pattern), so the field is filled before it paints.
  if (prefill && prefill.n !== seenPrefill) {
    setSeenPrefill(prefill.n)
    setText(prefill.text)
  }
  const [granting, setGranting] = useState(false)
  const [focused, setFocused] = useState(false)
  const [section, setSection] = useState<HTMLElement | null>(null)
  useComposerHeight(section)
  const ref = useRef<HTMLTextAreaElement>(null)
  const now = useNow()
  const roomId = state.id
  // Debounced so typing is not a storage write per key; a send clears it at once (see send).
  useEffect(() => {
    const t = setTimeout(() => writeStore('local', draftKey(roomId), text), 300)
    return () => clearTimeout(t)
  }, [roomId, text])
  const isDriver = canSteer(state, user)
  // Sent suggestions stay in state for the suggestions panel; the queue shows only the waiting ones.
  const pending = state.suggestions.filter((s) => s.status === 'pending')
  const chips = quickSteers(state)

  if (isViewer(user)) {
    return (
      <section ref={setSection} aria-label="Steer" className={`${BAR} flex items-center gap-2 pb-3`}>
        <Eye className="size-3.5 shrink-0 text-subtle-foreground" aria-hidden="true" />
        <p className="m-0 text-muted-foreground">
          View only: you can read this room but not steer it. Ask a member to take control.
        </p>
      </section>
    )
  }

  if (state.status !== 'open') {
    return (
      <section ref={setSection} aria-label="Steer" className={`${BAR} flex items-center gap-2 pb-3`}>
        <Lock className="size-3.5 shrink-0 text-subtle-foreground" aria-hidden="true" />
        <p className="m-0 text-muted-foreground">This room is closed. Nothing more to steer.</p>
      </section>
    )
  }

  const noDriver = state.driver === null
  const take = !isDriver && canTakeControl(state, user)
  const request = !isDriver && !take && canRequestControl(state, user)
  const mod = modKeyLabel()
  const leaseIso = state.driver?.lease_until
  const left = minutesLeft(leaseIso, now)
  const expired = leaseExpired(leaseIso, now)
  // "under a minute" while 0 whole minutes remain but the lease is still live.
  const leaseText = expired ? 'lease expired' : left === 0 ? 'lease ends in under a minute' : left != null ? `lease ${left}m` : null

  const plan = planComposer(text, isDriver ? 'driver' : 'suggester', state.driver?.name ?? null, mod)
  // The field clears only once the server took the message; a failed send keeps the words
  // and puts the cursor back in the field.
  const send = async () => {
    // "?" alone has nothing to ask.
    if (!text.trim() || sending || !plan.text.replace(/^(Question:|@agent)\s*/, '').trim()) return
    setSending(true)
    const ok = plan.send === 'comment' ? await actions.comment(plan.text) : await actions.steer(plan.text)
    setSending(false)
    setOffline(!ok)
    if (ok) {
      writeStore('session', lastSentKey(roomId), text.trim())
      writeStore('local', draftKey(roomId), '')
      setText('')
      if (ref.current) ref.current.style.height = ''
    } else {
      ref.current?.focus()
    }
  }
  const submit = (e: FormEvent) => {
    e.preventDefault()
    void send()
  }
  const onKeyDown = (e: KeyboardEvent<HTMLTextAreaElement>) => {
    if (e.key === 'Enter' && (e.metaKey || e.ctrlKey) && !e.nativeEvent.isComposing) {
      e.preventDefault()
      void send()
    } else if (e.key === 'ArrowUp' && !text && !e.shiftKey && !e.metaKey && !e.ctrlKey && !e.altKey) {
      const last = readStore('session', lastSentKey(roomId))
      if (last) {
        e.preventDefault()
        setText(last)
      }
    }
  }
  const pick = (chip: string) => {
    setText(chip)
    ref.current?.focus()
  }
  // Grow with the text, up to about six lines.
  const grow = (el: HTMLTextAreaElement) => {
    el.style.height = 'auto'
    el.style.height = `${Math.min(el.scrollHeight, 140)}px`
  }

  // The server's lease signal (control.lease_expired) or, before it arrives, a clock that has run out
  // (minutesLeft is 0 only once the lease has run out; it rounds up while time remains).
  const signalled = state.driver?.lease_expired === true
  const isLeaseExpired = signalled || expired
  const leaseEnding = isDriver && !signalled && !expired && left != null && left <= LEASE_WARN_MIN
  let status: string
  if (isDriver) {
    status = signalled
      ? 'Your lease expired — you still drive until someone takes over'
      : leaseEnding && left != null && left > 0
        ? `Lease ends in ${left}m — any action renews`
        : leaseText
          ? `You are driving · ${leaseText}`
          : 'You are driving'
  } else if (noDriver) {
    status = 'No driver · your suggestion is queued for whoever takes control'
  } else {
    const name = state.driver?.name ?? 'Someone'
    const lease = leaseText ? ` · ${leaseText.replace(/^lease (\d+m)$/, 'lease $1 left')}` : ''
    status = signalled
      ? `${name}'s lease expired · anyone can take control`
      : `${name} is driving${lease} · your message goes to ${name}'s queue`
  }

  // Empty and not focused: one row (status, then the field as a placeholder). It grows on focus or text.
  const idle =
    !text && !focused && !granting && !(isDriver && (pending.length > 0 || state.control_requests.length > 0))

  return (
    <section
      ref={setSection}
      aria-label="Steer"
      data-idle={idle || undefined}
      className={cn(BAR, idle ? 'flex h-14 flex-row items-center gap-3 py-0' : 'flex flex-col gap-2')}
    >
      {isDriver && pending.length > 0 && (
        <ul className="m-0 flex list-none flex-col gap-1.5 p-0">
          {pending.map((s) => (
            <QueueRow
              key={s.suggestion_id}
              s={s}
              onSend={() => actions.sendSuggestion(s.suggestion_id)}
              onDecline={(reason) => actions.declineSuggestion(s.suggestion_id, reason)}
            />
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
                <strong className="font-medium">{r.from.name}</strong>
                <span className="text-muted-foreground"> wants control</span>
              </div>
              <Button size="sm" variant="secondary" onClick={() => actions.grantControl(r.from.id)}>
                Grant control
              </Button>
            </li>
          ))}
        </ul>
      )}

      {isDriver && left != null && (
        <div className="absolute inset-x-0 top-0 h-0.5 bg-muted" aria-hidden="true">
          <div
            className="h-full bg-primary"
            data-testid="lease-bar"
            style={{ width: `${Math.min(100, Math.round((left / LEASE_MIN) * 100))}%` }}
          />
        </div>
      )}

      <div className={cn('flex items-center gap-x-2 gap-y-1', idle ? 'min-w-0 max-w-[45%] shrink-0' : 'flex-wrap')}>
        <StatusDot tone={noDriver || isLeaseExpired ? 'warn' : 'accent'} />
        <span
          className={cn(
            'text-xs text-muted-foreground',
            (leaseEnding || (isDriver && isLeaseExpired)) && 'text-warning-foreground',
            idle && 'truncate',
          )}
          title={idle ? status : undefined}
          aria-live="polite"
        >
          {status}
        </span>
        {isDriver && (leaseEnding || isLeaseExpired) && (
          <Button variant="ghost" className="h-7" onClick={() => actions.grantControl(user.id)}>
            Renew
          </Button>
        )}
        <div className="ml-auto flex items-center gap-1">
          {isDriver && (
            <Button variant="ghost" className="h-7 coarse:h-11" aria-expanded={granting} onClick={() => setGranting((g) => !g)}>
              Give control to…
            </Button>
          )}
          {/* With no driver the banner (and the empty state) already offer it; an expired lease still shows it here. */}
          {take && !noDriver && (
            <Button variant="ghost" className="h-7 coarse:h-11" aria-keyshortcuts="T" onClick={() => actions.grantControl(user.id)}>
              Take control
              <Kbd aria-hidden="true">T</Kbd>
            </Button>
          )}
          {request && (
            <Button variant="ghost" className="h-7 coarse:h-11" aria-keyshortcuts="T" onClick={actions.requestControl}>
              Request control
              <Kbd aria-hidden="true">T</Kbd>
            </Button>
          )}
        </div>
      </div>

      {isDriver && (
        <GrantPicker user={user} open={granting} onClose={() => setGranting(false)} onGrant={actions.grantControl} />
      )}

      {!idle && !isDriver && <MySuggestions state={state} user={user} />}

      {!idle && chips.length > 0 && (
        <div className="flex flex-wrap gap-1.5 [@media(pointer:coarse)]:hidden">
          {chips.map((c) => (
            <Button key={c} type="button" variant="ghost" className="h-7" onClick={() => pick(c)}>
              {c}
            </Button>
          ))}
        </div>
      )}

      <form className={cn('flex gap-2', idle ? 'min-w-0 flex-1' : 'flex-col')} onSubmit={submit}>
        <Textarea
          ref={ref}
          value={text}
          onChange={(e) => {
            setText(e.target.value)
            grow(e.currentTarget)
          }}
          onKeyDown={onKeyDown}
          onFocus={() => setFocused(true)}
          onBlur={() => setFocused(false)}
          className={idle ? 'min-h-8 py-1' : undefined}
          placeholder={
            noDriver
              ? 'Suggest a first step…'
              : isDriver
                ? 'Steer the agent…'
                : `Suggest a step for ${state.driver?.name ?? 'the driver'}…`
          }
          aria-label="Steer the agent"
          rows={1}
        />
        {!idle && (
          <div className="flex items-center gap-2">
            <p className="m-0 min-w-0 text-xs text-subtle-foreground" aria-live="polite">
              {plan.hint}
            </p>
            <Button
              type="submit"
              variant={isDriver ? 'default' : 'secondary'}
              className="ml-auto"
              disabled={!text.trim() || sending}
              aria-keyshortcuts={`${mod === '⌘' ? 'Meta' : 'Control'}+Enter`}
            >
              {sending ? 'Sending…' : sendLabel(plan)}
              {!sending && <Kbd aria-hidden="true">{mod}⏎</Kbd>}
            </Button>
          </div>
        )}
      </form>
      {offline && <OfflineStrip onRetry={() => void send()} disabled={sending} />}
      {!offline && conn === 'reconnecting' && (
        <p className="m-0 text-xs text-muted-foreground">Reconnecting — messages are kept until the room is back</p>
      )}
    </section>
  )
}

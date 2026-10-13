// TEMP until P1 merges (spec section 6). These are minimal stand-ins for the P1
// primitives, with the same names and signatures, so P4 can build and test the room
// screen on its own. When ui.tsx exports Icon, hueFor, Avatar, StatusPill and
// Skeleton, delete this file and change the `./ui-p4-temp` imports to `./ui`.
import type { ReactNode } from 'react'
import type { Actor } from '../api/types'
import { agentStatusChip } from '../lib/tones'

export type IconName =
  | 'check'
  | 'x'
  | 'alert'
  | 'clock'
  | 'shield'
  | 'lock'
  | 'info'
  | 'send'
  | 'copy'
  | 'wheel'
  | 'back'
  | 'pipe'

const PATHS: Record<IconName, ReactNode> = {
  check: <path d="M5 12.5l4.5 4.5L19 7" />,
  x: <path d="M6 6l12 12M18 6L6 18" />,
  alert: (
    <>
      <path d="M12 3l10 18H2L12 3z" />
      <path d="M12 10v5M12 18v.5" />
    </>
  ),
  clock: (
    <>
      <circle cx="12" cy="12" r="9" />
      <path d="M12 7v5l3 2" />
    </>
  ),
  shield: (
    <>
      <path d="M12 3l7 3v6c0 4.5-3 7.5-7 9-4-1.5-7-4.5-7-9V6l7-3z" />
      <path d="M9 12l2 2 4-4" />
    </>
  ),
  lock: (
    <>
      <rect x="5" y="10" width="14" height="10" rx="2" />
      <path d="M8 10V7a4 4 0 0 1 8 0v3" />
    </>
  ),
  info: (
    <>
      <circle cx="12" cy="12" r="9" />
      <path d="M12 11v5M12 8v.5" />
    </>
  ),
  send: <path d="M4 12l16-8-6 16-2-6-8-2z" />,
  copy: (
    <>
      <rect x="9" y="9" width="11" height="11" rx="2" />
      <path d="M5 15V6a2 2 0 0 1 2-2h9" />
    </>
  ),
  wheel: (
    <>
      <circle cx="12" cy="12" r="8" />
      <circle cx="12" cy="12" r="2.2" fill="currentColor" stroke="none" />
      <path d="M12 4v5.5M4.5 14l5.5-1.5M19.5 14L14 12.5" />
    </>
  ),
  back: <path d="M15 6l-6 6 6 6" />,
  pipe: (
    <>
      <circle cx="4" cy="12" r="2.2" fill="currentColor" stroke="none" />
      <circle cx="12" cy="12" r="2.2" fill="currentColor" stroke="none" />
      <circle cx="20" cy="12" r="2.2" fill="currentColor" stroke="none" />
      <path d="M6.5 12h3M14.5 12h3" />
    </>
  ),
}

export function Icon({
  name,
  size = 16,
  className = '',
}: {
  name: IconName
  size?: number
  className?: string
}) {
  return (
    <svg
      className={`icon ${className}`.trim()}
      width={size}
      height={size}
      viewBox="0 0 24 24"
      fill="none"
      stroke="currentColor"
      strokeWidth="2"
      strokeLinecap="round"
      strokeLinejoin="round"
      aria-hidden="true"
      focusable="false"
    >
      {PATHS[name]}
    </svg>
  )
}

/** Stable 1..6 hue for a person id. */
// eslint-disable-next-line react-refresh/only-export-components -- the P1 contract exports hueFor from ui.tsx too
export function hueFor(id: string): 1 | 2 | 3 | 4 | 5 | 6 {
  let h = 0
  for (let i = 0; i < id.length; i++) h = (h * 31 + id.charCodeAt(i)) >>> 0
  return ((h % 6) + 1) as 1 | 2 | 3 | 4 | 5 | 6
}

export function Avatar({
  actor,
  size = 'md',
  driver = false,
}: {
  actor: Actor
  size?: 'xs' | 'sm' | 'md' | 'lg'
  driver?: boolean
}) {
  const initials = (actor.name || actor.id || '?').slice(0, 2).toUpperCase()
  const kind =
    actor.kind === 'agent'
      ? 'avatar--agent'
      : actor.kind === 'system'
        ? 'avatar--system'
        : `avatar--h${hueFor(actor.id || actor.name || '?')}`
  return (
    <span
      className={`avatar avatar--${size} ${kind}${driver ? ' avatar--driver' : ''}`}
      aria-hidden="true"
      title={driver ? `${actor.name} · driver` : actor.name}
    >
      {initials}
    </span>
  )
}

/**
 * Agent status in the topbar. The live region is always there; the status role is left
 * off while idle so a quiet room has no status region beside its banners.
 */
export function StatusPill({ state, detail }: { state: string; detail?: string }) {
  const { tone, label } = agentStatusChip(state, detail)
  const pillTone = state === 'working' ? 'agent' : state === 'idle' ? 'muted' : tone
  const live = state === 'working' || state === 'waiting_approval'
  return (
    <span
      className={`status-pill status-pill--${pillTone} chip chip--${tone}`}
      role={state === 'idle' ? undefined : 'status'}
      aria-live="polite"
    >
      <span className={`status-pill__dot${live ? ' is-live' : ''}`} aria-hidden="true" />
      {label}
    </span>
  )
}

export function Skeleton({
  h = 14,
  w = '100%',
  className = '',
}: {
  h?: number
  w?: string | number
  className?: string
}) {
  return (
    <span
      className={`skeleton ${className}`.trim()}
      style={{ display: 'block', height: h, width: w }}
      aria-hidden="true"
    />
  )
}

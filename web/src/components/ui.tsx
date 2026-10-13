/* eslint-disable react-refresh/only-export-components -- hueFor and the types sit with the primitives they belong to */
import type { ReactNode } from 'react'
import type { Actor } from '../api/types'
import { agentStatusChip } from '../lib/tones'

// Shared UI primitives (docs: reviews/ui-refresh.md section 6). Other
// components build on these signatures, so change them only with care.

export type Tone = 'fail' | 'warn' | 'ok' | 'info' | 'agent' | 'muted'

// ---- icons: an inline SVG sprite, currentColor, no icon library ----

export type IconName =
  | 'check'
  | 'x'
  | 'alert'
  | 'clock'
  | 'shield'
  | 'lock'
  | 'cloud-off'
  | 'info'
  | 'send'
  | 'copy'
  | 'wheel'
  | 'back'
  | 'chevron-right'
  | 'pipe'
  | 'fingerprint'
  | 'sun'
  | 'moon'

const ICONS: Record<IconName, ReactNode> = {
  check: <path d="M20 6 9 17l-5-5" />,
  x: <path d="M18 6 6 18M6 6l12 12" />,
  alert: (
    <>
      <path d="M10.29 3.86 1.82 18a2 2 0 0 0 1.71 3h16.94a2 2 0 0 0 1.71-3L13.71 3.86a2 2 0 0 0-3.42 0z" />
      <path d="M12 9v4M12 17h.01" />
    </>
  ),
  clock: (
    <>
      <circle cx="12" cy="12" r="10" />
      <path d="M12 6v6l4 2" />
    </>
  ),
  shield: <path d="M12 22s8-4 8-10V5l-8-3-8 3v7c0 6 8 10 8 10z" />,
  lock: (
    <>
      <rect x="3" y="11" width="18" height="11" rx="2" />
      <path d="M7 11V7a5 5 0 0 1 10 0v4" />
    </>
  ),
  'cloud-off': (
    <>
      <path d="m2 2 20 20" />
      <path d="M5.78 5.78A7 7 0 0 0 9 19h8.5a4.5 4.5 0 0 0 1.31-.19" />
      <path d="M21.53 16.5A4.5 4.5 0 0 0 17.5 10h-1.79A7 7 0 0 0 10 5.07" />
    </>
  ),
  info: (
    <>
      <circle cx="12" cy="12" r="10" />
      <path d="M12 16v-4M12 8h.01" />
    </>
  ),
  send: <path d="m22 2-7 20-4-9-9-4zM22 2 11 13" />,
  copy: (
    <>
      <rect x="9" y="9" width="13" height="13" rx="2" />
      <path d="M5 15H4a2 2 0 0 1-2-2V4a2 2 0 0 1 2-2h9a2 2 0 0 1 2 2v1" />
    </>
  ),
  wheel: (
    <>
      <circle cx="12" cy="12" r="9" />
      <circle cx="12" cy="12" r="2.5" />
      <path d="M12 3v6.5M3.6 16.5l5.9-3.4M20.4 16.5l-5.9-3.4" />
    </>
  ),
  back: <path d="m12 19-7-7 7-7M19 12H5" />,
  'chevron-right': <path d="m9 18 6-6-6-6" />,
  pipe: (
    <>
      <circle cx="5" cy="12" r="2" />
      <circle cx="12" cy="12" r="2" />
      <circle cx="19" cy="12" r="2" />
      <path d="M7 12h3M14 12h3" />
    </>
  ),
  fingerprint: (
    <>
      <path d="M12 10a2 2 0 0 0-2 2c0 1.02-.1 2.51-.26 4" />
      <path d="M14 13.12c0 2.38 0 6.38-1 8.88" />
      <path d="M17.29 21.02c.12-.6.43-2.3.5-3.02" />
      <path d="M2 12a10 10 0 0 1 18-6" />
      <path d="M2 16h.01" />
      <path d="M21.8 16c.2-2 .13-5.35 0-6" />
      <path d="M5 19.5C5.5 18 6 15 6 12a6 6 0 0 1 .34-2" />
      <path d="M8.65 22c.21-.66.45-1.32.57-2" />
      <path d="M9 6.8a6 6 0 0 1 9 5.2v2" />
    </>
  ),
  sun: (
    <>
      <circle cx="12" cy="12" r="4" />
      <path d="M12 2v2M12 20v2M4.93 4.93l1.41 1.41M17.66 17.66l1.41 1.41M2 12h2M20 12h2M6.34 17.66l-1.41 1.41M19.07 4.93l-1.41 1.41" />
    </>
  ),
  moon: <path d="M12 3a6 6 0 0 0 9 9 9 9 0 1 1-9-9z" />,
}

export function Icon({
  name,
  size = 16,
  className,
}: {
  name: IconName
  size?: number
  className?: string
}) {
  return (
    <svg
      className={className ? `icon ${className}` : 'icon'}
      width={size}
      height={size}
      style={size === 16 ? undefined : { width: size, height: size }}
      viewBox="0 0 24 24"
      aria-hidden="true"
      focusable="false"
    >
      {ICONS[name]}
    </svg>
  )
}

// ---- avatar ----

/** A stable colour slot (1 to 6) for a person, from their id. */
export function hueFor(id: string): 1 | 2 | 3 | 4 | 5 | 6 {
  let h = 0
  for (let i = 0; i < id.length; i++) h = (Math.imul(h, 31) + id.charCodeAt(i)) >>> 0
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
      title={actor.name}
    >
      {initials}
      {driver && (
        <span className="avatar__badge" title="driver">
          <Icon name="wheel" size={8} />
        </span>
      )}
    </span>
  )
}

// ---- chip, kicker, layer tag ----

export function Chip({
  tone,
  children,
  title,
  icon,
}: {
  tone: Tone | string
  children: ReactNode
  title?: string
  icon?: IconName
}) {
  return (
    <span className={`chip chip--${tone}`} title={title}>
      {icon && <Icon name={icon} size={12} />}
      {children}
    </span>
  )
}

/** The only uppercase text in the product (12px). */
export function Kicker({ children }: { children: ReactNode }) {
  return <span className="kicker">{children}</span>
}

export function LayerTag({ layer }: { layer: string }) {
  return <span className={`layer-tag layer-tag--${layer}`}>{layer}</span>
}

// ---- panel, empty state, skeleton ----

export function Panel({
  title,
  count,
  right,
  children,
  className = '',
  as: Tag = 'section',
}: {
  title?: ReactNode
  count?: ReactNode
  right?: ReactNode
  children: ReactNode
  className?: string
  as?: 'section' | 'div' | 'aside' | 'article'
}) {
  return (
    <Tag className={`panel ${className}`.trim()}>
      {(title || right) && (
        <header className="panel__head">
          <h2 className="panel__title">
            {title}
            {count !== undefined && count !== null && <span className="panel__count">{count}</span>}
          </h2>
          {right && <div className="panel__right">{right}</div>}
        </header>
      )}
      <div className="panel__body">{children}</div>
    </Tag>
  )
}

/** With only children this is the old one-line `<p class="empty">`. */
export function Empty({
  icon,
  title,
  children,
}: {
  icon?: IconName
  title?: ReactNode
  children?: ReactNode
}) {
  if (!icon && !title) return <p className="empty">{children}</p>
  return (
    <div className="empty">
      {icon && <Icon name={icon} size={22} />}
      {title && <div className="empty__title">{title}</div>}
      {children && <div className="empty__hint">{children}</div>}
    </div>
  )
}

export function Skeleton({
  h = 14,
  w = '100%',
  className,
}: {
  h?: number
  w?: string | number
  className?: string
}) {
  return (
    <span
      className={className ? `skeleton ${className}` : 'skeleton'}
      style={{ height: h, width: w }}
      aria-hidden="true"
    />
  )
}

// ---- status pill ----

/** The agent's state in one pill. Announces changes politely. */
export function StatusPill({ state, detail }: { state: string; detail?: string }) {
  const chip = agentStatusChip(state, detail)
  // The working agent gets its own hue; other states use the chip tone.
  const tone = state === 'working' ? 'agent' : chip.tone
  const live = state === 'working' || state === 'waiting_approval'
  return (
    <span className={`status-pill status-pill--${tone}`} role="status" aria-live="polite">
      {state === 'error' ? (
        <Icon name="x" size={12} />
      ) : state === 'done' ? (
        <Icon name="check" size={12} />
      ) : (
        <span className={`status-pill__dot${live ? ' is-live' : ''}`} aria-hidden="true" />
      )}
      {chip.label}
    </span>
  )
}

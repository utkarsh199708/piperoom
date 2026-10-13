// TEMP until P1 (ui.tsx) lands: minimal stand-ins for the spec §6 primitives that this
// package needs and the current ui.tsx does not have yet (Icon, hue Avatar, Kicker,
// Skeleton, LayerTag, Empty with icon). Signatures match §6 exactly.
// At rebase: delete this file and import these from './ui' instead.
import type { ReactNode } from 'react'
import type { Actor } from '../api/types'

export type IconName = 'check' | 'alert' | 'chevron-right' | 'pipe' | 'info'

const PATHS: Record<IconName, ReactNode> = {
  check: <path d="M5 12.5l4.5 4.5L19 7.5" />,
  alert: (
    <>
      <path d="M12 3.5l9.5 16.5h-19z" />
      <path d="M12 10v4.5M12 17.5v.01" />
    </>
  ),
  'chevron-right': <path d="M9.5 6l6 6-6 6" />,
  pipe: (
    <>
      <rect x="2.5" y="8" width="5" height="8" rx="1.5" />
      <rect x="16.5" y="8" width="5" height="8" rx="1.5" />
      <path d="M7.5 12h9" />
    </>
  ),
  info: (
    <>
      <circle cx="12" cy="12" r="9" />
      <path d="M12 11v5M12 8v.01" />
    </>
  ),
}

export function Icon({ name, size = 16, className }: { name: IconName; size?: number; className?: string }) {
  return (
    <svg
      className={`icon${className ? ` ${className}` : ''}`}
      width={size}
      height={size}
      viewBox="0 0 24 24"
      aria-hidden="true"
      focusable="false"
    >
      {PATHS[name]}
    </svg>
  )
}

// Stable hash of the id, 1..6. Not exported: P1 exports the real hueFor.
function hueFor(id: string): 1 | 2 | 3 | 4 | 5 | 6 {
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
    actor.kind === 'agent' ? 'avatar--agent' : actor.kind === 'system' ? 'avatar--system' : `avatar--h${hueFor(actor.id)}`
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

export function Kicker({ children }: { children: ReactNode }) {
  return <span className="kicker">{children}</span>
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
      className={`skeleton${className ? ` ${className}` : ''}`}
      style={{ display: 'block', height: h, width: w }}
      aria-hidden="true"
    />
  )
}

export function LayerTag({ layer }: { layer: string }) {
  return <span className={`layer-tag layer-tag--${layer}`}>{layer}</span>
}

export function Empty({ icon, title, children }: { icon?: IconName; title?: ReactNode; children?: ReactNode }) {
  if (!icon && !title) return <p className="empty">{children}</p>
  return (
    <div className="empty">
      {icon && <Icon name={icon} size={22} />}
      {title && <b>{title}</b>}
      {children && <span>{children}</span>}
    </div>
  )
}

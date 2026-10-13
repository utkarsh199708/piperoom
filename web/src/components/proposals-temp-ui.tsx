// TEMP until P1 (ui/p1-foundation) merges.
//
// Minimal stand-ins for the spec section 6 primitives that ProposalsPanel needs, with the
// signatures the spec gives. When P1 lands, delete this file and import `Icon`,
// `LayerTag` and `Empty` from './ui' in ProposalsPanel.tsx instead.
import type { ReactNode } from 'react'

export type IconName =
  | 'check'
  | 'x'
  | 'alert'
  | 'shield'
  | 'lock'
  | 'copy'
  | 'back'
  | 'fingerprint'

const PATHS: Record<IconName, ReactNode> = {
  check: <path d="M5 12.5l4.5 4.5L19 7" />,
  x: <path d="M6 6l12 12M18 6L6 18" />,
  alert: (
    <>
      <path d="M12 3l10 18H2L12 3z" />
      <path d="M12 10v5M12 18v.5" />
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
  copy: (
    <>
      <rect x="9" y="9" width="11" height="11" rx="2" />
      <path d="M5 15V6a2 2 0 0 1 2-2h9" />
    </>
  ),
  back: <path d="M9 14L4 9l5-5M4 9h10a6 6 0 0 1 0 12h-3" />,
  fingerprint: <path d="M6 11a6 6 0 0 1 12 0v2M9 11a3 3 0 0 1 6 0v5M12 11v8M4 8a9 9 0 0 1 16 0" />,
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

export function LayerTag({ layer }: { layer: string }) {
  return <span className={`layer-tag layer-tag--${layer}`}>{layer}</span>
}

export function Empty({ icon, title, children }: { icon?: IconName; title?: ReactNode; children?: ReactNode }) {
  if (!icon && !title) return <p className="empty">{children}</p>
  return (
    <div className="empty empty--block">
      {icon && <Icon name={icon} size={22} />}
      {title && <div className="empty__title">{title}</div>}
      {children && <div className="empty__help">{children}</div>}
    </div>
  )
}

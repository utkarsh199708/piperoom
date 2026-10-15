import type { ReactNode } from 'react'
import type { Actor } from '../api/types'
import { initials as peopleInitials } from '../lib/people'
import { neutralizeBadgeText } from '../lib/verification'

export function Avatar({ actor, size = 'md' }: { actor: Actor; size?: 'sm' | 'md' | 'lg' }) {
  const initials = peopleInitials(actor.name, actor.id)
  const kind = actor.kind === 'agent' ? 'avatar--agent' : actor.kind === 'system' ? 'avatar--system' : ''
  return (
    <span className={`avatar avatar--${size} ${kind}`} aria-hidden="true" title={neutralizeBadgeText(actor.name)}>
      {initials}
    </span>
  )
}

export function Chip({
  tone,
  children,
  title,
}: {
  tone: string
  children: ReactNode
  title?: string
}) {
  return (
    <span className={`chip chip--${tone}`} title={title}>
      {children}
    </span>
  )
}

export function Panel({
  title,
  right,
  children,
  className = '',
}: {
  title?: ReactNode
  right?: ReactNode
  children: ReactNode
  className?: string
}) {
  return (
    <section className={`panel ${className}`}>
      {(title || right) && (
        <header className="panel__head">
          <h2 className="panel__title">{title}</h2>
          {right && <div className="panel__right">{right}</div>}
        </header>
      )}
      <div className="panel__body">{children}</div>
    </section>
  )
}

export function Empty({ children }: { children: ReactNode }) {
  return <p className="empty">{children}</p>
}

import { Sparkles } from 'lucide-react'
import { Avatar, AvatarFallback } from '@/components/ui/avatar'
import { initials, personColor } from '@/lib/people'
import { neutralizeBadgeText } from '@/lib/verification'
import { cn } from '@/lib/utils'
import type { Actor } from '@/api/types'

const SIZE = {
  20: 'size-5 text-[9px]',
  24: 'size-6 text-[10px]',
  28: 'size-7 text-[11px]',
} as const

/**
 * One person (hue from their id, white initials), the agent (violet, sparkle) or a
 * system actor (muted). `driver` draws the ring. Decorative unless `label` is given:
 * the name is nearly always printed next to it.
 */
export function PersonAvatar({
  actor,
  size = 24,
  driver = false,
  stacked = false,
  label,
  className,
}: {
  actor: Pick<Actor, 'id' | 'name'> & { kind?: Actor['kind'] }
  size?: keyof typeof SIZE
  driver?: boolean
  /** Overlapping avatar group: a background-coloured edge separates the circles. */
  stacked?: boolean
  label?: string
  className?: string
}) {
  const kind = actor.kind ?? 'human'
  const a11y = label ? { role: 'img', 'aria-label': label } : { 'aria-hidden': true as const }
  return (
    <Avatar
      className={cn(
        SIZE[size],
        driver && 'ring-2 ring-primary ring-offset-2 ring-offset-background',
        stacked && 'border-2 border-background',
        className,
      )}
      title={neutralizeBadgeText(label ?? actor.name)}
      {...a11y}
    >
      <AvatarFallback
        className={cn(
          'text-[length:inherit]',
          kind === 'agent' && 'bg-agent',
          kind === 'system' && 'bg-muted text-subtle-foreground',
        )}
        style={kind === 'human' ? { backgroundColor: personColor(actor.id) } : undefined}
      >
        {kind === 'agent' ? (
          <Sparkles className={size === 20 ? 'size-3' : 'size-3.5'} aria-hidden="true" />
        ) : (
          initials(actor.name, actor.id)
        )}
      </AvatarFallback>
    </Avatar>
  )
}

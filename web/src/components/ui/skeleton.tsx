import * as React from 'react'
import { cn } from '@/lib/utils'

// rounded-sm bg-muted. The shimmer only runs under prefers-reduced-motion: no-preference.
function Skeleton({ className, ...props }: React.ComponentProps<'div'>) {
  return <div data-slot="skeleton" aria-hidden="true" className={cn('rounded-sm bg-muted motion-safe:animate-shimmer', className)} {...props} />
}

export { Skeleton }

import * as React from 'react'
import * as ProgressPrimitive from '@radix-ui/react-progress'
import { cn } from '@/lib/utils'

// Radix has no indeterminate animation, so without a `value` the indicator is a
// child div with a CSS keyframe (tailwind.config.ts: animate-indeterminate).
// Under prefers-reduced-motion it stays a still bar.
function Progress({
  className,
  value,
  indicatorClassName,
  ...props
}: React.ComponentProps<typeof ProgressPrimitive.Root> & { indicatorClassName?: string }) {
  const indeterminate = value === undefined || value === null
  return (
    <ProgressPrimitive.Root
      data-slot="progress"
      value={indeterminate ? null : value}
      className={cn('relative h-1.5 w-full overflow-hidden rounded-full bg-muted', className)}
      {...props}
    >
      <ProgressPrimitive.Indicator
        data-slot="progress-indicator"
        className={cn(
          'h-full rounded-full bg-primary transition-transform',
          indeterminate && 'w-2/5 motion-safe:animate-indeterminate motion-reduce:w-full',
          indicatorClassName,
        )}
        style={indeterminate ? undefined : { transform: `translateX(-${100 - (value ?? 0)}%)`, width: '100%' }}
      />
    </ProgressPrimitive.Root>
  )
}

export { Progress }

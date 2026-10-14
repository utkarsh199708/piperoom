import * as React from 'react'
import { cn } from '@/lib/utils'

// h-8. The solo confirm input is font-mono with paste blocked (callers add both).
function Input({ className, type, ...props }: React.ComponentProps<'input'>) {
  return (
    <input
      type={type}
      data-slot="input"
      className={cn(
        'flex h-8 min-h-0 w-full min-w-0 rounded-md border border-input-border bg-background px-2.5 py-0 text-sm text-foreground',
        'placeholder:text-subtle-foreground file:border-0 file:bg-transparent file:text-sm file:font-medium',
        'focus-visible:border-ring focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring/30',
        'disabled:cursor-not-allowed disabled:opacity-50 aria-invalid:border-destructive',
        className,
      )}
      {...props}
    />
  )
}

export { Input }

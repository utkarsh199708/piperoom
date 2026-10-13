import * as React from 'react'
import { cn } from '@/lib/utils'

// Local component (shadcn has none). A key cap: every action shows its key.
function Kbd({ className, ...props }: React.ComponentProps<'kbd'>) {
  return (
    <kbd
      data-slot="kbd"
      className={cn(
        'inline-flex h-[18px] min-w-[18px] items-center justify-center rounded-sm border border-input bg-muted px-1 font-mono text-[11px] font-medium text-subtle-foreground',
        className,
      )}
      {...props}
    />
  )
}

export { Kbd }

import * as React from 'react'
import { cn } from '@/lib/utils'

// The composer: bg-muted, no border, inside a border-t bg-background div (no nested card).
function Textarea({ className, ...props }: React.ComponentProps<'textarea'>) {
  return (
    <textarea
      data-slot="textarea"
      className={cn(
        'flex min-h-10 w-full resize-none rounded-md border-0 bg-muted px-3 py-2.5 text-sm text-foreground',
        'placeholder:text-subtle-foreground focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring',
        'disabled:cursor-not-allowed disabled:opacity-50',
        className,
      )}
      {...props}
    />
  )
}

export { Textarea }

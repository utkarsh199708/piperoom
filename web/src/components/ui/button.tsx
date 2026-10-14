import * as React from 'react'
import { Slot } from '@radix-ui/react-slot'
import { cva } from 'class-variance-authority'
import type { VariantProps } from 'class-variance-authority'
import { cn } from '@/lib/utils'

// Variants and sizes from docs/design/ui-v2 (Handoff.dc.html): default, secondary,
// outline, ghost, plus success and ghost-danger. Pass <Kbd> as a child; inside the
// filled variants it turns into a translucent white chip.
const buttonVariants = cva(
  [
    'inline-flex shrink-0 items-center justify-center gap-1.5 whitespace-nowrap rounded-md border border-transparent',
    'text-sm font-medium transition-colors',
    'focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring focus-visible:ring-offset-2 focus-visible:ring-offset-background',
    'disabled:pointer-events-none disabled:opacity-50',
    '[&_svg]:pointer-events-none [&_svg]:size-4 [&_svg]:shrink-0',
    // Phones: every button is at least a 44px touch target (WCAG 2.5.5), whatever height a
    // screen gives it. min-height wins over the h-* a caller sets, so no screen has to opt in.
    'max-[599px]:min-h-11',
  ],
  {
    variants: {
      variant: {
        default:
          'bg-primary text-primary-foreground hover:bg-primary/90 [&_kbd]:border-transparent [&_kbd]:bg-white/20 [&_kbd]:text-inherit',
        secondary: 'border-border bg-secondary text-secondary-foreground hover:bg-accent',
        outline: 'border-input bg-transparent text-foreground hover:bg-accent',
        ghost: 'text-muted-foreground hover:bg-accent hover:text-foreground',
        success:
          'bg-success-solid text-white hover:bg-success-solid/90 [&_kbd]:border-transparent [&_kbd]:bg-white/20 [&_kbd]:text-inherit',
        'ghost-danger': 'text-destructive-fg hover:bg-destructive-subtle',
        destructive:
          'bg-destructive text-destructive-foreground hover:bg-destructive/90 [&_kbd]:border-transparent [&_kbd]:bg-white/20 [&_kbd]:text-inherit',
        link: 'h-auto px-0 text-primary-fg underline-offset-4 hover:underline',
      },
      size: {
        sm: 'h-8 px-2.5',
        default: 'h-9 px-3',
        icon: 'h-8 w-8 border-0 p-0 max-[599px]:min-w-11',
      },
    },
    defaultVariants: { variant: 'default', size: 'default' },
  },
)

type ButtonProps = React.ComponentProps<'button'> &
  VariantProps<typeof buttonVariants> & {
    asChild?: boolean
  }

function Button({ className, variant, size, asChild = false, type, ...props }: ButtonProps) {
  const Comp = asChild ? Slot : 'button'
  return (
    <Comp
      data-slot="button"
      // A <button> inside a form submits it unless told otherwise; default to the safe choice.
      {...(asChild ? {} : { type: type ?? 'button' })}
      className={cn(buttonVariants({ variant, size }), className)}
      {...props}
    />
  )
}

export { Button, buttonVariants }
export type { ButtonProps }

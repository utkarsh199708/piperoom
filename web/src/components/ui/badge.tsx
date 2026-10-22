import * as React from 'react'
import { cva } from 'class-variance-authority'
import type { VariantProps } from 'class-variance-authority'
import { cn } from '@/lib/utils'

// h-5 px-1.5 rounded-md text-xs font-medium. Never fully rounded; the pill form is
// <Badge variant="outline" className="rounded-full"> and only when it carries a dot.
const badgeVariants = cva(
  'inline-flex h-5 shrink-0 items-center gap-1.5 whitespace-nowrap rounded-md border border-transparent px-1.5 text-xs font-medium',
  {
    variants: {
      variant: {
        neutral: 'bg-muted text-muted-foreground',
        sev1: 'bg-destructive-subtle text-destructive-fg',
        sev2: 'bg-warning-subtle text-warning-foreground',
        sev3: 'bg-primary-subtle text-primary-fg',
        sev4: 'bg-success-subtle text-success-foreground',
        success: 'bg-success-subtle text-success-foreground',
        warning: 'bg-warning-subtle text-warning-foreground',
        destructive: 'bg-destructive-subtle text-destructive-fg',
        info: 'bg-info-subtle text-info-foreground',
        accent: 'bg-primary-subtle text-primary-fg',
        agent: 'bg-agent-subtle text-agent-foreground',
        outline: 'border-input bg-transparent text-muted-foreground',
        'layer-postgres': 'bg-layer-postgres/[.12] text-layer-postgres',
        'layer-kafka-connect': 'bg-layer-kafka-connect/[.12] text-layer-kafka-connect',
        'layer-schema-registry': 'bg-layer-schema-registry/[.12] text-layer-schema-registry',
        'layer-kafka': 'bg-layer-kafka/[.12] text-layer-kafka',
        'layer-consumer': 'bg-layer-consumer/[.12] text-layer-consumer',
        'layer-warehouse': 'bg-layer-warehouse/[.12] text-layer-warehouse',
        'layer-orchestrator': 'bg-layer-orchestrator/[.12] text-layer-orchestrator',
        'layer-transform': 'bg-layer-transform/[.12] text-layer-transform',
      },
      mono: {
        true: 'font-mono text-[11.5px]',
        false: '',
      },
    },
    defaultVariants: { variant: 'neutral', mono: false },
  },
)

type BadgeVariant = NonNullable<VariantProps<typeof badgeVariants>['variant']>

function Badge({
  className,
  variant,
  mono,
  ...props
}: React.ComponentProps<'span'> & VariantProps<typeof badgeVariants>) {
  return <span data-slot="badge" className={cn(badgeVariants({ variant, mono }), className)} {...props} />
}

/** Badge variant per pipeline layer name. Keys must stay in sync with the `--color-layer-*` entries in src/index.css. */
export const LAYER_VARIANTS: Record<string, BadgeVariant> = {
  postgres: 'layer-postgres',
  'kafka-connect': 'layer-kafka-connect',
  'schema-registry': 'layer-schema-registry',
  kafka: 'layer-kafka',
  consumer: 'layer-consumer',
  warehouse: 'layer-warehouse',
  orchestrator: 'layer-orchestrator',
  transform: 'layer-transform',
}

/** The badge variant for a pipeline layer name; unknown layers get the neutral badge. */
function layerVariant(layer: string): BadgeVariant {
  return Object.hasOwn(LAYER_VARIANTS, layer) ? LAYER_VARIANTS[layer] : 'neutral'
}

export { Badge, badgeVariants, layerVariant }

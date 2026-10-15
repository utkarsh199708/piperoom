import { ShieldAlert, ShieldCheck } from 'lucide-react'
import { Tooltip, TooltipContent, TooltipProvider, TooltipTrigger } from '@/components/ui/tooltip'
import {
  allVerified,
  neutralizeBadgeText,
  personVerification,
  type PersonFacts,
  type Verification,
} from '@/lib/verification'
import { cn } from '@/lib/utils'

/**
 * The verified / unverified badge. It is drawn (an SVG shield inside a bordered
 * pill), never a character, so no name, comment or alert text can produce one.
 * Callers pass a Verification built from server-set fields only (lib/verification.ts).
 */
export function VerifiedBadge({ verification, className }: { verification: Verification; className?: string }) {
  const ok = verification.state === 'verified'
  const Icon = ok ? ShieldCheck : ShieldAlert
  const word = ok ? 'verified' : 'unverified'
  return (
    // Own provider: the badge also renders outside AppShell (tests, dialogs in portals).
    <TooltipProvider>
      <Tooltip>
        <TooltipTrigger asChild>
          <span
            data-slot="verified-badge"
            data-verification={verification.state}
            tabIndex={0}
            className={cn(
              'inline-flex h-5 shrink-0 select-none items-center gap-1 rounded-full border px-1.5 text-[11px] font-semibold leading-none',
              'outline-none focus-visible:ring-2 focus-visible:ring-inset focus-visible:ring-ring',
              ok
                ? 'border-success bg-success-subtle text-success-foreground'
                : 'border-dashed border-warning bg-warning-subtle text-warning-foreground',
              className,
            )}
          >
            <Icon className="size-3.5 shrink-0" aria-hidden="true" strokeWidth={2.5} />
            <span aria-hidden="true">{word}</span>
            <span className="sr-only">
              {word}: {verification.detail}
            </span>
          </span>
        </TooltipTrigger>
        <TooltipContent>{verification.detail}</TooltipContent>
      </Tooltip>
    </TooltipProvider>
  )
}

/**
 * A person's name with their badge. The name is untrusted (an IdP user sets
 * their own), so it is shown as plain text with badge look-alikes neutralized,
 * clipped to its own box, and always followed by the real badge. An unverified
 * person also gets their server id next to the name (docs/api.md "Verified people").
 */
export function PersonName({
  person,
  name,
  verified,
  className,
  nameClassName,
}: {
  person: PersonFacts & { name?: string }
  /** Display name override; defaults to person.name, then person.id. */
  name?: string
  /** Several server reports of the same person (event actor, presence): verified only if all agree. */
  verified?: unknown[]
  className?: string
  nameClassName?: string
}) {
  const facts: PersonFacts = verified
    ? { ...person, verified: allVerified(...verified) ? true : verified.includes(false) ? false : undefined }
    : person
  const v = personVerification(facts)
  const shown = neutralizeBadgeText(name ?? person.name ?? '') || person.id
  // The server may already have suffixed a look-alike name with "(<id>)" or "(<email>)" (docs/api.md);
  // either identifies the person, so no second suffix.
  const needsId =
    v.state === 'unverified' && shown !== person.id && !shown.endsWith(`(${person.id})`) && !/\([^()]*@[^()]*\)$/.test(shown)
  return (
    <span className={cn('inline-flex min-w-0 max-w-full items-center gap-1.5 overflow-hidden', className)} data-slot="person-name">
      <span className={cn('min-w-0 truncate', nameClassName)}>{shown}</span>
      {needsId && (
        <span className="shrink-0 font-mono text-xs font-normal text-subtle-foreground">({person.id})</span>
      )}
      <VerifiedBadge verification={v} />
    </span>
  )
}

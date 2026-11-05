import { useEffect, useMemo, useRef } from 'react'
import { personLabel, usePeople } from '../hooks/usePeople'
import { PersonName } from './VerifiedBadge'
import { neutralizeBadgeText } from '@/lib/verification'
import { Skeleton } from '@/components/ui/skeleton'

// Picker for "who gets control" and "who gets the hand-off". The list comes from
// GET /api/people in every mode. A viewer gets 403 viewer from it: no picker then.
export function PersonSelect({
  id,
  selfId,
  value,
  onChange,
  signInIds,
  noSignInText,
}: {
  id: string
  /** The current user is left out of the list. */
  selfId: string
  value: string
  onChange: (personId: string) => void
  /**
   * The people who can sign in on this server, when that is narrower than /api/people. Every
   * offered person must be able to accept what they are given (solo mode: only the solo user).
   * Undefined means no filter: in OIDC and plain dev every /api/people person can sign in.
   */
  signInIds?: ReadonlySet<string>
  /** Why the list is empty when the sign-in filter removed everyone (defaults to a plain sentence). */
  noSignInText?: string
}) {
  const people = usePeople()
  const list = people.status === 'ready' ? people.people : null
  const candidates = useMemo(() => (list ?? []).filter((p) => p.id !== selfId), [list, selfId])
  const options = useMemo(
    () => (signInIds ? candidates.filter((p) => signInIds.has(p.id)) : candidates),
    [candidates, signInIds],
  )

  // The person the list shows: the value while it is in the list, else the first person. Derived
  // during render, so the <select> and the badge never wait for an effect.
  const effective = options.some((p) => p.id === value) ? value : (options[0]?.id ?? '')
  // The person picked in the <select>, written before the parent hears of it. A default effect
  // from an earlier render can run after the pick (it still sees value ''), and must not undo it.
  const picked = useRef<string | null>(null)

  // Tell the parent about the default (empty or gone from the list -> the first person).
  useEffect(() => {
    if (list === null || effective === value) return
    if (picked.current !== null && options.some((p) => p.id === picked.current)) return
    onChange(effective)
  }, [list, options, effective, value, onChange])

  if (people.status === 'loading') {
    return (
      <div role="status" aria-label="Loading people" className="flex flex-col gap-1.5">
        <span className="sr-only">Loading people…</span>
        <Skeleton className="h-3 w-24" />
        <Skeleton className="h-9 w-full rounded-md" />
      </div>
    )
  }
  if (people.status === 'forbidden') {
    return <p className="m-0 text-sm text-muted-foreground">View only: you cannot give control or hand off.</p>
  }
  if (people.status === 'error') {
    return (
      <p role="alert" className="m-0 text-sm text-destructive-fg">
        Could not load people: {people.message}{' '}
        <button type="button" className="font-medium text-foreground underline underline-offset-2" onClick={people.reload}>
          Try again
        </button>
      </p>
    )
  }
  if (options.length === 0) {
    // Distinguish "there is nobody" from "everyone /api/people knows cannot sign in here".
    const filteredOut = candidates.length > 0
    const text =
      noSignInText ?? (filteredOut ? 'No other person can sign in on this server.' : 'No one else is available right now.')
    return <p className="m-0 text-sm text-muted-foreground">{text}</p>
  }
  const selected = options.find((p) => p.id === effective)
  return (
    <>
      {/* An <option> holds only text, so it can say "unverified" but cannot draw the badge;
          the drawn badge for the chosen person sits under the list. */}
      <select
        id={id}
        className="h-9 w-full rounded-md border border-input-border bg-background px-2 text-sm text-foreground focus-visible:border-ring focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring/30"
        value={effective}
        onChange={(e) => {
          picked.current = e.target.value
          onChange(e.target.value)
        }}
      >
        {options.map((p) => (
          <option key={p.id} value={p.id}>
            {neutralizeBadgeText(personLabel(p))}
            {p.verified === true ? '' : ' — unverified'}
          </option>
        ))}
      </select>
      {selected && (
        <p className="m-0 mt-1 text-sm" data-testid={`${id}-selected`}>
          <PersonName person={{ id: selected.id, name: selected.name, kind: 'human', verified: selected.verified }} />
        </p>
      )}
    </>
  )
}

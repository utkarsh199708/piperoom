import { useEffect, useMemo } from 'react'
import { personLabel, usePeople } from '../hooks/usePeople'
import { PersonName } from './VerifiedBadge'
import { neutralizeBadgeText } from '@/lib/verification'

// Picker for "who gets control" and "who gets the hand-off". The list comes from
// GET /api/people in every mode. A viewer gets 403 viewer from it: no picker then.
export function PersonSelect({
  id,
  selfId,
  value,
  onChange,
}: {
  id: string
  /** The current user is left out of the list. */
  selfId: string
  value: string
  onChange: (personId: string) => void
}) {
  const people = usePeople()
  const list = people.status === 'ready' ? people.people : null
  const options = useMemo(() => (list ?? []).filter((p) => p.id !== selfId), [list, selfId])

  // Keep the value valid: empty or gone from the list -> the first person.
  useEffect(() => {
    if (list === null) return
    if (!options.some((p) => p.id === value)) onChange(options[0]?.id ?? '')
  }, [list, options, value, onChange])

  if (people.status === 'loading') return <p className="muted">Loading people…</p>
  if (people.status === 'forbidden') {
    return <p className="muted">View only: you cannot give control or hand off.</p>
  }
  if (people.status === 'error') {
    return (
      <p className="error-text">
        Could not load people: {people.message}{' '}
        <button type="button" className="btn btn--link" onClick={people.reload}>
          Try again
        </button>
      </p>
    )
  }
  if (options.length === 0) return <p className="muted">No one else is available right now.</p>
  const selected = options.find((p) => p.id === value)
  return (
    <>
      {/* An <option> holds only text, so it can say "unverified" but cannot draw the badge;
          the drawn badge for the chosen person sits under the list. */}
      <select id={id} value={value} onChange={(e) => onChange(e.target.value)}>
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

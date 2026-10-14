import { useCallback, useEffect, useState } from 'react'
import type { Person } from '../api/types'
import { ApiClientError, api, isFixtures } from '../api/client'
import { DEV_USERS } from '../fixtures/users'
import { errorText } from '../lib/errors'

export type PeopleState =
  | { status: 'loading' }
  | { status: 'ready'; people: Person[] }
  // GET /api/people answers 403 viewer for a viewer: no picker at all.
  | { status: 'forbidden' }
  | { status: 'error'; message: string }

/**
 * The people a driver may grant control or hand off to (GET /api/people).
 * Used in dev mode too: the server answers in both modes. Fixture mode has no
 * server, so the built-in dev users stand in.
 */
export function usePeople(): PeopleState & { reload: () => void } {
  const [state, setState] = useState<PeopleState>({ status: 'loading' })
  const [tick, setTick] = useState(0)

  useEffect(() => {
    let cancelled = false
    if (isFixtures()) {
      setState({ status: 'ready', people: DEV_USERS.map((u) => ({ id: u.id, name: u.name })) })
      return
    }
    setState({ status: 'loading' })
    api
      .people()
      .then((people) => {
        if (!cancelled) setState({ status: 'ready', people: Array.isArray(people) ? people : [] })
      })
      .catch((e: unknown) => {
        if (cancelled) return
        if (e instanceof ApiClientError && e.status === 403 && e.code === 'viewer') {
          setState({ status: 'forbidden' })
        } else {
          setState({ status: 'error', message: errorText(e) })
        }
      })
    return () => {
      cancelled = true
    }
  }, [tick])

  const reload = useCallback(() => setTick((n) => n + 1), [])
  return { ...state, reload }
}

/** "Name <email>" when the server sent an email, else "Name (id)". */
export function personLabel(p: Person): string {
  return p.email ? `${p.name} <${p.email}>` : `${p.name} (${p.id})`
}

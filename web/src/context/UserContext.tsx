import { useEffect, useState } from 'react'
import type { ReactNode } from 'react'
import { useSearchParams } from 'react-router-dom'
import { DEFAULT_USER_ID, findUser } from '../dev/users'
import type { CurrentUser } from '../state/permissions'
import { api, isFixtures } from '../api/client'
import { UserContext } from './user'

export function UserProvider({ children }: { children: ReactNode }) {
  const [searchParams, setSearchParams] = useSearchParams()
  const rawId = searchParams.get('user')
  const id = rawId ?? DEFAULT_USER_ID
  const [user, setUser] = useState<CurrentUser | null>(null)
  const [error, setError] = useState<string | null>(null)

  useEffect(() => {
    let cancelled = false
    setError(null)

    if (isFixtures()) {
      // Fixture mode has no server; the client copy of dev/users.yaml stands in.
      const dev = findUser(id)
      if (!dev) {
        setError(`Unknown user: ${id}`)
        setUser(null)
      } else {
        setUser({ id: dev.id, name: dev.name, layers: dev.layers })
      }
      return () => {
        cancelled = true
      }
    }

    // Live mode: identity (and the layers the user may approve) comes from the
    // server, not the client copy. An unknown X-User yields 401 and surfaces here.
    void (async () => {
      try {
        const me = await api.me(id)
        if (cancelled) return
        setUser({ id: me.id, name: me.name, layers: me.layers })
      } catch (e) {
        if (cancelled) return
        setError(e instanceof Error ? e.message : String(e))
      }
    })()
    return () => {
      cancelled = true
    }
  }, [id])

  const setUserId = (next: string) => {
    setSearchParams((prev) => {
      const p = new URLSearchParams(prev)
      if (next) p.set('user', next)
      else p.delete('user')
      return p
    })
  }

  if (!user) {
    return (
      <div className="shell">
        <main className="roomlist">
          <p className="empty">{error ?? 'Loading user…'}</p>
        </main>
      </div>
    )
  }

  return <UserContext.Provider value={{ user, setUserId }}>{children}</UserContext.Provider>
}

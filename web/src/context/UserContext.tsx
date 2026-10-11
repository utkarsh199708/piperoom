import { useMemo } from 'react'
import type { ReactNode } from 'react'
import { useSearchParams } from 'react-router-dom'
import { findUser } from '../dev/users'
import type { CurrentUser } from '../state/permissions'
import { UserContext } from './user'

export function UserProvider({ children }: { children: ReactNode }) {
  const [searchParams, setSearchParams] = useSearchParams()
  const rawId = searchParams.get('user')
  const dev = findUser(rawId)

  const user = useMemo<CurrentUser>(
    () => ({ id: dev.id, name: dev.name, layers: dev.layers }),
    [dev],
  )

  const setUserId = (id: string) => {
    setSearchParams((prev) => {
      const next = new URLSearchParams(prev)
      if (id) next.set('user', id)
      else next.delete('user')
      return next
    })
  }

  return <UserContext.Provider value={{ user, setUserId }}>{children}</UserContext.Provider>
}

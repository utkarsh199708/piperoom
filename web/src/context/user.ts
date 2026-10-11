import { createContext, useContext } from 'react'
import type { CurrentUser } from '../state/permissions'

export interface UserContextValue {
  user: CurrentUser
  setUserId: (id: string) => void
}

export const UserContext = createContext<UserContextValue | null>(null)

export function useCurrentUser(): UserContextValue {
  const ctx = useContext(UserContext)
  if (!ctx) throw new Error('useCurrentUser must be used within UserProvider')
  return ctx
}

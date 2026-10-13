import { createContext, useContext } from 'react'
import type { CurrentUser } from '../state/permissions'

export interface UserContextValue {
  user: CurrentUser
  /** Dev mode: sign in as this users.yaml user (POST /auth/dev-login), then reload /api/me. */
  setUserId: (id: string) => void
  /** POST /auth/logout, then show the login screen (dev) or go to the IdP (oidc). */
  logout: () => void
  /** Bumps on every login or logout, so data hooks reload and the WebSocket reconnects. */
  sessionEpoch: number
}

export const UserContext = createContext<UserContextValue | null>(null)

export function useCurrentUser(): UserContextValue {
  const ctx = useContext(UserContext)
  if (!ctx) throw new Error('useCurrentUser must be used within UserProvider')
  return ctx
}

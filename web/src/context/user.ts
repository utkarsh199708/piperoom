import { createContext, useContext } from 'react'
import type { CurrentUser } from '../state/permissions'

export interface UserContextValue {
  user: CurrentUser
  /** Dev mode: sign in as this users.yaml user (POST /auth/dev-login), then reload /api/me. */
  setUserId: (id: string) => void
  /** POST /auth/logout, then show the sign-in choice for the server mode (never an automatic redirect). */
  logout: () => void
  /** A dev-login or logout is in flight: the user switcher and Log out button are disabled meanwhile. */
  busy?: boolean
  /** Bumps on every login or logout, so data hooks reload and the WebSocket reconnects. */
  sessionEpoch: number
}

export const UserContext = createContext<UserContextValue | null>(null)

export function useCurrentUser(): UserContextValue {
  const ctx = useContext(UserContext)
  if (!ctx) throw new Error('useCurrentUser must be used within UserProvider')
  return ctx
}

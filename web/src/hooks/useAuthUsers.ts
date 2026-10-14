import { api, isFixtures } from '../api/client'
import type { AuthUser } from '../api/types'
import { DEV_USERS } from '../fixtures/users'
import { useLoad } from './useLoad'
import type { LoadState } from './useLoad'

const fixtureUsers = () => Promise.resolve<AuthUser[]>(DEV_USERS.map(({ id, name, layers }) => ({ id, name, layers })))
const liveUsers = () => api.authUsers().then((u) => (Array.isArray(u) ? u : []))

/** The users the dev login screen and the dev switcher offer: GET /auth/users (fixtures: the fixture users). */
export function useAuthUsers(): LoadState<AuthUser[]> & { reload: () => void } {
  return useLoad(isFixtures() ? fixtureUsers : liveUsers)
}

import { isDev } from '../lib/env'
import { DEV_USERS } from '../dev/users'
import { useCurrentUser } from '../context/user'

export function UserSwitcher() {
  const { user, setUserId } = useCurrentUser()
  // The switcher lets a client impersonate any user (F1): it must never render
  // in a production build, where identity comes from the server's session.
  if (!isDev) return null
  return (
    <label className="user-switcher">
      <span className="user-switcher__label">You are</span>
      <select value={user.id} onChange={(e) => setUserId(e.target.value)} aria-label="Switch user">
        {DEV_USERS.map((u) => (
          <option key={u.id} value={u.id}>
            {u.name}
          </option>
        ))}
      </select>
    </label>
  )
}

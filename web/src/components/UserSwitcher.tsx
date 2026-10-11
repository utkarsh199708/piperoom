import { DEV_USERS } from '../dev/users'
import { useCurrentUser } from '../context/user'

export function UserSwitcher() {
  const { user, setUserId } = useCurrentUser()
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

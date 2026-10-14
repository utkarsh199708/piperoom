import { isDev } from '../lib/env'
import { isFixtures } from '../api/client'
import { DEV_USERS } from '../dev/users'
import { useCurrentUser } from '../context/user'
import { isViewer } from '../state/permissions'
import { useInShell } from './shell/context'

export function UserSwitcher() {
  const { user, setUserId, logout, busy } = useCurrentUser()
  // Inside the app shell the sidebar's account menu does all of this.
  const inShell = useInShell()
  if (inShell) return null
  return (
    <span className="user-switcher">
      {/* The switcher lets a client sign in as any user (F1): it must never
          render in a production build, where identity comes from the server's
          session. In dev, picking a user calls POST /auth/dev-login. */}
      {isDev ? (
        <label className="user-switcher">
          <span className="user-switcher__label">You are</span>
          <select
            value={user.id}
            onChange={(e) => setUserId(e.target.value)}
            disabled={busy}
            aria-label="Switch user"
          >
            {DEV_USERS.map((u) => (
              <option key={u.id} value={u.id}>
                {u.name}
              </option>
            ))}
          </select>
        </label>
      ) : (
        <span className="user-switcher__label">{user.name}</span>
      )}
      {isViewer(user) && (
        <span className="chip chip--muted view-only" title="You can read rooms but not change anything">
          View only
        </span>
      )}
      {!isFixtures() && (
        <button type="button" className="btn btn--ghost btn--small" onClick={logout} disabled={busy}>
          Log out
        </button>
      )}
    </span>
  )
}

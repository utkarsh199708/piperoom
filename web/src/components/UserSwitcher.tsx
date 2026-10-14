import { isDev } from '../lib/env'
import { isFixtures } from '../api/client'
import { useAuthUsers } from '../hooks/useAuthUsers'
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
          <DevUserSelect value={user.id} name={user.name} busy={busy} onChange={setUserId} />
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

// Options come from GET /auth/users; until they load (or if that fails) the select shows only the current user.
function DevUserSelect({
  value,
  name,
  busy,
  onChange,
}: {
  value: string
  name: string
  busy?: boolean
  onChange: (id: string) => void
}) {
  const users = useAuthUsers()
  const options = users.status === 'ready' ? users.data : [{ id: value, name }]
  return (
    <select value={value} onChange={(e) => onChange(e.target.value)} disabled={busy} aria-label="Switch user">
      {options.map((u) => (
        <option key={u.id} value={u.id}>
          {u.name}
        </option>
      ))}
    </select>
  )
}

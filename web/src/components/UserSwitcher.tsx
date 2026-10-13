import { useEffect, useState } from 'react'
import { isDev } from '../lib/env'
import { isFixtures } from '../api/client'
import { DEV_USERS } from '../dev/users'
import { useCurrentUser } from '../context/user'
import { isViewer } from '../state/permissions'
import { effectiveTheme, setTheme } from '../lib/theme'
import { Avatar, Icon } from './ui'

export function UserSwitcher() {
  const { user, setUserId, logout, busy } = useCurrentUser()
  const [theme, setThemeState] = useState(effectiveTheme)
  // A system-following theme changes when the OS switches.
  useEffect(() => {
    if (typeof window.matchMedia !== 'function') return
    const mq = window.matchMedia('(prefers-color-scheme: light)')
    const onChange = () => setThemeState(effectiveTheme())
    mq.addEventListener('change', onChange)
    return () => mq.removeEventListener('change', onChange)
  }, [])
  const toggleTheme = () => {
    const next = theme === 'dark' ? 'light' : 'dark'
    setTheme(next)
    setThemeState(next)
  }
  return (
    <span className="user-switcher">
      {/* The switcher lets a client sign in as any user (F1): it must never
          render in a production build, where identity comes from the server's
          session. In dev, picking a user calls POST /auth/dev-login. */}
      <span className="user-pill">
        <Avatar actor={{ kind: 'human', id: user.id, name: user.name }} size="sm" />
        {isDev ? (
          <label className="user-switcher__pick">
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
          <span className="user-pill__name">{user.name}</span>
        )}
      </span>
      {isViewer(user) && (
        <span className="chip chip--muted view-only" title="You can read rooms but not change anything">
          <Icon name="lock" size={12} />
          View only
        </span>
      )}
      <button
        type="button"
        className="icon-btn theme-btn"
        onClick={toggleTheme}
        aria-label="Switch theme"
        title={theme === 'dark' ? 'Switch to light theme' : 'Switch to dark theme'}
      >
        <Icon name={theme === 'dark' ? 'sun' : 'moon'} size={18} />
      </button>
      {!isFixtures() && (
        <button type="button" className="btn btn--ghost btn--small" onClick={logout} disabled={busy}>
          Log out
        </button>
      )}
    </span>
  )
}

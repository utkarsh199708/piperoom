import { DEV_USERS } from '../dev/users'

// Dev-mode login: pick a users.yaml user. This calls POST /auth/dev-login, which
// issues a real session cookie, so dev and production share one cookie path.
export function LoginScreen({
  onPick,
  busy,
}: {
  onPick: (userId: string) => void
  busy: boolean
}) {
  return (
    <main className="roomlist">
      <section className="panel">
        <h2>Sign in</h2>
        <p className="empty">Your session has ended or has not started. Pick a user to continue.</p>
        <div className="login-users">
          {DEV_USERS.map((u) => (
            <button
              key={u.id}
              type="button"
              className="btn btn--primary"
              disabled={busy}
              onClick={() => onPick(u.id)}
            >
              {u.name}
            </button>
          ))}
        </div>
      </section>
    </main>
  )
}

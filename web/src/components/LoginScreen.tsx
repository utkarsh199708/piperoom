import { Fragment, useState } from 'react'
import { DEV_USERS } from '../dev/users'
import { Avatar, Icon, LayerTag } from './ui'

// Dev-mode login: pick a users.yaml user. This calls POST /auth/dev-login, which
// issues a real session cookie, so dev and production share one cookie path.
export function LoginScreen({
  onPick,
  busy,
}: {
  onPick: (userId: string) => void
  busy: boolean
}) {
  // The row that was clicked shows the spinner while the login is in flight.
  const [picked, setPicked] = useState<string | null>(null)
  return (
    <main className="auth-page">
      <div className="auth-lane" aria-hidden="true">
        <i />
        <i />
        <i />
        <i />
        <i />
      </div>
      <section className="auth-card">
        <div className="auth-brand">
          <Icon name="pipe" size={20} />
          Piperoom
        </div>
        <h1 className="auth-card__title">Sign in</h1>
        <p className="auth-card__lead">Your session has ended or has not started. Pick a user to continue.</p>
        <div className="login-users">
          {DEV_USERS.map((u) => (
            <Fragment key={u.id}>
              {/* The layers are read out as a description, so the button's name stays the user name. */}
              <span id={`login-layers-${u.id}`} className="sr">
                Layers: {u.layers.join(', ')}
              </span>
              <button
                type="button"
                className="login-user"
                disabled={busy}
                aria-describedby={`login-layers-${u.id}`}
                onClick={() => {
                  setPicked(u.id)
                  onPick(u.id)
                }}
              >
                <Avatar actor={{ kind: 'human', id: u.id, name: u.name }} size="md" />
                <span className="login-user__name">{u.name}</span>
                <span className="login-user__layers" aria-hidden="true">
                  {u.layers.map((l) => (
                    <LayerTag key={l} layer={l} />
                  ))}
                </span>
                {busy && picked === u.id && <span className="spinner" aria-hidden="true" />}
              </button>
            </Fragment>
          ))}
        </div>
      </section>
    </main>
  )
}

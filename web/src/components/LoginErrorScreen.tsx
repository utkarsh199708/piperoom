import { LOGIN_AGAIN_URL } from '../lib/loginError'
import type { LoginErrorCode, LoginErrorInfo } from '../lib/loginError'
import { Icon } from './ui'
import type { IconName } from './ui'

const GLYPH: Record<LoginErrorCode, IconName> = {
  login_expired: 'clock',
  unauthenticated: 'shield',
  not_provisioned: 'lock',
  rate_limited: 'clock',
  idp_unavailable: 'cloud-off',
  unknown: 'alert',
}

// Shown instead of the app when the OIDC callback sent the person back with an
// error. No automatic redirect: the link is clicked by hand, so a failing IdP
// cannot make a loop.
export function LoginErrorScreen({ info }: { info: LoginErrorInfo }) {
  return (
    <div className="shell">
      <main className="auth-page">
        <div className="auth-lane" aria-hidden="true">
          <i />
          <i />
          <i />
          <i />
          <i />
        </div>
        <section className="auth-card login-error" data-code={info.code}>
          <span
            className={`login-error__glyph${info.code === 'not_provisioned' ? ' login-error__glyph--quiet' : ''}`}
          >
            <Icon name={GLYPH[info.code]} size={40} />
          </span>
          <h1 className="auth-card__title">{info.title}</h1>
          <p role="alert" className="login-error__body">
            {info.body}
          </p>
          {info.canRetry && (
            <p className="login-error__retry">
              <a className="btn btn--primary" href={LOGIN_AGAIN_URL}>
                Sign in again
              </a>
            </p>
          )}
          <p className="login-error__code">error: {info.code}</p>
        </section>
      </main>
    </div>
  )
}

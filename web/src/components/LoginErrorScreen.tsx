import { LOGIN_AGAIN_URL } from '../lib/loginError'
import type { LoginErrorInfo } from '../lib/loginError'

// Shown instead of the app when the OIDC callback sent the person back with an
// error. No automatic redirect: the link is clicked by hand, so a failing IdP
// cannot make a loop.
export function LoginErrorScreen({ info }: { info: LoginErrorInfo }) {
  return (
    <div className="shell">
      <main className="roomlist">
        <section className="panel login-error" data-code={info.code}>
          <div className="panel__body">
            <h2 className="panel__title">{info.title}</h2>
            <p role="alert">{info.body}</p>
            {info.canRetry && (
              <p>
                <a className="btn btn--primary" href={LOGIN_AGAIN_URL}>
                  Sign in again
                </a>
              </p>
            )}
          </div>
        </section>
      </main>
    </div>
  )
}

import { problemText } from '../lib/auth'
import type { AuthProblem } from '../lib/auth'

// Clear message for 403 proxy_identity, 421 bad_host and 403 viewer. These are
// not things the person can fix by clicking, so the banner says what happened
// and who can fix it (docs/api.md, Errors).
export function AuthBanner({ problem, onDismiss }: { problem: AuthProblem; onDismiss: () => void }) {
  return (
    <div className="banner banner--error" role="alert" data-kind={problem.kind}>
      <span>{problemText(problem)}</span>
      <button type="button" className="btn btn--ghost btn--small" onClick={onDismiss}>
        Dismiss
      </button>
    </div>
  )
}

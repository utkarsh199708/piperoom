import type { RoomState } from '../state/types'
import type { CurrentUser } from '../state/permissions'
import type { RoomActions } from '../hooks/useRoom'

// Webhook-opened rooms have no driver until someone takes control, and nobody
// can steer the agent until then. control/grant is open to anyone while there
// is no driver (docs/api.md), so the button grants control to the current user;
// a losing race answers `lease_held` and the error banner shows the friendly
// text from lib/errors.ts.
export function NoDriverBanner({
  state,
  user,
  actions,
}: {
  state: RoomState
  user: CurrentUser
  actions: RoomActions
}) {
  if (state.status !== 'open' || state.driver !== null) return null
  return (
    <div className="banner banner--nodriver" role="status">
      <span>
        <strong>No one is driving</strong> — take control to steer the agent.
      </span>
      <button
        type="button"
        className="btn btn--primary"
        onClick={() => actions.grantControl(user.id)}
      >
        Take control
      </button>
    </div>
  )
}

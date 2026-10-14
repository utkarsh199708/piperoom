import { TriangleAlert } from 'lucide-react'
import type { RoomState } from '../state/types'
import type { CurrentUser } from '../state/permissions'
import { isViewer } from '../state/permissions'
import type { RoomActions } from '../hooks/useRoom'
import { ageLabel } from '../lib/time'
import { Button } from '@/components/ui/button'
import { Kbd } from '@/components/ui/kbd'

// Webhook-opened rooms have no driver until someone takes control, and nobody
// can steer the agent until then. control/grant is open to anyone while there
// is no driver (docs/api.md), so the button grants control to the current user;
// a losing race answers `lease_held` and the error banner shows the friendly
// text from lib/errors.ts. The T key does the same (RoomScreen).
export function NoDriverBanner({
  state,
  user,
  actions,
}: {
  state: RoomState
  user: CurrentUser
  actions: RoomActions
}) {
  if (state.status !== 'open' || state.driver !== null || isViewer(user)) return null
  const age = state.opened_at ? ageLabel(state.opened_at) : ''
  return (
    <div
      role="status"
      className="flex min-h-10 flex-wrap items-center gap-x-2.5 gap-y-1 border-b border-border bg-warning-subtle px-4 py-1.5 font-medium text-warning-foreground sm:px-6"
    >
      <TriangleAlert className="size-3.5 shrink-0" aria-hidden="true" />
      <span>No one is driving — take control to steer the agent.</span>
      {age && <span className="text-xs font-normal text-muted-foreground">opened {age} ago</span>}
      <Button
        className="ml-auto h-7 px-2.5"
        aria-keyshortcuts="T"
        onClick={() => actions.grantControl(user.id)}
      >
        Take control
        <Kbd aria-hidden="true">T</Kbd>
      </Button>
    </div>
  )
}

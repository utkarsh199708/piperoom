import { Bell } from 'lucide-react'
import type { NotifyPermission } from '../../lib/notify'
import { Button } from '@/components/ui/button'

// Asks for browser notification permission on a click, never on load.
export function NotifyToggle({ permission, onEnable }: { permission: NotifyPermission; onEnable: () => void }) {
  if (permission === 'unsupported') return null
  return (
    <section aria-label="Notifications" className="flex flex-col gap-1.5 px-4 py-3.5">
      {permission === 'default' && (
        <>
          <Button variant="secondary" size="sm" className="w-fit" onClick={onEnable}>
            <Bell aria-hidden="true" className="size-3.5" />
            Notify me about this room
          </Button>
          <p className="m-0 text-xs text-subtle-foreground">
            When this tab is in the background: proposals (including ones that need your approval), hand-offs and driver changes.
          </p>
        </>
      )}
      {permission === 'granted' && (
        <p className="m-0 text-xs text-muted-foreground">
          Notifications on for proposals (including ones that need your approval), hand-offs and driver changes while this tab is hidden.
        </p>
      )}
      {permission === 'denied' && (
        <p className="m-0 text-xs text-muted-foreground">
          Notifications are blocked in your browser settings. The tab title still shows an unread count.
        </p>
      )}
    </section>
  )
}

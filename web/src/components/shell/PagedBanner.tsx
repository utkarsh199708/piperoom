import { BellRing, X } from 'lucide-react'
import { Link } from 'react-router-dom'
import type { PagedRoom } from '@/lib/pages'
import type { NotifyPermission } from '@/lib/notify'
import { plainText } from '@/lib/title'
import { timeAgo } from '@/lib/time'
import { Button } from '@/components/ui/button'

const MAX_ROWS = 3

function byWhom(p: PagedRoom['page']): string {
  if (p.auto) return 'automatically, because no one was driving'
  if (p.by.kind === 'agent') return 'by the agent'
  return `by ${plainText(p.by.name || p.by.id, 40)}`
}

// Shown across the app when someone asked this person, as an owner of a layer, to look at a room (Q0, QA G2).
// The reason can be the agent's words, so it is plain text, clipped. It goes away when the person opens the
// room or dismisses it; the choice is remembered in this browser (lib/pages.ts).
export function PagedBanner({
  items,
  onDismiss,
  onDismissAll,
  permission,
  onEnableNotifications,
}: {
  items: PagedRoom[]
  onDismiss: (item: PagedRoom) => void
  onDismissAll: () => void
  permission: NotifyPermission
  /** Called from a click only: the browser asks for permission then. */
  onEnableNotifications: () => void
}) {
  if (items.length === 0) return null
  const shown = items.slice(0, MAX_ROWS)
  const more = items.length - shown.length
  return (
    <section
      role="alert"
      aria-label="You were paged"
      className="flex shrink-0 flex-col gap-1 border-b border-border bg-warning-subtle px-3 py-2 text-warning-foreground md:px-6"
    >
      <ul className="m-0 flex list-none flex-col gap-1 p-0">
        {shown.map((item) => {
          const { room, page } = item
          const reason = plainText(page.reason, 160)
          return (
            <li key={`${item.key}@${page.at}`} className="flex flex-wrap items-center gap-x-2.5 gap-y-1">
              <BellRing className="size-3.5 shrink-0" aria-hidden="true" />
              <span className="min-w-0 break-words font-medium">
                You were paged for <span className="font-mono">{plainText(page.layer, 40)}</span> in{' '}
                {plainText(room.title, 80) || 'a room'}
              </span>
              <span className="text-xs font-normal text-muted-foreground">
                {byWhom(page)} · {timeAgo(page.at)}
              </span>
              {reason && <span className="min-w-0 break-words text-xs font-normal text-muted-foreground">{reason}</span>}
              <span className="ml-auto flex items-center gap-1">
                <Link
                  to={`/rooms/${encodeURIComponent(room.id)}`}
                  onClick={() => onDismiss(item)}
                  className="inline-flex h-7 items-center rounded-md bg-primary px-2.5 text-xs font-medium text-primary-foreground hover:bg-primary/90 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring coarse:h-11 coarse:px-4"
                >
                  Open room
                </Link>
                <Button
                  variant="ghost"
                  size="icon"
                  className="size-7 coarse:size-11"
                  aria-label={`Dismiss the page for ${plainText(room.title, 40) || 'this room'}`}
                  onClick={() => onDismiss(item)}
                >
                  <X className="size-3.5" aria-hidden="true" />
                </Button>
              </span>
            </li>
          )
        })}
      </ul>
      {(more > 0 || permission === 'default') && (
        <div className="flex flex-wrap items-center gap-x-3 gap-y-1 text-xs font-normal">
          {more > 0 && (
            <>
              <span>+{more} more</span>
              <Button variant="ghost" className="h-6 px-1.5 text-xs" onClick={onDismissAll}>
                Dismiss all
              </Button>
            </>
          )}
          {permission === 'default' && (
            <Button variant="ghost" className="h-6 px-1.5 text-xs" onClick={onEnableNotifications}>
              Notify me when I am paged
            </Button>
          )}
        </div>
      )}
    </section>
  )
}

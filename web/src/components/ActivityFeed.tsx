import type { ActivityEntry } from '../state/types'
import { clockTime } from '../lib/time'
import { ClampedText } from './room/ClampedText'
import { PersonAvatar } from './room/PersonAvatar'

export function ActivityFeed({
  activity,
  history,
}: {
  activity: ActivityEntry[]
  /** The log has not been replayed yet: still loading, or the live socket is down. */
  history?: 'loading' | 'down'
}) {
  const empty =
    history === 'down'
      ? 'Activity is unavailable while live updates are down.'
      : history === 'loading'
        ? 'Loading activity…'
        : 'No activity yet.'
  return (
    <section aria-labelledby="activity-heading" className="flex flex-1 flex-col gap-2 px-4 py-3.5">
      <h2 id="activity-heading" className="m-0 text-sm font-medium">
        Activity
      </h2>
      <ul className="m-0 flex list-none flex-col gap-2 p-0">
        {activity.length === 0 && <li className="text-muted-foreground">{empty}</li>}
        {activity.map((a) => (
          <li key={a.seq} className="flex min-h-6 items-start gap-2">
            <PersonAvatar actor={a.actor} size={20} className="mt-0.5" />
            <ClampedText text={a.text} className="min-w-0 flex-1 text-muted-foreground" />
            <time
              dateTime={a.at}
              className="ml-auto shrink-0 font-mono text-xs tabular-nums text-subtle-foreground"
            >
              {clockTime(a.at)}
            </time>
          </li>
        ))}
      </ul>
    </section>
  )
}

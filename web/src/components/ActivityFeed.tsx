import type { ActivityEntry } from '../state/types'
import { clockTime } from '../lib/time'
import { PersonAvatar } from './room/PersonAvatar'
import { VerifiedBadge } from './VerifiedBadge'
import { neutralizeBadgeText, personVerification } from '@/lib/verification'

export function ActivityFeed({ activity }: { activity: ActivityEntry[] }) {
  return (
    <section aria-labelledby="activity-heading" className="flex flex-1 flex-col gap-2 px-4 py-3.5">
      <h2 id="activity-heading" className="m-0 text-sm font-medium">
        Activity
      </h2>
      <ul className="m-0 flex list-none flex-col gap-2 p-0">
        {activity.length === 0 && <li className="text-muted-foreground">No activity yet.</li>}
        {activity.map((a) => (
          <li key={a.seq} className="flex min-h-6 items-start gap-2">
            <PersonAvatar actor={a.actor} size={20} className="mt-0.5" />
            {/* A person's row gets their server-set badge, and its sentence (their name and their
                text, both untrusted) is neutralized so it cannot add a second verdict. That also
                blanks a legitimate "verified" or "✅" in a comment here; the Timeline shows the
                comment as written. Agent and system rows carry no badge and keep their text. */}
            {a.actor.kind === 'human' && <VerifiedBadge verification={personVerification(a.actor)} className="mt-0.5" />}
            <span className="min-w-0 break-words text-muted-foreground">
              {a.actor.kind === 'human' ? neutralizeBadgeText(a.text) : a.text}
            </span>
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

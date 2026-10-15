import { Fragment, useState } from 'react'
import type { ActivityEntry } from '../state/types'
import { Clock } from './room/Clock'
import { clockTime } from '../lib/time'
import { Button } from '@/components/ui/button'
import { PersonAvatar } from './room/PersonAvatar'
import { VerifiedBadge } from './VerifiedBadge'
import { personVerification } from '@/lib/verification'

const SHOW_ALL_KEY = 'piperoom.activity.all'
const HOUR_MS = 60 * 60 * 1000
const GROUP_MS = 5 * 60 * 1000

function readShowAll(): boolean {
  try {
    return localStorage.getItem(SHOW_ALL_KEY) === '1'
  } catch {
    return false
  }
}

// `{tool}` in the text stands for the tool name: it goes in <code>. Everything else is a text node.
function Line({ entry }: { entry: ActivityEntry }) {
  const parts = entry.tool ? entry.text.split('{tool}') : [entry.text]
  return (
    <span className="min-w-0 line-clamp-1 break-words text-muted-foreground" title={entry.detail ?? entry.text.replace('{tool}', entry.tool ?? '')}>
      {parts.map((part, i) => (
        <Fragment key={i}>
          {i > 0 && <code className="font-mono text-xs text-foreground">{entry.tool}</code>}
          {part}
        </Fragment>
      ))}
    </span>
  )
}

// One curated line per event. The server's chatter (status flips, tool calls, lease renewals)
// is `system` and shows only after "Show all events".
export function ActivityFeed({ activity, openedAt }: { activity: ActivityEntry[]; openedAt?: string }) {
  const [all, setAll] = useState(readShowAll)
  const toggle = () => {
    const next = !all
    setAll(next)
    try {
      localStorage.setItem(SHOW_ALL_KEY, next ? '1' : '0')
    } catch {
      /* storage blocked: the choice lasts for this page only */
    }
  }
  const rows = all ? activity : activity.filter((a) => a.kind !== 'system')
  const opened = openedAt ? Date.parse(openedAt) : NaN
  const grouped = !Number.isNaN(opened) && Date.now() - opened > HOUR_MS
  let lastGroup = -1
  return (
    <section aria-labelledby="activity-heading" className="flex flex-1 flex-col gap-2 px-4 py-3.5">
      <h2 id="activity-heading" className="m-0 text-sm font-medium">
        Activity
      </h2>
      <ul className="m-0 flex list-none flex-col gap-2 p-0">
        {rows.length === 0 && <li className="text-muted-foreground">No activity yet.</li>}
        {rows.map((a) => {
          const t = Date.parse(a.at)
          const group = Number.isNaN(t) ? -1 : Math.floor(t / GROUP_MS)
          const header = grouped && group !== lastGroup && group !== -1
          lastGroup = group
          return (
            <Fragment key={a.seq}>
              {header && (
                <li role="presentation" className="pt-1 font-mono text-xs tabular-nums text-subtle-foreground">
                  {clockTime(new Date(group * GROUP_MS).toISOString())}
                </li>
              )}
              <li className="flex min-h-6 items-start gap-2">
                <PersonAvatar actor={a.actor} size={20} className="mt-0.5" />
                {/* A person's row gets their server-set badge. Names inside the sentence were neutralized
                    where describe.ts built it; the person's own words stay as written. */}
                {a.actor.kind === 'human' && <VerifiedBadge verification={personVerification(a.actor)} className="mt-0.5" />}
                <Line entry={a} />
                <Clock iso={a.at} className="ml-auto shrink-0 font-mono text-xs tabular-nums text-subtle-foreground" />
              </li>
            </Fragment>
          )
        })}
      </ul>
      <Button variant="ghost" className="h-7 self-start" aria-pressed={all} onClick={toggle}>
        {all ? 'Show less' : 'Show all events'}
      </Button>
    </section>
  )
}

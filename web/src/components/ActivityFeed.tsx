import type { ActivityEntry } from '../state/types'
import { Avatar, Panel } from './ui'
import { clockTime } from '../lib/time'

export function ActivityFeed({ activity }: { activity: ActivityEntry[] }) {
  return (
    <Panel className="activity" title="Activity">
      <ul className="activity__list">
        {activity.length === 0 && <li className="empty">No activity yet.</li>}
        {activity.map((a) => (
          <li key={a.seq} className="activity__row">
            <Avatar actor={a.actor} size="sm" />
            <span className="activity__text">{a.text}</span>
            <span className="muted activity__time">{clockTime(a.at)}</span>
          </li>
        ))}
      </ul>
    </Panel>
  )
}

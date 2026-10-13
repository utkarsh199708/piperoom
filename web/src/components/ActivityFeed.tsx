import type { ActivityEntry } from '../state/types'
import { Panel } from './ui'
import { Avatar } from './ui-p4-temp'
import { clockTime } from '../lib/time'

export function ActivityFeed({ activity }: { activity: ActivityEntry[] }) {
  return (
    <Panel className="activity" title="Activity">
      <ul className="activity__list feed" aria-live="polite">
        {activity.length === 0 && <li className="empty">No activity yet.</li>}
        {activity.map((a) => (
          <li key={a.seq} className="activity__row feed__row">
            <Avatar actor={a.actor} size="xs" />
            <span className="activity__text feed__text">{a.text}</span>
            <span className="activity__time feed__time">{clockTime(a.at)}</span>
          </li>
        ))}
      </ul>
    </Panel>
  )
}

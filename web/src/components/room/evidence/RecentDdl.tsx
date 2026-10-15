import { Badge } from '@/components/ui/badge'
import { isDdlList } from '@/lib/evidenceShapes'
import { timeAgo, utcLabel } from '@/lib/time'
import type { ToolCallState } from '@/state/types'
import { Raw } from './Raw'

/** pg_recent_ddl: one entry per DDL change, newest as the tool returned them. */
export function RecentDdl({ t }: { t: ToolCallState }) {
  const d = t.data
  if (!isDdlList(d)) return <Raw t={t} />
  if (d.length === 0) return <p className="m-0 text-xs text-subtle-foreground">no DDL changes</p>
  return (
    <ul className="m-0 flex list-none flex-col gap-2 p-0 text-xs">
      {d.map((c, i) => (
        <li key={i} className="flex flex-col gap-1">
          <p className="m-0 flex flex-wrap items-center gap-x-2 gap-y-0.5">
            <time dateTime={c.at} title={utcLabel(c.at)} className="text-subtle-foreground">
              {timeAgo(c.at)}
            </time>
            <Badge mono>{c.command_tag}</Badge>
            <span className="break-all font-mono">{c.object_identity}</span>
            <span className="text-muted-foreground">{c.username}</span>
          </p>
          {c.query && (
            <pre className="m-0 max-h-24 overflow-auto whitespace-pre-wrap break-words rounded-md border border-border bg-muted px-2.5 py-1.5 font-mono text-xs leading-[17px] text-muted-foreground">
              {c.query}
            </pre>
          )}
        </li>
      ))}
    </ul>
  )
}

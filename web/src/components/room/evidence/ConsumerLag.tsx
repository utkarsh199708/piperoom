import { cn } from '@/lib/utils'
import { isConsumerLag } from '@/lib/evidenceShapes'
import type { ToolCallState } from '@/state/types'
import { Raw } from './Raw'

/** kafka_consumer_lag: the group line and the worst partitions; lag above zero is in the warning tone. */
export function ConsumerLag({ t }: { t: ToolCallState }) {
  const d = t.data
  if (!isConsumerLag(d)) return <Raw t={t} />
  return (
    <div className="flex flex-col gap-1.5 text-xs">
      <p className="m-0 flex flex-wrap items-center gap-x-2 font-mono">
        <span className="font-medium">{d.group}</span>
        <span>
          {d.state} · {d.members} {d.members === 1 ? 'member' : 'members'} · total lag{' '}
          <span className={cn('tabular-nums', d.total_lag > 0 && 'text-warning-foreground')}>{d.total_lag}</span>
        </span>
      </p>
      {d.worst_partitions.length > 0 && (
        <div className="overflow-x-auto">
          <table className="w-full border-collapse text-left font-mono">
            <thead>
              <tr className="font-sans text-subtle-foreground">
                <th scope="col" className="py-1 pr-3 font-medium">topic/partition</th>
                <th scope="col" className="py-1 pr-3 text-right font-medium">committed</th>
                <th scope="col" className="py-1 pr-3 text-right font-medium">end</th>
                <th scope="col" className="py-1 text-right font-medium">lag</th>
              </tr>
            </thead>
            <tbody>
              {d.worst_partitions.map((p) => (
                <tr key={`${p.topic}/${p.partition}`}>
                  <td className="py-1 pr-3">
                    {p.topic}/{p.partition}
                  </td>
                  <td className="py-1 pr-3 text-right tabular-nums">{p.committed}</td>
                  <td className="py-1 pr-3 text-right tabular-nums">{p.end}</td>
                  <td className={cn('py-1 text-right tabular-nums', p.lag > 0 && 'text-warning-foreground')}>{p.lag}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </div>
  )
}

import { cn } from '@/lib/utils'
import { humanBytes, isSlotList } from '@/lib/evidenceShapes'
import type { ToolCallState } from '@/state/types'
import { Raw } from './Raw'

/** pg_replication_slots: slot, active, wal_status, retained WAL. An inactive slot or any status but "reserved" is in the failure tone. */
export function ReplicationSlots({ t }: { t: ToolCallState }) {
  const d = t.data
  if (!isSlotList(d)) return <Raw t={t} />
  if (d.length === 0) return <p className="m-0 text-xs text-subtle-foreground">no replication slots</p>
  return (
    <div className="overflow-x-auto text-xs">
      <table className="w-full border-collapse text-left font-mono">
        <thead>
          <tr className="font-sans text-subtle-foreground">
            <th scope="col" className="py-1 pr-3 font-medium">slot</th>
            <th scope="col" className="py-1 pr-3 font-medium">active</th>
            <th scope="col" className="py-1 pr-3 font-medium">wal_status</th>
            <th scope="col" className="py-1 text-right font-medium">retained</th>
          </tr>
        </thead>
        <tbody>
          {d.map((s) => (
            <tr key={s.name}>
              <td className="py-1 pr-3">{s.name}</td>
              <td className={cn('py-1 pr-3', !s.active && 'text-destructive-fg')}>{String(s.active)}</td>
              <td className={cn('py-1 pr-3', s.wal_status !== 'reserved' && 'text-destructive-fg')}>{s.wal_status || '-'}</td>
              <td className="py-1 text-right tabular-nums">{humanBytes(s.retained_wal_bytes)}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  )
}

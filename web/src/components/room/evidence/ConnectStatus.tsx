import { Badge } from '@/components/ui/badge'
import { isConnectStatus } from '@/lib/evidenceShapes'
import type { ToolCallState } from '@/state/types'
import { StatusDot } from '../StatusDot'
import type { DotTone } from '../StatusDot'
import { Raw } from './Raw'

const tone = (state: string): DotTone => (state === 'RUNNING' ? 'ok' : state === 'PAUSED' ? 'warn' : state === 'FAILED' ? 'fail' : 'muted')

/** connect_status: the connector line and one row per task; a task error keeps its trace, first line bold. */
export function ConnectStatus({ t }: { t: ToolCallState }) {
  const d = t.data
  if (!isConnectStatus(d)) return <Raw t={t} />
  return (
    <div className="flex flex-col gap-1.5 text-xs">
      <p className="m-0 flex flex-wrap items-center gap-x-2 font-mono">
        <span className="font-medium">{d.name}</span>
        <Badge variant="neutral" mono>
          <StatusDot tone={tone(d.state)} />
          {d.state}
        </Badge>
        <span className="text-subtle-foreground">{d.worker}</span>
      </p>
      {d.tasks.length > 0 && (
        <div className="overflow-x-auto">
          <table className="w-full border-collapse text-left">
            <thead>
              <tr className="text-subtle-foreground">
                <th scope="col" className="py-1 pr-3 font-medium">task</th>
                <th scope="col" className="py-1 pr-3 font-medium">state</th>
                <th scope="col" className="py-1 font-medium">error</th>
              </tr>
            </thead>
            <tbody>
              {d.tasks.map((k) => {
                const [first, ...rest] = (k.error ?? '').split('\n')
                return (
                  <tr key={k.id} className="align-top">
                    <td className="py-1 pr-3 font-mono tabular-nums">{k.id}</td>
                    <td className="py-1 pr-3">
                      <span className="inline-flex items-center gap-1.5 font-mono">
                        <StatusDot tone={tone(k.state)} />
                        {k.state}
                      </span>
                    </td>
                    <td className="py-1">
                      {k.error ? (
                        <pre className="m-0 max-h-32 overflow-auto whitespace-pre-wrap break-words font-mono text-xs leading-[17px] text-destructive-fg">
                          <strong>{first}</strong>
                          {rest.length > 0 && `\n${rest.join('\n')}`}
                        </pre>
                      ) : null}
                    </td>
                  </tr>
                )
              })}
            </tbody>
          </table>
        </div>
      )}
    </div>
  )
}

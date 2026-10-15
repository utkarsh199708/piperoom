import { Badge } from '@/components/ui/badge'
import { isSchemaDiff } from '@/lib/evidenceShapes'
import type { ToolCallState } from '@/state/types'
import { Raw } from './Raw'

const KIND: Record<string, 'success' | 'destructive' | 'warning' | 'neutral'> = {
  added: 'success',
  removed: 'destructive',
  changed: 'warning',
}

/** schema_registry_diff: `v2 → v3` and one row per field change. */
export function SchemaDiff({ t }: { t: ToolCallState }) {
  const d = t.data
  if (!isSchemaDiff(d)) return <Raw t={t} />
  return (
    <div className="flex flex-col gap-1.5 text-xs">
      <p className="m-0 flex flex-wrap items-center gap-x-2 font-mono">
        <span className="font-medium">{d.subject}</span>
        <span className="tabular-nums">
          v{d.from_version} → v{d.to_version}
        </span>
      </p>
      {d.changes.length === 0 ? (
        <p className="m-0 text-subtle-foreground">no field changes</p>
      ) : (
        <ul className="m-0 flex list-none flex-col gap-1 p-0">
          {d.changes.map((c, i) => (
            <li key={i} className="flex flex-wrap items-center gap-x-2 gap-y-0.5 font-mono">
              <span className="font-medium">{c.field}</span>
              <Badge variant={KIND[c.kind] ?? 'neutral'}>{c.kind}</Badge>
              {(c.from !== undefined || c.to !== undefined) && (
                <span className="break-all text-muted-foreground">
                  {c.from ?? '∅'} → {c.to ?? '∅'}
                </span>
              )}
            </li>
          ))}
        </ul>
      )}
    </div>
  )
}

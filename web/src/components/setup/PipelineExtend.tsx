// The pipeline-extend control (U1-12b; docs/api.md POST /api/rooms/{id}/setup/pipeline). The
// provisional pipeline is what the room scopes its tools and actions to, so a table or connector
// the agent will need is named here; hosts go to the targets control instead.
import { useId, useState } from 'react'
import type { FormEvent } from 'react'
import { Workflow } from 'lucide-react'
import { api } from '@/api/client'
import type { RoomState } from '@/state/types'
import type { CurrentUser } from '@/state/permissions'
import { canSteer } from '@/state/permissions'
import { EMPTY_EXTENSION, extensionBody } from '@/lib/setupControls'
import type { SetupExtensionValues } from '@/lib/setupControls'
import { Badge } from '@/components/ui/badge'
import { Button } from '@/components/ui/button'
import { Card } from '@/components/ui/card'
import { Textarea } from '@/components/ui/textarea'
import { FIELD_LABEL, SetupAlert, SetupStatus } from './SetupFields'
import { useSetupAction } from './useSetupAction'

const FIELDS: { key: keyof SetupExtensionValues; label: string; placeholder: string }[] = [
  { key: 'connectors', label: 'Add connectors', placeholder: 'orders-src' },
  { key: 'tables', label: 'Add tables', placeholder: 'public.shipments' },
  { key: 'consumerGroups', label: 'Add consumer groups', placeholder: 'warehouse-loader' },
  { key: 'sinkTables', label: 'Add sink tables', placeholder: 'analytics.orders' },
]

export function PipelineExtend({ state, user }: { state: RoomState; user: CurrentUser }) {
  const setup = state.setup
  const formId = useId()
  const canEdit = canSteer(state, user)
  const { busy, error, done, run } = useSetupAction()
  const [values, setValues] = useState<SetupExtensionValues>(EMPTY_EXTENSION)
  const [formError, setFormError] = useState<string | null>(null)
  if (!setup) return null

  const submit = async (e: FormEvent<HTMLFormElement>) => {
    e.preventDefault()
    const body = extensionBody(values)
    if (!body) {
      setFormError('Name at least one connector, table, consumer group or sink table.')
      return
    }
    setFormError(null)
    const ok = await run(
      () => api.setupPipeline(state.id, body),
      'Pipeline extended. The room re-derived its tool and action scope.',
    )
    if (ok) setValues(EMPTY_EXTENSION)
  }

  const nameList = (names: string[]) =>
    names.length > 0 ? names.join(' · ') : 'none'

  return (
    <Card
      role="region"
      aria-labelledby={`${formId}-title`}
      className="mx-4 mt-2 flex min-w-0 flex-col gap-3 p-3 sm:mx-6"
    >
      <div className="flex min-w-0 flex-wrap items-center gap-2">
        <Workflow className="size-4 shrink-0 text-muted-foreground" aria-hidden="true" />
        <h2 id={`${formId}-title`} className="m-0 text-sm font-semibold">
          Pipeline
        </h2>
        <Badge variant="outline" mono>
          {setup.provisional.topic_prefix || 'no prefix'}
        </Badge>
      </div>
      <p className="m-0 text-xs text-muted-foreground">
        Tables: {nameList(setup.provisional.tables)}. Connectors: {nameList(setup.provisional.connectors)}.
        Names follow the owners rules for that list; the server refuses one it cannot read.
      </p>

      {canEdit ? (
        <form className="flex min-w-0 flex-col gap-2" onSubmit={submit} aria-label="Extend the pipeline">
          <div className="grid min-w-0 gap-2 sm:grid-cols-2">
            {FIELDS.map((field) => (
              <div key={field.key} className="grid gap-1">
                <label htmlFor={`${formId}-${field.key}`} className={FIELD_LABEL}>
                  {field.label}
                </label>
                <Textarea
                  id={`${formId}-${field.key}`}
                  className="min-h-10 border border-input-border bg-background font-mono text-xs"
                  value={values[field.key]}
                  onChange={(e) => setValues((v) => ({ ...v, [field.key]: e.target.value }))}
                  placeholder={field.placeholder}
                  disabled={busy}
                  rows={2}
                />
              </div>
            ))}
          </div>
          <div>
            <Button type="submit" variant="outline" size="sm" className="coarse:h-11" disabled={busy}>
              Extend the pipeline
            </Button>
          </div>
          {formError && <span className="text-xs text-destructive-fg">{formError}</span>}
        </form>
      ) : (
        <p className="m-0 text-xs text-subtle-foreground">Only the driver can extend the pipeline.</p>
      )}

      {error && <SetupAlert>{error}</SetupAlert>}
      <SetupStatus>{done ?? ''}</SetupStatus>
    </Card>
  )
}

import { useState } from 'react'
import type { ModelPanel } from '@/api/types'
import { Button } from '@/components/ui/button'
import { Skeleton } from '@/components/ui/skeleton'
import { useModels } from '@/hooks/useModels'
import { isEditable, needsFirstRun } from '@/lib/models'
import { ModelPanelView } from './ModelPanelView'
import { ModelPicker } from './ModelPicker'

/**
 * The Model panel with its states: loading, error with Retry, the read-only list, and (standalone or
 * quickstart only, not for a viewer) the button that opens the picker. A team server shows no picker.
 */
export function ModelSection({ canEdit = true, compact = false }: { canEdit?: boolean; compact?: boolean }) {
  const state = useModels()
  const [override, setOverride] = useState<ModelPanel | null>(null)
  const [pickerOpen, setPickerOpen] = useState(false)

  if (state.status === 'loading') {
    return (
      <div role="status" aria-label="Loading the model" className="flex flex-col gap-2">
        <Skeleton className="h-4 w-48 max-w-full" />
        <Skeleton className="h-20 w-full" />
      </div>
    )
  }
  if (state.status === 'error') {
    return (
      <div className="flex flex-wrap items-center gap-3">
        <p className="m-0 text-muted-foreground">Could not load the model. {state.message}</p>
        <Button type="button" variant="outline" size="sm" onClick={state.reload}>
          Retry
        </Button>
      </div>
    )
  }

  const panel = override ?? state.data
  const editable = isEditable(panel) && canEdit
  return (
    <div className="flex min-w-0 flex-col gap-3">
      {panel.state === 'no_model' && (
        <p className="m-0 text-warning-foreground">No model is set yet. Piperoom cannot investigate until you choose one.</p>
      )}
      <ModelPanelView panel={panel} />
      {editable ? (
        <div>
          <Button type="button" variant="outline" size="sm" onClick={() => setPickerOpen(true)}>
            {needsFirstRun(panel) ? 'Choose a model' : 'Change model'}
          </Button>
        </div>
      ) : (
        !compact && panel.mode === 'team' && (
          <p className="m-0 text-xs text-subtle-foreground">
            Read only. The model is set by whoever runs this server (environment variables or Helm values).
          </p>
        )
      )}
      {editable && (
        <ModelPicker
          panel={panel}
          open={pickerOpen}
          onOpenChange={setPickerOpen}
          onApplied={(p) => {
            setOverride(p)
            setPickerOpen(false)
          }}
        />
      )}
    </div>
  )
}

import { useState } from 'react'
import type { ModelPanel } from '@/api/types'
import { useModels } from '@/hooks/useModels'
import { needsFirstRun } from '@/lib/models'
import { ModelPicker } from './ModelPicker'

/**
 * Opens the picker before anything else on a standalone or quickstart server that has no usable model
 * (GET /api/models answers state `no_model`). It renders nothing otherwise, and nothing while loading or
 * when the call fails: a team server and an older server without the route never see it.
 */
export function FirstRunModelGate() {
  const state = useModels()
  const [applied, setApplied] = useState<ModelPanel | null>(null)
  const panel = applied ?? (state.status === 'ready' ? state.data : null)
  if (!panel || !needsFirstRun(panel)) return null
  return <ModelPicker panel={panel} open required onOpenChange={() => undefined} onApplied={setApplied} />
}

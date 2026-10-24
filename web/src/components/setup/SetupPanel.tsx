// The setup room's controls (U1-12b): rung 8's canary prompt, the targets, the pipeline extender,
// the maintenance window, the secret field, the verified card and the registration card. Rendered
// under the stepper on a setup room only; every write is the server's and driver-only.
import type { RoomState } from '@/state/types'
import type { CurrentUser } from '@/state/permissions'
import { CanaryPrompt } from './CanaryPrompt'
import { TargetControls } from './TargetControls'
import { PipelineExtend } from './PipelineExtend'
import { MaintenanceWindow } from './MaintenanceWindow'
import { SecretField } from './SecretField'
import { VerifiedCard } from './VerifiedCard'
import { RegistrationCard } from './RegistrationCard'

export function SetupPanel({ state, user }: { state: RoomState; user: CurrentUser }) {
  if (!state.setup) return null
  return (
    <div className="flex min-w-0 flex-col">
      <CanaryPrompt state={state} />
      <TargetControls state={state} user={user} />
      <PipelineExtend state={state} user={user} />
      <MaintenanceWindow state={state} user={user} />
      <SecretField state={state} user={user} />
      <VerifiedCard state={state} />
      <RegistrationCard state={state} user={user} />
    </div>
  )
}

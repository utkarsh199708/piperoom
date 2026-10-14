// Solo mode (docs/design/auth.md 5.3). The server has no field that says "solo";
// the only sign is the answer to an approve call. The solo user's approve without
// the right `confirm` gets
//   400 validation: solo mode: type confirm "<pipeline_id>/<layer>" to approve
// (internal/room/access.go, authorizeLayer). The UI then asks for the string.
import { ApiClientError } from '../api/client'

const SOLO_PREFIX = 'solo mode:'

/** True when an approve call failed only because solo mode wants a typed `confirm`. */
export function isSoloConfirmError(e: unknown): boolean {
  return (
    e instanceof ApiClientError &&
    e.status === 400 &&
    e.code === 'validation' &&
    e.message.toLowerCase().startsWith(SOLO_PREFIX)
  )
}

/** The string the person must type for this layer. The same rule as the server. */
export function soloConfirmText(pipelineId: string, layer: string): string {
  return `${pipelineId}/${layer}`
}

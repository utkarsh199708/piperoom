// "Register read credentials" (U1-12b; docs/api.md POST /api/rooms/{id}/setup/credentials and
// design §3.7). The click asks the paired runner and nothing else: it sends no body, the values
// never pass through the browser, and the runner waits for a y on its own terminal before it
// sends them to this server's secret store.
import { useId } from 'react'
import { KeyRound } from 'lucide-react'
import { api } from '@/api/client'
import type { RoomState } from '@/state/types'
import type { CurrentUser } from '@/state/permissions'
import { canSteer } from '@/state/permissions'
import { Badge } from '@/components/ui/badge'
import { Button } from '@/components/ui/button'
import { SetupAlert, SetupStatus } from './SetupFields'
import { useSetupAction } from './useSetupAction'

export function CredentialsButton({ state, user }: { state: RoomState; user: CurrentUser }) {
  const setup = state.setup
  const id = useId()
  const { busy, error, done, run } = useSetupAction()
  if (!setup) return null

  if (setup.credentials) {
    return (
      <div className="flex min-w-0 flex-wrap items-center gap-1.5 text-xs" role="status">
        <KeyRound className="size-4 shrink-0 text-muted-foreground" aria-hidden="true" />
        <span className="text-muted-foreground">Read credentials registered:</span>
        {setup.credentials.names.length > 0 ? (
          setup.credentials.names.map((name) => (
            <Badge key={name} variant="outline" mono>
              {name}
            </Badge>
          ))
        ) : (
          <span>none</span>
        )}
        {setup.credentials.runner_label && (
          <span className="text-subtle-foreground">from {setup.credentials.runner_label}</span>
        )}
      </div>
    )
  }

  if (!canSteer(state, user)) {
    return (
      <p className="m-0 text-xs text-subtle-foreground">
        Only the driver can ask the runner to register read credentials.
      </p>
    )
  }

  const ready = setup.runner?.connected === true
  return (
    <div className="flex min-w-0 flex-col gap-1.5">
      <div className="flex min-w-0 flex-wrap items-center gap-2">
        <Button
          variant="outline"
          size="sm"
          className="coarse:h-11"
          disabled={busy || !ready}
          aria-describedby={`${id}-hint`}
          onClick={() => run(() => api.setupCredentials(state.id), "Asked the runner. Confirm on the runner's terminal.")}
        >
          <KeyRound aria-hidden="true" />
          Register read credentials
        </Button>
        <span id={`${id}-hint`} className="text-xs text-muted-foreground">
          {ready
            ? "Asks only the runner; the values never enter this request. The runner prints the server origin and the secret names and waits for y/N on its terminal."
            : 'Pair a runner first: it holds the read credentials.'}
        </span>
      </div>
      {error && <SetupAlert>{error}</SetupAlert>}
      <SetupStatus>{done ?? ''}</SetupStatus>
    </div>
  )
}

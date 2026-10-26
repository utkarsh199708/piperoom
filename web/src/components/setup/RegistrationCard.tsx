// The registration card (U1-12b; design §3.6 and §3.7, docs/api.md "Registration"). It names who
// approves the registration and where the pipeline will be watched, and it holds the button that
// asks the runner to register the read credentials. The pipeline_register step itself (U1-14) is
// not in this build, so its button is disabled and says so: no call is invented for it.
import { useId } from 'react'
import { RadioTower, ShieldCheck } from 'lucide-react'
import type { RoomState } from '@/state/types'
import type { CurrentUser } from '@/state/permissions'
import { approverCopy, watchedByCopy, watchedByFor } from '@/lib/setupControls'
import { stripBidi } from '@/lib/title'
import { Badge } from '@/components/ui/badge'
import { Button } from '@/components/ui/button'
import { Card } from '@/components/ui/card'
import { CredentialsButton } from './CredentialsButton'

export function RegistrationCard({ state, user }: { state: RoomState; user: CurrentUser }) {
  const id = useId()
  const setup = state.setup
  if (!setup) return null
  const registered = setup.watched_by !== ''
  const watchedBy = watchedByFor(setup.targets, setup.watched_by)
  const solo = state.mode?.solo === true
  const notAvailableId = `${id}-not-available`

  return (
    <Card
      role="region"
      aria-labelledby={`${id}-title`}
      className="mx-4 mt-2 flex min-w-0 flex-col gap-3 p-3 sm:mx-6"
    >
      <h2 id={`${id}-title`} className="m-0 flex items-center gap-2 text-sm font-semibold">
        <ShieldCheck className="size-4 shrink-0 text-muted-foreground" aria-hidden="true" />
        Register the pipeline
        {registered && <Badge variant="success">registered</Badge>}
      </h2>

      <section aria-labelledby={`${id}-approver-title`} className="flex min-w-0 flex-col gap-0.5">
        <h3 id={`${id}-approver-title`} className="m-0 text-xs font-semibold text-foreground">
          Who approves
        </h3>
        <p className="m-0 text-xs text-muted-foreground">{approverCopy(solo)}</p>
      </section>

      <section
        aria-labelledby={`${id}-watched-title`}
        className="flex min-w-0 flex-col gap-0.5"
      >
        <h3
          id={`${id}-watched-title`}
          className="m-0 flex items-center gap-1.5 text-xs font-semibold text-foreground"
        >
          <RadioTower className="size-3.5 shrink-0 text-muted-foreground" aria-hidden="true" />
          Where it will be watched
        </h3>
        <p className="m-0 text-xs text-muted-foreground">{watchedByCopy(watchedBy, registered)}</p>
        {setup.targets.length > 0 && (
          <ul aria-label="Watched targets" className="m-0 flex list-none flex-wrap gap-1.5 p-0">
            {setup.targets.map((target) => (
              <li key={target.address}>
                <Badge variant="outline" mono>
                  {stripBidi(target.address)} · {target.class || 'shared'}
                </Badge>
              </li>
            ))}
          </ul>
        )}
      </section>

      {watchedBy === 'server' ? (
        <CredentialsButton state={state} user={user} />
      ) : (
        <p className="m-0 text-xs text-subtle-foreground">
          Nothing to register with a team server: the local server reads the same{' '}
          <code>.piperoom</code> files.
        </p>
      )}

      {!registered && (
        <div className="flex min-w-0 flex-col gap-1">
          <div>
            {/* U1-14 owns pipeline_register; until it lands there is no route to call. */}
            <Button
              variant="outline"
              size="sm"
              className="coarse:h-11"
              disabled
              aria-describedby={notAvailableId}
            >
              Register pipeline
            </Button>
          </div>
          <p id={notAvailableId} className="m-0 text-xs text-subtle-foreground">
            Not available in this build: the <code>pipeline_register</code> step (U1-14) has not landed.
          </p>
        </div>
      )}
    </Card>
  )
}

import { useState } from 'react'
import { ChevronRight, LogIn } from 'lucide-react'
import { Avatar, AvatarFallback } from '@/components/ui/avatar'
import { Badge, layerVariant } from '@/components/ui/badge'
import { Button } from '@/components/ui/button'
import { Card } from '@/components/ui/card'
import { Kbd } from '@/components/ui/kbd'
import { cn } from '@/lib/utils'
import { useAuthUsers } from '../hooks/useAuthUsers'
import { envLabel } from '@/lib/envLabel'
import { initials, personColor } from '@/lib/people'
import { BrandMark } from './shell/BrandMark'

// Sign-in (docs/design/ui-v2 Sign-In.dc.html). Dev-mode login: pick a users.yaml user.
// This calls POST /auth/dev-login, which issues a real session cookie, so dev and
// production share one cookie path.
//
// `ssoHref` is where "Continue with SSO" goes. This screen is only shown when the
// server runs dev auth, which has no SSO; without a link the button is there but off.
//
// The people come from GET /auth/users, so the list is the server's users.yaml and nothing is baked in here.
export function LoginScreen({
  onPick,
  busy,
  ssoHref,
  notice,
}: {
  onPick: (userId: string) => void
  busy: boolean
  ssoHref?: string
  /** Why the person landed here from an open room (text nodes only). */
  notice?: { kind: 'expired' | 'replaced'; title?: string }
}) {
  const env = envLabel()
  const loaded = useAuthUsers()
  const users = loaded.status === 'ready' ? loaded.data : []
  // Another row is hovered or focused: the first row gives up its "on" highlight, so only one row is lit.
  const [other, setOther] = useState(false)
  return (
    <main className="flex flex-1 flex-col items-center justify-center gap-6 bg-background px-4 py-12 text-sm text-foreground">
      <div className="flex items-center gap-2">
        <BrandMark className="size-6 rounded-md" />
        <span className="text-base font-semibold">Piperoom</span>
      </div>

      <Card className="flex w-full max-w-[400px] flex-col gap-4 p-6 shadow-md">
        <div className="flex flex-col gap-1">
          <h1 className="text-xl font-semibold tracking-tight">Sign in</h1>
          <p className="text-muted-foreground">Your identity is attached to every approval you give.</p>
          {notice && (
            <p role="status" className="m-0 text-xs text-muted-foreground">
              {notice.kind === 'replaced'
                ? 'This tab was signed out because someone signed in as another user in a different tab. Sign in again'
                : 'Your session ended \u2014 sign in'}
              {notice.title ? ` to return to \u201c${notice.title}\u201d.` : ' to return.'}
            </p>
          )}
        </div>

        {ssoHref ? (
          <Button asChild className="h-9 w-full">
            <a href={ssoHref}>
              <LogIn aria-hidden="true" />
              Continue with SSO
            </a>
          </Button>
        ) : (
          <>
            <Button className="h-9 w-full" disabled aria-describedby="sso-off">
              <LogIn aria-hidden="true" />
              Continue with SSO
            </Button>
            <p id="sso-off" className="-mt-2 text-xs text-subtle-foreground">
              SSO is not enabled on this server.
            </p>
          </>
        )}

        <div className="flex items-center gap-2.5 text-xs text-subtle-foreground" aria-hidden="true">
          <span className="h-px flex-1 bg-border" />
          or pick a dev user
          <span className="h-px flex-1 bg-border" />
        </div>

        {loaded.status === 'loading' && (
          <p role="status" className="m-0 text-muted-foreground">
            Loading users…
          </p>
        )}
        {loaded.status === 'error' && (
          <div role="alert" className="flex flex-col items-start gap-2">
            <p className="m-0 text-destructive-fg">Could not load the user list: {loaded.message}</p>
            <Button variant="secondary" onClick={loaded.reload}>
              Try again
            </Button>
          </div>
        )}
        {loaded.status === 'ready' && users.length === 0 && (
          <p className="m-0 text-muted-foreground">The server has no users to sign in as. Check its users.yaml.</p>
        )}

        <ul className="-mx-1 flex flex-col gap-0.5" aria-label="Dev users" hidden={users.length === 0}>
          {users.map((u, i) => {
            // The first row is the one Enter picks (Sign-In.dc.html "on" state): highlighted, with the ⏎ key.
            const first = i === 0
            return (
              <li key={u.id}>
                <button
                  type="button"
                  disabled={busy}
                  onClick={() => onPick(u.id)}
                  // Name is the person's name alone; the layers are in the description.
                  aria-label={u.name}
                  aria-describedby={`dev-user-${u.id}-layers`}
                  data-on={first && !other ? true : undefined}
                  onMouseEnter={first ? undefined : () => setOther(true)}
                  onMouseLeave={first ? undefined : () => setOther(false)}
                  onFocus={first ? undefined : () => setOther(true)}
                  onBlur={first ? undefined : () => setOther(false)}
                  autoFocus={first}
                  className={cn(
                    'flex min-h-11 w-full items-center gap-2.5 rounded-md px-2.5 py-1.5 text-left text-foreground transition-colors hover:bg-accent focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring disabled:cursor-not-allowed disabled:opacity-50',
                    first && !other && 'bg-accent',
                  )}
                >
                  <Avatar className="size-7">
                    <AvatarFallback className="text-[11px]" style={{ backgroundColor: personColor(u.id) }} aria-hidden="true">
                      {initials(u.name)}
                    </AvatarFallback>
                  </Avatar>
                  <span className="font-medium">{u.name}</span>
                  <span id={`dev-user-${u.id}-layers`} className="flex min-w-0 flex-wrap items-center gap-1.5">
                    {u.layers.map((l) => (
                      <Badge key={l} variant={layerVariant(l)} mono>
                        {l}
                      </Badge>
                    ))}
                  </span>
                  {first ? (
                    <Kbd className="ml-auto" aria-hidden="true">
                      ⏎
                    </Kbd>
                  ) : (
                    <ChevronRight className="ml-auto size-4 shrink-0 text-subtle-foreground" aria-hidden="true" />
                  )}
                </button>
              </li>
            )
          })}
        </ul>
      </Card>

      <p className="font-mono text-xs text-subtle-foreground">
        {env ? `${env} · ` : ''}dev users are off in production
      </p>
    </main>
  )
}

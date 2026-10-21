import { Download, Lock } from 'lucide-react'
import { ADMIN_CONFIG_EXPORT_URL } from '@/api/client'
import type { AdminConfig } from '@/api/types'
import { Button } from '@/components/ui/button'
import { Skeleton } from '@/components/ui/skeleton'
import { Tabs, TabsContent, TabsList, TabsTrigger } from '@/components/ui/tabs'
import { useCurrentUser } from '@/context/user'
import { useAdminConfig } from '@/hooks/useAdminConfig'
import { lines } from '@/lib/settings'
import { ConnectionsTab } from './ConnectionsTab'
import { PeopleTab } from './PeopleTab'
import { PipelinesTab } from './PipelinesTab'

// Admin settings, phase 1 (docs/design/onboarding.md): shows what the server runs with and tests its
// connections. Nothing here writes configuration; the owners file, users file and environment stay the
// source of truth. The shell's top bar carries the page title ("Settings").
export function SettingsPage() {
  const { user } = useCurrentUser()
  const admin = user.admin === true
  // A non-admin never asks: the server would answer 403, and the state below is the same.
  const { view, reload } = useAdminConfig(admin)

  if (!admin || view.phase === 'forbidden') return <AdminsOnly />

  return (
    <div className="mx-auto flex w-full max-w-5xl flex-col gap-5 px-4 py-6 text-sm sm:px-6">
      {view.phase === 'loading' && (
        <div role="status" aria-label="Loading settings" className="flex flex-col gap-4">
          <Skeleton className="h-4 w-80 max-w-full" />
          <Skeleton className="h-9 w-72 max-w-full" />
          <Skeleton className="h-[88px] w-full" />
          <Skeleton className="h-64 w-full" />
        </div>
      )}
      {view.phase === 'error' && (
        <div role="alert" className="flex flex-wrap items-center gap-3">
          <p className="m-0 font-medium text-destructive-fg">{view.message}</p>
          <Button type="button" variant="outline" size="sm" onClick={reload}>
            Retry
          </Button>
        </div>
      )}
      {view.phase === 'ready' && <Loaded config={view.config} />}
    </div>
  )
}

function AdminsOnly() {
  return (
    <div className="mx-auto flex w-full max-w-xl flex-col items-center gap-2 px-4 py-16 text-center text-sm">
      <Lock className="size-6 text-subtle-foreground" aria-hidden="true" />
      <h2 className="m-0 text-base font-semibold">Settings are for admins</h2>
      <p className="m-0 text-muted-foreground">
        Ask an admin of this Piperoom (<code className="font-mono">admin: true</code> in the users file) if you need
        to see how it is configured.
      </p>
    </div>
  )
}

function Loaded({ config }: { config: AdminConfig }) {
  const connections = lines(config.connections)
  const pipelines = lines(config.pipelines)
  const people = lines(config.people)
  return (
    <>
      <section aria-label="Where this configuration comes from" className="flex flex-wrap items-start justify-between gap-3">
        <div className="flex min-w-0 flex-col gap-1">
          <p className="m-0 text-muted-foreground">The systems, pipelines and people this Piperoom runs with.</p>
          <p className="m-0 text-xs leading-5 text-subtle-foreground">
            <Lock className="mr-1.5 inline size-3.5 -translate-y-px align-middle" aria-hidden="true" />
            Read only. Managed in{' '}
            {[config.owners_file, config.users_file].filter(Boolean).map((f) => (
              <span key={f}>
                <code className="font-mono text-muted-foreground">{f}</code>
                {', '}
              </span>
            ))}
            and environment variables; change those and restart. Editing here is coming.
          </p>
        </div>
        <Button asChild variant="outline" size="sm">
          <a href={ADMIN_CONFIG_EXPORT_URL} download="piperoom-config.yaml">
            <Download aria-hidden="true" />
            Export YAML
          </a>
        </Button>
      </section>

      <Tabs defaultValue="connections" className="flex flex-col gap-4">
        <TabsList aria-label="Settings sections" className="self-start">
          <TabsTrigger value="connections">
            Connections <Count n={connections.length} />
          </TabsTrigger>
          <TabsTrigger value="pipelines">
            Pipelines <Count n={pipelines.length} />
          </TabsTrigger>
          <TabsTrigger value="people">
            People <Count n={people.length} />
          </TabsTrigger>
        </TabsList>
        <TabsContent value="connections">
          <ConnectionsTab connections={connections} />
        </TabsContent>
        <TabsContent value="pipelines">
          <PipelinesTab pipelines={pipelines} />
        </TabsContent>
        <TabsContent value="people">
          <PeopleTab people={people} />
        </TabsContent>
      </Tabs>
    </>
  )
}

function Count({ n }: { n: number }) {
  return <span className="font-mono text-[11px] text-subtle-foreground">{n}</span>
}

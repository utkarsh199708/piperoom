import { Download, Info, Lock } from 'lucide-react'
import { ADMIN_CONFIG_EXPORT_URL } from '@/api/client'
import type { AdminConfig } from '@/api/types'
import { Button } from '@/components/ui/button'
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
    <div className="mx-auto flex w-full max-w-5xl flex-col gap-4 px-4 py-6 text-sm sm:px-6">
      {view.phase === 'loading' && (
        <p role="status" className="m-0 text-muted-foreground">
          Loading settings…
        </p>
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
      <section
        aria-label="Where this configuration comes from"
        className="flex flex-wrap items-start gap-3 rounded-lg border border-border bg-info-subtle p-3 text-info-foreground"
      >
        <Info className="mt-0.5 size-4 shrink-0" aria-hidden="true" />
        <p className="m-0 min-w-0 flex-1">
          <strong className="font-semibold">Read only.</strong> This configuration comes from the owners file
          {config.owners_file && (
            <>
              {' '}
              (<code className="font-mono">{config.owners_file}</code>)
            </>
          )}
          , the users file
          {config.users_file && (
            <>
              {' '}
              (<code className="font-mono">{config.users_file}</code>)
            </>
          )}{' '}
          and the environment. Editing in the app comes later; for now change those and restart.
        </p>
        <Button asChild variant="outline" size="sm" className="bg-background text-foreground">
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

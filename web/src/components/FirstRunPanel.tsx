import { useState } from 'react'
import type { ReactNode } from 'react'
import { Check, Copy, Send } from 'lucide-react'
import { toast } from 'sonner'
import { api, ApiClientError, isFixtures } from '../api/client'
import { useLoad } from '../hooks/useLoad'
import { usePipelines } from '../hooks/usePipelines'
import { DAG_PLACEHOLDER, PIPELINE_PLACEHOLDER, testAlertCommand } from '../lib/firstRun'
import { QUICKSTART_DOC_PATH } from '../lib/writeMode'
import { Button } from './ui/button'
import { Card } from './ui/card'
import { Skeleton } from './ui/skeleton'

export const FIRST_RUN_TITLE = 'No rooms yet. Send a test alert:'

type AuthGuess = 'dev' | 'oidc'

// The session does not say the server's auth mode, but GET /auth/users is mounted in dev mode only
// (docs/api.md, Login picker): a 404 means oidc, where the unsigned webhook is not mounted either.
// Anything else (a network error, an old server) keeps the dev command: it is what the quickstart
// runs. Module level, so useLoad gets a stable loader.
const loadAuthGuess = (): Promise<AuthGuess> =>
  isFixtures()
    ? Promise.resolve('dev')
    : Promise.resolve()
        .then(() => api.authUsers())
        .then(
          (): AuthGuess => 'dev',
          (e: unknown): AuthGuess => (e instanceof ApiClientError && e.status === 404 ? 'oidc' : 'dev'),
        )

/**
 * First run (P1e-2): the room list is empty, so the most useful thing on screen is the one command
 * that opens a room. RoomList shows it only while the server has no room at all and only to a
 * person who may post alerts (a viewer gets 403 viewer).
 */
export function FirstRunPanel({ userId }: { userId: string }) {
  const auth = useLoad(loadAuthGuess)
  const pipelines = usePipelines()
  const [copied, setCopied] = useState(false)

  if (auth.status === 'ready' && auth.data === 'oidc') {
    return (
      <Shell>
        <p className="m-0 text-sm text-muted-foreground">
          This server signs people in with SSO, so the unsigned test webhook is off. Point your alerting at a signed
          route, <code className="font-mono text-[0.95em]">POST /api/alerts/&#123;source&#125;</code>; see{' '}
          <code className="font-mono text-[0.95em]">docs/pilot/setup.md</code> §6.
        </p>
      </Shell>
    )
  }

  if (auth.status === 'loading' || pipelines.status === 'loading') {
    return (
      <Shell>
        <div role="status" aria-label="Loading the test alert" className="flex flex-col gap-2">
          <Skeleton className="h-4 w-full" />
          <Skeleton className="h-4 w-2/3" />
        </div>
      </Shell>
    )
  }

  const list = pipelines.status === 'ready' ? pipelines.data : null
  const alert = testAlertCommand(window.location.origin, userId, list)
  const copy = () => {
    if (!navigator.clipboard) {
      toast.error('Copy is not available in this browser. Select the command instead.')
      return
    }
    navigator.clipboard.writeText(alert.command).then(
      () => {
        setCopied(true)
        toast.success('Test alert command copied')
      },
      () => toast.error('Could not copy. Select the command instead.'),
    )
  }

  return (
    <Shell>
      <div className="flex min-w-0 items-start gap-2">
        {/* whitespace-pre keeps the two lines exactly as copied; on a phone the block scrolls, the page does not. */}
        <pre
          aria-label="Test alert command"
          className="m-0 min-w-0 flex-1 overflow-x-auto whitespace-pre rounded-md border border-border bg-muted px-3 py-2 font-mono text-xs"
        >
          {alert.command}
        </pre>
        <Button
          variant="outline"
          size="icon"
          className="shrink-0 coarse:size-11"
          aria-label={copied ? 'Copied test alert command' : 'Copy test alert command'}
          onClick={copy}
        >
          {copied ? <Check aria-hidden="true" /> : <Copy aria-hidden="true" />}
        </Button>
      </div>
      <p className="m-0 text-xs text-muted-foreground">
        {alert.pipeline ? (
          <>
            Opens a room on <span className="font-mono">{alert.pipeline.id}</span>
            {list && list.length > 1 ? `, the first of ${list.length} pipelines in owners.yaml` : ''}. The agent starts
            at once.
          </>
        ) : pipelines.status === 'error' ? (
          <>
            Could not load the pipelines ({pipelines.message}). Replace{' '}
            <code className="font-mono">{PIPELINE_PLACEHOLDER}</code> with a pipeline id from owners.yaml.
          </>
        ) : (
          <>
            This server knows no pipeline yet. Write owners.yaml with{' '}
            <code className="font-mono">go run ./cmd/discover -write-owners owners.yaml</code>, restart the server, and
            replace <code className="font-mono">{PIPELINE_PLACEHOLDER}</code> with its id.
          </>
        )}
        {alert.batch && (
          <>
            {' '}
            A batch pipeline: put one of its DAG ids in place of{' '}
            <code className="font-mono">{DAG_PLACEHOLDER}</code>; an Airflow alert that sends only{' '}
            <code className="font-mono">dag_id</code> finds the pipeline by it.
          </>
        )}{' '}
        More in <code className="font-mono">{QUICKSTART_DOC_PATH}</code>.
      </p>
    </Shell>
  )
}

function Shell({ children }: { children: ReactNode }) {
  return (
    <Card role="region" aria-labelledby="first-run-title" className="flex min-w-0 flex-col gap-3 p-4">
      <h2 id="first-run-title" className="m-0 flex items-center gap-2 text-base font-semibold">
        <Send className="size-4 shrink-0 text-muted-foreground" aria-hidden="true" />
        {FIRST_RUN_TITLE}
      </h2>
      {children}
    </Card>
  )
}

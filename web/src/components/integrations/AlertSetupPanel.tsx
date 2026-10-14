import { useCallback, useEffect, useState } from 'react'
import { RefreshCw, TriangleAlert } from 'lucide-react'
import { integrationsApi } from '../../api/integrations'
import type { AlertSetup } from '../../api/integrations'
import { ApiClientError } from '../../api/client'
import { integrationErrorText } from '../../lib/integrationErrors'
import { Button } from '../ui/button'
import { Dialog, DialogContent, DialogDescription, DialogFooter, DialogHeader, DialogTitle } from '../ui/dialog'
import { Input } from '../ui/input'
import { CopyButton } from './CopyButton'

export type AlertsApi = Pick<typeof integrationsApi, 'alertSetup' | 'rotateAlertSecret'>

/**
 * How to point an alert tool at Piperoom: the webhook URL, the copy-paste snippet (with
 * `<secret>` in place of the secret) and, for sources whose secret Piperoom makes, a button
 * that generates a new one. The new secret is shown here once and is never readable again.
 * `ownsSecret`: the tool makes the secret itself (PagerDuty); it is pasted into the form instead.
 */
export function AlertSetupPanel({
  source,
  ownsSecret,
  api = integrationsApi,
  onChanged,
}: {
  source: string
  ownsSecret: boolean
  api?: AlertsApi
  onChanged?: () => void
}) {
  const [setup, setSetup] = useState<AlertSetup | null>(null)
  const [error, setError] = useState<string | null>(null)
  const [secret, setSecret] = useState<string | null>(null)
  const [restartRequired, setRestartRequired] = useState(false)
  const [confirm, setConfirm] = useState(false)
  const [busy, setBusy] = useState(false)

  const load = useCallback(async () => {
    setError(null)
    try {
      setSetup(await api.alertSetup(source))
    } catch (e) {
      setError(`Could not load the alert setup: ${integrationErrorText(e)}`)
    }
  }, [api, source])
  useEffect(() => {
    void load()
  }, [load])

  const rotate = async () => {
    setConfirm(false)
    setBusy(true)
    setError(null)
    try {
      const res = await api.rotateAlertSecret(source)
      const { secret: value, restart_required: restart, ...rest } = res
      setSetup(rest)
      setSecret(value)
      setRestartRequired(restart === true)
      onChanged?.()
    } catch (e) {
      setError(
        e instanceof ApiClientError && e.code === 'managed_by_env'
          ? 'This secret is set by an environment variable; change it on the server.'
          : integrationErrorText(e),
      )
    } finally {
      setBusy(false)
    }
  }

  if (!setup) {
    return error ? (
      <p role="alert" className="m-0 text-sm text-destructive-fg">
        {error}
      </p>
    ) : (
      <p className="m-0 text-sm text-muted-foreground">Loading alert setup…</p>
    )
  }

  const fromEnv = setup.secret_source === 'env'
  const canRotate = !ownsSecret && !fromEnv
  const urlId = `alert-url-${source}`
  return (
    <section aria-label="Alert source setup" className="flex flex-col gap-4">
      <div className="flex flex-col gap-1.5">
        <label htmlFor={urlId} className="text-sm font-medium">
          Webhook URL
        </label>
        <div className="flex gap-2">
          <Input id={urlId} readOnly value={setup.webhook_url} className="font-mono" onFocus={(e) => e.target.select()} />
          <CopyButton text={setup.webhook_url} label="Copy webhook URL" />
        </div>
      </div>

      <div className="flex flex-col gap-1.5">
        <div className="flex items-center justify-between gap-2">
          <h3 className="m-0 text-sm font-medium">Configuration snippet</h3>
          <CopyButton text={setup.snippet} label="Copy configuration snippet" />
        </div>
        <pre
          className="m-0 max-h-72 overflow-auto whitespace-pre rounded-md border border-border bg-sidebar p-3 font-mono text-xs"
          data-lang={setup.snippet_lang}
        >
          {setup.snippet}
        </pre>
        <p className="m-0 text-xs text-muted-foreground">
          <code className="font-mono">&lt;secret&gt;</code> stands for the signing secret.
        </p>
      </div>

      <div className="flex flex-col gap-2 rounded-md border border-border p-3">
        <div className="flex flex-wrap items-center gap-2">
          <span className="text-sm font-medium">Signing secret</span>
          <span className="text-sm text-muted-foreground" role="status">
            {setup.secret_set
              ? fromEnv
                ? 'Set by an environment variable'
                : 'Set'
              : ownsSecret
                ? 'Not set: paste the one the tool shows below'
                : 'Not set yet'}
          </span>
          {canRotate && (
            <Button
              variant="secondary"
              size="sm"
              className="ml-auto"
              disabled={busy}
              onClick={() => (setup.secret_set ? setConfirm(true) : void rotate())}
            >
              <RefreshCw aria-hidden="true" />
              {setup.secret_set ? 'Rotate secret' : 'Generate secret'}
            </Button>
          )}
        </div>
        {secret && (
          <div className="flex flex-col gap-2 rounded-md bg-warning-subtle p-3" role="alert">
            <p className="m-0 flex items-center gap-1.5 text-sm text-warning-foreground">
              <TriangleAlert className="size-4" aria-hidden="true" />
              Copy it now. Piperoom never shows it again.
            </p>
            {restartRequired && (
              <p className="m-0 text-sm text-warning-foreground">
                The server accepts the new secret after its next restart. Until then the old secret keeps
                working and alerts signed with the new one are refused.
              </p>
            )}
            <div className="flex gap-2">
              <label htmlFor={`${urlId}-secret`} className="sr-only">
                New signing secret
              </label>
              <Input id={`${urlId}-secret`} readOnly value={secret} className="font-mono" onFocus={(e) => e.target.select()} />
              <CopyButton text={secret} label="Copy new signing secret" />
            </div>
            <div>
              <Button variant="ghost" size="sm" onClick={() => setSecret(null)}>
                I have copied it
              </Button>
            </div>
          </div>
        )}
      </div>
      {error && (
        <p role="alert" className="m-0 text-sm text-destructive-fg">
          {error}
        </p>
      )}

      <Dialog open={confirm} onOpenChange={setConfirm}>
        <DialogContent>
          <DialogHeader>
            <DialogTitle>Rotate the signing secret?</DialogTitle>
            <DialogDescription>
              A new secret is made and shown once. The server accepts it after its next restart; until then the
              current secret keeps working, so update the tool and restart Piperoom together.
            </DialogDescription>
          </DialogHeader>
          <DialogFooter>
            <Button variant="secondary" onClick={() => setConfirm(false)}>
              Cancel
            </Button>
            <Button variant="destructive" onClick={() => void rotate()}>
              Rotate secret
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>
    </section>
  )
}

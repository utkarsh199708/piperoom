import { useCallback, useEffect, useState } from 'react'
import { Bug } from 'lucide-react'
import { Button } from '@/components/ui/button'
import {
  DropdownMenu,
  DropdownMenuContent,
  DropdownMenuLabel,
  DropdownMenuTrigger,
} from '@/components/ui/dropdown-menu'
import { isDev } from '@/lib/env'

type Op = 'break' | 'fix'

// A `devctl serve` API failure (cmd/devctl/serve.go). 409 means another run is
// already in flight ("busy"); any other non-2xx or a network failure is a plain
// error. A 404 (unknown scenario) and a 500 (the run itself failed) both come
// back as status "error" with an `error` field.
class DevctlError extends Error {
  status: number
  constructor(status: number, message: string) {
    super(message)
    this.name = 'DevctlError'
    this.status = status
  }
  get busy(): boolean {
    return this.status === 409
  }
}

// Same-origin only: the Vite dev server proxies /devctl to `devctl serve` (see
// vite.config.ts), which strips the prefix. POSTs carry Content-Type:
// application/json because serve.go requires a non-simple marker before it runs
// anything (so a cross-site "simple" request cannot skip the CORS preflight).
async function devctl(path: string, init?: RequestInit): Promise<Response> {
  const res = await fetch(path, init)
  if (res.ok) return res
  let message = ''
  try {
    const body = (await res.json()) as { error?: string }
    message = body.error ?? ''
  } catch {
    // A non-JSON error body (e.g. the proxy could not reach devctl); keep statusText.
  }
  throw new DevctlError(res.status, message || res.statusText)
}

async function listScenarios(): Promise<string[]> {
  const res = await devctl('/devctl/scenarios')
  const body = (await res.json()) as { scenarios?: string[] }
  return body.scenarios ?? []
}

async function runDevctl(op: Op, name: string): Promise<void> {
  await devctl(`/devctl/${op}/${encodeURIComponent(name)}`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: '{}',
  })
}

type Load =
  | { status: 'loading' }
  | { status: 'error'; message: string }
  | { status: 'ready'; scenarios: string[] }

type Notice = { kind: 'busy' | 'error'; text: string }

// The dev-only "Break something" menu. It never renders in a production build:
// AppShell loads it behind `import.meta.env.DEV` (so the chunk and its "/devctl"
// strings are tree-shaken out of dist), and isDev is false in prod anyway.
export function BreakSomethingMenu() {
  if (!isDev) return null
  return (
    <DropdownMenu>
      <DropdownMenuTrigger asChild>
        <Button variant="ghost" size="icon" aria-label="Break something" title="Break something (dev)">
          <Bug aria-hidden="true" />
        </Button>
      </DropdownMenuTrigger>
      <DropdownMenuContent align="end" className="w-80">
        <DropdownMenuLabel>Break something (dev)</DropdownMenuLabel>
        <ScenarioList />
      </DropdownMenuContent>
    </DropdownMenu>
  )
}

// The scenario rows. It is mounted only while the menu is open, so the GET
// /devctl/scenarios happens once per open and never for a menu that is not used.
function ScenarioList() {
  const [load, setLoad] = useState<Load>({ status: 'loading' })
  const [running, setRunning] = useState<string | null>(null)
  const [notice, setNotice] = useState<Notice | null>(null)

  const reload = useCallback(async () => {
    setLoad({ status: 'loading' })
    setNotice(null)
    try {
      setLoad({ status: 'ready', scenarios: await listScenarios() })
    } catch (err) {
      setLoad({ status: 'error', message: err instanceof Error ? err.message : String(err) })
    }
  }, [])

  useEffect(() => {
    void reload()
  }, [reload])

  const run = useCallback(async (op: Op, name: string) => {
    setRunning(`${op}:${name}`)
    setNotice(null)
    try {
      await runDevctl(op, name)
    } catch (err) {
      if (err instanceof DevctlError && err.busy) {
        setNotice({ kind: 'busy', text: 'Busy: another run is in progress.' })
      } else {
        setNotice({ kind: 'error', text: err instanceof Error ? err.message : String(err) })
      }
    } finally {
      setRunning(null)
    }
  }, [])

  if (load.status === 'loading') {
    return <DropdownMenuLabel className="font-normal">Loading scenarios…</DropdownMenuLabel>
  }
  if (load.status === 'error') {
    return (
      <div className="px-2 py-1.5">
        <p className="text-xs text-destructive-fg">Could not load scenarios: {load.message}</p>
        <Button variant="outline" size="sm" className="mt-1" onClick={() => void reload()}>
          Retry
        </Button>
      </div>
    )
  }
  const busy = running !== null
  return (
    <div className="flex flex-col gap-0.5 py-1">
      {notice && (
        <p role="status" className="px-2 py-1 text-xs text-destructive-fg">
          {notice.text}
        </p>
      )}
      {load.scenarios.map((name) => (
        <div key={name} className="flex items-center gap-2 px-2 py-1">
          <span className="min-w-0 flex-1 truncate font-mono text-xs">{name}</span>
          <Button
            variant="outline"
            size="sm"
            disabled={busy}
            aria-label={`Break ${name}`}
            onClick={() => void run('break', name)}
          >
            Break
          </Button>
          <Button
            variant="outline"
            size="sm"
            disabled={busy}
            aria-label={`Fix ${name}`}
            onClick={() => void run('fix', name)}
          >
            Fix
          </Button>
        </div>
      ))}
    </div>
  )
}

import { useCallback, useEffect, useMemo, useState } from 'react'
import { RefreshCw } from 'lucide-react'
import { integrationsApi } from '../../api/integrations'
import type { CatalogKind, Integration } from '../../api/integrations'
import { useCurrentUser } from '../../context/user'
import { checklistRows, integrationForKind, secretManagers } from '../../lib/integrations'
import type { ChecklistRow } from '../../lib/integrations'
import { Button } from '../ui/button'
import { Skeleton } from '../ui/skeleton'
import { CatalogGrid } from './CatalogGrid'
import { ConnectDrawer } from './ConnectDrawer'
import { integrationErrorText } from '../../lib/integrationErrors'
import type { DrawerApi } from './ConnectDrawer'
import { SetupChecklist } from './SetupChecklist'

interface Target {
  /** Bumps on every open, so the drawer starts fresh for each card. */
  token: number
  kindId: string
  integrationId?: string
}

/**
 * Settings → Integrations (integrations-ux.md §3.3, §3.4, §4). Admins get the setup checklist,
 * the catalog and the connect drawer. Everyone else sees the catalog read-only: what each
 * card asks for and what Piperoom reads (GET /catalog is the one route open to every user).
 */
export function IntegrationsPage({ api = integrationsApi }: { api?: DrawerApi }) {
  const { user } = useCurrentUser()
  const admin = user.admin === true
  const [catalog, setCatalog] = useState<CatalogKind[] | null>(null)
  const [list, setList] = useState<Integration[]>([])
  const [error, setError] = useState<string | null>(null)
  const [target, setTarget] = useState<Target | null>(null)
  const [drawerOpen, setDrawerOpen] = useState(false)
  const [secretKeySet, setSecretKeySet] = useState(true)

  const loadList = useCallback(async () => {
    if (!admin) return
    try {
      setList(await api.list())
      setError(null)
    } catch (e) {
      setError(integrationErrorText(e))
    }
  }, [api, admin])

  const load = useCallback(async () => {
    setError(null)
    try {
      const [cat] = await Promise.all([api.catalog(), loadList()])
      setCatalog(cat)
    } catch (e) {
      setError(integrationErrorText(e))
    }
  }, [api, loadList])

  useEffect(() => {
    void load()
  }, [load])

  // Whether typed-in secrets can be saved; on any error the drawer stays as it was.
  useEffect(() => {
    if (!admin) return
    let live = true
    void (async () => {
      try {
        const st = await api.status()
        if (live) setSecretKeySet(st.secret_key_set)
      } catch {
        // An older server without the route: keep the default.
      }
    })()
    return () => {
      live = false
    }
  }, [api, admin])

  const rows = useMemo(() => checklistRows(catalog ?? [], list), [catalog, list])
  const managers = useMemo(() => secretManagers(list), [list])

  const open = (kind: CatalogKind, existing?: Integration) => {
    setTarget((t) => ({ token: (t?.token ?? 0) + 1, kindId: kind.id, integrationId: existing?.id }))
    setDrawerOpen(true)
  }
  const openRow = (row: ChecklistRow) => {
    const first = row.integrations[0]
    const kind = (first && row.kinds.find((k) => k.id === first.kind)) ?? row.kinds[0]
    if (kind) open(kind, first ?? integrationForKind(kind, list))
  }
  const openIntegration = (i: Integration) => {
    const kind = catalog?.find((k) => k.id === i.kind)
    if (kind) open(kind, i)
  }

  const kind = target ? catalog?.find((k) => k.id === target.kindId) : undefined
  const existing = target?.integrationId ? list.find((i) => i.id === target.integrationId) : undefined

  const onSaved = (i: Integration) => {
    setList((l) => (l.some((x) => x.id === i.id) ? l.map((x) => (x.id === i.id ? i : x)) : [...l, i]))
    setTarget((t) => (t ? { ...t, integrationId: i.id } : t))
  }
  const onDeleted = (id: string) => {
    setList((l) => l.filter((x) => x.id !== id))
    setDrawerOpen(false)
  }

  return (
    <div className="mx-auto flex w-full max-w-[1120px] flex-col gap-6 px-4 py-6 md:px-10 md:py-8">
      <header className="flex flex-wrap items-end justify-between gap-4">
        <div className="flex flex-col gap-1">
          <h2 className="m-0 text-2xl font-semibold tracking-tight">Connect your pipeline</h2>
          <p className="m-0 text-sm text-muted-foreground">
            {admin
              ? 'Each card is tested before it is saved. Secrets stay write-only; prefer a secret manager reference.'
              : 'Only an admin can change integrations. You can see what each card asks for and what Piperoom reads.'}
          </p>
        </div>
        <Button variant="secondary" size="sm" onClick={() => void load()}>
          <RefreshCw aria-hidden="true" />
          Refresh
        </Button>
      </header>

      {error && (
        <p role="alert" className="m-0 rounded-md border border-border bg-destructive-subtle p-3 text-sm text-destructive-fg">
          {error}
        </p>
      )}

      {catalog === null && !error && (
        <div className="flex flex-col gap-3" aria-busy="true" aria-label="Loading integrations">
          <Skeleton className="h-20 w-full" />
          <Skeleton className="h-64 w-full" />
        </div>
      )}

      {catalog !== null && (
        <>
          {admin && (
            <SetupChecklist
              rows={rows}
              managers={managers}
              admin={admin}
              onOpenRow={openRow}
              onOpenIntegration={openIntegration}
            />
          )}
          <section aria-labelledby="catalog-heading" className="flex flex-col gap-4">
            <h2 id="catalog-heading" className="m-0 text-[15px] font-semibold">
              Catalog
            </h2>
            <CatalogGrid catalog={catalog} list={list} admin={admin} onOpen={open} />
          </section>
        </>
      )}

      {target && kind && (
        <ConnectDrawer
          key={target.token}
          open={drawerOpen}
          onOpenChange={setDrawerOpen}
          kind={kind}
          existing={existing}
          managers={managers.filter((m) => m.id !== existing?.id)}
          admin={admin}
          api={api}
          onSaved={onSaved}
          onDeleted={onDeleted}
          onReload={loadList}
          onRefresh={() => void loadList()}
          secretKeySet={secretKeySet}
        />
      )}
    </div>
  )
}

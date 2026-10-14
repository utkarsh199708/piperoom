import type { ReactNode } from 'react'
import type { Integration } from '../../api/integrations'
import { checklistProgress, joinAnd, managerName, verifiedReadOnly, viewStatus } from '../../lib/integrations'
import type { ChecklistRow } from '../../lib/integrations'
import { Button } from '../ui/button'
import { Progress } from '../ui/progress'
import { EnvPill, StatusPill } from './StatusPill'

function Logo({ text }: { text: string }) {
  return (
    <span
      aria-hidden="true"
      className="flex size-10 shrink-0 items-center justify-center rounded-lg border border-input bg-secondary font-mono text-xs font-semibold uppercase"
    >
      {text.slice(0, 2)}
    </span>
  )
}

function rowDetail(row: ChecklistRow): string {
  if (row.integrations.length === 0) {
    return row.kinds.map((k) => k.title).join(' · ')
  }
  if (row.status === 'attention') {
    // Name what is wrong, as the mockup does, rather than who set it up.
    for (const i of row.integrations) {
      const failed = i.last_test.find((c) => c.status === 'fail')
      if (failed) return row.integrations.length > 1 ? `${i.title}: ${failed.title}` : failed.title
    }
  }
  const titles = row.integrations.map((i) => i.title).join(', ')
  if (row.managedByEnv) return `${titles} · set by environment variables`
  const i = row.integrations[0]
  return i.updated_by ? `${titles} · by ${i.updated_by}` : titles
}

function Row({ row, admin, onOpen }: { row: ChecklistRow; admin: boolean; onOpen: (row: ChecklistRow) => void }) {
  const has = row.integrations.length > 0
  const action = row.managedByEnv || !admin ? 'View' : has ? 'Manage' : 'Connect'
  return (
    <li className="flex flex-wrap items-center gap-4 border-t border-border px-5 py-4" data-category={row.category}>
      <Logo text={row.kinds[0]?.logo || row.category} />
      <div className="min-w-[200px] flex-1">
        <div className="font-medium">{row.label}</div>
        <div className="truncate text-sm text-muted-foreground">{rowDetail(row)}</div>
      </div>
      {row.managedByEnv && <EnvPill />}
      <StatusPill status={row.status} readOnlyVerified={row.readOnlyVerified} />
      <Button
        variant={action === 'Connect' ? 'default' : 'secondary'}
        size="sm"
        onClick={() => onOpen(row)}
        aria-label={`${action} ${row.label}`}
      >
        {action}
      </Button>
    </li>
  )
}

function Group({ title, children }: { title: string; children: ReactNode }) {
  const id = `checklist-${title.replace(/\W+/g, '-').toLowerCase()}`
  return (
    <div>
      <h2 id={id} className="m-0 border-t border-border px-5 py-3 text-[15px] font-semibold first:border-t-0">
        {title}
      </h2>
      <ul aria-labelledby={id} className="m-0 list-none p-0">
        {children}
      </ul>
    </div>
  )
}

/**
 * The home of setup (integrations-ux.md §3.3): a progress bar, the rows needed for a first
 * room, then the optional ones, then the connected secret managers.
 */
export function SetupChecklist({
  rows,
  managers,
  admin,
  onOpenRow,
  onOpenIntegration,
}: {
  rows: ChecklistRow[]
  managers: Integration[]
  admin: boolean
  onOpenRow: (row: ChecklistRow) => void
  onOpenIntegration: (i: Integration) => void
}) {
  const progress = checklistProgress(rows)
  const pct = progress.total === 0 ? 0 : Math.round((progress.connected / progress.total) * 100)
  const required = rows.filter((r) => r.required)
  const optional = rows.filter((r) => !r.required)
  return (
    <div className="flex flex-col gap-6">
      <section aria-label="Setup progress" className="flex flex-col gap-3 rounded-lg border border-border bg-card px-5 py-4">
        <div className="flex flex-wrap justify-between gap-2">
          <span className="font-medium">
            {progress.connected} of {progress.total} connected
          </span>
          <span className="text-sm text-muted-foreground">
            {progress.missingRequired.length === 0
              ? 'Everything needed for a first room is connected.'
              : `You can open your first room once ${joinAnd(progress.missingRequired)} ${progress.missingRequired.length === 1 ? 'is' : 'are'} connected.`}
          </span>
        </div>
        <Progress
          value={pct}
          aria-label="Connections set up"
          aria-valuetext={`${progress.connected} of ${progress.total} connected`}
          className="h-2"
          indicatorClassName="bg-success"
        />
      </section>

      <section aria-label="Connections" className="rounded-lg border border-border bg-card">
        {required.length > 0 && (
          <Group title="Required for a first room">
            {required.map((r) => (
              <Row key={r.category} row={r} admin={admin} onOpen={onOpenRow} />
            ))}
          </Group>
        )}
        {optional.length > 0 && (
          <Group title="Optional for now">
            {optional.map((r) => (
              <Row key={r.category} row={r} admin={admin} onOpen={onOpenRow} />
            ))}
          </Group>
        )}
        {managers.length > 0 && (
          <Group title="Secret managers">
            {managers.map((m) => {
              const env = m.source === 'env'
              const label = `${managerName(m) || m.id} · ${m.title}`
              return (
                <li key={m.id} className="flex flex-wrap items-center gap-4 border-t border-border px-5 py-4">
                  <Logo text="SM" />
                  <div className="min-w-[200px] flex-1">
                    <div className="font-medium">{label}</div>
                    <div className="text-sm text-muted-foreground">
                      Credentials stay in your vault; Piperoom reads them at run time.
                    </div>
                  </div>
                  {env && <EnvPill />}
                  <StatusPill status={viewStatus(m)} readOnlyVerified={verifiedReadOnly(m)} />
                  <Button variant="secondary" size="sm" onClick={() => onOpenIntegration(m)} aria-label={`${env || !admin ? 'View' : 'Manage'} ${label}`}>
                    {env || !admin ? 'View' : 'Manage'}
                  </Button>
                </li>
              )
            })}
          </Group>
        )}
      </section>
    </div>
  )
}

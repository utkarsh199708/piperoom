// Q7g: the whole change a repo_open_pull_request step will write, read before anyone approves
// (docs/design/fix-pr.md §10). Per file a collapsible block: status, +n -m, line numbers, every added
// and removed line marked by +/- as well as by colour. Every string came from a repository: it is
// rendered as text nodes only, never as HTML or markdown.
import { useId, useState } from 'react'
import { ChevronRight, ExternalLink, GitBranch, TriangleAlert } from 'lucide-react'
import type { DiffPreview as DiffPreviewData, FileDiff } from '../../api/types'
import { Badge, layerLabel, layerVariant } from '@/components/ui/badge'
import { cn } from '@/lib/utils'
import {
  PR_APPROVAL_NOTE,
  countText,
  countWords,
  hasHidden,
  parseUnified,
  revealHidden,
  safeHref,
  shortSha,
} from '../../lib/fixpr'
import type { DiffLine } from '../../lib/fixpr'

const ROW: Record<DiffLine['kind'], string> = {
  add: 'bg-success-subtle',
  del: 'bg-destructive-subtle',
  ctx: '',
  hunk: 'bg-muted text-subtle-foreground',
  note: 'text-subtle-foreground',
}

const MARKER_WORD: Record<string, string> = { '+': 'added', '-': 'removed', ' ': 'unchanged' }

function FileBlock({ file, index }: { file: FileDiff; index: number }) {
  const [open, setOpen] = useState(true)
  const bodyId = useId()
  const lines = parseUnified(file.unified)
  const path = revealHidden(file.path)
  const added = file.status === 'added'
  return (
    <li className="flex min-w-0 flex-col rounded border border-border bg-card" data-testid={`diff-file-${index}`}>
      <h5 className="m-0 text-xs font-normal">
        <button
          type="button"
          aria-expanded={open}
          aria-controls={bodyId}
          onClick={() => setOpen((o) => !o)}
          className="flex w-full min-w-0 flex-wrap items-center gap-x-2 gap-y-1 rounded px-2 py-1.5 text-left outline-none hover:bg-muted/60 focus-visible:ring-2 focus-visible:ring-ring"
        >
          <ChevronRight className={cn('size-3.5 shrink-0 transition-transform', open && 'rotate-90')} aria-hidden="true" />
          <Badge variant={added ? 'info' : 'neutral'}>{added ? 'added' : file.status === 'modified' ? 'modified' : revealHidden(file.status)}</Badge>
          <span className="min-w-0 flex-1 break-all font-mono text-[12.5px] font-medium">{path}</span>
          <span className="font-mono text-xs tabular-nums" aria-hidden="true">
            <span className="text-success-foreground">+{file.added}</span>{' '}
            <span className="text-destructive-fg">-{file.removed}</span>
          </span>
          <span className="sr-only">{countWords(file.added, file.removed)}</span>
        </button>
      </h5>
      <div id={bodyId} hidden={!open} className="border-t border-border">
        <table className="w-full table-fixed border-collapse font-mono text-[12px] leading-[18px]">
          <caption className="sr-only">
            {`Changes to ${path}: ${countWords(file.added, file.removed)} (${countText(file.added, file.removed)})`}
          </caption>
          <colgroup>
            <col className="w-9" />
            <col className="w-9" />
            <col className="w-5" />
            <col />
          </colgroup>
          <thead className="sr-only">
            <tr>
              <th scope="col">Base line</th>
              <th scope="col">New line</th>
              <th scope="col">Change</th>
              <th scope="col">Text</th>
            </tr>
          </thead>
          <tbody>
            {lines.map((l, i) =>
              l.kind === 'hunk' || l.kind === 'note' ? (
                <tr key={i} className={ROW[l.kind]}>
                  <td colSpan={4} className="whitespace-pre-wrap break-all px-2 py-0.5">
                    {l.text}
                  </td>
                </tr>
              ) : (
                <tr key={i} className={ROW[l.kind]} data-kind={l.kind}>
                  <td className="select-none px-1 text-right align-top tabular-nums text-subtle-foreground">{l.oldNo ?? ''}</td>
                  <td className="select-none px-1 text-right align-top tabular-nums text-subtle-foreground">{l.newNo ?? ''}</td>
                  <td
                    className={cn(
                      'select-none text-center align-top font-semibold',
                      l.kind === 'add' && 'text-success-foreground',
                      l.kind === 'del' && 'text-destructive-fg',
                    )}
                    data-testid="diff-marker"
                  >
                    <span aria-hidden="true">{l.marker === ' ' ? ' ' : l.marker}</span>
                    <span className="sr-only">{MARKER_WORD[l.marker]}</span>
                  </td>
                  <td className="whitespace-pre-wrap break-all pr-2 align-top">{l.text}</td>
                </tr>
              ),
            )}
          </tbody>
        </table>
      </div>
    </li>
  )
}

/** The diff, hosts, warnings, the PR body's footer and the "nothing changes until merged" line. */
export function DiffPreview({ diff, footer, stepIndex }: { diff: DiffPreviewData; footer?: string; stepIndex: number }) {
  const listId = useId()
  const files = diff.files ?? []
  const added = files.reduce((n, f) => n + f.added, 0)
  const removed = files.reduce((n, f) => n + f.removed, 0)
  const repoHref = safeHref(diff.repo_url)
  const hosts = diff.hosts ?? []
  const warnings = diff.warnings ?? []
  const hidden = files.some((f) => hasHidden(f.unified) || hasHidden(f.path))
  return (
    <div className="flex min-w-0 flex-col gap-2" data-testid={`diff-preview-${stepIndex}`}>
      <p className="m-0 flex flex-wrap items-center gap-x-2 gap-y-1 text-xs">
        <span className="font-medium">Pull request</span>
        {repoHref ? (
          <a href={repoHref} target="_blank" rel="noopener noreferrer" className="inline-flex items-center gap-1 font-mono underline">
            {diff.repo}
            <ExternalLink className="size-3" aria-hidden="true" />
            <span className="sr-only">(opens in a new tab)</span>
          </a>
        ) : (
          <span className="font-mono">{diff.repo}</span>
        )}
        <span className="text-muted-foreground">
          into <span className="font-mono">{diff.base}</span> at <code title={diff.base_sha}>{shortSha(diff.base_sha)}</code>
        </span>
        <span className="inline-flex min-w-0 items-center gap-1 text-muted-foreground">
          <GitBranch className="size-3 shrink-0" aria-hidden="true" />
          <span className="break-all font-mono">{diff.branch}</span>
        </span>
        {diff.layer && (
          <Badge variant={layerVariant(diff.layer)} mono title="The layer whose owners approve this change (from the files it touches)">
            {layerLabel(diff.layer)}
          </Badge>
        )}
        {diff.draft && <Badge variant="outline">draft</Badge>}
      </p>

      <h5 id={listId} className="m-0 text-xs font-medium text-subtle-foreground">
        {files.length} {files.length === 1 ? 'file' : 'files'} · <span aria-hidden="true">{countText(added, removed)}</span>
        <span className="sr-only">{countWords(added, removed)}</span>
      </h5>
      <ul aria-labelledby={listId} className="m-0 flex list-none flex-col gap-1.5 p-0">
        {files.map((f, i) => (
          <FileBlock key={`${i}:${f.path}`} file={f} index={i} />
        ))}
      </ul>

      {hidden && (
        <p className="m-0 inline-flex items-start gap-1.5 text-xs font-medium text-warning-foreground" data-testid="diff-hidden-chars">
          <TriangleAlert className="mt-0.5 size-3.5 shrink-0" aria-hidden="true" />
          This change contains invisible or direction-changing characters, shown above as &lt;U+…&gt;.
        </p>
      )}

      <div className="flex flex-col gap-0.5 text-xs">
        <h5 className="m-0 font-medium text-subtle-foreground">Hosts this change fetches from</h5>
        {hosts.length > 0 ? (
          <ul className="m-0 flex list-none flex-wrap gap-1.5 p-0" data-testid="diff-hosts">
            {hosts.map((h) => (
              <li key={h} className="break-all rounded bg-muted px-1.5 font-mono">
                {revealHidden(h)}
              </li>
            ))}
          </ul>
        ) : (
          <p className="m-0 text-muted-foreground">none</p>
        )}
      </div>

      {warnings.length > 0 && (
        <div className="flex flex-col gap-0.5 text-xs">
          <h5 className="m-0 font-medium text-warning-foreground">Warnings</h5>
          <ul className="m-0 flex list-none flex-col gap-0.5 p-0" data-testid="diff-warnings">
            {warnings.map((w, i) => (
              <li key={i} className="flex items-start gap-1.5 break-words text-warning-foreground">
                <TriangleAlert className="mt-0.5 size-3 shrink-0" aria-hidden="true" />
                <span>
                  <span className="sr-only">Warning: </span>
                  {revealHidden(w)}
                </span>
              </li>
            ))}
          </ul>
        </div>
      )}

      {footer && (
        <div className="flex flex-col gap-0.5 text-xs">
          <h5 className="m-0 font-medium text-subtle-foreground">The pull request body ends with</h5>
          <p className="m-0 whitespace-pre-wrap break-words rounded bg-muted px-2 py-1 text-muted-foreground" data-testid="diff-footer">
            {footer}
          </p>
        </div>
      )}

      <p className="m-0 text-[13px] font-medium" data-testid="pr-approval-note">
        {PR_APPROVAL_NOTE}
      </p>
    </div>
  )
}

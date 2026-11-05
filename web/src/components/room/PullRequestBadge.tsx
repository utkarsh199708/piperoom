// Q7g: the pull request a fix step opened, after Execute (docs/design/fix-pr.md §10): open, merged or
// closed; its checks (pending, passing, failing with names); "edited after approval" when a reviewer
// pushed to it; a link to it. Every state is a word, never a colour alone. Check names are the
// repository CI's text: shown as plain text.
import { ExternalLink, GitMerge, GitPullRequest, GitPullRequestClosed, TriangleAlert } from 'lucide-react'
import type { PullRequestState } from '../../state/types'
import { Badge } from '@/components/ui/badge'
import { checksTone, checksWords, failedNames, prStateTone, prStateWord, safeHref } from '../../lib/fixpr'
import { plainText } from '../../lib/title'

function StateIcon({ state }: { state: string }) {
  if (state === 'merged') return <GitMerge className="size-3" aria-hidden="true" />
  if (state === 'closed') return <GitPullRequestClosed className="size-3" aria-hidden="true" />
  return <GitPullRequest className="size-3" aria-hidden="true" />
}

export function PullRequestBadge({ pr, stepIndex }: { pr: PullRequestState; stepIndex: number }) {
  const href = safeHref(pr.url)
  const failed = pr.checks?.state === 'failure' ? failedNames(pr.checks) : []
  const label = `Pull request #${pr.number} in ${plainText(pr.repo, 80)}`
  return (
    <div className="flex min-w-0 flex-col gap-1 text-xs" data-testid={`pr-badge-${stepIndex}`} role="group" aria-label={label}>
      <div className="flex flex-wrap items-center gap-1.5">
        <Badge variant={prStateTone(pr.state)} data-testid="pr-state">
          <StateIcon state={pr.state} />
          {prStateWord(pr.state)}
        </Badge>
        {href ? (
          <a href={href} target="_blank" rel="noopener noreferrer" className="inline-flex items-center gap-1 font-mono underline">
            {plainText(pr.repo, 80)}#{pr.number}
            <ExternalLink className="size-3" aria-hidden="true" />
            <span className="sr-only">(opens in a new tab)</span>
          </a>
        ) : (
          <span className="font-mono">
            {plainText(pr.repo, 80)}#{pr.number}
          </span>
        )}
        <Badge variant={checksTone(pr.checks)} data-testid="pr-checks">
          {checksWords(pr.checks)}
        </Badge>
        {pr.head_changed && (
          <Badge
            variant="warning"
            data-testid="pr-edited"
            title="A reviewer pushed to the pull request after it was approved here: the change is no longer exactly the approved diff"
          >
            <TriangleAlert className="size-3" aria-hidden="true" />
            edited after approval
          </Badge>
        )}
      </div>
      {failed.length > 0 && (
        <p className="m-0 break-words text-destructive-fg" data-testid="pr-failed-checks">
          Failing: {failed.join(', ')}
        </p>
      )}
      {pr.state === 'open' && (
        <p className="m-0 text-muted-foreground">Nothing changes in any system until a reviewer merges it.</p>
      )}
      {pr.state === 'closed' && <p className="m-0 text-muted-foreground">Closed without merge: nothing was applied.</p>}
    </div>
  )
}

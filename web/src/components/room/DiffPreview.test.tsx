import { describe, expect, it, vi } from 'vitest'
import { fireEvent, render, screen, within } from '@testing-library/react'
import { DiffPreview } from './DiffPreview'
import { PullRequestBadge } from './PullRequestBadge'
import { VerificationCard } from './VerificationCard'
import { ProposalsPanel } from '../ProposalsPanel'
import type { RoomActions } from '../../hooks/useRoom'
import type { CurrentUser } from '../../state/permissions'
import type { PullRequestState, RoomState } from '../../state/types'
import { fromSnapshot } from '../../state/types'
import { applyEvent } from '../../state/reducer'
import type { DiffPreview as DiffData, Event } from '../../api/types'
import { FOOTER, fixprApproved, fixprCreated, fixprDiff, fixprExecuted, fixprPreviewed, fixprSnapshot, prUpdated } from '../../fixtures/fixpr'

// Q7g: the fix PR's diff on the preview card and the PR badge after Execute (docs/design/fix-pr.md §10).

describe('DiffPreview', () => {
  it('shows every file with status, +n -m and +/- markers as text, not colour alone', () => {
    render(<DiffPreview diff={fixprDiff} footer={FOOTER} stepIndex={0} />)
    const files = screen.getAllByRole('listitem').filter((li) => li.dataset.testid?.startsWith('diff-file-'))
    expect(files).toHaveLength(2)
    const docker = within(screen.getByTestId('diff-file-0'))
    expect(docker.getByText('modified')).toBeInTheDocument()
    expect(docker.getByText('docker/connect/Dockerfile')).toBeInTheDocument()
    expect(docker.getByText('+2')).toBeInTheDocument()
    expect(docker.getByText('-1')).toBeInTheDocument()
    expect(within(within(screen.getByTestId('diff-file-1')).getByRole('button')).getByText('added')).toBeInTheDocument()

    const added = docker.getAllByRole('row').filter((r) => r.dataset.kind === 'add')
    const removed = docker.getAllByRole('row').filter((r) => r.dataset.kind === 'del')
    expect(added).toHaveLength(2)
    expect(removed).toHaveLength(1)
    // The marker is a visible character, and the screen reader hears a word.
    expect(within(added[0]).getByTestId('diff-marker')).toHaveTextContent('+added')
    expect(within(removed[0]).getByTestId('diff-marker')).toHaveTextContent('-removed')
    expect(removed[0]).toHaveTextContent('ENV LOG_LEVEL=INFO')
    // Line numbers: the removed line is base line 2, the first added line is new line 2.
    expect(within(removed[0]).getAllByRole('cell')[0]).toHaveTextContent('2')
    expect(within(added[0]).getAllByRole('cell')[1]).toHaveTextContent('2')
    // The --- / +++ header is not a line of the change.
    expect(docker.queryByText(/^\+\+\+ b\//)).not.toBeInTheDocument()
  })

  it('shows hosts, warnings, the PR body footer and the approval line', () => {
    render(<DiffPreview diff={fixprDiff} footer={FOOTER} stepIndex={0} />)
    expect(screen.getByText('Hosts this change fetches from')).toBeInTheDocument()
    expect(within(screen.getByTestId('diff-hosts')).getByText('repo1.maven.org')).toBeInTheDocument()
    expect(within(screen.getByTestId('diff-warnings')).getByText(/a download \(curl\)/)).toBeInTheDocument()
    expect(screen.getByTestId('diff-footer')).toHaveTextContent(FOOTER)
    expect(screen.getByTestId('pr-approval-note')).toHaveTextContent(
      'Approving opens a pull request. Nothing changes in any system until a reviewer merges it.',
    )
    expect(screen.getByRole('link', { name: /connect-config/ })).toHaveAttribute('href', 'https://github.com/acme/connect-config')
  })

  it('says "none" when the change fetches from no host', () => {
    render(<DiffPreview diff={{ ...fixprDiff, hosts: undefined, warnings: undefined }} stepIndex={0} />)
    expect(screen.getByText('none')).toBeInTheDocument()
    expect(screen.queryByTestId('diff-warnings')).not.toBeInTheDocument()
    expect(screen.queryByTestId('diff-footer')).not.toBeInTheDocument()
  })

  it('the file list is keyboard and screen-reader usable: named buttons that expand and collapse their table', () => {
    render(<DiffPreview diff={fixprDiff} stepIndex={0} />)
    expect(screen.getByRole('list', { name: /2 files/ })).toBeInTheDocument()
    const button = screen.getByRole('button', { name: /docker\/connect\/Dockerfile.*2 lines added, 1 removed/ })
    expect(button.tagName).toBe('BUTTON')
    expect(button).toHaveAttribute('aria-expanded', 'true')
    const body = document.getElementById(button.getAttribute('aria-controls') as string)
    expect(body).not.toBeNull()
    expect(within(body as HTMLElement).getByRole('table', { name: /Changes to docker\/connect\/Dockerfile/ })).toBeInTheDocument()
    expect(within(body as HTMLElement).getAllByRole('columnheader').map((h) => h.textContent)).toEqual([
      'Base line',
      'New line',
      'Change',
      'Text',
    ])
    button.focus()
    expect(button).toHaveFocus()
    fireEvent.click(button)
    expect(button).toHaveAttribute('aria-expanded', 'false')
    expect(body).not.toBeVisible()
    fireEvent.click(button)
    expect(body).toBeVisible()
  })

  it('renders repository text as text: no HTML is ever injected', () => {
    const evil: DiffData = {
      ...fixprDiff,
      files: [
        {
          path: '<img src=x onerror=alert(1)>.json',
          status: 'added',
          unified: '@@ -0,0 +1 @@\n+<script>alert(1)</script><b>bold</b>\n',
          added: 1,
          removed: 0,
        },
      ],
      warnings: ['<a href="javascript:alert(1)">click</a>'],
      hosts: ['<i>evil</i>'],
      repo_url: 'javascript:alert(1)',
    }
    const { container } = render(<DiffPreview diff={evil} footer={'<b>footer</b>'} stepIndex={0} />)
    expect(container.querySelector('img, script, b, i')).toBeNull()
    expect(container.querySelectorAll('a')).toHaveLength(0)
    expect(screen.getByText('<script>alert(1)</script><b>bold</b>')).toBeInTheDocument()
    expect(screen.getByText('<img src=x onerror=alert(1)>.json')).toBeInTheDocument()
  })

  it('reveals bidi characters and says so', () => {
    render(
      <DiffPreview
        diff={{ ...fixprDiff, files: [{ path: 'a.py', status: 'modified', unified: '@@ -1 +1 @@\n-x\n+if ok:\u202e #\n', added: 1, removed: 1 }] }}
        stepIndex={0}
      />,
    )
    expect(screen.getByText('if ok:<U+202E> #')).toBeInTheDocument()
    expect(screen.getByTestId('diff-hidden-chars')).toBeInTheDocument()
  })

  it('long lines wrap instead of scrolling the page on a phone', () => {
    render(<DiffPreview diff={fixprDiff} stepIndex={0} />)
    const text = screen.getByText('ENV LOG_LEVEL=INFO')
    expect(text.className).toMatch(/whitespace-pre-wrap/)
    expect(text.className).toMatch(/break-all/)
    expect(text.closest('table')?.className).toMatch(/table-fixed/)
  })
})

const open: PullRequestState = {
  repo: 'connect-config',
  number: 12,
  url: 'https://github.com/acme/connect-config/pull/12',
  branch: 'piperoom/r_pr-p_pr-0',
  head_sha: 'c0ffee0',
  state: 'open',
  head_changed: false,
  checks: { state: 'pending', total: 3 },
  updated_at: '2026-10-22T10:00:05Z',
}

describe('PullRequestBadge', () => {
  it('open: state in words, checks pending, a link to the PR, nothing changes until merged', () => {
    render(<PullRequestBadge pr={open} stepIndex={0} />)
    expect(screen.getByRole('group', { name: 'Pull request #12 in connect-config' })).toBeInTheDocument()
    expect(screen.getByTestId('pr-state')).toHaveTextContent('open')
    expect(screen.getByTestId('pr-checks')).toHaveTextContent('checks pending (3)')
    expect(screen.getByRole('link', { name: /connect-config#12/ })).toHaveAttribute('href', open.url)
    expect(screen.getByText('Nothing changes in any system until a reviewer merges it.')).toBeInTheDocument()
    expect(screen.queryByTestId('pr-edited')).not.toBeInTheDocument()
  })

  it('merged with passing checks and edited after approval', () => {
    render(<PullRequestBadge pr={{ ...open, state: 'merged', head_changed: true, checks: { state: 'success', total: 3 } }} stepIndex={0} />)
    expect(screen.getByTestId('pr-state')).toHaveTextContent('merged')
    expect(screen.getByTestId('pr-checks')).toHaveTextContent('checks passing (3)')
    expect(screen.getByTestId('pr-edited')).toHaveTextContent('edited after approval')
  })

  it('closed with failing checks names them, as text', () => {
    render(
      <PullRequestBadge
        pr={{ ...open, state: 'closed', checks: { state: 'failure', total: 3, failed: ['lint', '<b>e2e</b>'] } }}
        stepIndex={0}
      />,
    )
    expect(screen.getByTestId('pr-state')).toHaveTextContent('closed without merge')
    expect(screen.getByTestId('pr-checks')).toHaveTextContent('checks failing (2 of 3)')
    expect(screen.getByTestId('pr-failed-checks')).toHaveTextContent('Failing: lint, <b>e2e</b>')
    expect(document.querySelector('[data-testid="pr-failed-checks"] b')).toBeNull()
    expect(screen.getByText('Closed without merge: nothing was applied.')).toBeInTheDocument()
  })

  it('a non-http URL is never a link', () => {
    render(<PullRequestBadge pr={{ ...open, url: 'javascript:alert(1)' }} stepIndex={0} />)
    expect(screen.queryByRole('link')).not.toBeInTheDocument()
    expect(screen.getByText('connect-config#12')).toBeInTheDocument()
  })
})

describe('VerificationCard on a fix PR', () => {
  const nudge = { roomOpen: true, isDriver: true }
  it('says not applied for a PR that closed unmerged, never "not recovered"', () => {
    render(
      <VerificationCard
        verification={{ proposal_id: 'p_pr', checks: [], recovered: false, verdict: 'The pull request was closed.', outcome: 'not_applied' }}
        title="t"
        nudge={nudge}
      />,
    )
    expect(screen.getByTestId('fixpr-outcome')).toHaveTextContent('Not applied · the pull request closed without merge')
    expect(screen.queryByText(/not recovered/)).not.toBeInTheDocument()
  })
  it('says merged, not yet seen, for not_deployed', () => {
    render(
      <VerificationCard
        verification={{ proposal_id: 'p_pr', checks: [], recovered: false, verdict: 'Merged 2 h ago.', outcome: 'not_deployed' }}
        title="t"
        nudge={nudge}
      />,
    )
    expect(screen.getByTestId('fixpr-outcome')).toHaveTextContent('Merged · not yet seen in the system')
  })
})

const actions: RoomActions = {
  comment: vi.fn(),
  steer: vi.fn(),
  sendSuggestion: vi.fn(),
  declineSuggestion: vi.fn(),
  requestControl: vi.fn(),
  grantControl: vi.fn(),
  close: vi.fn(),
  approve: vi.fn(),
  reject: vi.fn(),
  handoff: vi.fn(),
  acceptHandoff: vi.fn(),
  label: vi.fn(),
  checklist: vi.fn(),
  trace: vi.fn(),
  hypothesisVerdict: vi.fn(),
  nudge: vi.fn().mockResolvedValue({ ok: true }),
}
const arjun: CurrentUser = { id: 'arjun', name: 'Arjun', layers: ['kafka-connect'] }
const room = (...events: Event[]): RoomState => events.reduce(applyEvent, fromSnapshot(fixprSnapshot))

describe('the proposal card of a PR step', () => {
  it('shows the diff under the preview, with the footer apart from Expected', () => {
    render(<ProposalsPanel state={room(fixprCreated, fixprPreviewed)} user={arjun} actions={actions} />)
    const preview = screen.getByTestId('preview-step-0')
    expect(within(preview).getByTestId('diff-preview-0')).toBeInTheDocument()
    const line = within(preview).getByText('Expected:', { exact: false }).parentElement as HTMLElement
    expect(line).toHaveTextContent('a pull request from piperoom/r_pr-p_pr-0 into main')
    expect(line).not.toHaveTextContent('Opened by Piperoom')
    expect(line).not.toHaveTextContent('The body ends with')
    expect(within(preview).getByTestId('diff-footer')).toHaveTextContent(FOOTER)
    expect(within(preview).getByTestId('pr-approval-note')).toBeInTheDocument()
  })

  it('after Execute: the badge on the step, and "waiting for a reviewer to merge" instead of verifying', () => {
    const { rerender } = render(
      <ProposalsPanel state={room(fixprCreated, fixprPreviewed, fixprApproved, fixprExecuted)} user={arjun} actions={actions} />,
    )
    const step = screen.getByTestId('exec-step-0')
    expect(within(step).getByTestId('pr-badge-0')).toBeInTheDocument()
    expect(within(step).getByTestId('pr-checks')).toHaveTextContent('checks not read yet')
    expect(screen.getByText('Ran 1 of 1 · waiting for a reviewer to merge')).toBeInTheDocument()
    expect(screen.queryByLabelText('Execution in progress')).not.toBeInTheDocument()

    rerender(
      <ProposalsPanel
        state={room(
          fixprCreated,
          fixprPreviewed,
          fixprApproved,
          fixprExecuted,
          prUpdated(5, { state: 'merged', head_changed: true, checks: { state: 'success', total: 3 } }),
        )}
        user={arjun}
        actions={actions}
      />,
    )
    expect(within(screen.getByTestId('exec-step-0')).getByTestId('pr-state')).toHaveTextContent('merged')
    expect(screen.getByTestId('pr-edited')).toBeInTheDocument()
    expect(screen.getByText('Ran 1 of 1 · verifying…')).toBeInTheDocument()
  })
})

import { beforeEach, describe, expect, it, vi } from 'vitest'
import { fireEvent, render, screen, waitFor, within } from '@testing-library/react'
import { DropdownMenu, DropdownMenuContent, DropdownMenuItem, DropdownMenuTrigger } from '@/components/ui/dropdown-menu'
import { toast } from 'sonner'
import { ProposalsPanel } from './ProposalsPanel'
import { StaleBanner } from './StaleBanner'
import { hideStaleToast, showStaleToast } from '../lib/staleToast'
import type { RoomActions, StaleInfo } from '../hooks/useRoom'
import type { CurrentUser } from '../state/permissions'
import { emptyRoomState } from '../state/types'
import type { ProposalState } from '../state/types'

vi.mock('sonner', () => ({
  toast: Object.assign(vi.fn(), { warning: vi.fn(), dismiss: vi.fn() }),
}))

const approve = vi.fn()
const reject = vi.fn()
const actions: RoomActions = {
  comment: vi.fn(),
  steer: vi.fn(),
  sendSuggestion: vi.fn(),
  declineSuggestion: vi.fn(),
  requestControl: vi.fn(),
  grantControl: vi.fn(),
  close: vi.fn(),
  approve,
  reject,
  handoff: vi.fn(),
  acceptHandoff: vi.fn(),
  label: vi.fn(),
  checklist: vi.fn(),
  trace: vi.fn(),
  hypothesisVerdict: vi.fn(),
  nudge: vi.fn().mockResolvedValue({ ok: true }),
}

const HASH = '9f2c1e7ab04d58c3d6e1f0a7b92c4d835e6a1f0b7c3d9e2a48b5c6d7e8f90a1b'
const NEW_HASH = 'b31d77e0aaaaaaaabbbbbbbbccccccccddddddddeeeeeeeeffffffff00004c92'

const arjun: CurrentUser = { id: 'arjun', name: 'Arjun', layers: ['kafka-connect', 'kafka'] }
const viewer: CurrentUser = { id: 'meera', name: 'Meera', layers: ['kafka-connect'], role: 'viewer' }
const kabir: CurrentUser = { id: 'kabir', name: 'Kabir', layers: ['consumer'] }

// Priya has approved schema-registry; kafka-connect waits for Arjun (design: Room-Approval).
const proposal: ProposalState = {
  proposal_id: 'p1',
  title: 'Set subject compatibility to NONE, then restart the connector task',
  steps: [
    {
      action: 'set_compatibility',
      layer: 'schema-registry',
      params: { subject: 'pg.public.orders-value', compatibility: { from: 'BACKWARD', to: 'NONE' } },
      description: 'Allow the new schema to register.',
    },
    {
      action: 'restart_connector_task',
      layer: 'kafka-connect',
      params: { connector: 'pg-orders-connector', task: 0 },
      description: 'Restart the task.',
    },
  ],
  risk: 'low',
  rollback: 'set compatibility back to BACKWARD',
  required_layers: ['schema-registry', 'kafka-connect'],
  approvals: { 'schema-registry': 'approved', 'kafka-connect': 'pending' },
  status: 'open',
  decisions: {
    'schema-registry': {
      outcome: 'approved',
      by: { kind: 'human', id: 'priya', name: 'Priya' },
      identity: { iss: 'https://idp.example.com', sub: 'priya', method: 'oidc', grant: 'user:priya' },
      params_hash: HASH,
      at: '2026-10-11T10:05:00Z',
    },
  },
  params_hash: HASH,
  executed: [],
}
const roomWith = (p: ProposalState) => ({ ...emptyRoomState('r1'), pipeline_id: 'orders', proposals: [p] })
const show = (user: CurrentUser, p: ProposalState = proposal, stale: StaleInfo | null = null) =>
  render(<ProposalsPanel state={roomWith(p)} user={user} actions={actions} stale={stale} />)

// The approval that completes the set asks "Run this fix now?" first.
const confirmRun = async () => fireEvent.click(await screen.findByRole('button', { name: 'Approve and run' }))

beforeEach(() => {
  approve.mockReset()
  reject.mockReset()
  vi.mocked(toast.warning).mockReset()
  vi.mocked(toast.dismiss).mockReset()
})

describe('the approval moment', () => {
  it('shows the ribbon for the layer you can decide, once', () => {
    show(arjun)
    expect(screen.getAllByText(/Needs your approval/)).toHaveLength(1)
    expect(screen.getByText(/Needs your approval/)).toHaveTextContent('Needs your approval · kafka-connect')
    // Priya's decision is mentioned on the ribbon as well.
    expect(screen.getByText(/approved schema-registry/)).toHaveTextContent(/Priya.* approved schema-registry/)
  })

  it('shows no ribbon and no buttons to someone who cannot decide, and says who it waits for', () => {
    show(kabir)
    expect(screen.queryByText(/Needs your approval/)).not.toBeInTheDocument()
    expect(screen.queryByRole('button', { name: 'Approve' })).not.toBeInTheDocument()
    expect(screen.getByText(/waiting for kafka-connect owner · 1 of 2 approved/)).toBeInTheDocument()
    expect(screen.getByText('Fingerprint')).toBeInTheDocument()
  })

  it('a viewer sees the whole card and no controls, even on a layer they would own', () => {
    show(viewer)
    expect(screen.queryByText(/Needs your approval/)).not.toBeInTheDocument()
    expect(screen.queryByRole('button', { name: 'Approve' })).not.toBeInTheDocument()
    expect(screen.queryByRole('button', { name: 'Reject' })).not.toBeInTheDocument()
    expect(screen.getByTestId('params-hash-short')).toHaveTextContent('9f2c 1e7a … 0a1b')
    expect(screen.getByRole('button', { name: 'Copy hash' })).toBeInTheDocument()
    // Keys do nothing for a viewer.
    fireEvent.keyDown(window, { key: 'a' })
    fireEvent.keyDown(window, { key: 'r' })
    expect(approve).not.toHaveBeenCalled()
    expect(screen.queryByLabelText('Rejection reason for kafka-connect')).not.toBeInTheDocument()
  })

  it('numbers the steps, badges their layers and marks the target in the params', () => {
    show(arjun)
    const steps = screen.getAllByRole('listitem').filter((li) => li.querySelector('[data-testid="step-params"]'))
    expect(steps).toHaveLength(2)
    expect(within(steps[0]).getByLabelText('Step 1')).toHaveTextContent('1')
    expect(within(steps[0]).getByText('schema-registry')).toBeInTheDocument()
    expect(within(steps[1]).getByLabelText('Step 2')).toHaveTextContent('2')
    // Target fields carry the marker; a plain field does not.
    expect(within(steps[0]).getAllByText('target')).toHaveLength(1)
    expect(within(steps[1]).getAllByText('target')).toHaveLength(2) // connector and task
    // A { from, to } value is drawn as a diff.
    expect(within(steps[0]).getByText('BACKWARD').tagName).toBe('S')
    expect(within(steps[0]).getByText('NONE')).toBeInTheDocument()
  })

  it('the approval matrix shows who decided, grant, method and time; pending rows say pending', () => {
    show(arjun)
    const done = screen.getByTestId('decision-schema-registry')
    expect(done).toHaveTextContent(/approved by Priya \(user:priya, oidc\) at \d/)
    const row = done.closest('[data-layer]') as HTMLElement
    expect(within(row).getByText('user:priya')).toBeInTheDocument()
    const mine = screen.getAllByRole('button', { name: 'Approve' })[0].closest('[data-layer]') as HTMLElement
    expect(mine).toHaveAttribute('data-layer', 'kafka-connect')
    expect(within(mine).getByText('pending · your layer')).toBeInTheDocument()
    expect(within(mine).getByText('user:arjun')).toBeInTheDocument()
  })

  it('the full hash is inside a details element and the copy button copies it', async () => {
    const writeText = vi.fn().mockResolvedValue(undefined)
    Object.defineProperty(navigator, 'clipboard', { value: { writeText }, configurable: true })
    show(arjun)
    const full = screen.getByLabelText('Full params hash')
    expect(full.closest('details')).not.toBeNull()
    expect(full).toHaveValue(HASH)
    fireEvent.click(screen.getByRole('button', { name: 'Copy hash' }))
    expect(writeText).toHaveBeenCalledWith(HASH)
  })
})

describe('the panel', () => {
  it('has no heading of its own (the room rail has it) and is a labelled region', () => {
    show(arjun)
    expect(screen.getByRole('region', { name: 'Proposals' })).toBeInTheDocument()
    expect(screen.queryByRole('heading', { name: /^Proposals/ })).not.toBeInTheDocument()
    // The card heading is still there.
    expect(screen.getByRole('heading', { level: 3 })).toHaveTextContent(/Set subject compatibility/)
  })
})

describe('keys A and R', () => {
  it('A approves the layer you own with the hash on screen', async () => {
    approve.mockResolvedValue({ ok: true })
    show(arjun)
    fireEvent.keyDown(window, { key: 'a' })
    await confirmRun()
    await waitFor(() => expect(approve).toHaveBeenCalledWith('p1', 'kafka-connect', { params_hash: HASH }))
  })

  it('R opens the reason field', () => {
    show(arjun)
    fireEvent.keyDown(window, { key: 'r' })
    expect(screen.getByLabelText('Rejection reason for kafka-connect')).toBeInTheDocument()
  })

  it('shows the key on the buttons', () => {
    show(arjun)
    expect(screen.getByRole('button', { name: 'Approve' })).toHaveAttribute('aria-keyshortcuts', 'A')
    expect(screen.getByRole('button', { name: 'Reject' })).toHaveAttribute('aria-keyshortcuts', 'R')
  })

  it('never fire inside a field', () => {
    show(arjun)
    fireEvent.keyDown(window, { key: 'r' })
    const reason = screen.getByLabelText('Rejection reason for kafka-connect')
    fireEvent.keyDown(reason, { key: 'a', bubbles: true })
    expect(approve).not.toHaveBeenCalled()
    // An outside field blocks them too.
    const outside = document.createElement('textarea')
    document.body.appendChild(outside)
    fireEvent.keyDown(outside, { key: 'a', bubbles: true })
    expect(approve).not.toHaveBeenCalled()
    outside.remove()
  })

  it('ignore modifier combinations', () => {
    show(arjun)
    fireEvent.keyDown(window, { key: 'a', ctrlKey: true })
    fireEvent.keyDown(window, { key: 'a', metaKey: true })
    expect(approve).not.toHaveBeenCalled()
  })

  it('do nothing while an open menu (the account menu) owns the keyboard', async () => {
    show(arjun)
    render(
      <DropdownMenu open>
        <DropdownMenuTrigger>Account</DropdownMenuTrigger>
        <DropdownMenuContent>
          <DropdownMenuItem>Sign out</DropdownMenuItem>
        </DropdownMenuContent>
      </DropdownMenu>,
    )
    expect(await screen.findByRole('menu')).toBeInTheDocument()
    fireEvent.keyDown(window, { key: 'a' })
    fireEvent.keyDown(window, { key: 'r' })
    expect(approve).not.toHaveBeenCalled()
    expect(screen.queryByLabelText('Rejection reason for kafka-connect')).not.toBeInTheDocument()
  })

  it.each(['alertdialog', 'listbox'])('do nothing while a %s is open', (role) => {
    show(arjun)
    const overlay = document.createElement('div')
    overlay.setAttribute('role', role)
    document.body.appendChild(overlay)
    fireEvent.keyDown(window, { key: 'a' })
    fireEvent.keyDown(window, { key: 'r' })
    expect(approve).not.toHaveBeenCalled()
    expect(screen.queryByLabelText('Rejection reason for kafka-connect')).not.toBeInTheDocument()
    overlay.remove()
  })

  it('ignore key repeat and IME composition', () => {
    show(arjun)
    fireEvent.keyDown(window, { key: 'a', repeat: true })
    fireEvent.keyDown(window, { key: 'a', isComposing: true })
    expect(approve).not.toHaveBeenCalled()
  })

  it('A does nothing without a hash', () => {
    show(arjun, { ...proposal, params_hash: '' })
    fireEvent.keyDown(window, { key: 'a' })
    expect(approve).not.toHaveBeenCalled()
  })
})

describe('the solo confirm dialog', () => {
  const soloUser: CurrentUser = { ...arjun, solo: true }

  it('is a dialog with a labelled field, a hint, paste off, and Approve only on an exact match', () => {
    show(soloUser)
    fireEvent.click(screen.getByRole('button', { name: 'Approve' }))
    const dialog = screen.getByRole('dialog')
    expect(within(dialog).getByRole('heading')).toHaveTextContent('Approve kafka-connect in solo mode')
    const field = within(dialog).getByLabelText(/Solo mode: type orders\/kafka-connect to approve/)
    expect(field).toHaveAccessibleDescription('Type it by hand. Paste is off.')
    expect(within(dialog).getByText('fingerprint 9f2c 1e7a … 0a1b')).toBeInTheDocument()
    const go = within(dialog).getByRole('button', { name: 'Approve' })
    expect(go).toBeDisabled()
    fireEvent.change(field, { target: { value: 'orders/kafka-connec' } })
    expect(go).toBeDisabled()
    expect(within(dialog).getByText('1 character to go')).toBeInTheDocument()
    fireEvent.change(field, { target: { value: 'orders/kafka-connect' } })
    expect(go).toBeEnabled()
    expect(fireEvent.paste(field, { clipboardData: { getData: () => 'x' } })).toBe(false)
  })

  it('A does not open a second dialog or approve while the dialog is open', () => {
    show(soloUser)
    fireEvent.click(screen.getByRole('button', { name: 'Approve' }))
    fireEvent.keyDown(window, { key: 'a' })
    expect(screen.getAllByRole('dialog')).toHaveLength(1)
    expect(approve).not.toHaveBeenCalled()
  })

  it('a params_mismatch closes the dialog, shows what was looked at and is not retried', async () => {
    approve.mockResolvedValue({ ok: false, mismatch: true })
    const first = show(soloUser)
    fireEvent.click(screen.getByRole('button', { name: 'Approve' }))
    fireEvent.change(screen.getByLabelText(/Solo mode: type/), { target: { value: 'orders/kafka-connect' } })
    fireEvent.click(within(screen.getByRole('dialog')).getByRole('button', { name: 'Approve' }))
    await waitFor(() => expect(screen.queryByRole('dialog')).not.toBeInTheDocument())
    expect(approve).toHaveBeenCalledTimes(1)
    // The room reloads: the whole panel is unmounted ("Loading room…") and comes back with the
    // server's hash. useRoom kept what the person looked at; the card says what changed under them.
    first.unmount()
    show(soloUser, { ...proposal, params_hash: NEW_HASH }, { proposalId: 'p1', sentHash: HASH })
    expect(screen.getByText('You looked at')).toBeInTheDocument()
    expect(screen.getByText('9f2c 1e7a … 0a1b').closest('span[title]')).toHaveClass('line-through')
    // The server's hash shows in the fingerprint row and in this note.
    expect(screen.getAllByText('b31d 77e0 … 4c92')).toHaveLength(2)
    expect(approve).toHaveBeenCalledTimes(1)
  })
})

describe('the stale note', () => {
  it('is not shown while the card still has the hash that was sent, or for another proposal', () => {
    const { unmount } = show(arjun, proposal, { proposalId: 'p1', sentHash: HASH })
    expect(screen.queryByText('You looked at')).not.toBeInTheDocument()
    unmount()
    show(arjun, { ...proposal, params_hash: NEW_HASH }, { proposalId: 'other', sentHash: HASH })
    expect(screen.queryByText('You looked at')).not.toBeInTheDocument()
  })

  it('"Got it" hides it', () => {
    show(arjun, { ...proposal, params_hash: NEW_HASH }, { proposalId: 'p1', sentHash: HASH })
    fireEvent.click(screen.getByRole('button', { name: 'Got it' }))
    expect(screen.queryByText('You looked at')).not.toBeInTheDocument()
  })
})

describe('focus after the solo dialog', () => {
  it('goes back to Approve when the person had clicked it (solo user, dialog opens at once)', async () => {
    show({ ...arjun, solo: true })
    const approveButton = screen.getByRole('button', { name: 'Approve' })
    approveButton.focus()
    fireEvent.click(approveButton)
    expect(screen.getByRole('dialog')).toBeInTheDocument()
    fireEvent.click(within(screen.getByRole('dialog')).getByRole('button', { name: /Cancel/ }))
    await waitFor(() => expect(screen.queryByRole('dialog')).not.toBeInTheDocument())
    await waitFor(() => expect(screen.getByRole('button', { name: 'Approve' })).toHaveFocus())
  })

  it('goes back to Approve when the server asked for the confirm (Approve was disabled while it ran)', async () => {
    let answer: (v: unknown) => void = () => undefined
    approve.mockReturnValueOnce(new Promise((r) => (answer = r)))
    show(arjun)
    const approveButton = screen.getByRole('button', { name: 'Approve' })
    approveButton.focus()
    fireEvent.click(approveButton)
    await confirmRun()
    // While the call runs Approve is off, and a disabled button gives up focus.
    expect(approveButton).toBeDisabled()
    // (A browser drops focus on its own; jsdom does not, so do it by hand.)
    approveButton.removeAttribute('disabled')
    approveButton.blur()
    expect(document.body).toHaveFocus()
    answer({ ok: false, needsConfirm: true })
    const dialog = await screen.findByRole('dialog')
    expect(screen.getByLabelText(/Solo mode: type/)).toHaveFocus()
    fireEvent.click(within(dialog).getByRole('button', { name: /Cancel/ }))
    await waitFor(() => expect(screen.queryByRole('dialog')).not.toBeInTheDocument())
    await waitFor(() => expect(screen.getByRole('button', { name: 'Approve' })).toHaveFocus())
  })

  it('goes to the card heading when Approve is gone', async () => {
    approve.mockResolvedValueOnce({ ok: false, needsConfirm: true })
    const state = roomWith(proposal)
    const { rerender } = render(<ProposalsPanel state={state} user={arjun} actions={actions} />)
    fireEvent.click(screen.getByRole('button', { name: 'Approve' }))
    await confirmRun()
    const dialog = await screen.findByRole('dialog')
    // The decision lands while the dialog is open: this layer has no buttons any more.
    approve.mockResolvedValueOnce({ ok: true })
    const decided: ProposalState = {
      ...proposal,
      approvals: { 'schema-registry': 'approved', 'kafka-connect': 'approved' },
      decisions: {
        ...proposal.decisions,
        'kafka-connect': {
          outcome: 'approved',
          by: { kind: 'human', id: 'arjun', name: 'Arjun' },
          params_hash: HASH,
          at: '2026-10-11T10:06:00Z',
        },
      },
    }
    rerender(<ProposalsPanel state={roomWith(decided)} user={arjun} actions={actions} />)
    expect(dialog.isConnected).toBe(false)
    await waitFor(() => expect(screen.getByRole('heading', { level: 3 })).toHaveFocus())
  })
})

describe('executed steps', () => {
  it('shows the hash chip and the differs warning', () => {
    const run: ProposalState = {
      ...proposal,
      status: 'approved',
      approvals: { 'schema-registry': 'approved', 'kafka-connect': 'approved' },
      executed: [
        { proposal_id: 'p1', step_index: 0, ok: true, output: 'done', params_hash: HASH },
        { proposal_id: 'p1', step_index: 1, ok: true, output: 'done too', params_hash: NEW_HASH },
      ],
    }
    show(kabir, run)
    expect(screen.getByTestId('exec-hash-0')).toHaveTextContent(HASH.slice(0, 12))
    expect(screen.getAllByText('hash differs from proposal')).toHaveLength(1)
  })
})

describe('the stale 409 banner and toast', () => {
  it('mounting the banner raises no toast (useRoom raises it where the 409 happens)', () => {
    const { unmount } = render(<StaleBanner onReload={vi.fn()} onDismiss={vi.fn()} />)
    unmount()
    render(<StaleBanner onReload={vi.fn()} onDismiss={vi.fn()} />)
    expect(toast.warning).not.toHaveBeenCalled()
    expect(toast.dismiss).not.toHaveBeenCalled()
  })

  it('raises one Sonner toast with a Reload action, without retrying', () => {
    const onReload = vi.fn()
    showStaleToast(onReload)
    expect(toast.warning).toHaveBeenCalledTimes(1)
    const [title, options] = vi.mocked(toast.warning).mock.calls[0]
    expect(title).toBe('Proposal changed since you looked')
    expect(options?.duration).toBe(Infinity)
    const action = options?.action as { label: string; onClick: () => void }
    expect(action.label).toBe('Reload')
    action.onClick()
    expect(onReload).toHaveBeenCalledTimes(1)
  })

  it('takes the toast down by its id', () => {
    hideStaleToast()
    expect(toast.dismiss).toHaveBeenCalledWith('stale-params-mismatch')
  })
})

describe('the run confirm (the approval that completes the set)', () => {
  const nonCompleting: ProposalState = {
    ...proposal,
    required_layers: ['schema-registry', 'kafka-connect', 'consumer'],
    approvals: { 'schema-registry': 'approved', 'kafka-connect': 'pending', consumer: 'pending' },
  }
  const soloArjun: CurrentUser = { ...arjun, solo: true }

  it('A as the set-completing approver opens the run confirm, with the steps', () => {
    show(arjun)
    fireEvent.keyDown(window, { key: 'a' })
    const dialog = screen.getByRole('alertdialog')
    expect(within(dialog).getByText('Run this fix now?')).toBeInTheDocument()
    expect(dialog).toHaveTextContent('runs 2 steps on schema-registry and kafka-connect right away')
    expect(dialog).toHaveTextContent('1. schema-registry · set_compatibility')
    expect(dialog).toHaveTextContent('2. kafka-connect · restart_connector_task')
    expect(approve).not.toHaveBeenCalled()
  })

  it('Enter in the run confirm approves with the on-screen params_hash', async () => {
    approve.mockResolvedValue({ ok: true })
    show(arjun)
    fireEvent.keyDown(window, { key: 'a' })
    fireEvent.keyDown(screen.getByRole('alertdialog'), { key: 'Enter' })
    await waitFor(() => expect(approve).toHaveBeenCalledWith('p1', 'kafka-connect', { params_hash: HASH }))
  })

  it('Esc leaves the proposal open and calls nothing', async () => {
    show(arjun)
    fireEvent.keyDown(window, { key: 'a' })
    fireEvent.keyDown(screen.getByRole('alertdialog'), { key: 'Escape' })
    await waitFor(() => expect(screen.queryByRole('alertdialog')).not.toBeInTheDocument())
    expect(approve).not.toHaveBeenCalled()
    expect(screen.getByRole('button', { name: 'Approve' })).toBeInTheDocument()
  })

  it('solo users get the typed dialog, not the run confirm', () => {
    show(soloArjun)
    fireEvent.keyDown(window, { key: 'a' })
    expect(screen.queryByRole('alertdialog')).not.toBeInTheDocument()
    expect(screen.getByLabelText(/Solo mode: type/)).toBeInTheDocument()
  })

  it('a non-completing approval needs no confirm', async () => {
    approve.mockResolvedValue({ ok: true })
    show(arjun, nonCompleting)
    fireEvent.keyDown(window, { key: 'a' })
    expect(screen.queryByRole('alertdialog')).not.toBeInTheDocument()
    await waitFor(() => expect(approve).toHaveBeenCalledWith('p1', 'kafka-connect', { params_hash: HASH }))
  })
})

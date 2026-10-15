// The approval moment (docs/design/ui-v2: Room-Approval, Solo-Confirm, Stale-409, Room-Viewer).
//
// Rules that do not move with the look:
//  - Approve and Reject send the params_hash that is ON SCREEN, never one fetched at click time.
//  - The server checks the hash (409 params_mismatch) before it asks for the solo confirm.
//    A 409 is never retried: the stale banner and toast tell the person, they look again.
//  - `canDecideLayer` is the only gate for the buttons (plus "no hash" and "busy", which only
//    switch them off). A viewer or a non-owner gets no buttons at all.
import { PersonName } from '@/components/VerifiedBadge'
import { neutralizeBadgeText, personVerification } from '@/lib/verification'
import { useContext, useEffect, useId, useRef, useState } from 'react'
import type { ReactNode } from 'react'
import { Ban, Copy, Hash, Lock, RotateCcw, TriangleAlert, Check, X } from 'lucide-react'
import type { ProposalState, RoomState } from '../state/types'
import type { CurrentUser } from '../state/permissions'
import { canDecideLayer, canWithdraw, isViewer } from '../state/permissions'
import { soloConfirmText } from '../lib/solo'
import { describeIdentity, identityDetails } from '../lib/hash'
import { clockTime, timeAgo } from '../lib/time'
import { Clock } from './room/Clock'
import { BlastRadius } from './room/BlastRadius'
import { ExecutionCard } from './room/ExecutionCard'
import { VerificationCard } from './room/VerificationCard'
import type { CloseNudge } from './room/VerificationCard'
import { cn } from '@/lib/utils'
import type { RoomActions, StaleInfo } from '../hooks/useRoom'
import { isEditableTarget } from './shell/keys'
import { DecisionHandleContext } from './shell/context'
import { completesSet } from '../lib/decision'
import { PROPOSAL_ANCHOR } from './room/ProposalsSummary'
import { proposalCardAnchor } from '../lib/triage'
import { PersonAvatar } from './room/PersonAvatar'
import { WaitingFor } from './room/WaitingFor'
import type { Person, PresenceMember, Topology } from '../api/types'
import type { NudgeState } from '../state/types'

/** What a pending layer row needs to say who it waits on and to nudge them (F9). */
export interface WaitingContext {
  roomId: string
  roomTitle: string
  people?: Person[]
  presence: PresenceMember[]
  nudges: Record<string, NudgeState>
  canNudge: boolean
}
import { Badge, layerVariant } from '@/components/ui/badge'
import { Button } from '@/components/ui/button'
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from '@/components/ui/dialog'
import { Input } from '@/components/ui/input'
import { Kbd } from '@/components/ui/kbd'
import { Separator } from '@/components/ui/separator'

// Fields that identify what an action actually targets. Approvers must see these
// highlighted (F2): the layer owner should never rubber-stamp a description
// without noticing which connector/subject/table/task the step will touch.
const TARGET_KEYS = new Set(['connector', 'subject', 'table', 'task', 'task_id'])

/** The server refuses a longer withdraw reason (docs/api.md, 500 characters). */
const MAX_WITHDRAW_REASON = 500

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value)
}

function formatParamValue(value: unknown): string {
  if (typeof value === 'string') return value
  return JSON.stringify(value) ?? String(value)
}

/** A value that changes: `{ from, to }`. It is drawn as a diff (old struck through, then the new one). */
function asDiff(value: unknown): { from: unknown; to: unknown } | null {
  if (!isRecord(value)) return null
  const keys = Object.keys(value)
  return keys.length === 2 && 'from' in value && 'to' in value ? { from: value.from, to: value.to } : null
}

/** "9f2c 1e7a … 0a1b": the first eight and the last four characters, in groups of four. */
function fingerprint(hash: string): string {
  if (hash.length <= 12) return hash.replace(/(.{4})(?=.)/g, '$1 ')
  return `${hash.slice(0, 4)} ${hash.slice(4, 8)} … ${hash.slice(-4)}`
}

// The risk level the badge shows (Q0: `risk_level`). A proposal from before Q0 has none, but its `risk`
// text was often just the level, so that still counts.
function riskLevelOf(proposal: ProposalState): string {
  const level = proposal.risk_level?.trim().toLowerCase()
  if (level) return level.slice(0, 20)
  const legacy = proposal.risk.trim().toLowerCase()
  return legacy === 'low' || legacy === 'medium' || legacy === 'high' ? legacy : ''
}

// High is amber, not red: red is for a rejection or a failed step.
function riskVariant(level: string): 'success' | 'warning' | 'neutral' {
  switch (level) {
    case 'low':
      return 'success'
    case 'high':
      return 'warning'
    default:
      return 'neutral'
  }
}

// What the agent wrote about the risk, without the level it merely repeats.
function riskTextOf(proposal: ProposalState): string {
  const text = proposal.risk.trim()
  return text.toLowerCase() === riskLevelOf(proposal) ? '' : text
}

// Keys A and R stay out of the way of anything that owns the keyboard right now: a field, or an
// open overlay (solo dialog, command menu, hand-off dialog, the account menu, a select list).
const OVERLAY_OPEN = '[role="dialog"], [role="alertdialog"], [role="menu"], [role="listbox"]'

function keyIsForTheCard(e: KeyboardEvent): boolean {
  if (e.defaultPrevented || e.repeat || e.isComposing) return false
  if (e.metaKey || e.ctrlKey || e.altKey || e.shiftKey) return false
  if (isEditableTarget(e.target)) return false
  if (e.target instanceof Element && e.target.closest('[role="textbox"], [role="combobox"]')) return false
  return document.querySelector(OVERLAY_OPEN) === null
}

function StepParams({ params }: { params: unknown }) {
  const box =
    'grid grid-cols-[max-content_minmax(0,1fr)] gap-x-4 gap-y-1 rounded-md border border-border bg-muted px-2.5 py-2 font-mono text-xs'
  if (params === null || params === undefined) {
    return <div className={cn(box, 'block text-subtle-foreground')}>no parameters</div>
  }
  if (!isRecord(params)) {
    return (
      <div className={cn(box, 'block whitespace-pre-wrap break-all')} data-testid="step-params">
        {JSON.stringify(params, null, 2)}
      </div>
    )
  }
  const entries = Object.entries(params)
  if (entries.length === 0) {
    return <div className={cn(box, 'block text-subtle-foreground')}>{'{}'}</div>
  }
  return (
    <div className={box} data-testid="step-params">
      {entries.map(([key, value]) => {
        const target = TARGET_KEYS.has(key.toLowerCase())
        const diff = asDiff(value)
        return (
          <div key={key} className="contents">
            <span
              className={cn(
                'proposal__param-key text-subtle-foreground',
                target && 'proposal__param-key--target text-muted-foreground',
              )}
            >
              {key}
            </span>
            <span className="flex min-w-0 flex-wrap items-center gap-x-1.5 gap-y-1">
              {diff ? (
                <>
                  <s className="break-all text-subtle-foreground">{formatParamValue(diff.from)}</s>
                  <span aria-hidden="true">→</span>
                  <span className="break-all font-medium text-foreground">{formatParamValue(diff.to)}</span>
                </>
              ) : (
                <span
                  className={cn(
                    'proposal__param-val break-all',
                    target && 'proposal__param-val--target font-medium text-foreground',
                  )}
                >
                  {formatParamValue(value)}
                </span>
              )}
              {target && (
                <Badge variant="outline" className="h-4 px-1 font-sans text-[11px]">
                  target
                </Badge>
              )}
            </span>
          </div>
        )
      })}
    </div>
  )
}

function HashPill({ hash, struck = false, primary = false }: { hash: string; struck?: boolean; primary?: boolean }) {
  return (
    <span
      title={hash}
      className={cn(
        'inline-flex h-6 items-center gap-1.5 rounded-md border border-input bg-muted px-2 font-mono text-xs font-medium text-foreground',
        struck && 'text-subtle-foreground line-through',
      )}
    >
      <Hash className="size-3 shrink-0" aria-hidden="true" />
      <span data-testid={primary ? 'params-hash-short' : undefined}>{fingerprint(hash)}</span>
    </span>
  )
}

// The fingerprint of what the person is about to approve (auth PR 4): the short form to glance at,
// the full value in a read-only field to compare, and a copy button. This is the value approve and
// reject send back.
function ParamsHash({ hash, deciding, noteId }: { hash: string; deciding: boolean; noteId: string }) {
  const [copied, setCopied] = useState(false)
  if (!hash) return null
  const copy = () => {
    if (!navigator.clipboard) return
    void navigator.clipboard.writeText(hash).then(
      () => {
        setCopied(true)
        window.setTimeout(() => setCopied(false), 1500)
      },
      () => undefined,
    )
  }
  return (
    <div className="flex flex-col gap-1.5" id={`${noteId}-hash`}>
      <div className="flex flex-wrap items-center gap-2">
        <span className="text-muted-foreground">{deciding ? 'Approving exactly this' : 'Fingerprint'}</span>
        <HashPill hash={hash} primary />
        <Button
          variant="ghost"
          className="h-6 gap-1 px-1.5 text-xs"
          onClick={copy}
          aria-label={copied ? 'Copied' : 'Copy hash'}
        >
          {copied ? <Check className="size-3" aria-hidden="true" /> : <Copy className="size-3" aria-hidden="true" />}
          <span aria-hidden="true">{copied ? 'Copied' : 'Copy'}</span>
        </Button>
        {deciding && (
          <span className="ml-auto text-xs text-subtle-foreground">
            if the steps change, this fingerprint changes and your approval is void
          </span>
        )}
      </div>
      <details className="text-xs">
        <summary className="w-fit cursor-pointer select-none rounded-sm text-subtle-foreground hover:text-foreground focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring">
          Full hash
        </summary>
        <Input
          className="mono mt-1.5 h-7 font-mono text-xs"
          readOnly
          value={hash}
          onFocus={(e) => e.currentTarget.select()}
          aria-label="Full params hash"
        />
      </details>
    </div>
  )
}

// What the fix will do to this layer, for the solo dialog: "kafka-connect · restart_connector_task · pg-orders-connector task 0".
function layerSummary(proposal: ProposalState, layer: string): string[] {
  const mine = proposal.steps.filter((s) => s.layer === layer)
  if (mine.length === 0) return [proposal.title]
  return mine.map((s) => {
    const targets = isRecord(s.params)
      ? Object.entries(s.params)
          .filter(([k]) => TARGET_KEYS.has(k.toLowerCase()))
          .map(([k, v]) => (['task', 'task_id'].includes(k.toLowerCase()) ? `${k}\u00a0${formatParamValue(v)}` : formatParamValue(v)))
          .join(' ')
      : ''
    return [layer, s.action, targets].filter(Boolean).join(' · ')
  })
}

// Solo mode (docs/design/auth.md 5.3): the person types "<pipeline_id>/<layer>" by
// hand. A speed bump, not a control: no copy button, and paste is blocked.
function SoloConfirmDialog({
  proposal,
  layer,
  want,
  busy,
  rejected,
  onConfirm,
  onCancel,
  onClosed,
}: {
  proposal: ProposalState
  layer: string
  want: string
  busy: boolean
  /** The server refused the typed text once already. */
  rejected: boolean
  onConfirm: (typed: string) => void
  onCancel: () => void
  /** The dialog has closed: put focus back (Radix would use the element focused when it opened). */
  onClosed: () => void
}) {
  const [typed, setTyped] = useState('')
  const inputRef = useRef<HTMLInputElement>(null)
  const inputId = useId()
  const hintId = `${inputId}-hint`
  const match = typed === want
  const submit = () => {
    if (match && !busy) onConfirm(typed)
  }
  const progress = match
    ? 'It matches.'
    : want.startsWith(typed)
      ? `${want.length - typed.length} character${want.length - typed.length === 1 ? '' : 's'} to go`
      : 'This does not match yet.'
  return (
    <Dialog open onOpenChange={(open) => !open && onCancel()}>
      <DialogContent
        hideClose
        className="max-w-[480px]"
        onOpenAutoFocus={(e) => {
          e.preventDefault()
          inputRef.current?.focus()
        }}
        onCloseAutoFocus={(e) => {
          e.preventDefault()
          onClosed()
        }}
      >
        <form
          className="grid gap-4"
          onSubmit={(e) => {
            e.preventDefault()
            submit()
          }}
        >
          <DialogHeader className="gap-1.5">
            <DialogTitle className="flex items-start gap-2">
              <Lock className="mt-0.5 size-4 shrink-0 text-warning-foreground" aria-hidden="true" />
              <span>Approve {layer} in solo mode</span>
            </DialogTitle>
            <DialogDescription>No second owner is online to check this.</DialogDescription>
          </DialogHeader>
          <div className="flex flex-col gap-1.5 rounded-lg border border-border bg-muted px-3 py-2.5">
            <span className="text-xs text-subtle-foreground">You are approving</span>
            {layerSummary(proposal, layer).map((line) => (
              <span key={line} className="font-medium">
                {line}
              </span>
            ))}
            {proposal.params_hash && (
              <span className="font-mono text-xs text-subtle-foreground">fingerprint {fingerprint(proposal.params_hash)}</span>
            )}
          </div>
          <div className="flex flex-col gap-1.5">
            {/* The label holds the exact string, so a screen reader announces what to type. */}
            <label htmlFor={inputId} className="text-sm font-medium">
              Solo mode: type <code className="font-mono">{want}</code> to approve
            </label>
            <Input
              ref={inputRef}
              id={inputId}
              value={typed}
              onChange={(e) => setTyped(e.target.value)}
              onPaste={(e) => e.preventDefault()}
              onDrop={(e) => e.preventDefault()}
              onKeyDown={(e) => {
                if (e.key === 'Enter' && (e.metaKey || e.ctrlKey)) {
                  e.preventDefault()
                  submit()
                }
              }}
              aria-describedby={hintId}
              aria-invalid={rejected || undefined}
              autoComplete="off"
              spellCheck={false}
              className="h-9 font-mono text-base font-medium"
            />
            <span id={hintId} className="text-xs text-subtle-foreground">
              Type it by hand. Paste is off.
            </span>
            <span className="text-xs text-muted-foreground">{progress}</span>
            {rejected && (
              <span className="text-xs font-medium text-destructive-fg" role="alert">
                The server did not accept that text.
              </span>
            )}
          </div>
          <DialogFooter className="items-center">
            <Button variant="ghost" onClick={onCancel}>
              Cancel
              <Kbd aria-hidden="true">esc</Kbd>
            </Button>
            <Button type="submit" variant="success" disabled={busy || !match}>
              Approve
              <Kbd aria-hidden="true">⌘⏎</Kbd>
            </Button>
          </DialogFooter>
        </form>
      </DialogContent>
    </Dialog>
  )
}

// The approval that completes the set runs the fix right away. One more look, with the steps
// that will run, before a production change (Enter or Ctrl/⌘⏎ confirms, Esc cancels).
function RunConfirmDialog({
  proposal,
  layer,
  busy,
  onConfirm,
  onCancel,
  onClosed,
}: {
  proposal: ProposalState
  layer: string
  busy: boolean
  onConfirm: () => void
  onCancel: () => void
  onClosed: () => void
}) {
  const n = proposal.steps.length
  const layers = [...new Set(proposal.steps.map((s) => s.layer))]
  return (
    <Dialog open onOpenChange={(open) => !open && onCancel()}>
      <DialogContent
        hideClose
        role="alertdialog"
        className="max-w-[480px]"
        onCloseAutoFocus={(e) => {
          e.preventDefault()
          onClosed()
        }}
        onKeyDown={(e) => {
          // A held Enter that opened this dialog must not also confirm it, not even through the
          // focused button's own activation: the repeats are swallowed.
          if (e.key === 'Enter' && e.repeat) {
            e.preventDefault()
            return
          }
          if (e.key === 'Enter' && !busy && !(e.target instanceof HTMLButtonElement && e.target.dataset.cancel)) {
            e.preventDefault()
            onConfirm()
          }
        }}
      >
        <DialogHeader className="gap-1.5">
          <DialogTitle>Run this fix now?</DialogTitle>
          <DialogDescription>
            Your approval completes the set. Piperoom runs {n} {n === 1 ? 'step' : 'steps'} on {layers.join(' and ') || layer}{' '}
            right away.
          </DialogDescription>
        </DialogHeader>
        <ul className="m-0 flex list-none flex-col gap-1 rounded-lg border border-border bg-muted p-3 font-mono text-xs">
          {proposal.steps.map((s, i) => (
            <li key={i}>
              {i + 1}. {s.layer} · {s.action}
            </li>
          ))}
        </ul>
        <DialogFooter className="items-center">
          <Button variant="ghost" data-cancel="1" onClick={onCancel}>
            Cancel
            <Kbd aria-hidden="true">esc</Kbd>
          </Button>
          <Button variant="success" disabled={busy} autoFocus onClick={onConfirm}>
            Approve and run
            <Kbd aria-hidden="true">⏎</Kbd>
          </Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  )
}

// One row of the approval matrix: a layer, who decided it (grant and method), when, and for
// the layer you own, the buttons.
function LayerRow({
  proposal,
  layer,
  user,
  actions,
  pipelineId,
  shortcuts,
  noHashNoteId,
  headingId,
  onSolo,
  waiting,
  roomOpen,
}: {
  proposal: ProposalState
  layer: string
  user: CurrentUser
  actions: RoomActions
  pipelineId: string
  /** This row's buttons answer to A and R (the first layer you can decide). */
  shortcuts: boolean
  noHashNoteId: string
  /** The card heading: focus goes there when the Approve button is gone after the dialog closes. */
  headingId: string
  /** The server asked for a solo confirm: the panel shows the solo banner. */
  onSolo: () => void
  waiting?: WaitingContext
  /** A closed room, or a withdrawn proposal, waits on nobody: an undecided layer reads "Not decided". */
  roomOpen: boolean
}) {
  const [rejecting, setRejecting] = useState(false)
  const [reason, setReason] = useState('')
  const [confirming, setConfirming] = useState(false)
  const [runConfirm, setRunConfirm] = useState(false)
  const [busy, setBusy] = useState(false)
  const [refused, setRefused] = useState(false)
  const approveRef = useRef<HTMLButtonElement>(null)

  // After the solo dialog closes focus goes back to Approve. On the server-asks path Approve was
  // disabled while the call ran, so the browser had already dropped focus to the page body.
  const restoreFocus = () => {
    const button = approveRef.current
    if (button && !button.disabled) button.focus()
    else document.getElementById(headingId)?.focus()
  }

  // Not solo (as far as we know): the first approve goes without `confirm`, and a
  // 400 "solo mode:" answer opens the box. Solo (`/api/me.solo`): the box opens
  // up front and no call is made until the text is typed.
  //
  // `proposal.params_hash` is the hash this card shows. It is sent as it is, not
  // fetched again at click time: if the fix changed under the screen, the server
  // answers 409 params_mismatch (before it looks at `confirm`) and the stale banner
  // tells the person (auth PR 4).
  const approve = async (confirm?: string) => {
    setBusy(true)
    try {
      const res = await actions.approve(proposal.proposal_id, layer, {
        params_hash: proposal.params_hash,
        ...(confirm !== undefined ? { confirm } : {}),
      })
      if (res?.mismatch) {
        // Stale screen: close the box. No retry; the room is reloading, and the hook keeps
        // what the person looked at for the card to show afterwards.
        setConfirming(false)
        setRefused(false)
      } else if (res?.needsConfirm) {
        onSolo()
        setRefused(confirm !== undefined)
        setConfirming(true)
      } else {
        setConfirming(false)
        setRefused(false)
      }
    } finally {
      setBusy(false)
    }
  }
  const state = proposal.approvals[layer] ?? 'pending'
  const decision = proposal.decisions?.[layer]
  // No params_hash (the server could not fingerprint the fix): no approval can carry
  // a hash the server would accept, so Approve stays off. Reject stays on: the server
  // takes a rejection with an empty hash for exactly this open proposal (params_hash.go).
  const hashed = proposal.params_hash !== ''
  // A closed room takes no decision, whatever state its log left the proposal in.
  const canDecide = roomOpen && canDecideLayer(proposal, user, layer)
  // Q0 (QA G5): a layer the steps do not write to, whose owner must approve all the same.
  const impacted =
    (proposal.impacted_layers ?? []).includes(layer) && !proposal.steps.some((s) => s.layer === layer)
  const settled = !roomOpen || proposal.status === 'withdrawn'

  // Solo: the typed box. The approval that completes the set: the run confirm. Otherwise at once.
  const completing = completesSet(proposal, layer)
  const startApprove = () => (user.solo ? setConfirming(true) : completing ? setRunConfirm(true) : void approve())

  // A and R. The listener is added once; it calls the handler of the latest render.
  const latest = useRef<(key: 'a' | 'r') => boolean>(() => false)
  useEffect(() => {
    latest.current = (key) => {
      if (!canDecide || rejecting || confirming || runConfirm || busy) return false
      if (key === 'a') {
        if (!hashed) return false
        startApprove()
      } else {
        setRejecting(true)
      }
      return true
    }
  })
  useEffect(() => {
    if (!shortcuts) return
    const onKey = (e: KeyboardEvent) => {
      const key = e.key.toLowerCase()
      if (key !== 'a' && key !== 'r') return
      if (!keyIsForTheCard(e)) return
      if (latest.current(key)) e.preventDefault()
    }
    window.addEventListener('keydown', onKey)
    return () => window.removeEventListener('keydown', onKey)
  }, [shortcuts])

  // The decision bar acts through this row, so A, R, the bar and the buttons share startApprove.
  const registry = useContext(DecisionHandleContext)
  useEffect(() => {
    if (!shortcuts) return
    registry?.set({
      approve: () => void latest.current('a'),
      reject: () => void latest.current('r'),
      scrollTo: () => {
        const el = document.getElementById(PROPOSAL_ANCHOR)
        const calm = window.matchMedia?.('(prefers-reduced-motion: reduce)').matches
        el?.scrollIntoView?.({ behavior: calm ? 'auto' : 'smooth', block: 'start' })
        document.getElementById(headingId)?.focus({ preventScroll: true })
      },
    })
    return () => registry?.set(null)
  })

  const reasonText = decision?.outcome === 'rejected' ? decision.reason : undefined
  const decidedAt = decision?.at ? clockTime(decision.at) : ''
  const decidedName = decision ? neutralizeBadgeText(decision.by.name || decision.by.id) : ''

  return (
    <div
      className={cn(
        'flex min-h-11 flex-wrap items-center gap-x-2.5 gap-y-1.5 px-3.5 py-1.5',
        canDecide && 'bg-warning-subtle',
      )}
      data-layer={layer}
      data-state={state}
    >
      <Badge variant={layerVariant(layer)} mono>
        {layer}
      </Badge>
      {impacted && (
        <Badge
          variant="outline"
          data-testid={`impacted-${layer}`}
          title="The fix does not write to this layer, but it changes this layer's data: its owner must approve too"
        >
          impacted
        </Badge>
      )}
      {decision ? (
        <>
          {/* The sentence is for screen readers (and tests); the pieces next to it are the same facts, drawn. */}
          <span className="sr-only" data-testid={`decision-${layer}`}>
            {decision.outcome} by {decidedName}
            {decision.identity && ` (${describeIdentity(decision.identity)})`}
            {reasonText ? `: ${reasonText}` : ''}
            {decidedAt && ` at ${decidedAt}`}
            {personVerification(decision.by).state !== 'verified' && `. ${decision.by.id} is not verified`}
          </span>
          <span className="inline-flex min-w-0 items-center gap-2.5" aria-hidden="true">
            <PersonAvatar actor={{ id: decision.by.id, name: decidedName }} size={20} />
            <PersonName person={decision.by} name={decidedName} nameClassName="font-medium" />
            {decision.identity?.grant && (
              <span className="font-mono text-xs text-subtle-foreground">{decision.identity.grant}</span>
            )}
          </span>
          <Badge
            variant={decision.outcome === 'approved' ? 'success' : 'destructive'}
            className="ml-auto"
            title={[describeIdentity(decision.identity), identityDetails(decision.identity)].filter(Boolean).join('\n') || undefined}
            aria-hidden="true"
          >
            {decision.outcome === 'approved' ? (
              <Check className="size-3" aria-hidden="true" />
            ) : (
              <X className="size-3" aria-hidden="true" />
            )}
            {decision.outcome === 'approved' ? 'Approved' : 'Rejected'}
            {decision.at && (
              <>
                {' · '}
                <Clock iso={decision.at} />
              </>
            )}
          </Badge>
          {reasonText && (
            <span className="basis-full text-xs text-muted-foreground" aria-hidden="true">
              Reason: {reasonText}
            </span>
          )}
        </>
      ) : canDecide ? (
        <>
          <PersonAvatar actor={{ id: user.id, name: user.name }} size={20} />
          <PersonName person={{ id: user.id, name: user.name, kind: 'human', verified: user.verified }} nameClassName="font-medium" />
          <span className="text-xs text-muted-foreground">you</span>
          {/* muted, not subtle: subtle text is 4.2:1 on the amber tint of this row */}
          <span className="font-mono text-xs text-muted-foreground">user:{user.id}</span>
          <span className="text-xs font-medium text-warning-foreground">pending · your layer</span>
        </>
      ) : waiting && state === 'pending' && !settled ? (
        <WaitingFor
          proposalId={proposal.proposal_id}
          layer={layer}
          roomId={waiting.roomId}
          roomTitle={waiting.roomTitle}
          people={waiting.people}
          presence={waiting.presence}
          nudge={waiting.nudges[`${proposal.proposal_id}/${layer}`]}
          canNudge={waiting.canNudge}
          actions={actions}
        />
      ) : (
        <Badge className="ml-auto" variant="neutral">
          {settled && state === 'pending' ? (
            'Not decided'
          ) : (
            <>
              <span className="size-1.5 rounded-full bg-warning" aria-hidden="true" />
              Pending
            </>
          )}
        </Badge>
      )}

      {canDecide && !rejecting && (
        <span className="ml-auto inline-flex items-center gap-2">
          <Button variant="ghost-danger" size="sm" className="coarse:h-11 coarse:px-4" onClick={() => setRejecting(true)} aria-keyshortcuts={shortcuts ? 'R' : undefined}>
            Reject
            {shortcuts && <Kbd aria-hidden="true">R</Kbd>}
          </Button>
          <Button
            variant="success"
            size="sm"
            className="coarse:h-11 coarse:px-4"
            ref={approveRef}
            disabled={busy || !hashed}
            aria-describedby={hashed ? undefined : noHashNoteId}
            aria-keyshortcuts={shortcuts ? 'A' : undefined}
            onClick={startApprove}
          >
            Approve
            {shortcuts && <Kbd aria-hidden="true">A</Kbd>}
          </Button>
        </span>
      )}
      {canDecide && rejecting && (
        <form
          className="ml-auto flex min-w-0 flex-1 basis-72 items-center justify-end gap-2"
          onSubmit={(e) => {
            e.preventDefault()
            if (!reason.trim()) return
            // The hash on screen goes with the reason, "" included for an unhashable open proposal.
            actions.reject(proposal.proposal_id, layer, reason.trim(), proposal.params_hash)
            setRejecting(false)
            setReason('')
          }}
        >
          <Input
            value={reason}
            onChange={(e) => setReason(e.target.value)}
            onKeyDown={(e) => {
              if (e.key === 'Escape') {
                e.stopPropagation()
                setRejecting(false)
                setReason('')
              }
            }}
            placeholder="Reason"
            aria-label={`Rejection reason for ${layer}`}
            className="min-w-0 flex-1"
            autoComplete="off"
            autoFocus
          />
          <Button type="submit" variant="destructive" size="sm" className="coarse:h-11 coarse:px-4" disabled={!reason.trim()}>
            Confirm
          </Button>
          <Button
            variant="ghost"
            size="sm"
            className="coarse:h-11 coarse:px-4"
            onClick={() => {
              setRejecting(false)
              setReason('')
            }}
          >
            Cancel
          </Button>
        </form>
      )}
      {canDecide && runConfirm && (
        <RunConfirmDialog
          proposal={proposal}
          layer={layer}
          busy={busy}
          onCancel={() => setRunConfirm(false)}
          onConfirm={() => {
            setRunConfirm(false)
            void approve()
          }}
          onClosed={restoreFocus}
        />
      )}
      {canDecide && confirming && (
        <SoloConfirmDialog
          proposal={proposal}
          layer={layer}
          want={soloConfirmText(pipelineId, layer)}
          busy={busy}
          rejected={refused}
          onConfirm={(typed) => void approve(typed)}
          onCancel={() => {
            setConfirming(false)
            setRefused(false)
          }}
          onClosed={restoreFocus}
        />
      )}
    </div>
  )
}

function latestDecision(proposal: ProposalState): ReactNode {
  let best: { at: number; text: ReactNode } | null = null
  for (const [layer, d] of Object.entries(proposal.decisions ?? {})) {
    const at = d.at ? Date.parse(d.at) : NaN
    if (Number.isNaN(at) || (best && at <= best.at)) continue
    best = {
      at,
      text: (
        <span className="inline-flex flex-wrap items-center gap-x-1">
          <PersonName person={d.by} /> {d.outcome} {layer} {timeAgo(d.at as string)}
        </span>
      ),
    }
  }
  return best?.text ?? null
}

function Proposal({
  proposal,
  user,
  actions,
  pipelineId,
  shortcutLayer,
  sentHash,
  ribbon,
  onSolo,
  waiting,
  topology,
  nudge,
  withdrawable,
}: {
  nudge: CloseNudge
  topology: Topology | null
  /** The driver may withdraw this proposal (open room, open proposal; Q0). */
  withdrawable: boolean
  proposal: ProposalState
  user: CurrentUser
  actions: RoomActions
  pipelineId: string
  /** The layer whose buttons answer to A and R in this card, if any. */
  shortcutLayer: string | undefined
  /** The hash the person looked at when the server answered 409 for this proposal (kept by useRoom). */
  sentHash: string | null
  /** False while the decision bar already says "Needs your approval": one amber per fact. */
  ribbon: boolean
  onSolo: () => void
  waiting: WaitingContext
}) {
  const titleId = useId()
  const noteId = useId()
  // "Got it" hides the note for one stale hash. The note itself comes from `sentHash`, which
  // useRoom keeps across the room reload (this card is unmounted while the room loads).
  const [acknowledged, setAcknowledged] = useState<string | null>(null)
  // The driver's Withdraw: a reason box first (optional), the call second, the server's refusal below it.
  const [withdrawing, setWithdrawing] = useState(false)
  const [reason, setReason] = useState('')
  const [withdrawBusy, setWithdrawBusy] = useState(false)
  const [withdrawError, setWithdrawError] = useState<string | null>(null)
  // The server took it; the proposal.withdrawn event that flips this card may still be on its way.
  const [withdrawSent, setWithdrawSent] = useState(false)
  const reasonId = useId()
  const withdrawButton = useRef<HTMLButtonElement>(null)
  const cancelWithdraw = () => {
    setWithdrawing(false)
    setReason('')
    setWithdrawError(null)
    // The form takes its own focus with it; put it back on the button that opened it.
    window.setTimeout(() => withdrawButton.current?.focus(), 0)
  }
  const withdraw = async () => {
    if (!actions.withdraw || withdrawBusy) return
    setWithdrawBusy(true)
    setWithdrawError(null)
    const res = await actions.withdraw(proposal.proposal_id, reason.trim() || undefined)
    setWithdrawBusy(false)
    if (res.ok) {
      setWithdrawSent(true)
      setWithdrawing(false)
      setReason('')
    } else setWithdrawError(res.error ?? 'The fix could not be withdrawn.')
  }

  // A closed room waits on nobody, whatever state its log left the proposal in.
  const open = proposal.status === 'open' && nudge.roomOpen
  const mine = open ? proposal.required_layers.filter((l) => canDecideLayer(proposal, user, l)) : []
  const hashed = proposal.params_hash !== ''
  const pending = proposal.required_layers.filter((l) => (proposal.approvals[l] ?? 'pending') === 'pending')
  const approved = proposal.required_layers.filter((l) => proposal.approvals[l] === 'approved').length
  const previous = latestDecision(proposal)
  const level = riskLevelOf(proposal)
  const riskText = riskTextOf(proposal)
  const reasons = proposal.impact_reasons ?? []
  const withdrawn = proposal.status === 'withdrawn' ? proposal.withdrawn : undefined
  const withdrawnBy = withdrawn?.by && withdrawn.by.kind !== 'system' ? withdrawn.by : undefined

  return (
    // overflow-wrap:anywhere is inherited by every line of the card: an unbroken risk, rollback, description or
    // reason wraps instead of running under the 300px rail (QA G12), and it lets the flex children below shrink.
    <article
      id={proposalCardAnchor(proposal.proposal_id)}
      className="flex min-w-0 flex-col rounded-lg border border-border bg-card text-sm text-card-foreground [overflow-wrap:anywhere]"
      aria-labelledby={titleId}
    >
      {proposal.status === 'withdrawn' && (
        <div
          className="flex min-h-9 flex-wrap items-center gap-x-2 gap-y-0.5 rounded-t-lg border-b border-border bg-muted px-3.5 py-1.5 text-sm text-muted-foreground"
          data-testid="withdrawn-note"
        >
          <Ban className="size-3.5 shrink-0" aria-hidden="true" />
          <span className="min-w-0 font-medium text-foreground">
            {withdrawn?.reason ? `Withdrawn — ${withdrawn.reason}` : 'Withdrawn'}
          </span>
          {withdrawnBy && (
            <span className="inline-flex min-w-0 items-center gap-1 text-xs">
              by <PersonName person={withdrawnBy} />
            </span>
          )}
          {withdrawn?.at && (
            <span className="text-xs">
              <Clock iso={withdrawn.at} />
            </span>
          )}
        </div>
      )}
      {ribbon && mine.length > 0 && (
        <div className="flex min-h-9 flex-wrap items-center gap-x-2 gap-y-0.5 rounded-t-lg border-b border-border bg-warning-subtle px-3.5 py-1.5 text-sm font-medium text-warning-foreground">
          <TriangleAlert className="size-3.5 shrink-0" aria-hidden="true" />
          <span>Needs your approval · {mine.join(' · ')}</span>
          {previous && <span className="ml-auto text-xs font-normal text-muted-foreground">{previous}</span>}
        </div>
      )}
      <div className="flex min-w-0 flex-col gap-3 px-3.5 pb-3 pt-3.5">
        <div className="flex flex-wrap items-start gap-2.5">
          <div className="flex min-w-0 flex-1 basis-80 flex-col gap-1.5">
            <h3 id={titleId} tabIndex={-1} className="text-lg font-semibold outline-none [text-wrap:balance]">
              {proposal.title}
            </h3>
            {level && (
              <p className="m-0 flex flex-wrap items-center gap-x-1.5 gap-y-1">
                <Badge variant={riskVariant(level)} data-testid="risk-level">
                  {level === 'high' && <TriangleAlert className="size-3" aria-hidden="true" />}
                  risk {level}
                </Badge>
              </p>
            )}
            {riskText && (
              <p className="m-0 min-w-0 text-xs text-muted-foreground" data-testid="risk-text">
                {!level && 'risk: '}
                {riskText}
              </p>
            )}
            {reasons.length > 0 && (
              <ul
                className="m-0 flex min-w-0 list-disc flex-col gap-0.5 pl-4 text-xs text-muted-foreground"
                aria-label="Why this risk"
                data-testid="impact-reasons"
              >
                {reasons.map((r, i) => (
                  <li key={i} className="min-w-0">
                    {r}
                  </li>
                ))}
              </ul>
            )}
            {proposal.rollback && (
              <p className="m-0 flex min-w-0 items-start gap-1 text-xs text-muted-foreground" data-testid="rollback-text">
                <RotateCcw className="mt-0.5 size-3 shrink-0" aria-hidden="true" />
                <span className="min-w-0">rollback: {proposal.rollback}</span>
              </p>
            )}
          </div>
          <div className="flex min-w-0 max-w-full flex-col items-start gap-1.5 sm:items-end">
            <span className="flex min-w-0 flex-wrap items-center gap-2 text-xs text-subtle-foreground">
              {proposal.status === 'approved' && <Badge variant="success">approved</Badge>}
              {proposal.status === 'rejected' && <Badge variant="destructive">rejected</Badge>}
              {proposal.status === 'withdrawn' && <Badge variant="neutral">withdrawn</Badge>}
              {open && mine.length === 0 && pending.length > 0
                ? `waiting for ${pending.join(', ')} ${pending.length === 1 ? 'owner' : 'owners'} · ${approved} of ${proposal.required_layers.length} approved`
                : 'proposed by the agent'}
            </span>
            {withdrawable && actions.withdraw && !withdrawing && !withdrawSent && (
              <Button
                ref={withdrawButton}
                variant="ghost"
                size="sm"
                className="coarse:h-11 coarse:px-4"
                onClick={() => setWithdrawing(true)}
              >
                Withdraw
              </Button>
            )}
          </div>
        </div>

        {withdrawable && actions.withdraw && withdrawing && (
          <form
            className="flex min-w-0 flex-wrap items-center gap-2"
            onSubmit={(e) => {
              e.preventDefault()
              void withdraw()
            }}
          >
            <label htmlFor={reasonId} className="sr-only">
              Reason for withdrawing this fix (optional)
            </label>
            <Input
              id={reasonId}
              value={reason}
              onChange={(e) => setReason(e.target.value)}
              onKeyDown={(e) => {
                if (e.key === 'Escape') {
                  e.stopPropagation()
                  cancelWithdraw()
                }
              }}
              placeholder="Reason (optional)"
              maxLength={MAX_WITHDRAW_REASON}
              autoComplete="off"
              autoFocus
              className="min-w-0 flex-1 basis-60"
            />
            <Button type="submit" variant="destructive" size="sm" className="coarse:h-11 coarse:px-4" disabled={withdrawBusy}>
              {withdrawBusy ? 'Withdrawing…' : 'Withdraw fix'}
            </Button>
            <Button type="button" variant="ghost" size="sm" className="coarse:h-11 coarse:px-4" onClick={cancelWithdraw}>
              Cancel
            </Button>
          </form>
        )}
        {withdrawError && (
          <p role="alert" className="m-0 text-xs font-medium text-destructive-fg" data-testid="withdraw-error">
            {withdrawError}
          </p>
        )}

        {proposal.steps.length > 0 && (
          <ol className="grid list-none grid-cols-[repeat(auto-fit,minmax(min(100%,16rem),1fr))] gap-3 p-0">
            {proposal.steps.map((s, i) => (
              <li key={i} className="grid grid-cols-[22px_minmax(0,1fr)] items-start gap-2.5">
                <span
                  className="inline-flex size-[22px] items-center justify-center rounded-full bg-muted font-mono text-[11px] leading-none text-subtle-foreground"
                  aria-label={`Step ${i + 1}`}
                >
                  {i + 1}
                </span>
                <div className="flex min-w-0 flex-col gap-1.5">
                  <div className="flex flex-wrap items-center gap-2">
                    <Badge variant={layerVariant(s.layer)} mono>
                      {s.layer}
                    </Badge>
                    <span className="font-mono font-medium">{s.action}</span>
                  </div>
                  {s.description && <p className="text-xs text-muted-foreground">{s.description}</p>}
                  {s.verify && <p className="text-xs text-muted-foreground">After it runs — {s.verify}</p>}
                  <StepParams params={s.params} />
                </div>
              </li>
            ))}
          </ol>
        )}

        <BlastRadius proposal={proposal} topology={topology} />

        {hashed ? (
          <ParamsHash hash={proposal.params_hash} deciding={mine.length > 0} noteId={noteId} />
        ) : open ? (
          <p
            id={noteId}
            className="text-xs font-medium text-destructive-fg"
            data-testid="params-hash-missing"
            role="note"
          >
            This fix can't be approved here: its parameters could not be fingerprinted (see the server log). It can still be
            rejected.
          </p>
        ) : (
          <p className="text-xs text-subtle-foreground" data-testid="params-hash-none">
            no params hash recorded
          </p>
        )}

        {sentHash && hashed && sentHash !== proposal.params_hash && acknowledged !== sentHash && (
          <div className="flex flex-wrap items-center gap-2 text-muted-foreground">
            <span>You looked at</span>
            <HashPill hash={sentHash} struck />
            <span>· server now has</span>
            <HashPill hash={proposal.params_hash} />
            <Button variant="ghost" size="sm" className="ml-auto h-6 px-1.5 text-xs" onClick={() => setAcknowledged(sentHash)}>
              Got it
            </Button>
          </div>
        )}
      </div>

      <Separator />
      <div className="flex flex-col divide-y divide-border">
        {proposal.required_layers.map((layer) => (
          <LayerRow
            key={layer}
            proposal={proposal}
            layer={layer}
            user={user}
            actions={actions}
            pipelineId={pipelineId}
            shortcuts={shortcutLayer === layer}
            noHashNoteId={noteId}
            headingId={titleId}
            onSolo={onSolo}
            waiting={waiting}
            roomOpen={nudge.roomOpen}
          />
        ))}
      </div>
      <ExecutionCard
        proposal={proposal}
        rollback={
          isViewer(user)
            ? undefined
            : { asDriver: nudge.isDriver, onSteer: actions.steer, sends: actions.sends }
        }
      />
      {proposal.verification && <VerificationCard verification={proposal.verification} title={proposal.title} nudge={nudge} />}
    </article>
  )
}

export function ProposalsPanel({
  state,
  user,
  actions,
  stale = null,
  ribbon = true,
  people,
  presence = [],
  topology = null,
  onCloseRoom,
}: {
  /** Opens the close dialog with the resolution filled in (the driver's "Close room" after a verified fix). */
  onCloseRoom?: (initialText: string) => void
  /** From useTopology: names the downstream node ids a preview lists. */
  topology?: Topology | null
  state: RoomState
  user: CurrentUser
  actions: RoomActions
  /** From useRoom: the 409 that is being shown, if any. Survives the room reload. */
  stale?: StaleInfo | null
  /** Draw each card's amber "Needs your approval" ribbon. RoomScreen turns it off when the decision bar shows it. */
  ribbon?: boolean
  /** From RoomScreen: GET /api/people (members only) and the presence frame. */
  people?: Person[]
  presence?: PresenceMember[]
}) {
  const waiting: WaitingContext = {
    roomId: state.id,
    roomTitle: state.title,
    people,
    presence,
    nudges: state.nudges,
    canNudge: !isViewer(user) && state.status === 'open',
  }
  const proposals = state.proposals
  // A closed room takes no decisions (the server answers 409 room_closed), but its proposals keep
  // status "open": with no decidable layers no card shows Approve, Reject or a "your layer" row,
  // and A and R do nothing, matching the decision bar, which already hides on a closed room.
  const decider: CurrentUser = state.status === 'open' ? user : { ...user, layers: [] }
  const nudge: CloseNudge = {
    roomOpen: state.status === 'open',
    driver: state.driver ?? undefined,
    isDriver: state.driver?.id === user.id,
    onClose: onCloseRoom,
  }
  // `/api/me.solo` says so up front; on an older server the first approve that
  // asks for a confirm tells us.
  const [soloSeen, setSoloSeen] = useState(false)
  const solo = user.solo === true || soloSeen

  // A and R belong to one row at a time: the first layer you can decide, in the first such proposal.
  let shortcutKey = ''
  for (const p of proposals) {
    const layer = p.required_layers.find((l) => canDecideLayer(p, decider, l))
    if (layer) {
      shortcutKey = `${p.proposal_id}/${layer}`
      break
    }
  }

  return (
    <section className="flex flex-col gap-2.5 text-sm" aria-label="Proposals">
      {solo && (
        <p
          className="flex flex-wrap items-center gap-x-2.5 gap-y-1 rounded-md bg-warning-subtle px-3 py-1.5 text-sm font-medium text-warning-foreground"
          role="status"
        >
          <Lock className="size-3.5 shrink-0" aria-hidden="true" />
          Solo mode
          <span className="text-xs font-normal text-muted-foreground">
            You can approve every layer. Each approval needs a typed confirmation and is logged with your identity.
          </span>
        </p>
      )}
      {proposals.length === 0 ? (
        <p className="text-sm text-muted-foreground">No proposals yet.</p>
      ) : (
        <div className="flex flex-col gap-3">
          {proposals.map((p) => (
            <Proposal
              key={p.proposal_id}
              proposal={p}
              user={decider}
              actions={actions}
              pipelineId={state.pipeline_id}
              sentHash={stale?.proposalId === p.proposal_id ? stale.sentHash : null}
              shortcutLayer={shortcutKey.startsWith(`${p.proposal_id}/`) ? shortcutKey.slice(p.proposal_id.length + 1) : undefined}
              ribbon={ribbon}
              onSolo={() => setSoloSeen(true)}
              waiting={waiting}
              topology={topology}
              nudge={nudge}
              withdrawable={canWithdraw(state, user, p)}
            />
          ))}
        </div>
      )}
    </section>
  )
}

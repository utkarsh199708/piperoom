// The approval moment (docs/design/ui-v2: Room-Approval, Solo-Confirm, Stale-409, Room-Viewer).
//
// Rules that do not move with the look:
//  - Approve and Reject send the params_hash that is ON SCREEN, never one fetched at click time.
//  - The server checks the hash (409 params_mismatch) before it asks for the solo confirm.
//    A 409 is never retried: the stale banner and toast tell the person, they look again.
//  - `canDecideLayer` is the only gate for a layer row's buttons, and `approveAllFor` for the card's
//    one "Approve and run" that replaces them (plus "no hash" and "busy", which only switch them
//    off). A viewer or a non-owner gets no buttons at all.
import { PersonName } from '@/components/VerifiedBadge'
import { neutralizeBadgeText, personVerification } from '@/lib/verification'
import { useContext, useEffect, useId, useRef, useState } from 'react'
import type { ReactNode, RefObject } from 'react'
import { Copy, Hash, Lock, RotateCcw, TriangleAlert, Check, X } from 'lucide-react'
import type { ProposalState, RoomState } from '../state/types'
import type { CurrentUser } from '../state/permissions'
import { canDecideLayer, isFallbackLayer, isViewer } from '../state/permissions'
import { soloConfirmText } from '../lib/solo'
import { approveAllFor, approveAllLabel, soloAllConfirmText } from '../lib/approveAll'
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
import { writeModeOff } from '../lib/writeMode'
import { PROPOSAL_ANCHOR } from './room/ProposalsSummary'
import { PersonAvatar } from './room/PersonAvatar'
import { PolicyCountdown } from './room/PolicyCountdown'
import { canCancelSchedule } from '../lib/countdown'
import type { CancelResult } from '../hooks/useRoom'
import { WaitingFor } from './room/WaitingFor'
import type { Person, PresenceMember, Topology } from '../api/types'
import type { DriverInfo, NudgeState } from '../state/types'

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

function riskVariant(risk: string): 'success' | 'warning' | 'destructive' | 'neutral' {
  switch (risk.toLowerCase()) {
    case 'low':
      return 'success'
    case 'medium':
      return 'warning'
    case 'high':
      return 'destructive'
    default:
      return 'neutral'
  }
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
  const match = typed === want
  const submit = () => {
    if (match && !busy) onConfirm(typed)
  }
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
          <TypedConfirm
            want={want}
            what="to approve"
            value={typed}
            onChange={setTyped}
            onSubmit={submit}
            rejected={rejected}
            inputRef={inputRef}
          />
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

// The typed text of solo mode (docs/design/auth.md 5.3), for one layer or for approve-all. The
// label holds the exact string, so a screen reader announces what to type; there is no copy button,
// paste and drop are off, and Ctrl/⌘⏎ submits.
function TypedConfirm({
  want,
  what,
  value,
  onChange,
  onSubmit,
  rejected,
  inputRef,
}: {
  want: string
  /** The end of the label: "to approve", "to approve every layer". */
  what: string
  value: string
  onChange: (typed: string) => void
  onSubmit: () => void
  /** The server refused the typed text once already. */
  rejected: boolean
  inputRef: RefObject<HTMLInputElement | null>
}) {
  const inputId = useId()
  const hintId = `${inputId}-hint`
  const left = want.length - value.length
  const progress =
    value === want
      ? 'It matches.'
      : want.startsWith(value)
        ? `${left} character${left === 1 ? '' : 's'} to go`
        : 'This does not match yet.'
  return (
    <div className="flex flex-col gap-1.5">
      <label htmlFor={inputId} className="text-sm font-medium">
        Solo mode: type <code className="font-mono">{want}</code> {what}
      </label>
      <Input
        ref={inputRef}
        id={inputId}
        value={value}
        onChange={(e) => onChange(e.target.value)}
        onPaste={(e) => e.preventDefault()}
        onDrop={(e) => e.preventDefault()}
        onKeyDown={(e) => {
          if (e.key === 'Enter' && (e.metaKey || e.ctrlKey)) {
            e.preventDefault()
            onSubmit()
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
  )
}

// The approval that completes the set runs the fix right away. One more look, with the steps
// that will run, before a production change (Enter or Ctrl/⌘⏎ confirms, Esc cancels). With write
// mode off the step stays, because the approval is still a decision on record, but the words say
// that nothing runs: the dialog must never promise what the server will not do.
function RunConfirmDialog({
  proposal,
  layer,
  busy,
  writeOff,
  onConfirm,
  onCancel,
  onClosed,
}: {
  proposal: ProposalState
  layer: string
  busy: boolean
  writeOff: boolean
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
          <DialogTitle>{writeOff ? 'Approve this fix?' : 'Run this fix now?'}</DialogTitle>
          <DialogDescription>
            {writeOff ? (
              'Your approval completes the set. Write mode is off, so nothing runs: the fix waits for write mode.'
            ) : (
              <>
                Your approval completes the set. Piperoom runs {n} {n === 1 ? 'step' : 'steps'} on{' '}
                {layers.join(' and ') || layer} right away.
              </>
            )}
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
            {writeOff ? 'Approve (will not run)' : 'Approve and run'}
            <Kbd aria-hidden="true">⏎</Kbd>
          </Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  )
}

// A and R, and the decision bar's buttons, for the one place of the room that owns them (a layer
// row, or the card's "Approve and run"). The listener is added once and calls the handler of the
// latest render; `handle` answers whether it acted, so a key it ignores is left alone.
function useDecisionKeys(shortcuts: boolean, headingId: string, handle: (key: 'a' | 'r') => boolean) {
  const latest = useRef(handle)
  useEffect(() => {
    latest.current = handle
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

  // The decision bar acts through the same handler, so A, R, the bar and the buttons share one path.
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
}

// The reason a rejection needs (Esc or Cancel drops it).
function RejectForm({
  label,
  onReject,
  onCancel,
}: {
  label: string
  onReject: (reason: string) => void
  onCancel: () => void
}) {
  const [reason, setReason] = useState('')
  return (
    <form
      className="ml-auto flex min-w-0 flex-1 basis-72 items-center justify-end gap-2"
      onSubmit={(e) => {
        e.preventDefault()
        if (!reason.trim()) return
        onReject(reason.trim())
      }}
    >
      <Input
        value={reason}
        onChange={(e) => setReason(e.target.value)}
        onKeyDown={(e) => {
          if (e.key === 'Escape') {
            e.stopPropagation()
            onCancel()
          }
        }}
        placeholder="Reason"
        aria-label={label}
        className="min-w-0 flex-1"
        autoComplete="off"
        autoFocus
      />
      <Button type="submit" variant="destructive" size="sm" className="coarse:h-11 coarse:px-4" disabled={!reason.trim()}>
        Confirm
      </Button>
      <Button variant="ghost" size="sm" className="coarse:h-11 coarse:px-4" onClick={onCancel}>
        Cancel
      </Button>
    </form>
  )
}

// The last look before one call records every layer (P2c). It names each layer with what the fix
// does there and the fingerprint the approvals bind to, and asks the solo user for
// "<pipeline_id>/*" typed by hand once (docs/design/auth.md 5.3; paste off, as for one layer).
// With write mode off it says that nothing runs, in the run confirm's words (P1e).
function ApproveAllDialog({
  proposal,
  layers,
  want,
  busy,
  refusedText,
  error,
  writeOff,
  onConfirm,
  onCancel,
  onClosed,
}: {
  proposal: ProposalState
  /** The layers this call records, in required_layers order. */
  layers: string[]
  /** The text the solo user must type, or null when no typed confirm is needed. */
  want: string | null
  busy: boolean
  /** The server refused the typed text once already. */
  refusedText: boolean
  /** Why the last call failed (it is also in the room banner, which this dialog covers). */
  error: string | null
  writeOff: boolean
  onConfirm: (typed?: string) => void
  onCancel: () => void
  onClosed: () => void
}) {
  const [typed, setTyped] = useState('')
  const inputRef = useRef<HTMLInputElement>(null)
  const match = want === null || typed === want
  const submit = () => {
    if (match && !busy) onConfirm(want === null ? undefined : typed)
  }
  const n = proposal.steps.length
  const these = layers.length === 1 ? 'the layer' : `all ${layers.length} layers`
  const before = proposal.required_layers.filter((l) => !layers.includes(l))
  return (
    <Dialog open onOpenChange={(open) => !open && onCancel()}>
      <DialogContent
        hideClose
        role="alertdialog"
        className="max-w-[520px]"
        onOpenAutoFocus={(e) => {
          if (want === null) return
          e.preventDefault()
          inputRef.current?.focus()
        }}
        onCloseAutoFocus={(e) => {
          e.preventDefault()
          onClosed()
        }}
        onKeyDown={(e) => {
          // A held Enter that opened this dialog must not also confirm it on the focused button.
          if (e.key === 'Enter' && e.repeat) e.preventDefault()
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
            <DialogTitle>{writeOff ? 'Approve this fix?' : 'Approve and run this fix?'}</DialogTitle>
            <DialogDescription>
              {writeOff
                ? `This records your approval for ${these} below. Write mode is off, so nothing runs: the fix waits for write mode.`
                : `This records your approval for ${these} below, then Piperoom runs ${n} ${n === 1 ? 'step' : 'steps'} right away.`}
            </DialogDescription>
          </DialogHeader>
          <div className="flex flex-col gap-1.5 rounded-lg border border-border bg-muted px-3 py-2.5">
            <span className="text-xs text-subtle-foreground">You are approving</span>
            <ul className="m-0 flex list-none flex-col gap-1 p-0" aria-label="Layers you are approving">
              {layers.map((layer) => (
                <li key={layer} className="flex flex-col gap-0.5" data-layer={layer}>
                  {layerSummary(proposal, layer).map((line, i) => (
                    <span key={i} className="font-medium">
                      {line}
                    </span>
                  ))}
                </li>
              ))}
            </ul>
            {before.length > 0 && (
              <span className="text-xs text-subtle-foreground">Already approved: {before.join(', ')}</span>
            )}
            {proposal.params_hash && (
              <span className="font-mono text-xs text-subtle-foreground">fingerprint {fingerprint(proposal.params_hash)}</span>
            )}
            <span className="text-xs text-subtle-foreground">
              if the steps change, this fingerprint changes and your approval is void
            </span>
          </div>
          {want !== null && (
            <TypedConfirm
              want={want}
              what="to approve every layer"
              value={typed}
              onChange={setTyped}
              onSubmit={submit}
              rejected={refusedText}
              inputRef={inputRef}
            />
          )}
          {error && (
            <p className="text-sm font-medium text-destructive-fg" role="alert">
              {error}
            </p>
          )}
          <DialogFooter className="items-center">
            <Button variant="ghost" onClick={onCancel}>
              Cancel
              <Kbd aria-hidden="true">esc</Kbd>
            </Button>
            <Button type="submit" variant="success" disabled={busy || !match} autoFocus={want === null}>
              {approveAllLabel(writeOff)}
              <Kbd aria-hidden="true">{want === null ? '⏎' : '⌘⏎'}</Kbd>
            </Button>
          </DialogFooter>
        </form>
      </DialogContent>
    </Dialog>
  )
}

// "Approve and run" (P2c): the person may approve every layer the proposal needs, so one button
// records them all with one call (POST /api/proposals/{pid}/approve-all) behind one confirm, in
// place of the per-layer buttons. Reject stays one click away; it is recorded on the first pending
// layer, which is enough to reject the whole proposal.
function ApproveAll({
  proposal,
  layers,
  user,
  actions,
  pipelineId,
  writeOff,
  shortcuts,
  noHashNoteId,
  headingId,
  onSolo,
}: {
  proposal: ProposalState
  /** The pending layers the call records (approveAllFor). */
  layers: string[]
  user: CurrentUser
  actions: RoomActions
  pipelineId: string
  writeOff: boolean
  /** The card's A and R keys and the decision bar act here. */
  shortcuts: boolean
  noHashNoteId: string
  headingId: string
  /** The server asked for a solo confirm: the panel shows the solo banner. */
  onSolo: () => void
}) {
  const [confirming, setConfirming] = useState(false)
  const [rejecting, setRejecting] = useState(false)
  const [busy, setBusy] = useState(false)
  // The server asked for the typed "<pipeline_id>/*" (an older /api/me did not say solo).
  const [asked, setAsked] = useState(false)
  const [refusedText, setRefusedText] = useState(false)
  const [error, setError] = useState<string | null>(null)
  const approveRef = useRef<HTMLButtonElement>(null)
  const hashed = proposal.params_hash !== ''
  const want = user.solo || asked ? soloAllConfirmText(pipelineId) : null

  const restoreFocus = () => {
    const button = approveRef.current
    if (button && !button.disabled) button.focus()
    else document.getElementById(headingId)?.focus()
  }
  const openConfirm = () => {
    setError(null)
    setRefusedText(false)
    setConfirming(true)
  }
  const closeConfirm = () => {
    setConfirming(false)
    setError(null)
    setRefusedText(false)
  }

  // The hash on screen is sent as it is, never fetched at click time: a fix that changed under the
  // screen answers 409 params_mismatch and the stale banner takes over (no retry).
  const approveAll = async (confirm?: string) => {
    if (!actions.approveAll) return
    setBusy(true)
    setError(null)
    try {
      const res = await actions.approveAll(proposal.proposal_id, {
        params_hash: proposal.params_hash,
        ...(confirm !== undefined ? { confirm } : {}),
      })
      if (res.needsConfirm) {
        onSolo()
        setRefusedText(confirm !== undefined)
        setAsked(true)
      } else if (res.ok || res.mismatch || res.refused) {
        // Done, stale (the room reloads) or refused (the card goes back to the per-layer rows and
        // says why): the dialog has nothing more to ask.
        closeConfirm()
      } else {
        setError(res.error ?? 'Piperoom did not record the approvals. Try again.')
      }
    } finally {
      setBusy(false)
    }
  }

  useDecisionKeys(shortcuts, headingId, (key) => {
    if (rejecting || confirming || busy) return false
    if (key === 'a') {
      if (!hashed) return false
      openConfirm()
    } else {
      setRejecting(true)
    }
    return true
  })

  return (
    <div
      className="flex min-h-11 flex-wrap items-center gap-x-2.5 gap-y-1.5 bg-warning-subtle px-3.5 py-1.5"
      data-testid="approve-all"
    >
      <span className="text-xs text-muted-foreground">Records your approval for {layers.join(', ')} at once</span>
      {!rejecting && (
        <span className="ml-auto inline-flex items-center gap-2">
          <Button
            variant="ghost-danger"
            size="sm"
            className="coarse:h-11 coarse:px-4"
            onClick={() => setRejecting(true)}
            aria-keyshortcuts={shortcuts ? 'R' : undefined}
          >
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
            onClick={openConfirm}
          >
            {approveAllLabel(writeOff)}
            {shortcuts && <Kbd aria-hidden="true">A</Kbd>}
          </Button>
        </span>
      )}
      {rejecting && (
        <RejectForm
          label="Rejection reason"
          onReject={(reason) => {
            // The hash on screen goes with the reason, as on a layer row.
            actions.reject(proposal.proposal_id, layers[0], reason, proposal.params_hash)
            setRejecting(false)
          }}
          onCancel={() => setRejecting(false)}
        />
      )}
      {confirming && (
        <ApproveAllDialog
          proposal={proposal}
          layers={layers}
          want={want}
          busy={busy}
          refusedText={refusedText}
          error={error}
          writeOff={writeOff}
          onConfirm={(typed) => void approveAll(typed)}
          onCancel={closeConfirm}
          onClosed={restoreFocus}
        />
      )}
    </div>
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
  writeOff,
  driver,
  bundled,
}: {
  proposal: ProposalState
  layer: string
  user: CurrentUser
  actions: RoomActions
  pipelineId: string
  /** The room's driver while the room is open (null otherwise): they approve a layer nobody owns (P2b). */
  driver: DriverInfo | null
  /** Write mode is off (P1a): an approval is recorded and nothing runs, and the button says so. */
  writeOff: boolean
  /**
   * The card's "Approve and run" decides this proposal (P2c): the row only says whose layer it is,
   * with no buttons, dialogs or keys of its own.
   */
  bundled: boolean
  /** This row's buttons answer to A and R (the first layer you can decide). */
  shortcuts: boolean
  noHashNoteId: string
  /** The card heading: focus goes there when the Approve button is gone after the dialog closes. */
  headingId: string
  /** The server asked for a solo confirm: the panel shows the solo banner. */
  onSolo: () => void
  waiting?: WaitingContext
}) {
  const [rejecting, setRejecting] = useState(false)
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
  const canDecide = !bundled && canDecideLayer(proposal, user, layer, driver?.id)
  // Pending and part of the card's one approval: shown as the person's own layer, decided below.
  const inBundle = bundled && state === 'pending'
  // Nobody owns this layer (P2b): the driver approves it, and the row says so instead of naming owners.
  const fallback = isFallbackLayer(proposal, layer)
  const driverName = driver ? neutralizeBadgeText(driver.name || driver.id) : ''

  // Solo: the typed box. The approval that completes the set: the run confirm. Otherwise at once.
  const completing = completesSet(proposal, layer)
  const startApprove = () => (user.solo ? setConfirming(true) : completing ? setRunConfirm(true) : void approve())

  // A and R, and the decision bar: they share startApprove with the buttons.
  useDecisionKeys(shortcuts, headingId, (key) => {
    if (!canDecide || rejecting || confirming || runConfirm || busy) return false
    if (key === 'a') {
      if (!hashed) return false
      startApprove()
    } else {
      setRejecting(true)
    }
    return true
  })

  const reasonText = decision?.outcome === 'rejected' ? decision.reason : undefined
  const decidedAt = decision?.at ? clockTime(decision.at) : ''
  const decidedName = decision ? neutralizeBadgeText(decision.by.name || decision.by.id) : ''

  return (
    <div
      className={cn(
        'flex min-h-11 flex-wrap items-center gap-x-2.5 gap-y-1.5 px-3.5 py-1.5',
        (canDecide || inBundle) && 'bg-warning-subtle',
      )}
      data-layer={layer}
      data-state={state}
    >
      <Badge variant={layerVariant(layer)} mono>
        {layer}
      </Badge>
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
      ) : canDecide || inBundle ? (
        <>
          <PersonAvatar actor={{ id: user.id, name: user.name }} size={20} />
          <PersonName person={{ id: user.id, name: user.name, kind: 'human', verified: user.verified }} nameClassName="font-medium" />
          <span className="text-xs text-muted-foreground">you</span>
          {/* muted, not subtle: subtle text is 4.2:1 on the amber tint of this row */}
          <span className="font-mono text-xs text-muted-foreground">user:{user.id}</span>
          {fallback ? (
            <span className="text-xs font-medium text-warning-foreground" data-testid={`fallback-${layer}`}>
              no owner for <span className="font-mono">{layer}</span> · driver approves
            </span>
          ) : (
            <span className="text-xs font-medium text-warning-foreground">pending · your layer</span>
          )}
        </>
      ) : fallback && state === 'pending' ? (
        // Greyed for everyone but the driver: no owner to wait on or nudge.
        <span className="inline-flex min-w-0 flex-wrap items-center gap-x-2 gap-y-1 text-xs text-muted-foreground">
          <span data-testid={`fallback-${layer}`}>
            no owner for <span className="font-mono">{layer}</span> · driver approves
          </span>
          {driver ? (
            <span className="inline-flex min-w-0 items-center gap-1.5">
              <PersonAvatar actor={{ id: driver.id, name: driverName }} size={20} />
              <PersonName person={{ ...driver, kind: 'human' }} name={driverName} />
            </span>
          ) : (
            <span>no driver yet</span>
          )}
        </span>
      ) : waiting && state === 'pending' ? (
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
          <span className="size-1.5 rounded-full bg-warning" aria-hidden="true" />
          Pending
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
            Approve{writeOff ? ' (will not run)' : ''}
            {shortcuts && <Kbd aria-hidden="true">A</Kbd>}
          </Button>
        </span>
      )}
      {canDecide && rejecting && (
        <RejectForm
          label={`Rejection reason for ${layer}`}
          onReject={(reason) => {
            // The hash on screen goes with the reason, "" included for an unhashable open proposal.
            actions.reject(proposal.proposal_id, layer, reason, proposal.params_hash)
            setRejecting(false)
          }}
          onCancel={() => setRejecting(false)}
        />
      )}
      {canDecide && runConfirm && (
        <RunConfirmDialog
          proposal={proposal}
          layer={layer}
          busy={busy}
          writeOff={writeOff}
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
  writeOff,
  driver,
  bundle,
  refused,
  onCancelSchedule,
}: {
  /** Set when this person may cancel the proposal's policy countdown (canCancelSchedule). */
  onCancelSchedule?: () => Promise<CancelResult>
  /** The room's driver while the room is open, else null (see LayerRow). */
  driver: DriverInfo | null
  nudge: CloseNudge
  topology: Topology | null
  proposal: ProposalState
  user: CurrentUser
  actions: RoomActions
  pipelineId: string
  /** The layers one "Approve and run" records (approveAllFor), or null for the per-layer rows. */
  bundle: string[] | null
  /** The server's 403 text when it refused this person's approve-all (state.approve_all_refused). */
  refused: string | undefined
  /** The layer whose buttons answer to A and R in this card ("*": the card's "Approve and run"), if any. */
  shortcutLayer: string | undefined
  /** The hash the person looked at when the server answered 409 for this proposal (kept by useRoom). */
  sentHash: string | null
  /** False while the decision bar already says "Needs your approval": one amber per fact. */
  ribbon: boolean
  onSolo: () => void
  waiting: WaitingContext
  writeOff: boolean
}) {
  const titleId = useId()
  const noteId = useId()
  // "Got it" hides the note for one stale hash. The note itself comes from `sentHash`, which
  // useRoom keeps across the room reload (this card is unmounted while the room loads).
  const [acknowledged, setAcknowledged] = useState<string | null>(null)

  const mine = bundle ?? proposal.required_layers.filter((l) => canDecideLayer(proposal, user, l, driver?.id))
  const open = proposal.status === 'open'
  const hashed = proposal.params_hash !== ''
  const pending = proposal.required_layers.filter((l) => (proposal.approvals[l] ?? 'pending') === 'pending')
  const approved = proposal.required_layers.filter((l) => proposal.approvals[l] === 'approved').length
  const previous = latestDecision(proposal)
  // A layer nobody owns waits on the driver, not on an owner (P2b).
  const ownersPending = pending.filter((l) => !isFallbackLayer(proposal, l))
  const waitingOn = [
    ...(ownersPending.length > 0 ? [`${ownersPending.join(', ')} ${ownersPending.length === 1 ? 'owner' : 'owners'}`] : []),
    ...(ownersPending.length < pending.length ? ['the driver'] : []),
  ].join(' and ')

  return (
    <article className="flex flex-col rounded-lg border border-border bg-card text-sm text-card-foreground" aria-labelledby={titleId}>
      {/* A closed room runs nothing more, so no countdown there (its proposals keep status "open"). */}
      {open && nudge.roomOpen && proposal.scheduled && <PolicyCountdown scheduled={proposal.scheduled} onCancel={onCancelSchedule} />}
      {ribbon && mine.length > 0 && (
        <div className="flex min-h-9 flex-wrap items-center gap-x-2 gap-y-0.5 rounded-t-lg border-b border-border bg-warning-subtle px-3.5 py-1.5 text-sm font-medium text-warning-foreground">
          <TriangleAlert className="size-3.5 shrink-0" aria-hidden="true" />
          <span>Needs your approval · {mine.join(' · ')}</span>
          {previous && <span className="ml-auto text-xs font-normal text-muted-foreground">{previous}</span>}
        </div>
      )}
      <div className="flex flex-col gap-3 px-3.5 pb-3 pt-3.5">
        <div className="flex flex-wrap items-start gap-2.5">
          <div className="flex min-w-0 flex-1 basis-80 flex-col gap-1">
            <h3 id={titleId} tabIndex={-1} className="text-lg font-semibold outline-none [text-wrap:balance]">
              {proposal.title}
            </h3>
            {(proposal.risk || proposal.rollback) && (
              <p className="flex flex-wrap items-center gap-x-1.5 gap-y-1 text-xs text-muted-foreground">
                {proposal.risk && <Badge variant={riskVariant(proposal.risk)}>risk {proposal.risk}</Badge>}
                {proposal.rollback && (
                  <span className="inline-flex items-center gap-1">
                    <RotateCcw className="size-3 shrink-0" aria-hidden="true" />
                    rollback: {proposal.rollback}
                  </span>
                )}
              </p>
            )}
          </div>
          <span className="flex shrink-0 items-center gap-2 text-xs text-subtle-foreground">
            {proposal.status === 'approved' && <Badge variant="success">approved</Badge>}
            {proposal.status === 'rejected' && <Badge variant="destructive">rejected</Badge>}
            {open && mine.length === 0 && pending.length > 0
              ? `waiting for ${waitingOn} · ${approved} of ${proposal.required_layers.length} approved`
              : 'proposed by the agent'}
          </span>
        </div>

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
      {refused !== undefined && open && (
        <p className="px-3.5 py-2 text-xs font-medium text-destructive-fg" role="alert" data-testid="approve-all-refused">
          Approve and run was refused: {refused}. Approve the layers you own one at a time below.
        </p>
      )}
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
            writeOff={writeOff}
            driver={driver}
            bundled={bundle !== null}
          />
        ))}
        {bundle && (
          <ApproveAll
            proposal={proposal}
            layers={bundle}
            user={user}
            actions={actions}
            pipelineId={pipelineId}
            writeOff={writeOff}
            shortcuts={shortcutLayer === '*'}
            noHashNoteId={noteId}
            headingId={titleId}
            onSolo={onSolo}
          />
        )}
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
  // Same for the driver's right to a layer nobody owns: none on a closed room.
  const driver = state.status === 'open' ? state.driver : null
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

  // "Approve and run" (P2c) where the person may approve every layer of a proposal. The same rule
  // as the decision bar's (lib/decision.ts); a host without approveAll keeps the per-layer rows.
  const bundleFor = (p: ProposalState) => (actions.approveAll ? approveAllFor(state, p, decider) : null)
  // Cancel of a policy countdown (P2c-3): the driver or an owner of any of its layers. `decider` and
  // `driver` already carry no rights on a closed room.
  const ownsEveryLayer = state.status === 'open' && (state.mode?.solo === true || user.solo === true)
  const cancelFor = (p: ProposalState) => {
    const cancel = actions.cancelSchedule
    return cancel && canCancelSchedule(p, decider, driver, ownsEveryLayer) ? () => cancel(p.proposal_id) : undefined
  }

  // A and R belong to one place at a time, in the first proposal that has one: its "Approve and
  // run" ("*"), else the first layer you can decide.
  let shortcutKey = ''
  for (const p of proposals) {
    const layer = bundleFor(p) ? '*' : p.required_layers.find((l) => canDecideLayer(p, decider, l, driver?.id))
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
              writeOff={writeModeOff(state.mode)}
              driver={driver}
              bundle={bundleFor(p)}
              refused={state.approve_all_refused?.[p.proposal_id]}
              onCancelSchedule={cancelFor(p)}
            />
          ))}
        </div>
      )}
    </section>
  )
}

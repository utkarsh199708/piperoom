// The approval moment (docs/design/ui-v2: Room-Approval, Solo-Confirm, Stale-409, Room-Viewer).
//
// Rules that do not move with the look:
//  - Approve and Reject send the params_hash that is ON SCREEN, never one fetched at click time.
//  - The server checks the hash (409 params_mismatch) before it asks for the solo confirm.
//    A 409 is never retried: the stale banner and toast tell the person, they look again.
//  - `canDecideLayer` is the only gate for the buttons (plus "no hash" and "busy", which only
//    switch them off). A viewer or a non-owner gets no buttons at all.
import { useEffect, useId, useRef, useState } from 'react'
import { CircleCheck, CircleX, Copy, Hash, Lock, RotateCcw, TriangleAlert, Check, X } from 'lucide-react'
import type { ProposalState, RoomState } from '../state/types'
import type { CurrentUser } from '../state/permissions'
import { canDecideLayer } from '../state/permissions'
import { soloConfirmText } from '../lib/solo'
import { describeIdentity, identityDetails, shortHash } from '../lib/hash'
import { initials, personColor } from '../lib/people'
import { clockTime, timeAgo } from '../lib/time'
import { cn } from '@/lib/utils'
import type { RoomActions } from '../hooks/useRoom'
import { Avatar, AvatarFallback } from '@/components/ui/avatar'
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

function isTypingTarget(target: EventTarget | null): boolean {
  return (
    target instanceof HTMLElement &&
    target.closest('input, textarea, select, [contenteditable=""], [contenteditable="true"], [role="textbox"], [role="combobox"]') !== null
  )
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
}: {
  proposal: ProposalState
  layer: string
  want: string
  busy: boolean
  /** The server refused the typed text once already. */
  rejected: boolean
  onConfirm: (typed: string) => void
  onCancel: () => void
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

function PersonAvatar({ id, name }: { id: string; name: string }) {
  return (
    <Avatar className="size-5" aria-hidden="true">
      <AvatarFallback className="text-[9px]" style={{ backgroundColor: personColor(id) }}>
        {initials(name)}
      </AvatarFallback>
    </Avatar>
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
  onSolo,
  onMismatch,
}: {
  proposal: ProposalState
  layer: string
  user: CurrentUser
  actions: RoomActions
  pipelineId: string
  /** This row's buttons answer to A and R (the first layer you can decide). */
  shortcuts: boolean
  noHashNoteId: string
  /** The server asked for a solo confirm: the panel shows the solo banner. */
  onSolo: () => void
  /** 409 params_mismatch on approve: the hash that was on screen. */
  onMismatch: (looked: string) => void
}) {
  const [rejecting, setRejecting] = useState(false)
  const [reason, setReason] = useState('')
  const [confirming, setConfirming] = useState(false)
  const [busy, setBusy] = useState(false)
  const [refused, setRefused] = useState(false)

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
        // Stale screen: close the box. No retry; the room is reloading.
        setConfirming(false)
        setRefused(false)
        onMismatch(proposal.params_hash)
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
  const canDecide = canDecideLayer(proposal, user, layer)

  const startApprove = () => (user.solo ? setConfirming(true) : void approve())

  // A and R. The listener is added once; it calls the handler of the latest render.
  const latest = useRef<(key: 'a' | 'r') => boolean>(() => false)
  useEffect(() => {
    latest.current = (key) => {
      if (!canDecide || rejecting || confirming || busy) return false
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
      if (e.defaultPrevented || e.repeat || e.metaKey || e.ctrlKey || e.altKey || e.shiftKey) return
      const key = e.key.toLowerCase()
      if (key !== 'a' && key !== 'r') return
      // Never inside a field, and not while a dialog (solo confirm, command menu, hand-off) is open.
      if (isTypingTarget(e.target) || document.querySelector('[role="dialog"]')) return
      if (latest.current(key)) e.preventDefault()
    }
    window.addEventListener('keydown', onKey)
    return () => window.removeEventListener('keydown', onKey)
  }, [shortcuts])

  const reasonText = decision?.outcome === 'rejected' ? decision.reason : undefined
  const decidedAt = decision?.at ? clockTime(decision.at) : ''
  const decidedName = decision ? decision.by.name || decision.by.id : ''

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
      {decision ? (
        <>
          {/* The sentence is for screen readers (and tests); the pieces next to it are the same facts, drawn. */}
          <span className="sr-only" data-testid={`decision-${layer}`}>
            {decision.outcome} by {decidedName}
            {decision.identity && ` (${describeIdentity(decision.identity)})`}
            {reasonText ? `: ${reasonText}` : ''}
            {decidedAt && ` at ${decidedAt}`}
          </span>
          <span className="inline-flex min-w-0 items-center gap-2.5" aria-hidden="true">
            <PersonAvatar id={decision.by.id} name={decidedName} />
            <span className="font-medium">{decidedName}</span>
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
            {decidedAt && ` · ${decidedAt}`}
          </Badge>
          {reasonText && (
            <span className="basis-full text-xs text-muted-foreground" aria-hidden="true">
              Reason: {reasonText}
            </span>
          )}
        </>
      ) : canDecide ? (
        <>
          <PersonAvatar id={user.id} name={user.name} />
          <span className="font-medium">{user.name}</span>
          <span className="text-xs text-muted-foreground">you</span>
          {/* muted, not subtle: subtle text is 4.2:1 on the amber tint of this row */}
          <span className="font-mono text-xs text-muted-foreground">user:{user.id}</span>
          <span className="text-xs font-medium text-warning-foreground">pending · your layer</span>
        </>
      ) : (
        <Badge className="ml-auto" variant="neutral">
          <span className="size-1.5 rounded-full bg-warning" aria-hidden="true" />
          Pending
        </Badge>
      )}

      {canDecide && !rejecting && (
        <span className="ml-auto inline-flex items-center gap-2">
          <Button variant="ghost-danger" size="sm" onClick={() => setRejecting(true)} aria-keyshortcuts={shortcuts ? 'R' : undefined}>
            Reject
            {shortcuts && <Kbd aria-hidden="true">R</Kbd>}
          </Button>
          <Button
            variant="success"
            size="sm"
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
          <Button type="submit" variant="destructive" size="sm" disabled={!reason.trim()}>
            Confirm
          </Button>
          <Button
            variant="ghost"
            size="sm"
            onClick={() => {
              setRejecting(false)
              setReason('')
            }}
          >
            Cancel
          </Button>
        </form>
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
        />
      )}
    </div>
  )
}

function ExecutedList({ proposal }: { proposal: ProposalState }) {
  if (proposal.executed.length === 0) return null
  return (
    <>
      <Separator />
      <ul className="flex flex-col gap-1.5 px-3.5 py-2.5 text-sm">
        {proposal.executed.map((ex) => {
          // The executor checked this hash before it ran the step. A different
          // one than the card shows is flagged; an event without one is not.
          const differs = !!ex.params_hash && !!proposal.params_hash && ex.params_hash !== proposal.params_hash
          return (
            <li key={ex.step_index} className="flex flex-wrap items-center gap-2">
              {ex.ok ? (
                <CircleCheck className="size-4 text-success-foreground" role="img" aria-label="succeeded" />
              ) : (
                <CircleX className="size-4 text-destructive-fg" role="img" aria-label="failed" />
              )}
              <span className="font-mono text-xs text-subtle-foreground">step {ex.step_index + 1}</span>
              <span className="min-w-0 break-words">{ex.output}</span>
              {ex.params_hash && (
                <code
                  className="mono exec__hash rounded-md border border-input bg-muted px-1.5 py-0.5 font-mono text-xs"
                  data-testid={`exec-hash-${ex.step_index}`}
                  title={ex.params_hash}
                >
                  {shortHash(ex.params_hash)}
                </code>
              )}
              {differs && (
                <Badge variant="destructive" title="The step ran with a different params_hash than this proposal shows">
                  <TriangleAlert className="size-3" aria-hidden="true" />
                  hash differs from proposal
                </Badge>
              )}
            </li>
          )
        })}
      </ul>
    </>
  )
}

function latestDecision(proposal: ProposalState): string {
  let best: { at: number; text: string } | null = null
  for (const [layer, d] of Object.entries(proposal.decisions ?? {})) {
    const at = d.at ? Date.parse(d.at) : NaN
    if (Number.isNaN(at) || (best && at <= best.at)) continue
    best = { at, text: `${d.by.name || d.by.id} ${d.outcome} ${layer} ${timeAgo(d.at as string)}` }
  }
  return best?.text ?? ''
}

function Proposal({
  proposal,
  user,
  actions,
  pipelineId,
  shortcutLayer,
  onSolo,
}: {
  proposal: ProposalState
  user: CurrentUser
  actions: RoomActions
  pipelineId: string
  /** The layer whose buttons answer to A and R in this card, if any. */
  shortcutLayer: string | undefined
  onSolo: () => void
}) {
  const titleId = useId()
  const noteId = useId()
  // The hash the person was looking at when the server said 409. Kept so the card can say
  // "you looked at X, the server now has Y" once the room has reloaded.
  const [looked, setLooked] = useState<string | null>(null)

  const mine = proposal.required_layers.filter((l) => canDecideLayer(proposal, user, l))
  const open = proposal.status === 'open'
  const hashed = proposal.params_hash !== ''
  const pending = proposal.required_layers.filter((l) => (proposal.approvals[l] ?? 'pending') === 'pending')
  const approved = proposal.required_layers.filter((l) => proposal.approvals[l] === 'approved').length
  const previous = latestDecision(proposal)

  return (
    <article className="flex flex-col rounded-lg border border-border bg-card text-sm text-card-foreground" aria-labelledby={titleId}>
      {mine.length > 0 && (
        <div className="flex min-h-9 flex-wrap items-center gap-x-2 gap-y-0.5 rounded-t-lg border-b border-border bg-warning-subtle px-3.5 py-1.5 text-sm font-medium text-warning-foreground">
          <TriangleAlert className="size-3.5 shrink-0" aria-hidden="true" />
          <span>Needs your approval · {mine.join(' · ')}</span>
          {previous && <span className="ml-auto text-xs font-normal text-muted-foreground">{previous}</span>}
        </div>
      )}
      <div className="flex flex-col gap-3 px-3.5 pb-3 pt-3.5">
        <div className="flex flex-wrap items-start gap-2.5">
          <div className="flex min-w-0 flex-1 basis-80 flex-col gap-1">
            <h3 id={titleId} className="text-lg font-semibold [text-wrap:balance]">
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
              ? `waiting for ${pending.join(', ')} ${pending.length === 1 ? 'owner' : 'owners'} · ${approved} of ${proposal.required_layers.length} approved`
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
                  <StepParams params={s.params} />
                </div>
              </li>
            ))}
          </ol>
        )}

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

        {looked !== null && hashed && looked !== proposal.params_hash && (
          <div className="flex flex-wrap items-center gap-2 text-muted-foreground">
            <span>You looked at</span>
            <HashPill hash={looked} struck />
            <span>· server now has</span>
            <HashPill hash={proposal.params_hash} />
            <Button variant="ghost" size="sm" className="ml-auto h-6 px-1.5 text-xs" onClick={() => setLooked(null)}>
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
            onSolo={onSolo}
            onMismatch={setLooked}
          />
        ))}
      </div>
      <ExecutedList proposal={proposal} />
    </article>
  )
}

export function ProposalsPanel({
  state,
  user,
  actions,
}: {
  state: RoomState
  user: CurrentUser
  actions: RoomActions
}) {
  const proposals = state.proposals
  const headingId = useId()
  // `/api/me.solo` says so up front; on an older server the first approve that
  // asks for a confirm tells us.
  const [soloSeen, setSoloSeen] = useState(false)
  const solo = user.solo === true || soloSeen
  const openCount = proposals.filter((p) => p.status === 'open').length

  // A and R belong to one row at a time: the first layer you can decide, in the first such proposal.
  let shortcutKey = ''
  for (const p of proposals) {
    const layer = p.required_layers.find((l) => canDecideLayer(p, user, l))
    if (layer) {
      shortcutKey = `${p.proposal_id}/${layer}`
      break
    }
  }

  return (
    <section className="flex flex-col gap-2.5 text-sm" aria-labelledby={headingId}>
      <h2 id={headingId} className="flex items-center gap-1.5 text-sm font-medium">
        Proposals
        <span className="font-mono text-[11px] font-medium text-subtle-foreground">
          {openCount > 0 ? `${openCount} open` : proposals.length}
        </span>
      </h2>
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
              user={user}
              actions={actions}
              pipelineId={state.pipeline_id}
              shortcutLayer={shortcutKey.startsWith(`${p.proposal_id}/`) ? shortcutKey.slice(p.proposal_id.length + 1) : undefined}
              onSolo={() => setSoloSeen(true)}
            />
          ))}
        </div>
      )}
    </section>
  )
}

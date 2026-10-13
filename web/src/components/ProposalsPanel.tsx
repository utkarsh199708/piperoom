import { useId, useState } from 'react'
import type { ProposalState, RoomState } from '../state/types'
import type { CurrentUser } from '../state/permissions'
import { canDecideLayer } from '../state/permissions'
import { soloConfirmText } from '../lib/solo'
import { describeIdentity, identityDetails, shortHash } from '../lib/hash'
import type { RoomActions } from '../hooks/useRoom'
import { Chip, Panel } from './ui'
// TEMP until P1 merges: Icon, LayerTag and Empty come from './ui' then (see the stand-in file's header).
import { Empty, Icon, LayerTag } from './proposals-temp-ui'
import '../styles/proposals.css'

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

/**
 * "7f3a 91c0 … 4be2": the first 8 hex digits as two groups, an ellipsis and the last 4.
 * A hash too short to elide is just grouped by 4.
 */
function fingerprint(hash: string): string {
  if (hash.length <= 12) return hash.match(/.{1,4}/g)?.join(' ') ?? hash
  return `${hash.slice(0, 4)} ${hash.slice(4, 8)} … ${hash.slice(-4)}`
}

const RISK_TONE: Record<string, string> = { low: 'ok', medium: 'warn', high: 'fail' }

function StepParams({ params }: { params: unknown }) {
  if (params === null || params === undefined) {
    return <div className="proposal__params proposal__params--empty">no parameters</div>
  }
  if (!isRecord(params)) {
    return (
      <div className="proposal__params" data-testid="step-params">
        {JSON.stringify(params, null, 2)}
      </div>
    )
  }
  const entries = Object.entries(params)
  if (entries.length === 0) {
    return <div className="proposal__params proposal__params--empty">{'{}'}</div>
  }
  return (
    <div className="proposal__params" data-testid="step-params">
      {entries.map(([key, value]) => {
        const target = TARGET_KEYS.has(key.toLowerCase())
        return (
          <div key={key} className={`proposal__param${target ? ' proposal__param--target' : ''}`}>
            <span className={`proposal__param-key${target ? ' proposal__param-key--target' : ''}`}>
              {key}
            </span>
            <span className={`proposal__param-val${target ? ' proposal__param-val--target' : ''}`}>
              {formatParamValue(value)}
            </span>
          </div>
        )
      })}
    </div>
  )
}

// Solo mode (docs/design/auth.md 5.3): the person types "<pipeline_id>/<layer>" by
// hand. A speed bump, not a control: no copy button, and paste is blocked.
function SoloConfirm({
  want,
  busy,
  stale,
  rejected,
  onConfirm,
  onCancel,
}: {
  want: string
  busy: boolean
  /** The screen is out of date (409): nothing can be sent until it is reloaded. */
  stale: boolean
  /** The server refused the typed text once already. */
  rejected: boolean
  onConfirm: (typed: string) => void
  onCancel: () => void
}) {
  const [typed, setTyped] = useState('')
  const inputId = useId()
  const hintId = `${inputId}-hint`
  return (
    <form
      className="solo-confirm"
      onSubmit={(e) => {
        e.preventDefault()
        if (typed === want && !busy && !stale) onConfirm(typed)
      }}
    >
      {/* The label text holds the exact string, so a screen reader announces it. */}
      <label className="solo-confirm__label" htmlFor={inputId}>
        Solo mode: type <code className="mono">{want}</code> to approve
      </label>
      <div className="solo-confirm__row">
        <input
          id={inputId}
          className="mono"
          value={typed}
          onChange={(e) => setTyped(e.target.value)}
          onPaste={(e) => e.preventDefault()}
          onDrop={(e) => e.preventDefault()}
          aria-describedby={hintId}
          autoComplete="off"
          autoFocus
        />
        <button type="submit" className="btn btn--small btn--approve" disabled={busy || stale || typed !== want}>
          Approve
        </button>
        <button type="button" className="btn btn--small btn--ghost" onClick={onCancel}>
          Cancel
        </button>
      </div>
      <span id={hintId} className="muted solo-confirm__hint">
        Type it by hand. Paste is off.
      </span>
      {rejected && (
        <span className="error-text" role="alert">
          The server did not accept that text.
        </span>
      )}
    </form>
  )
}

function LayerDecision({
  proposal,
  layer,
  user,
  actions,
  pipelineId,
  stale,
  onSolo,
}: {
  proposal: ProposalState
  layer: string
  user: CurrentUser
  actions: RoomActions
  pipelineId: string
  /** The screen is out of date (409): Approve and Reject stay off until it is reloaded. */
  stale: boolean
  /** The server asked for a solo confirm: the panel shows the solo banner. */
  onSolo: () => void
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
  // answers 409 params_mismatch and the stale banner tells the person (auth PR 4).
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
  // No params_hash (the server could not fingerprint the fix): nothing can be
  // sent that the server would accept, so Approve and Reject stay off.
  const hashed = proposal.params_hash !== ''
  // The only gate for the buttons (and for the "your layer" styling).
  const canDecide = canDecideLayer(proposal, user, layer)
  const off = !hashed || stale

  return (
    <div className={`approval approval--${state}${canDecide ? ' approval--mine' : ''}`}>
      <span className="approval__layer">
        {layer}
        {canDecide && <span className="approval__you">your layer</span>}
      </span>
      <span className="approval__state">
        {state === 'approved' && <Icon name="check" size={14} />}
        {state === 'rejected' && <Icon name="x" size={14} />}
        <Chip tone={state === 'approved' ? 'ok' : state === 'rejected' ? 'fail' : 'warn'}>{state}</Chip>
        {decision && (
          <span
            className="approval__who"
            data-testid={`decision-${layer}`}
            title={identityDetails(decision.identity) || undefined}
          >
            {decision.outcome} by {decision.by.name || decision.by.id}
            {decision.identity && ` (${describeIdentity(decision.identity)})`}
            {decision.outcome === 'rejected' && decision.reason ? `: ${decision.reason}` : ''}
          </span>
        )}
        {state === 'pending' && !canDecide && proposal.status === 'open' && (
          <span className="approval__wait">waiting for {layer} owner</span>
        )}
      </span>
      {canDecide && !rejecting && !confirming && (
        <span className="approval__actions">
          <button
            type="button"
            className="btn btn--small btn--approve"
            disabled={busy || off}
            onClick={() => (user.solo ? setConfirming(true) : void approve())}
          >
            <Icon name="check" size={14} />
            Approve
          </button>
          <button type="button" className="btn btn--small btn--reject" disabled={off} onClick={() => setRejecting(true)}>
            Reject
          </button>
        </span>
      )}
      {canDecide && rejecting && (
        <span className="approval__actions">
          <input
            value={reason}
            onChange={(e) => setReason(e.target.value)}
            placeholder="Reason"
            aria-label={`Rejection reason for ${layer}`}
            autoFocus
          />
          <button
            type="button"
            className="btn btn--small btn--reject"
            disabled={!reason.trim() || stale}
            onClick={() => {
              actions.reject(proposal.proposal_id, layer, reason.trim(), proposal.params_hash)
              setRejecting(false)
              setReason('')
            }}
          >
            Confirm
          </button>
        </span>
      )}
      {canDecide && confirming && (
        <SoloConfirm
          want={soloConfirmText(pipelineId, layer)}
          busy={busy}
          stale={stale}
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

// The proposal's params_hash (auth PR 4): the fingerprint to glance at ("Approving
// exactly this 7f3a 91c0 … 4be2"), a Copy button for the full value, and the full
// value in a read-only monospace field (behind "Full hash") to compare. This is the
// value approve and reject send back.
function ParamsHash({ hash }: { hash: string }) {
  const [copied, setCopied] = useState(false)
  if (!hash) {
    return (
      <p className="proposal__hash proposal__hash--missing error-text" data-testid="params-hash-missing" role="note">
        This fix can't be approved or rejected here: its parameters could not be fingerprinted (see the server
        log).
      </p>
    )
  }
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
    <div className="proposal__hash">
      <div className="proposal__hash-row">
        <Icon name="fingerprint" size={14} />
        <span>Approving exactly this</span>
        <code className="mono proposal__fingerprint" data-testid="params-hash-fingerprint" title={hash}>
          {fingerprint(hash)}
        </code>
        <button
          type="button"
          className="btn btn--link proposal__hash-copy"
          aria-label={copied ? 'Copied' : 'Copy hash'}
          onClick={copy}
        >
          <Icon name={copied ? 'check' : 'copy'} size={14} />
          {copied ? 'Copied' : 'Copy'}
        </button>
      </div>
      <details className="proposal__hash-more">
        <summary>Full hash</summary>
        <div className="proposal__hash-full-row">
          <code className="mono proposal__hash-short" data-testid="params-hash-short">
            {shortHash(hash)}
          </code>
          <input
            className="mono proposal__hash-full"
            readOnly
            value={hash}
            onFocus={(e) => e.currentTarget.select()}
            aria-label="Full params hash"
          />
        </div>
      </details>
    </div>
  )
}

function Proposal({
  proposal,
  user,
  actions,
  pipelineId,
  stale,
  onReload,
  onSolo,
}: {
  proposal: ProposalState
  user: CurrentUser
  actions: RoomActions
  pipelineId: string
  stale: boolean
  onReload?: () => void
  onSolo: () => void
}) {
  // canDecideLayer is the only gate: no ribbon for a viewer, a settled card or a layer that is not mine.
  const mine = proposal.required_layers.filter((layer) => canDecideLayer(proposal, user, layer))
  const pendingCount = proposal.required_layers.filter(
    (l) => (proposal.approvals[l] ?? 'pending') === 'pending',
  ).length
  const total = proposal.required_layers.length
  const open = proposal.status === 'open'
  const allOk = proposal.executed.every((ex) => ex.ok)
  const allRan = proposal.steps.length > 0 && proposal.executed.length >= proposal.steps.length && allOk
  const running = proposal.status === 'approved' && proposal.executed.length < proposal.steps.length && allOk
  const cls = [
    'proposal',
    mine.length > 0 ? 'proposal--mine' : '',
    `proposal--${proposal.status}`,
    allRan ? 'proposal--ran' : '',
  ]
    .filter(Boolean)
    .join(' ')
  return (
    <article className={cls} id={`proposal-${proposal.proposal_id}`}>
      {mine.length > 0 && (
        <div className="proposal__ribbon">
          <Icon name="alert" size={14} />
          {`Needs your approval · ${mine[0]}`}
        </div>
      )}
      <div className="proposal__in">
        <div className="proposal__head">
          <h3 className="proposal__title">{proposal.title}</h3>
          {proposal.status === 'approved' && <Chip tone="ok">approved</Chip>}
          {proposal.status === 'rejected' && <Chip tone="fail">rejected</Chip>}
          {open && pendingCount > 0 && (
            <span className="proposal__status num">
              {pendingCount} of {total} approvals pending
            </span>
          )}
        </div>
        {proposal.risk && (
          <div className="proposal__meta">
            <Chip tone={RISK_TONE[proposal.risk] ?? 'muted'}>risk {proposal.risk}</Chip>
            {proposal.rollback && (
              <span className="proposal__rollback">
                <Icon name="back" size={14} />
                rollback: {proposal.rollback}
              </span>
            )}
          </div>
        )}
        <ol className="proposal__steps">
          {proposal.steps.map((s, i) => (
            <li key={i} className="proposal__step">
              <span className="proposal__step-n" aria-hidden="true">
                {i + 1}
              </span>
              <div className="proposal__step-head">
                <code className="mono">{s.action}</code>
                <LayerTag layer={s.layer} />
              </div>
              <p className="proposal__step-desc">{s.description}</p>
              <StepParams params={s.params} />
            </li>
          ))}
        </ol>
        <ParamsHash hash={proposal.params_hash} />
        {stale && open && (
          <div className="proposal__stale" role="alert">
            <Icon name="alert" size={14} />
            <span>This proposal changed since you loaded it. Reload to see the current steps.</span>
            {onReload && (
              <button type="button" className="btn btn--small btn--ghost" onClick={onReload}>
                Reload
              </button>
            )}
          </div>
        )}
        <div className="proposal__approvals">
          {proposal.required_layers.map((layer) => (
            <LayerDecision
              key={layer}
              proposal={proposal}
              layer={layer}
              user={user}
              actions={actions}
              pipelineId={pipelineId}
              stale={stale}
              onSolo={onSolo}
            />
          ))}
        </div>
        {proposal.executed.length > 0 && (
          <ul className="proposal__executed">
            {proposal.executed.map((ex) => {
              // The executor checked this hash before it ran the step. A different
              // one than the card shows is flagged; an event without one is not.
              const differs =
                !!ex.params_hash && !!proposal.params_hash && ex.params_hash !== proposal.params_hash
              return (
                <li key={ex.step_index} className={ex.ok ? 'exec-ok' : 'exec-fail'}>
                  <Icon name={ex.ok ? 'check' : 'x'} size={14} />
                  <span className="sr-only">{ex.ok ? 'ran ok:' : 'failed:'}</span>
                  <span className="mono">step {ex.step_index + 1}</span>
                  <span className="exec__out">{ex.output}</span>
                  {ex.params_hash && (
                    <code
                      className="mono exec__hash"
                      data-testid={`exec-hash-${ex.step_index}`}
                      title={ex.params_hash}
                    >
                      {shortHash(ex.params_hash)}
                    </code>
                  )}
                  {differs && (
                    <Chip tone="fail" title="The step ran with a different params_hash than this proposal shows">
                      hash differs from proposal
                    </Chip>
                  )}
                </li>
              )
            })}
          </ul>
        )}
        {running && (
          <div className="proposal__running">
            <span className="proposal__running-bar" aria-hidden="true" />
            <span className="small">
              Running step {proposal.executed.length + 1} of {proposal.steps.length}…
            </span>
          </div>
        )}
      </div>
    </article>
  )
}

export function ProposalsPanel({
  state,
  user,
  actions,
  stale = false,
  onReload,
}: {
  state: RoomState
  user: CurrentUser
  actions: RoomActions
  /**
   * The room is out of date after a 409 params_mismatch (`useRoom().stale`): open
   * cards show the stale line and keep Approve and Reject off. Optional, so the
   * panel works without it; RoomScreen passes it (with `reload`) when it is wired.
   */
  stale?: boolean
  onReload?: () => void
}) {
  const proposals = state.proposals
  // `/api/me.solo` says so up front; on an older server the first approve that
  // asks for a confirm tells us.
  const [soloSeen, setSoloSeen] = useState(false)
  const solo = user.solo === true || soloSeen
  return (
    <Panel
      className="proposals"
      title={
        <>
          Proposals <span className="panel__count">{proposals.length}</span>
        </>
      }
    >
      {solo && (
        <p className="solo-banner" role="status">
          <Icon name="lock" size={16} />
          <span>
            Solo mode: you can approve every layer. Each approval needs you to type the pipeline
            and layer by hand.
          </span>
        </p>
      )}
      {proposals.length === 0 ? (
        <Empty icon="shield" title="No fix proposed yet">
          The agent proposes a fix once the hypothesis is confident enough; each layer owner approves their
          part.
        </Empty>
      ) : (
        <div className="proposals__list">
          {proposals.map((p) => {
            const card = (
              <Proposal
                key={p.proposal_id}
                proposal={p}
                user={user}
                actions={actions}
                pipelineId={state.pipeline_id}
                stale={stale}
                onReload={onReload}
                onSolo={() => setSoloSeen(true)}
              />
            )
            // With several proposals, settled ones fold into a one-line summary. A card
            // the user can act on is always open (that needs status 'open' anyway).
            if (!(proposals.length > 1 && p.status !== 'open')) return card
            return (
              <details key={p.proposal_id} className="proposal-fold">
                <summary>
                  <Chip tone={p.status === 'approved' ? 'ok' : 'fail'}>{p.status}</Chip>
                  <span className="proposal-fold__title">{p.title}</span>
                </summary>
                {card}
              </details>
            )
          })}
        </div>
      )}
    </Panel>
  )
}

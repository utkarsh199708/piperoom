import { useId, useState } from 'react'
import type { ProposalState, RoomState } from '../state/types'
import type { CurrentUser } from '../state/permissions'
import { canDecideLayer } from '../state/permissions'
import { soloConfirmText } from '../lib/solo'
import { describeIdentity, identityDetails, shortHash } from '../lib/hash'
import type { RoomActions } from '../hooks/useRoom'
import { Chip, Empty, Panel } from './ui'

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
          <div key={key} className="proposal__param">
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
  rejected,
  onConfirm,
  onCancel,
}: {
  want: string
  busy: boolean
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
        if (typed === want && !busy) onConfirm(typed)
      }}
    >
      {/* The label text holds the exact string, so a screen reader announces it. */}
      <label className="solo-confirm__label" htmlFor={inputId}>
        Solo mode: type <code className="mono">{want}</code> to approve
      </label>
      <input
        id={inputId}
        value={typed}
        onChange={(e) => setTyped(e.target.value)}
        onPaste={(e) => e.preventDefault()}
        onDrop={(e) => e.preventDefault()}
        aria-describedby={hintId}
        autoComplete="off"
        autoFocus
      />
      <span id={hintId} className="muted solo-confirm__hint">
        Type it by hand. Paste is off.
      </span>
      {rejected && (
        <span className="error-text" role="alert">
          The server did not accept that text.
        </span>
      )}
      <button type="submit" className="btn btn--small btn--approve" disabled={busy || typed !== want}>
        Approve
      </button>
      <button type="button" className="btn btn--small btn--ghost" onClick={onCancel}>
        Cancel
      </button>
    </form>
  )
}

function LayerDecision({
  proposal,
  layer,
  user,
  actions,
  pipelineId,
  onSolo,
}: {
  proposal: ProposalState
  layer: string
  user: CurrentUser
  actions: RoomActions
  pipelineId: string
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
  const canDecide = canDecideLayer(proposal, user, layer)

  return (
    <div className={`approval approval--${state}`}>
      <span className="approval__layer">{layer}</span>
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
      {canDecide && confirming && (
        <SoloConfirm
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
      {canDecide && !rejecting && !confirming && (
        <span className="approval__actions">
          <button
            type="button"
            className="btn btn--small btn--approve"
            disabled={busy || !hashed}
            onClick={() => (user.solo ? setConfirming(true) : void approve())}
          >
            Approve
          </button>
          <button
            type="button"
            className="btn btn--small btn--reject"
            disabled={!hashed}
            onClick={() => setRejecting(true)}
          >
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
            disabled={!reason.trim()}
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
    </div>
  )
}

// The proposal's params_hash (auth PR 4): the short form to glance at, the full
// value in a read-only monospace field to compare, and a copy button. This is the
// value approve and reject send back.
function ParamsHash({ hash }: { hash: string }) {
  const [copied, setCopied] = useState(false)
  if (!hash) {
    return (
      <p className="proposal__hash error-text" data-testid="params-hash-missing" role="note">
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
      <span className="muted">params hash</span>
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
      <button type="button" className="btn btn--small btn--ghost" onClick={copy}>
        {copied ? 'Copied' : 'Copy hash'}
      </button>
    </div>
  )
}

function Proposal({ proposal, user, actions, pipelineId, onSolo }: {
  proposal: ProposalState
  user: CurrentUser
  actions: RoomActions
  pipelineId: string
  onSolo: () => void
}) {
  return (
    <article className="proposal">
      <div className="proposal__head">
        <h3 className="proposal__title">{proposal.title}</h3>
        {proposal.status === 'approved' && <Chip tone="ok">approved</Chip>}
        {proposal.status === 'rejected' && <Chip tone="fail">rejected</Chip>}
      </div>
      {proposal.risk && (
        <p className="proposal__meta muted">
          risk: {proposal.risk} · rollback: {proposal.rollback}
        </p>
      )}
      <ul className="proposal__steps">
        {proposal.steps.map((s, i) => (
          <li key={i} className="proposal__step">
            <div className="proposal__step-head">
              <span className="mono">{s.action}</span>
              <span className="muted">{s.layer}</span>
            </div>
            <span className="muted">{s.description}</span>
            <StepParams params={s.params} />
          </li>
        ))}
      </ul>
      <ParamsHash hash={proposal.params_hash} />
      <div className="proposal__approvals">
        {proposal.required_layers.map((layer) => (
          <LayerDecision
            key={layer}
            proposal={proposal}
            layer={layer}
            user={user}
            actions={actions}
            pipelineId={pipelineId}
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
                <span className="mono">step {ex.step_index + 1}</span>
                <span>{ex.output}</span>
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
  // `/api/me.solo` says so up front; on an older server the first approve that
  // asks for a confirm tells us.
  const [soloSeen, setSoloSeen] = useState(false)
  const solo = user.solo === true || soloSeen
  return (
    <Panel
      className="proposals"
      title="Proposals"
      right={<span className="muted">{proposals.length}</span>}
    >
      {solo && (
        <p className="solo-banner" role="status">
          Solo mode: you can approve every layer. Each approval needs you to type the pipeline
          and layer by hand.
        </p>
      )}
      {proposals.length === 0 ? (
        <Empty>No proposals yet.</Empty>
      ) : (
        <div className="proposals__list">
          {proposals.map((p) => (
            <Proposal
              key={p.proposal_id}
              proposal={p}
              user={user}
              actions={actions}
              pipelineId={state.pipeline_id}
              onSolo={() => setSoloSeen(true)}
            />
          ))}
        </div>
      )}
    </Panel>
  )
}

import { useState } from 'react'
import type { ProposalState, RoomState } from '../state/types'
import type { CurrentUser } from '../state/permissions'
import { canDecideLayer } from '../state/permissions'
import type { RoomActions } from '../hooks/useRoom'
import { Chip, Empty, Panel } from './ui'

function LayerDecision({
  proposal,
  layer,
  user,
  actions,
}: {
  proposal: ProposalState
  layer: string
  user: CurrentUser
  actions: RoomActions
}) {
  const [rejecting, setRejecting] = useState(false)
  const [reason, setReason] = useState('')
  const state = proposal.approvals[layer] ?? 'pending'
  const canDecide = canDecideLayer(proposal, user, layer)

  return (
    <div className={`approval approval--${state}`}>
      <span className="approval__layer">{layer}</span>
      <Chip tone={state === 'approved' ? 'ok' : state === 'rejected' ? 'fail' : 'warn'}>{state}</Chip>
      {canDecide && !rejecting && (
        <span className="approval__actions">
          <button
            type="button"
            className="btn btn--small btn--approve"
            onClick={() => actions.approve(proposal.proposal_id, layer)}
          >
            Approve
          </button>
          <button
            type="button"
            className="btn btn--small btn--reject"
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
              actions.reject(proposal.proposal_id, layer, reason.trim())
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

function Proposal({ proposal, user, actions }: {
  proposal: ProposalState
  user: CurrentUser
  actions: RoomActions
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
            <span className="mono">{s.action}</span>
            <span className="muted">
              {s.layer} · {s.description}
            </span>
          </li>
        ))}
      </ul>
      <div className="proposal__approvals">
        {proposal.required_layers.map((layer) => (
          <LayerDecision key={layer} proposal={proposal} layer={layer} user={user} actions={actions} />
        ))}
      </div>
      {proposal.executed.length > 0 && (
        <ul className="proposal__executed">
          {proposal.executed.map((ex) => (
            <li key={ex.step_index} className={ex.ok ? 'exec-ok' : 'exec-fail'}>
              <span className="mono">step {ex.step_index + 1}</span>
              <span>{ex.output}</span>
            </li>
          ))}
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
  return (
    <Panel
      className="proposals"
      title="Proposals"
      right={<span className="muted">{proposals.length}</span>}
    >
      {proposals.length === 0 ? (
        <Empty>No proposals yet.</Empty>
      ) : (
        <div className="proposals__list">
          {proposals.map((p) => (
            <Proposal key={p.proposal_id} proposal={p} user={user} actions={actions} />
          ))}
        </div>
      )}
    </Panel>
  )
}

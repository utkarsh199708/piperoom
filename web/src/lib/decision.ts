// What the room asks of this person right now, as one value the decision bar draws.
// Pure: the bar owns the 3 s "done" timer and the dismissed-rejection storage.
import type { Actor } from "../api/types";
import type { ProposalState, RoomState } from "../state/types";
import { neutralizeBadgeText } from "./verification";
import type { CurrentUser } from "../state/permissions";
import { canDecideLayer, isViewer } from "../state/permissions";
import { waitingForWriteMode } from "./writeMode";
import { approveAllFor } from "./approveAll";
import { setupStatus } from "./setup";
import type { SetupStepView } from "./setup";

/** The setup plan step a decision belongs to, so the bar can say "Step 2 of 7 · compose_up". */
export interface SetupStepContext {
  step: SetupStepView | null;
  number: number;
  total: number;
}

export type SetupDecisionStatus =
  | "no_runner"
  | "runner_lost"
  | "paired"
  | "planning"
  | "running"
  | "blocked"
  | "awaiting_change"
  | "registered";

export type Decision =
  /** `all`: the card offers one "Approve and run" for these layers (P2c); `layer` is the first of them. */
  | {
      kind: "needs-you";
      proposal: ProposalState;
      layer: string;
      completing: boolean;
      all?: string[];
      setup?: SetupStepContext;
    }
  | {
      kind: "waiting";
      proposal: ProposalState;
      pending: string[];
      approved: number;
      total: number;
      setup?: SetupStepContext;
    }
  | {
      kind: "executing";
      proposal: ProposalState;
      step: number;
      total: number;
      action: string;
    }
  | { kind: "done"; proposal: ProposalState; ok: boolean; failedStep?: number }
  /**
   * `layer` is the layer whose rejection stopped the proposal; `cancelled` are the layers it left
   * undecided (see cancelledLayers), so the bar never says they still wait on someone.
   */
  | {
      kind: "rejected";
      proposal: ProposalState;
      by: string;
      byActor?: Actor;
      layer: string;
      cancelled: string[];
    }
  /**
   * A setup room between proposals: no runner, planning, blocked on a question, or registered.
   * `status` is the setup state; `step` names the plan step it is about when there is one.
   */
  | {
      kind: "setup";
      status: SetupDecisionStatus;
      step?: SetupStepView | null;
      number?: number;
      total?: number;
      /** runner label, or the agent's blocked question; both are text, never markup */
      label?: string;
      question?: string;
      reason?: string;
      watchedBy?: string;
      action?: string;
      /** awaiting_change: the table the person must change, and an example statement */
      table?: string;
      exampleSQL?: string;
    };

function newest(
  proposals: ProposalState[],
  status: ProposalState["status"],
): ProposalState | undefined {
  return [...proposals].reverse().find((p) => p.status === status);
}

/** Every layer except `layer` has approved: this person's approval completes the set. */
export function completesSet(p: ProposalState, layer: string): boolean {
  return p.required_layers.every(
    (l) => l === layer || p.approvals[l] === "approved",
  );
}

/**
 * The layers a rejection left undone. One rejection rejects the whole proposal (the server writes
 * one `approval.rejected` for the deciding layer and sets the proposal `rejected`), so a layer that
 * is still `pending` on a rejected proposal was cancelled, not left waiting for its owner. Keeping
 * this rule in one place is what stops the card offering a Nudge for a proposal that is dead.
 */
export function cancelledLayers(p: ProposalState): string[] {
  if (p.status !== "rejected") return [];
  // Anything that is not a recorded decision counts as cancelled: `pending` today, and a server
  // that later sends the leftover layers as `cancelled` reads the same way.
  return p.required_layers.filter((l) => {
    const value = p.approvals[l] ?? "pending";
    return value !== "approved" && value !== "rejected";
  });
}

export function decisionFor(
  state: RoomState,
  user: CurrentUser,
): Decision | null {
  if (state.status !== "open") return null;
  // U: a setup room answers in plan steps first. The first step that needs a person wins over
  // everything else: pairing the runner, an approval, or the agent's blocked question.
  if (state.setup) {
    const setup = setupStatus(state, user);
    if (setup) {
      const planStep = "step" in setup ? setup.step : null;
      const number = "number" in setup ? setup.number : 0;
      const total = "total" in setup ? setup.total : 0;
      const ctx: SetupStepContext = { step: planStep ?? null, number, total };
      switch (setup.kind) {
        case "awaiting_approval": {
          if (setup.forYou) {
            return {
              kind: "needs-you",
              proposal: setup.proposal,
              layer: setup.layer,
              completing: setup.completing,
              ...(setup.all ? { all: setup.all } : {}),
              setup: ctx,
            };
          }
          return {
            kind: "waiting",
            proposal: setup.proposal,
            pending: setup.pending,
            approved: setup.proposal.required_layers.filter(
              (l) => setup.proposal.approvals[l] === "approved",
            ).length,
            total: setup.proposal.required_layers.length,
            setup: ctx,
          };
        }
        case "running":
          return {
            kind: "setup",
            status: "running",
            step: ctx.step,
            number: ctx.number,
            total: ctx.total,
            action: setup.action,
          };
        case "blocked":
          return {
            kind: "setup",
            status: "blocked",
            step: ctx.step,
            number: ctx.number,
            total: ctx.total,
            question: setup.question,
            reason: setup.reason,
          };
        case "awaiting_change":
          return {
            kind: "setup",
            status: "awaiting_change",
            step: ctx.step,
            number: ctx.number,
            total: ctx.total,
            table: setup.table,
            exampleSQL: setup.exampleSQL,
          };
        case "no_runner":
        case "runner_lost":
        case "paired":
        case "planning":
          return {
            kind: "setup",
            status: setup.kind,
            step: ctx.step,
            number: ctx.number,
            total: ctx.total,
            ...(setup.kind === "runner_lost" || setup.kind === "paired"
              ? { label: setup.label }
              : {}),
          };
        case "registered":
          return {
            kind: "setup",
            status: "registered",
            watchedBy: setup.watchedBy,
          };
      }
    }
  }
  const proposals = state.proposals;

  // The same place ProposalsPanel gives the A and R keys: the first proposal with its "Approve and
  // run", or with a layer you can decide.
  if (!isViewer(user)) {
    for (const p of proposals) {
      const all = approveAllFor(state, p, user);
      if (all)
        return {
          kind: "needs-you",
          proposal: p,
          layer: all[0],
          completing: true,
          all,
        };
      const layer = p.required_layers.find((l) =>
        canDecideLayer(p, user, l, state.driver?.id),
      );
      if (layer)
        return {
          kind: "needs-you",
          proposal: p,
          layer,
          completing: completesSet(p, layer),
        };
    }
  }

  const approved = newest(proposals, "approved");
  if (approved) {
    const total = approved.steps.length;
    // A finished fix is news only until the agent proposes the next one: then the bar follows that one.
    const superseded = proposals.indexOf(approved) < proposals.length - 1;
    const failed = approved.executed.findIndex((e) => !e.ok);
    if (failed >= 0) {
      if (!superseded)
        return {
          kind: "done",
          proposal: approved,
          ok: false,
          failedStep: failed + 1,
        };
    } else if (waitingForWriteMode(approved)) {
      // Write mode is off: the set is complete and nothing will run, so no "Running step 1" bar.
      // The card and the rail say so; the bar has no question to ask.
    } else if (approved.executed.length < total) {
      const i = approved.executed.length;
      return {
        kind: "executing",
        proposal: approved,
        step: i + 1,
        total,
        action: approved.steps[i].action,
      };
    } else if (approved.executed.length > 0 && !superseded) {
      return { kind: "done", proposal: approved, ok: true };
    }
  }

  const waiting = proposals.find(
    (p) =>
      p.status === "open" &&
      p.required_layers.some((l) => p.approvals[l] === "pending"),
  );
  if (waiting) {
    return {
      kind: "waiting",
      proposal: waiting,
      pending: waiting.required_layers.filter(
        (l) => waiting.approvals[l] === "pending",
      ),
      approved: waiting.required_layers.filter(
        (l) => waiting.approvals[l] === "approved",
      ).length,
      total: waiting.required_layers.length,
    };
  }

  // A rejection stays until a newer proposal exists (the bar also lets the person dismiss it).
  const rejected = newest(proposals, "rejected");
  if (rejected && proposals[proposals.length - 1] === rejected) {
    const layer =
      rejected.required_layers.find(
        (l) => rejected.decisions?.[l]?.outcome === "rejected",
      ) ?? "";
    const d = layer ? rejected.decisions[layer] : undefined;
    return {
      kind: "rejected",
      proposal: rejected,
      by: d ? neutralizeBadgeText(d.by.name || d.by.id) : "a reviewer",
      byActor: d?.by,
      layer,
      cancelled: cancelledLayers(rejected),
    };
  }
  return null;
}

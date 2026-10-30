// The one line that answers "what does this room need from me?" and stays on screen while the
// room scrolls (F1b). It draws `decisionFor`; its buttons go through the approval card's own
// handlers (DecisionHandleContext), so there is no second copy of the approval logic.
import { useContext, useEffect, useRef, useState } from "react";
import { ArrowDown, TriangleAlert, Wrench } from "lucide-react";
import type { RoomState } from "@/state/types";
import type { CurrentUser } from "@/state/permissions";
import { decisionFor } from "@/lib/decision";
import { writeModeOff } from "@/lib/writeMode";
import { approveAllLabel } from "@/lib/approveAll";
import { STAGING_REQUIRED_SHORT, stagingRequired } from "@/lib/staging";
import { focusSetupPairing, openSetupStepper, stepLabel } from "@/lib/setup";
import type { Decision } from "@/lib/decision";
import { cn } from "@/lib/utils";
import { canProposeFix } from "@/lib/proposeFix";
import { PersonName } from "@/components/VerifiedBadge";
import { Button } from "@/components/ui/button";
import { Kbd } from "@/components/ui/kbd";
import { Progress } from "@/components/ui/progress";
import { DecisionHandleContext } from "@/components/shell/context";

const DONE_MS = 3000;

function dismissedKey(id: string): string {
  return `piperoom.dismissed.${id}`;
}
function readDismissed(id: string, memory: Set<string>): boolean {
  if (memory.has(id)) return true;
  try {
    return window.sessionStorage.getItem(dismissedKey(id)) === "1";
  } catch {
    return false;
  }
}

const BAR =
  "flex min-h-11 flex-wrap items-center gap-x-3 gap-y-1 border-b border-border px-4 py-1.5 text-sm sm:px-6";

export function DecisionBar({
  state,
  user,
  placement = "top",
  onProposeFix,
}: {
  state: RoomState;
  user: CurrentUser;
  /** 'top': sticky under the header. 'sheet': fixed to the bottom on phones, buttons full width (F7c). */
  placement?: "top" | "sheet";
  /**
   * P3d-2: opens "Propose a fix yourself". The button shows for the room's driver only
   * (lib/proposeFix canProposeFix), and only in the 'top' placement: the phone sheet keeps to the
   * one decision it asks for. With nothing to decide, the driver still gets a quiet bar holding it.
   */
  onProposeFix?: () => void;
}) {
  const registry = useContext(DecisionHandleContext);
  const decision = decisionFor(state, user);
  const [, bump] = useState(0);
  const memory = useRef(new Set<string>());
  // State, not a ref: the bar also renders nothing while `decision` is set (a done fix not seen
  // here, a dismissed rejection), so "decision !== null" does not say when the node comes back.
  const [barEl, setBarEl] = useState<HTMLDivElement | null>(null);
  const sheet = placement === "sheet";
  const heightVar = sheet ? "--sheet-h" : "--decision-h";
  // Publishes the bar's height so the tab list can stick right under it, or, as a sheet, so the composer
  // and <main> keep clear of it (F7c).
  useEffect(() => {
    const el = barEl;
    if (!el) return;
    const root = document.documentElement;
    const write = () =>
      root.style.setProperty(
        heightVar,
        `${Math.round(el.getBoundingClientRect().height)}px`,
      );
    write();
    if (typeof ResizeObserver === "undefined")
      return () => root.style.removeProperty(heightVar);
    const ro = new ResizeObserver(write);
    ro.observe(el);
    return () => {
      ro.disconnect();
      root.style.removeProperty(heightVar);
    };
  }, [barEl, heightVar]);

  // "done ok" shows for 3 s, and only when this screen saw the proposal in some other state first
  // (a finished fix in an old room must not flash on load).
  const seen = useRef(new Set<string>());
  const [doneGone, setDoneGone] = useState<string | null>(null);
  const doneId =
    decision?.kind === "done" && decision.ok
      ? decision.proposal.proposal_id
      : null;
  const wasSeen = doneId !== null && seen.current.has(doneId);
  useEffect(() => {
    if (decision && "proposal" in decision && decision.kind !== "done")
      seen.current.add(decision.proposal.proposal_id);
  });
  useEffect(() => {
    if (doneId === null || !wasSeen) return;
    const t = window.setTimeout(() => setDoneGone(doneId), DONE_MS);
    return () => window.clearTimeout(t);
  }, [doneId, wasSeen]);

  const propose =
    !sheet && onProposeFix && canProposeFix(state, user) ? (
      <Button
        variant="outline"
        size="sm"
        className="coarse:h-11"
        onClick={onProposeFix}
      >
        <Wrench aria-hidden="true" />
        Propose a fix yourself
      </Button>
    ) : null;

  // The bar's frame, shared by every decision and by the driver's quiet bar.
  const frame = (
    kind: string,
    tone: string,
    text: React.ReactNode,
    actions: React.ReactNode,
  ) => (
    <div
      ref={setBarEl}
      role="region"
      aria-label="Decision"
      data-placement={placement}
      data-kind={kind}
      className={cn(
        "bg-background motion-safe:transition-opacity motion-safe:duration-200",
        sheet
          ? "fixed inset-x-0 bottom-0 z-30 border-t border-border shadow-lg"
          : "sticky top-0 z-20",
      )}
    >
      <div
        className={cn(
          BAR,
          tone,
          sheet &&
            "border-b-0 px-4 pt-3 pb-[max(env(safe-area-inset-bottom),12px)] sm:px-4",
          sheet && kind === "needs-you" && "flex-col flex-nowrap items-stretch",
        )}
      >
        <div className="flex min-w-0 flex-1 items-center gap-2">{text}</div>
        {actions && (
          <div
            className={cn(
              "flex flex-wrap items-center gap-2",
              sheet && kind === "needs-you" ? "w-full flex-nowrap" : "shrink-0",
            )}
          >
            {actions}
          </div>
        )}
        {(decision?.kind === "executing" ||
          (decision?.kind === "setup" && decision.status === "running")) && (
          <Progress
            className="h-0.5 basis-full bg-transparent"
            indicatorClassName="bg-agent"
            aria-label="Running the step"
          />
        )}
      </div>
    </div>
  );
  // Nothing to decide: the driver keeps "Propose a fix yourself", everyone else sees no bar.
  const idle = () =>
    propose
      ? frame(
          "idle",
          "bg-card text-muted-foreground",
          <span className="min-w-0 truncate">Nothing needs a decision</span>,
          propose,
        )
      : null;

  if (!decision) return idle();
  const handle = () => registry?.get() ?? null;

  let tone = "bg-card text-muted-foreground";
  let text: React.ReactNode = null;
  let actions: React.ReactNode = null;
  let live = "";

  // U: a setup room's bar names the plan step the ask belongs to ("Step 2 of 7 · compose_up").
  const setupCtx =
    decision.kind === "needs-you" || decision.kind === "waiting"
      ? decision.setup
      : undefined;
  const setupPrefix = setupCtx
    ? `${stepLabel(setupCtx.step, setupCtx.number, setupCtx.total)} — `
    : "";
  const setupStepsButton = (
    <Button
      variant="ghost"
      size="sm"
      className={sheet ? "h-11 flex-1" : "coarse:h-11"}
      onClick={openSetupStepper}
    >
      View steps
      <ArrowDown aria-hidden="true" />
    </Button>
  );

  const view = (
    <Button variant="ghost" size="sm" onClick={() => handle()?.scrollTo()}>
      View steps
      <ArrowDown aria-hidden="true" />
    </Button>
  );

  switch (decision.kind) {
    case "needs-you": {
      const title = decision.proposal.title;
      const writeOff = writeModeOff(state.mode);
      // X8: a healthy stage is owed first. The card says so; the bar says it too and keeps Approve off.
      const held = stagingRequired(decision.proposal);
      tone = "bg-warning-subtle text-warning-foreground";
      live = `${setupPrefix}Needs your approval · ${(decision.all ?? [decision.layer]).join(" · ")} — ${title}${held ? ` · ${STAGING_REQUIRED_SHORT.toLowerCase()}` : ""}`;
      text = (
        <>
          <TriangleAlert className="size-4 shrink-0" aria-hidden="true" />
          <span
            aria-live="polite"
            className="min-w-0 truncate font-medium"
            title={live}
          >
            {live}
          </span>
        </>
      );
      actions = (
        <>
          <Button
            variant="ghost-danger"
            size="sm"
            className={sheet ? "h-11 flex-1" : "coarse:h-11"}
            aria-keyshortcuts="R"
            onClick={() => handle()?.reject()}
          >
            Reject
            {!sheet && <Kbd aria-hidden="true">R</Kbd>}
          </Button>
          <Button
            variant="success"
            size="sm"
            className={sheet ? "h-11 flex-1" : "coarse:h-11"}
            aria-keyshortcuts="A"
            disabled={held}
            onClick={() => handle()?.approve()}
          >
            {decision.all
              ? approveAllLabel(writeOff)
              : `Approve ${decision.layer}${writeOff ? " (will not run)" : ""}`}
            {!sheet && <Kbd aria-hidden="true">A</Kbd>}
          </Button>
          {!sheet && view}
        </>
      );
      break;
    }
    case "waiting":
      live = `${setupPrefix}Waiting for ${decision.pending.join(" · ")} · ${decision.approved} of ${decision.total} approved`;
      text = <span aria-live="polite">{live}</span>;
      actions = (
        <Button variant="ghost" size="sm" onClick={() => handle()?.scrollTo()}>
          View
          <ArrowDown aria-hidden="true" />
        </Button>
      );
      break;
    case "executing":
      tone = "bg-agent-subtle text-agent-foreground";
      live = `Running step ${decision.step} of ${decision.total}`;
      text = (
        <span aria-live="polite" className="min-w-0 truncate">
          Running step {decision.step} of {decision.total} ·{" "}
          <code className="font-mono">{decision.action}</code>
        </span>
      );
      break;
    case "done":
      if (decision.ok) {
        if (!wasSeen || doneGone === decision.proposal.proposal_id) return idle();
        tone = "bg-success-subtle text-success-foreground";
        const n = decision.proposal.steps.length;
        text = <span aria-live="polite">{`Fix ran · ${n} of ${n} steps`}</span>;
      } else {
        tone = "bg-destructive-subtle text-destructive-fg";
        text = (
          <span aria-live="polite">{`Fix failed at step ${decision.failedStep} — see the card`}</span>
        );
      }
      break;
    case "rejected": {
      const id = decision.proposal.proposal_id;
      if (readDismissed(id, memory.current)) return idle();
      const who = decision.byActor ? (
        <PersonName person={decision.byActor} />
      ) : (
        decision.by
      );
      // One rejection rejects the whole proposal, so name the layer that stopped it and the layers
      // it cancelled: otherwise the bar reads as if the other layer still waits on its owner (KAN-393).
      text = (
        <span
          aria-live="polite"
          className="inline-flex flex-wrap items-center gap-x-1.5"
        >
          {decision.cancelled.length > 0 ? (
            <>
              Rejected by {who}
              {decision.layer ? ` (${decision.layer})` : ""};{" "}
              {decision.cancelled.join(", ")} cancelled
            </>
          ) : (
            <>
              Proposal rejected by {who}
              {decision.layer ? ` · ${decision.layer}` : ""}
            </>
          )}
        </span>
      );
      actions = (
        <Button
          variant="ghost"
          size="sm"
          aria-label="Dismiss"
          onClick={() => {
            memory.current.add(id);
            try {
              window.sessionStorage.setItem(dismissedKey(id), "1");
            } catch {
              // in-memory set above still hides it for this visit
            }
            bump((n) => n + 1);
          }}
        >
          Dismiss
        </Button>
      );
      break;
    }
    case "setup": {
      // The setup room between proposals: pairing, planning, running a step, a question, or
      // the registered pipeline. The step text is the plan's; `question` is agent text.
      const at = stepLabel(
        decision.step ?? null,
        decision.number ?? 0,
        decision.total ?? 0,
      );
      switch (decision.status) {
        case "no_runner":
          tone = "bg-warning-subtle text-warning-foreground";
          live =
            "No runner paired — paste the pairing command in a terminal to start the setup";
          text = (
            <>
              <TriangleAlert className="size-4 shrink-0" aria-hidden="true" />
              <span aria-live="polite" className="min-w-0 truncate">
                No runner paired — paste the pairing command in a terminal
              </span>
            </>
          );
          actions = (
            <>
              <Button
                variant="outline"
                size="sm"
                className={sheet ? "h-11 flex-1" : "coarse:h-11"}
                onClick={focusSetupPairing}
              >
                Pairing command
              </Button>
              {setupStepsButton}
            </>
          );
          break;
        case "runner_lost":
          tone = "bg-warning-subtle text-warning-foreground";
          live = `Runner ${decision.label ?? ""} lost — start it again to continue the setup`;
          text = (
            <>
              <TriangleAlert className="size-4 shrink-0" aria-hidden="true" />
              <span aria-live="polite" className="min-w-0 truncate">
                {live}
              </span>
            </>
          );
          actions = setupStepsButton;
          break;
        case "paired":
          tone = "bg-agent-subtle text-agent-foreground";
          live = `Runner ${decision.label ?? ""} paired — the agent is planning the setup`;
          text = (
            <span aria-live="polite" className="min-w-0 truncate">
              {live}
            </span>
          );
          actions = setupStepsButton;
          break;
        case "planning":
          live = `${at} — planning the next step`;
          text = (
            <span aria-live="polite" className="min-w-0 truncate">
              {live}
            </span>
          );
          actions = setupStepsButton;
          break;
        case "running":
          tone = "bg-agent-subtle text-agent-foreground";
          live = `${at} is running${decision.action ? ` · ${decision.action}` : ""}`;
          text = (
            <span aria-live="polite" className="min-w-0 truncate">
              {at}
              {decision.action ? (
                <>
                  {" "}
                  · <code className="font-mono">{decision.action}</code> is
                  running
                </>
              ) : (
                " is running"
              )}
            </span>
          );
          actions = setupStepsButton;
          break;
        case "blocked":
          tone = "bg-warning-subtle text-warning-foreground";
          live = `${at} blocked — ${decision.question ?? decision.reason ?? ""}`;
          text = (
            <>
              <TriangleAlert className="size-4 shrink-0" aria-hidden="true" />
              <span
                aria-live="polite"
                className="min-w-0 truncate"
                title={live}
              >
                {at} blocked — {decision.question || decision.reason}
              </span>
            </>
          );
          actions = setupStepsButton;
          break;
        case "awaiting_change":
          tone = "bg-warning-subtle text-warning-foreground";
          live = `${at} — waiting for a change you make to ${decision.table}; the agent verifies when it lands`;
          text = (
            <>
              <TriangleAlert className="size-4 shrink-0" aria-hidden="true" />
              <span
                aria-live="polite"
                className="min-w-0 truncate"
                title={decision.exampleSQL || live}
              >
                {at} — make a change to{" "}
                <code className="font-mono">{decision.table}</code> so the agent
                can verify
              </span>
            </>
          );
          actions = setupStepsButton;
          break;
        case "registered":
          tone = "bg-success-subtle text-success-foreground";
          live = `Pipeline registered — watched by ${decision.watchedBy === "local" ? "the local runner" : "the server"}`;
          text = (
            <span aria-live="polite" className="min-w-0 truncate">
              {live}
            </span>
          );
          break;
      }
      break;
    }
  }

  // The tints are translucent: an opaque base keeps the room from showing through the sticky bar.
  return frame(
    (decision as Decision).kind,
    tone,
    text,
    propose ? (
      <>
        {actions}
        {propose}
      </>
    ) : (
      actions
    ),
  );
}

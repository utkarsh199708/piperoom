import { describe, expect, it } from "vitest";
import { fireEvent, render, screen } from "@testing-library/react";
import { DecisionBar } from "./DecisionBar";
import { SETUP_ROOM_ID } from "../../fixtures/setup";
import { applyEvent } from "../../state/reducer";
import type { CurrentUser } from "../../state/permissions";
import {
  awaitingState,
  planningEvents,
  runningState,
  setupState,
} from "../../test/setupRoom";

const arjun: CurrentUser = {
  id: "arjun",
  name: "Arjun",
  layers: ["environment"],
};
const other: CurrentUser = { id: "priya", name: "Priya", layers: ["consumer"] };
const runner = { label: "arjun-laptop", connected: true, tools: {} };

const bar = () => screen.getByRole("region", { name: "Decision" });

describe("DecisionBar in a setup room (U1-12a)", () => {
  it("no runner: asks a person to pair one", () => {
    render(<DecisionBar state={setupState()} user={arjun} />);
    expect(bar()).toHaveTextContent(
      "No runner paired — paste the pairing command in a terminal",
    );
    expect(
      screen.getByRole("button", { name: "Pairing command" }),
    ).toBeInTheDocument();
    fireEvent.click(screen.getByRole("button", { name: /View steps/ }));
  });

  it("paired: names the runner", () => {
    render(
      <DecisionBar state={setupState({ snapshot: { runner } })} user={arjun} />,
    );
    expect(bar()).toHaveTextContent(
      "Runner arjun-laptop paired — the agent is planning the setup",
    );
    expect(bar()).not.toHaveTextContent("Needs your approval");
  });

  it("planning: names the step the agent is on", () => {
    render(
      <DecisionBar
        state={setupState({ events: planningEvents() })}
        user={arjun}
      />,
    );
    expect(bar()).toHaveTextContent(
      "Step 1 of 4 · docker_info — planning the next step",
    );
  });

  it("running step: names the step and the action", () => {
    render(<DecisionBar state={runningState()} user={arjun} />);
    expect(bar()).toHaveTextContent("Step 2 of 4 · env_detect");
    expect(bar()).toHaveTextContent("properties_write is running");
  });

  it("awaiting approval: the first step that needs a person comes first, with the step number", () => {
    render(<DecisionBar state={awaitingState()} user={arjun} />);
    expect(bar()).toHaveTextContent(
      "Step 1 of 4 · docker_info — Needs your approval · environment — Start the stack",
    );
    expect(
      screen.getByRole("button", { name: /^Approve environment/ }),
    ).toBeInTheDocument();
    expect(screen.getByRole("button", { name: /^Reject/ })).toBeInTheDocument();
  });

  it("awaiting approval for someone else: says who it waits for, with no Approve", () => {
    const state = awaitingState();
    state.mode = { write: true, solo: false, auto_approve: [] };
    render(<DecisionBar state={state} user={other} />);
    expect(bar()).toHaveTextContent(
      "Step 1 of 4 · docker_info — Waiting for environment · 0 of 1 approved",
    );
    expect(
      screen.queryByRole("button", { name: /^Approve/ }),
    ).not.toBeInTheDocument();
  });

  it("blocked: shows the agent question as text", () => {
    let state = setupState({ events: planningEvents() });
    state = applyEvent(state, {
      room_id: SETUP_ROOM_ID,
      seq: state.last_seq + 1,
      type: "setup.blocked",
      actor: { kind: "agent", id: "agent", name: "Agent" },
      at: "2026-10-22T10:06:00Z",
      payload: {
        reason: "attempts",
        step_index: 2,
        question: "The Connect container keeps exiting. Check Docker?",
      },
    });
    render(<DecisionBar state={state} user={arjun} />);
    expect(bar()).toHaveTextContent("Step 3 of 4 · compose_up blocked");
    expect(bar()).toHaveTextContent(
      "The Connect container keeps exiting. Check Docker?",
    );
  });

  it("awaiting change: names the table the person must change", () => {
    let state = setupState({ events: planningEvents() });
    state = applyEvent(state, {
      room_id: SETUP_ROOM_ID,
      seq: state.last_seq + 1,
      type: "setup.awaiting_change",
      actor: { kind: "system", id: "ladder", name: "Ladder" },
      at: "2026-10-22T10:07:00Z",
      payload: {
        table: "public.orders",
        example_sql: "UPDATE public.orders SET note = 'ping' WHERE id = 1",
      },
    });
    render(<DecisionBar state={state} user={arjun} />);
    expect(bar()).toHaveTextContent(
      "make a change to public.orders so the agent can verify",
    );
    expect(bar()).not.toHaveTextContent("planning the next step");
  });

  it("registered: says who watches the pipeline", () => {
    render(
      <DecisionBar
        state={setupState({ snapshot: { runner, watched_by: "local" } })}
        user={arjun}
      />,
    );
    expect(bar()).toHaveTextContent(
      "Pipeline registered — watched by the local runner",
    );
  });

  it("the bottom sheet keeps the setup buttons full width", () => {
    render(<DecisionBar state={setupState()} user={arjun} placement="sheet" />);
    expect(bar()).toHaveAttribute("data-placement", "sheet");
    expect(
      screen.getByRole("button", { name: "Pairing command" }).className,
    ).toContain("h-11");
  });

  it("an incident room decision is unchanged", () => {
    const state = setupState();
    delete state.setup;
    render(<DecisionBar state={state} user={arjun} />);
    expect(
      screen.queryByRole("region", { name: "Decision" }),
    ).not.toBeInTheDocument();
  });
});

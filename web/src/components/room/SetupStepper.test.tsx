import { afterEach, describe, expect, it, vi } from "vitest";
import { fireEvent, render, screen, waitFor } from "@testing-library/react";
import { SetupStepper } from "./SetupStepper";
import { SETUP_ROOM_ID } from "../../fixtures/setup";
import { applyEvent } from "../../state/reducer";
import {
  awaitingState,
  planningEvents,
  runningState,
  setupState,
} from "../../test/setupRoom";

const runner = {
  label: "arjun-laptop",
  connected: true,
  tools: { docker: "27.1.1" },
};

afterEach(() => vi.unstubAllGlobals());

describe("SetupStepper — one state per test (U1-12a)", () => {
  it("no runner: says so, and no plan yet", () => {
    render(<SetupStepper state={setupState()} />);
    const region = screen.getByRole("region", { name: "Setup" });
    expect(region).toHaveTextContent("No runner paired");
    expect(region).toHaveTextContent("The agent has not written the plan yet.");
  });

  it("paired: names the runner and waits for the plan", () => {
    render(<SetupStepper state={setupState({ snapshot: { runner } })} />);
    const region = screen.getByRole("region", { name: "Setup" });
    expect(region).toHaveTextContent("Runner arjun-laptop paired");
    expect(region).toHaveTextContent("No plan yet");
  });

  it("planning: lists every plan step in order and marks the current one", () => {
    render(<SetupStepper state={setupState({ events: planningEvents() })} />);
    const list = screen.getByRole("list", { name: "Setup steps" });
    const steps = screen
      .getAllByRole("listitem")
      .filter((el) => list.contains(el));
    expect(steps.map((el) => el.textContent)).toEqual([
      expect.stringContaining("docker_info"),
      expect.stringContaining("env_detect"),
      expect.stringContaining("compose_up"),
      expect.stringContaining("properties_write"),
    ]);
    expect(steps[0]).toHaveAttribute("aria-current", "step");
    expect(steps[0]).toHaveTextContent("Step 1");
    expect(screen.getByRole("region", { name: "Setup" })).toHaveTextContent(
      "0 of 4 steps done",
    );
  });

  it("a done step is marked done and the plan moves on", () => {
    const state = applyEvent(setupState({ events: planningEvents() }), {
      room_id: SETUP_ROOM_ID,
      seq: 4,
      type: "action.executed",
      actor: { kind: "system", id: "executor", name: "Executor" },
      at: "2026-10-22T10:05:00Z",
      payload: {
        proposal_id: "p_setup",
        step_index: 0,
        ok: true,
        output: "ok",
      },
    });
    render(<SetupStepper state={state} />);
    const steps = screen.getAllByRole("listitem");
    expect(steps[0]).toHaveTextContent("done");
    expect(steps[1]).toHaveAttribute("aria-current", "step");
  });

  it("running step: the step says running", () => {
    render(<SetupStepper state={runningState()} />);
    const steps = screen.getAllByRole("listitem");
    expect(steps[1]).toHaveTextContent("running");
  });

  it("awaiting approval: the current step says needs approval", () => {
    render(<SetupStepper state={awaitingState()} />);
    const steps = screen.getAllByRole("listitem");
    expect(steps[0]).toHaveTextContent("needs approval");
  });

  it("blocked: the step the server named says blocked", () => {
    let state = setupState({ events: planningEvents() });
    state = applyEvent(state, {
      room_id: SETUP_ROOM_ID,
      seq: state.last_seq + 1,
      type: "setup.blocked",
      actor: { kind: "agent", id: "agent", name: "Agent" },
      at: "2026-10-22T10:06:00Z",
      payload: { reason: "attempts", step_index: 2, question: "Check Docker." },
    });
    render(<SetupStepper state={state} />);
    const steps = screen.getAllByRole("listitem");
    expect(steps[2]).toHaveTextContent("blocked");
    expect(steps[2]).toHaveAttribute("aria-current", "step");
  });

  it("an incident room renders nothing", () => {
    const state = setupState();
    delete state.setup;
    const { container } = render(<SetupStepper state={state} />);
    expect(container).toBeEmptyDOMElement();
  });
});

describe("SetupStepper on a phone", () => {
  it("is one button that opens the steps as a bottom sheet, and focus returns on Escape", async () => {
    vi.stubGlobal("matchMedia", (query: string) => ({
      matches: false,
      media: query,
      onchange: null,
      addEventListener: () => undefined,
      removeEventListener: () => undefined,
      addListener: () => undefined,
      removeListener: () => undefined,
      dispatchEvent: () => false,
    }));
    render(<SetupStepper state={setupState({ events: planningEvents() })} />);
    const trigger = screen.getByRole("button", { name: /Open setup steps/ });
    expect(trigger).toHaveAttribute("aria-expanded", "false");
    trigger.focus();
    fireEvent.click(trigger);
    const sheet = screen.getByRole("dialog", { name: "Setup steps" });
    expect(sheet).toHaveAttribute("data-state", "open");
    expect(sheet.className).toContain("bottom-0");
    expect(sheet).toHaveTextContent("docker_info");
    fireEvent.keyDown(sheet, { key: "Escape" });
    await waitFor(() =>
      expect(screen.queryByRole("dialog")).not.toBeInTheDocument(),
    );
    expect(trigger).toHaveFocus();
  });
});

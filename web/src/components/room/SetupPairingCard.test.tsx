import { beforeEach, describe, expect, it, vi } from "vitest";
import { fireEvent, render, screen, waitFor } from "@testing-library/react";
import { SetupPairingCard } from "./SetupPairingCard";
import { SETUP_ROOM_ID } from "../../fixtures/setup";
import { rememberSetupPairing } from "../../lib/setup";
import { pairedEvents, setupState } from "../../test/setupRoom";

const command = "piperoom-setup --server http://127.0.0.1:8090 --code ABCD2345";

beforeEach(() => sessionStorage.clear());

describe("SetupPairingCard", () => {
  it("shows the command the room answered with and copies it with a visible confirmation", async () => {
    const writeText = vi.fn().mockResolvedValue(undefined);
    Object.defineProperty(navigator, "clipboard", {
      value: { writeText },
      configurable: true,
    });
    rememberSetupPairing(SETUP_ROOM_ID, {
      command,
      expires_at: "2026-10-22T10:10:00Z",
    });

    render(<SetupPairingCard state={setupState()} />);
    expect(
      screen.getByRole("region", { name: "Pair a runner" }),
    ).toBeInTheDocument();
    expect(screen.getByLabelText("Pairing command")).toHaveTextContent(command);
    expect(
      screen.queryByText("Pairing command copied"),
    ).not.toBeInTheDocument();

    fireEvent.click(
      screen.getByRole("button", { name: "Copy pairing command" }),
    );
    await waitFor(() => expect(writeText).toHaveBeenCalledWith(command));
    expect(
      await screen.findByText("Pairing command copied"),
    ).toBeInTheDocument();
    expect(
      screen.getByRole("button", { name: "Copied pairing command" }),
    ).toBeInTheDocument();
  });

  it("without the stored command, says it was shown once and offers the form", () => {
    render(<SetupPairingCard state={setupState()} />);
    const card = screen.getByRole("region", { name: "Pair a runner" });
    expect(card).toHaveTextContent(
      "The pairing command was shown once when this room opened",
    );
    expect(screen.getByRole("link", { name: "Set up" })).toHaveAttribute(
      "href",
      "#/setup",
    );
    expect(
      screen.queryByRole("button", { name: /Copy pairing/ }),
    ).not.toBeInTheDocument();
  });

  it("disappears once a runner is paired", () => {
    rememberSetupPairing(SETUP_ROOM_ID, { command });
    const state = setupState({
      snapshot: {
        runner: { label: "arjun-laptop", connected: true, tools: {} },
      },
    });
    const { container } = render(<SetupPairingCard state={state} />);
    expect(container).toBeEmptyDOMElement();
  });
});

describe("SetupPairingCard after pairing", () => {
  it("drops the stored command once a runner is attached", () => {
    rememberSetupPairing(SETUP_ROOM_ID, { command });
    const { container } = render(
      <SetupPairingCard state={setupState({ events: pairedEvents() })} />,
    );
    expect(container).toBeEmptyDOMElement();
    expect(
      sessionStorage.getItem(`piperoom.setup.pairing.${SETUP_ROOM_ID}`),
    ).toBeNull();
  });
});

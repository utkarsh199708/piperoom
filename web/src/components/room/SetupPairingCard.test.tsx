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

describe("SetupPairingCard fingerprint and Strimzi", () => {
  const strimzi = {
    context: "kind-test",
    namespace: "data",
    kafka: "prod-kafka",
    pipeline: "orders",
    connectors: ["orders-src", "orders-sink"],
    room: "01jbx0setup",
    server: "https://piperoom.example.com",
    code: "ABCD2345",
    registry: "registry.example.com",
  };

  it("shows the fingerprint next to the laptop command", () => {
    rememberSetupPairing(SETUP_ROOM_ID, {
      command,
      fingerprint: "3fa9c1d2e4b5",
      target: "compose",
    });
    render(<SetupPairingCard state={setupState()} />);
    expect(screen.getByLabelText("Pairing fingerprint")).toHaveTextContent(
      "3fa9c1d2e4b5",
    );
    expect(screen.getByLabelText("Pairing command")).toHaveTextContent(command);
  });

  it("drops a stored fingerprint that is not 12 hex characters", () => {
    rememberSetupPairing(SETUP_ROOM_ID, { command, fingerprint: "<b>x</b>" });
    render(<SetupPairingCard state={setupState()} />);
    expect(screen.queryByLabelText("Pairing fingerprint")).toBeNull();
  });

  it("for strimzi lists the in-cluster commands in order, ending with the confirm Secret", async () => {
    const writeText = vi.fn().mockResolvedValue(undefined);
    Object.defineProperty(navigator, "clipboard", {
      value: { writeText },
      configurable: true,
    });
    rememberSetupPairing(SETUP_ROOM_ID, {
      command: "piperoom-setup --server https://piperoom.example.com --code ABCD2345",
      fingerprint: "3fa9c1d2e4b5",
      target: "strimzi",
      strimzi,
    });
    render(<SetupPairingCard state={setupState()} />);
    const items = screen.getAllByRole("listitem");
    expect(items).toHaveLength(6);
    expect(items[0]).toHaveTextContent(
      "kubectl --context kind-test -n data create secret docker-registry orders-registry-push --docker-server=registry.example.com",
    );
    expect(items[1]).toHaveTextContent(
      "helm upgrade --install --kube-context kind-test piperoom-pipeline-orders deploy/helm/piperoom -n data --set setup.pipeline.enabled=true --set setup.pipeline.id=orders --set setup.pipeline.kafka=prod-kafka --set 'setup.pipeline.connectors={orders-src,orders-sink}'",
    );
    expect(items[2]).toHaveTextContent(
      "create secret generic piperoom-setup-01jbx0setup-pairing --from-literal=code=ABCD2345",
    );
    expect(items[3]).toHaveTextContent(
      "piperoom-setup-01jbx0setup deploy/helm/piperoom -n data --set setup.runner.enabled=true --set setup.runner.room=01jbx0setup --set setup.runner.pipeline=orders --set setup.runner.kafka=prod-kafka --set 'setup.runner.connectors={orders-src,orders-sink}' --set setup.runner.serverURL=https://piperoom.example.com",
    );
    expect(items[4]).toHaveTextContent(
      "kubectl --context kind-test -n data logs job/piperoom-setup-01jbx0setup",
    );
    expect(items[5]).toHaveTextContent(
      "create secret generic piperoom-setup-01jbx0setup-confirm --from-literal=fingerprint=3fa9c1d2e4b5",
    );
    // The laptop line is not offered: the Job is the runner.
    expect(screen.queryByLabelText("Pairing command")).toBeNull();
    expect(screen.getByLabelText("Pairing fingerprint")).toHaveTextContent(
      "3fa9c1d2e4b5",
    );
    const buttons = screen.getAllByRole("button", { name: /^Copy / });
    expect(buttons).toHaveLength(6);
    fireEvent.click(buttons[5]);
    await waitFor(() =>
      expect(writeText).toHaveBeenCalledWith(
        "kubectl --context kind-test -n data create secret generic piperoom-setup-01jbx0setup-confirm --from-literal=fingerprint=3fa9c1d2e4b5",
      ),
    );
  });

  it("for strimzi without usable targets keeps the laptop line and points at the guide", () => {
    rememberSetupPairing(SETUP_ROOM_ID, {
      command,
      fingerprint: "3fa9c1d2e4b5",
      target: "strimzi",
    });
    render(<SetupPairingCard state={setupState()} />);
    expect(screen.getByLabelText("Pairing command")).toHaveTextContent(command);
    expect(
      screen.getByRole("region", { name: "Pair a runner" }),
    ).toHaveTextContent("docs/pilot/strimzi-target.md");
  });
});

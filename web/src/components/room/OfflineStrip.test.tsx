import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { act, fireEvent, render, screen } from "@testing-library/react";
import { OfflineStrip, MAYBE_SENT_TEXT } from "./OfflineStrip";
import { SendLedger, UNCONFIRMED_WINDOW_MS } from "@/hooks/sendLedger";
import type { SendItem } from "@/hooks/sendLedger";
import type { Event } from "@/api/types";
import type { SendControls } from "@/hooks/useRoom";
import { NOT_CONFIRMED_TEXT, OFFLINE_TEXT } from "@/lib/errors";

beforeEach(() => {
  vi.useFakeTimers();
});
afterEach(() => {
  vi.useRealTimers();
});

const item: SendItem = { kind: "comment", text: "looks wrong", stepId: "s1" };

function setup(dedupes: boolean | null) {
  const post = vi
    .fn<(i: SendItem, id: string) => Promise<Event | undefined>>()
    .mockRejectedValueOnce(new TypeError("Failed to fetch"));
  const ledger = new SendLedger({
    post,
    setError: () => {},
    newId: () => "c1",
  });
  ledger.dedupes = dedupes;
  const sends = {
    unconfirmedLeft: (id: string) => ledger.unconfirmedLeft(id),
  } as SendControls;
  return { ledger, sends, post };
}

describe("OfflineStrip", () => {
  it('keeps "Retrying" with no ledger state to go by', () => {
    render(<OfflineStrip onRetry={() => {}} />);
    expect(screen.getByRole("alert").textContent).toContain(OFFLINE_TEXT);
    expect(
      (screen.getByRole("button", { name: "Retry" }) as HTMLButtonElement)
        .disabled,
    ).toBe(false);
  });

  it('first send of a visit: says it may have arrived, no "Retrying", Retry off; then asks for a Retry that posts once with the same id', async () => {
    const { ledger, sends, post } = setup(null);
    await ledger.send(item, "c1");
    const onRetry = vi.fn(() => void ledger.send(item, "c1"));
    render(
      <OfflineStrip onRetry={onRetry} sends={sends} actionId={() => "c1"} />,
    );
    const text = () => screen.getByRole("alert").textContent ?? "";
    const retry = () =>
      screen.getByRole("button", { name: "Retry" }) as HTMLButtonElement;
    expect(text()).toContain(MAYBE_SENT_TEXT);
    expect(text()).not.toContain("Retrying");
    expect(retry().disabled).toBe(true);

    // The window ends with no new send: the strip changes by itself.
    act(() => {
      vi.advanceTimersByTime(UNCONFIRMED_WINDOW_MS);
    });
    expect(text()).toContain(NOT_CONFIRMED_TEXT);
    expect(retry().disabled).toBe(false);

    post.mockResolvedValueOnce({
      room_id: "r",
      seq: 1,
      type: "comment.added",
      actor: { kind: "human", id: "u", name: "U" },
      payload: { client_id: "c1" },
      at: "",
    } as Event);
    await act(async () => {
      fireEvent.click(retry());
    });
    expect(post).toHaveBeenCalledTimes(2);
    expect(post.mock.calls[1][1]).toBe("c1");
  });

  it('a server known to dedupe: the ledger really retries, so it says "Retrying"', async () => {
    const { ledger, sends } = setup(true);
    await ledger.send(item, "c1");
    render(
      <OfflineStrip onRetry={() => {}} sends={sends} actionId={() => "c1"} />,
    );
    expect(screen.getByRole("alert").textContent).toContain(OFFLINE_TEXT);
    expect(
      (screen.getByRole("button", { name: "Retry" }) as HTMLButtonElement)
        .disabled,
    ).toBe(false);
  });
});

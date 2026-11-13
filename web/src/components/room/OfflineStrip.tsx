import { useEffect, useState } from "react";
import { Button } from "@/components/ui/button";
import { NOT_CONFIRMED_TEXT, OFFLINE_TEXT } from "@/lib/errors";
import type { SendControls } from "@/hooks/useRoom";

/** The strip's line while a send with no answer waits out the ledger's window: the Retry does nothing yet. */
export const MAYBE_SENT_TEXT =
  "Piperoom may have received your message \u2014 it can be sent again in about a minute.";

/**
 * Under a composer whose send never reached the server: the words are kept, the person can retry.
 * With `sends` and `actionId`, it says what the ledger really does with the action: retries it
 * ("Retrying"), holds it for its window (Retry disabled), or waits for a Retry click (KAN-586).
 */
export function OfflineStrip({
  onRetry,
  disabled,
  sends,
  actionId,
}: {
  onRetry: () => void;
  disabled?: boolean;
  sends?: SendControls;
  /** The client_id of the action this strip is for (read on each render). */
  actionId?: () => string | null | undefined;
}) {
  const id = actionId?.();
  const left = id ? sends?.unconfirmedLeft?.(id) : undefined;
  const [, setTick] = useState(0);
  // The window ends without any new send: draw the strip again then.
  useEffect(() => {
    if (!left) return;
    const t = setTimeout(() => setTick((n) => n + 1), left);
    return () => clearTimeout(t);
  }, [left]);
  const text =
    left === undefined
      ? OFFLINE_TEXT
      : left > 0
        ? MAYBE_SENT_TEXT
        : NOT_CONFIRMED_TEXT;
  return (
    <div
      role="alert"
      className="flex items-center gap-2 text-xs text-warning-foreground"
    >
      <span className="min-w-0 flex-1">{text}</span>
      <Button
        type="button"
        variant="ghost"
        size="sm"
        className="h-7"
        disabled={disabled || (left !== undefined && left > 0)}
        onClick={onRetry}
      >
        Retry
      </Button>
    </div>
  );
}

// The pairing command (U1-12a): POST /api/setup answers with the one line to paste into a
// terminal. The server keeps no copy of the code (it is a credential), so the browser holds it
// for this tab only (lib/setup.ts). Copying shows a visible confirmation next to the button.
import { useEffect, useState } from "react";
import { Check, Copy, Terminal } from "lucide-react";
import { toast } from "sonner";
import type { RoomState } from "@/state/types";
import { Button } from "@/components/ui/button";
import { Card } from "@/components/ui/card";
import { SETUP_PAIRING_ID, clearSetupPairing, setupPairing } from "@/lib/setup";

export function SetupPairingCard({ state }: { state: RoomState }) {
  const [pairing] = useState(() => setupPairing(state.id));
  const [copied, setCopied] = useState(false);
  // Once a runner is attached the code is spent; the card has nothing left to
  // say, and the stored command (a credential, even a spent one) is dropped.
  const attached = Boolean(state.setup?.runner);
  const roomId = state.id;
  useEffect(() => {
    if (attached) clearSetupPairing(roomId);
  }, [attached, roomId]);
  if (!state.setup || state.setup.runner) return null;

  const copy = () => {
    if (!pairing) return;
    if (!navigator.clipboard) {
      toast.error(
        "Copy is not available in this browser. Select the command instead.",
      );
      return;
    }
    navigator.clipboard.writeText(pairing.command).then(
      () => {
        setCopied(true);
        toast.success("Pairing command copied");
      },
      () => toast.error("Could not copy. Select the command instead."),
    );
  };

  return (
    <Card
      id={SETUP_PAIRING_ID}
      tabIndex={-1}
      role="region"
      aria-labelledby="setup-pairing-title"
      className="mx-4 mt-2 flex min-w-0 flex-col gap-2 p-3 sm:mx-6"
    >
      <h2
        id="setup-pairing-title"
        className="m-0 flex items-center gap-2 text-sm font-semibold"
      >
        <Terminal
          className="size-4 shrink-0 text-muted-foreground"
          aria-hidden="true"
        />
        Pair a runner
      </h2>
      {pairing ? (
        <>
          <p className="m-0 text-xs text-muted-foreground">
            Paste this line into a terminal on the machine that reaches your
            stack. The code is single use and expires ten minutes after the room
            opened.
          </p>
          <div className="flex min-w-0 items-start gap-2">
            <pre
              aria-label="Pairing command"
              className="m-0 min-w-0 flex-1 overflow-x-auto whitespace-pre-wrap break-all rounded-md border border-border bg-muted px-3 py-2 font-mono text-xs"
            >
              {pairing.command}
            </pre>
            <Button
              variant="outline"
              size="icon"
              className="shrink-0 coarse:size-11"
              aria-label={
                copied ? "Copied pairing command" : "Copy pairing command"
              }
              onClick={copy}
            >
              {copied ? (
                <Check aria-hidden="true" />
              ) : (
                <Copy aria-hidden="true" />
              )}
            </Button>
          </div>
          <span
            role="status"
            className="min-h-4 text-xs text-success-foreground"
          >
            {copied ? "Pairing command copied" : ""}
          </span>
        </>
      ) : (
        <p className="m-0 text-xs text-muted-foreground">
          The pairing command was shown once when this room opened, and this
          browser did not keep it. Start again from{" "}
          <a className="underline underline-offset-4" href="#/setup">
            Set up
          </a>{" "}
          to get a new one.
        </p>
      )}
    </Card>
  );
}

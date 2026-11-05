// The pairing command (U1-12a): POST /api/setup answers with the one line to paste into a
// terminal and the pairing fingerprint. The server keeps no copy of the code (it is a
// credential), so the browser holds it for this tab only (lib/setup.ts). Copying shows a
// visible confirmation next to the button. For target strimzi the runner is a Job in the
// cluster, so the card lists the in-cluster commands of docs/design/setup-strimzi.md
// §3.1–§3.2 instead of the laptop line, ending with the confirm Secret that carries the
// fingerprint.
import { useEffect, useState } from "react";
import { Check, Copy, Terminal } from "lucide-react";
import { toast } from "sonner";
import type { RoomState } from "@/state/types";
import { Button } from "@/components/ui/button";
import { Card } from "@/components/ui/card";
import {
  SETUP_PAIRING_ID,
  clearSetupPairing,
  setupPairing,
  strimziPairingSteps,
} from "@/lib/setup";

export function SetupPairingCard({ state }: { state: RoomState }) {
  const [pairing] = useState(() => setupPairing(state.id));
  // Once a runner is attached the code is spent; the card has nothing left to
  // say, and the stored command (a credential, even a spent one) is dropped.
  const attached = Boolean(state.setup?.runner);
  const roomId = state.id;
  useEffect(() => {
    if (attached) clearSetupPairing(roomId);
  }, [attached, roomId]);
  if (!state.setup || state.setup.runner) return null;

  const steps =
    pairing?.strimzi && strimziPairingSteps(pairing.strimzi, pairing.fingerprint ?? "");

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
      {!pairing ? (
        <p className="m-0 text-xs text-muted-foreground">
          The pairing command was shown once when this room opened, and this
          browser did not keep it. Start again from{" "}
          <a className="underline underline-offset-4" href="#/setup">
            Set up
          </a>{" "}
          to get a new one.
        </p>
      ) : steps ? (
        <>
          <p className="m-0 text-xs text-muted-foreground">
            The runner is a Job in namespace{" "}
            <code className="font-mono">{pairing.strimzi?.namespace}</code>. Run
            these with your own kubectl and helm, in order. The code is single
            use and expires ten minutes after the room opened.
          </p>
          <ol className="m-0 flex min-w-0 list-none flex-col gap-2 p-0">
            {steps.map((step, i) => (
              <li key={step.label} className="flex min-w-0 flex-col gap-1">
                <span className="text-xs font-medium">
                  {i + 1}. {step.label}
                </span>
                <CommandBlock command={step.command} name={step.label} />
              </li>
            ))}
          </ol>
          <Fingerprint value={pairing.fingerprint} job />
        </>
      ) : (
        <>
          <p className="m-0 text-xs text-muted-foreground">
            Paste this line into a terminal on the machine that reaches your
            stack. The code is single use and expires ten minutes after the room
            opened.
          </p>
          <CommandBlock command={pairing.command} name="pairing command" />
          {pairing.target === "strimzi" && (
            <p className="m-0 text-xs text-muted-foreground">
              The in-cluster commands could not be built from this room's
              targets (a kubernetes target{" "}
              <code className="font-mono">&lt;context&gt;/&lt;namespace&gt;</code>
              , a kafka target{" "}
              <code className="font-mono">strimzi://&lt;name&gt;</code> and a
              connector name). Follow docs/pilot/strimzi-target.md with this
              code.
            </p>
          )}
          <Fingerprint value={pairing.fingerprint} />
        </>
      )}
    </Card>
  );
}

// The fingerprint the runner shows before it pairs. A runner that shows another one is not
// talking to this room: the person does not confirm.
function Fingerprint({ value, job }: { value?: string; job?: boolean }) {
  if (!value) return null;
  return (
    <p className="m-0 text-xs text-muted-foreground">
      Fingerprint{" "}
      <code
        aria-label="Pairing fingerprint"
        className="rounded bg-muted px-1 font-mono text-foreground"
      >
        {value}
      </code>
      {job
        ? ": the Job logs the same 12 characters in its Welcome block. Create the confirm Secret only when they match."
        : ": the runner shows the same 12 characters before it asks you to confirm. Answer y only when they match."}
    </p>
  );
}

// One command with its copy button and a visible confirmation.
function CommandBlock({ command, name }: { command: string; name: string }) {
  const [copied, setCopied] = useState(false);
  const label = name === "pairing command" ? "Pairing command" : name;
  const copy = () => {
    if (!navigator.clipboard) {
      toast.error(
        "Copy is not available in this browser. Select the command instead.",
      );
      return;
    }
    navigator.clipboard.writeText(command).then(
      () => {
        setCopied(true);
        toast.success(`${label} copied`);
      },
      () => toast.error("Could not copy. Select the command instead."),
    );
  };
  return (
    <>
      <div className="flex min-w-0 items-start gap-2">
        <pre
          aria-label={label}
          className="m-0 min-w-0 flex-1 overflow-x-auto whitespace-pre-wrap break-all rounded-md border border-border bg-muted px-3 py-2 font-mono text-xs"
        >
          {command}
        </pre>
        <Button
          variant="outline"
          size="icon"
          className="shrink-0 coarse:size-11"
          aria-label={copied ? `Copied ${name}` : `Copy ${name}`}
          onClick={copy}
        >
          {copied ? <Check aria-hidden="true" /> : <Copy aria-hidden="true" />}
        </Button>
      </div>
      <span role="status" className="min-h-4 text-xs text-success-foreground">
        {copied ? `${label} copied` : ""}
      </span>
    </>
  );
}

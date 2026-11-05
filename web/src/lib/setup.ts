// Setup rooms (U wave, docs/design/setup-agent.md): what the stepper draws and what the
// decision bar asks of this person. Pure, so every state is a test with a fake room and no
// server. The server's "setup" snapshot carries runner, plan id, phase and step index; the
// plan's phases and the executed steps come from the log (state/reducer.ts).
import type { ProposalState, RoomState, SetupState } from "../state/types";
import { stripBidi } from "./title";
import type { CurrentUser } from "../state/permissions";
import { canDecideLayer, isViewer } from "../state/permissions";
import { approveAllFor } from "./approveAll";
import { waitingForWriteMode } from "./writeMode";

/** The stepper's id: the decision bar's "View steps" scrolls to it (or opens its sheet). */
export const SETUP_STEPPER_ID = "setup-stepper";
export const SETUP_PAIRING_ID = "setup-pairing";

export type SetupStepStatus =
  | "done"
  | "failed"
  | "running"
  | "awaiting"
  | "blocked"
  | "pending";

/** One step of the plan as the stepper shows it. */
export interface SetupStepView {
  index: number;
  /** 1-based position in the whole plan, for "Step 2 of 7". */
  number: number;
  total: number;
  phase: string;
  op: string;
  target: string;
  tier: string;
  layer: string;
  verify?: string;
  status: SetupStepStatus;
}

export type SetupStatus =
  | { kind: "no_runner" }
  | { kind: "runner_lost"; label: string }
  | { kind: "paired"; label: string }
  | {
      kind: "planning";
      step: SetupStepView | null;
      number: number;
      total: number;
    }
  | {
      kind: "running";
      step: SetupStepView | null;
      number: number;
      total: number;
      action: string;
    }
  | {
      kind: "awaiting_approval";
      step: SetupStepView | null;
      number: number;
      total: number;
      proposal: ProposalState;
      layer: string;
      pending: string[];
      /** True when this person can decide a pending layer (or approve them all). */
      forYou: boolean;
      completing: boolean;
      all?: string[];
    }
  | {
      kind: "blocked";
      step: SetupStepView | null;
      number: number;
      total: number;
      reason: string;
      /** agent text, never a badge or markup input */
      question: string;
    }
  | {
      kind: "awaiting_change";
      step: SetupStepView | null;
      number: number;
      total: number;
      /** rung 8: the table the person must change so the ladder can verify */
      table: string;
      exampleSQL: string;
    }
  | { kind: "registered"; watchedBy: string };

/** The step the plan is on: the blocked step when the server named one, else setup.step_index. */
export function currentSetupStep(state: RoomState): SetupStepView | null {
  const steps = setupSteps(state.setup);
  const index =
    state.setup?.blocked?.step_index ?? state.setup?.step_index ?? 0;
  return steps.find((s) => s.index === index) ?? steps[0] ?? null;
}

/** The open proposal that is still missing a layer, if any (the "awaiting approval" step). */
function openProposal(state: RoomState): ProposalState | undefined {
  return state.proposals.find(
    (p) =>
      p.status === "open" &&
      p.required_layers.some(
        (l) => (p.approvals[l] ?? "pending") === "pending",
      ),
  );
}

/** The approved proposal still running its steps, if any. */
function runningProposal(state: RoomState): ProposalState | undefined {
  return state.proposals.find(
    (p) =>
      p.status === "approved" &&
      !waitingForWriteMode(p) &&
      p.executed.length < p.steps.length,
  );
}

/** Flatten the plan's phases into numbered steps with their status. */
export function setupSteps(setup: SetupState | undefined): SetupStepView[] {
  if (!setup) return [];
  const flat = setup.phases.flatMap((phase) =>
    phase.steps.map((step) => ({ ...step, phase: phase.name })),
  );
  const total = flat.length;
  return flat.map((step, i) => {
    const run = setup.executed[step.index];
    let status: SetupStepStatus = "pending";
    if (run) status = run.ok ? "done" : "failed";
    return { ...step, number: i + 1, total, status };
  });
}

/**
 * The steps with the current one marked running, awaiting or blocked. Kept separate from
 * setupSteps because it needs the proposals; the stepper calls this.
 */
export function setupStepViews(state: RoomState): SetupStepView[] {
  const steps = setupSteps(state.setup);
  if (!state.setup) return steps;
  const current = state.setup.step_index;
  const proposal = openProposal(state);
  const running = runningProposal(state);
  return steps.map((step) => {
    if (step.status === "done" || step.status === "failed") return step;
    if (
      state.setup?.blocked &&
      (state.setup.blocked.step_index ?? current) === step.index
    ) {
      return { ...step, status: "blocked" };
    }
    if (step.index !== current) return step;
    if (proposal) return { ...step, status: "awaiting" };
    if (running) return { ...step, status: "running" };
    return step;
  });
}

/** What this room needs from people right now. First match wins. */
export function setupStatus(
  state: RoomState,
  user: CurrentUser,
): SetupStatus | null {
  const setup = state.setup;
  if (!setup) return null;
  const step = currentSetupStep(state);
  const number = step?.number ?? 0;
  const total = step?.total ?? setupSteps(setup).length;

  if (!setup.runner) return { kind: "no_runner" };
  if (!setup.runner.connected)
    return { kind: "runner_lost", label: stripBidi(setup.runner.label) };
  if (setup.watched_by)
    return { kind: "registered", watchedBy: setup.watched_by };

  if (setup.blocked) {
    return {
      kind: "blocked",
      step,
      number,
      total,
      reason: setup.blocked.reason,
      question: stripBidi(setup.blocked.question),
    };
  }

  if (setup.awaiting_change) {
    return {
      kind: "awaiting_change",
      step,
      number,
      total,
      table: stripBidi(setup.awaiting_change.table),
      exampleSQL: stripBidi(setup.awaiting_change.example_sql ?? ""),
    };
  }

  const proposal = openProposal(state);
  if (proposal) {
    const pending = proposal.required_layers.filter(
      (l) => (proposal.approvals[l] ?? "pending") === "pending",
    );
    const all = isViewer(user)
      ? undefined
      : approveAllFor(state, proposal, user);
    const layer =
      all?.[0] ??
      pending.find((l) =>
        canDecideLayer(proposal, user, l, state.driver?.id),
      ) ??
      pending[0] ??
      "";
    return {
      kind: "awaiting_approval",
      step,
      number,
      total,
      proposal,
      layer,
      pending,
      forYou:
        !isViewer(user) &&
        (all != null ||
          pending.some((l) =>
            canDecideLayer(proposal, user, l, state.driver?.id),
          )),
      completing: proposal.required_layers.every(
        (l) => l === layer || proposal.approvals[l] === "approved",
      ),
      ...(all ? { all } : {}),
    };
  }

  const running = runningProposal(state);
  if (running) {
    const i = Math.min(running.executed.length, running.steps.length - 1);
    return {
      kind: "running",
      step,
      number,
      total,
      action: running.steps[i]?.action ?? "",
    };
  }

  if (!setup.plan_id)
    return { kind: "paired", label: stripBidi(setup.runner.label) };
  return { kind: "planning", step, number, total };
}

/** "Step 2 of 7 · compose_up" — one line for the bar and the stepper trigger. */
export function stepLabel(
  step: SetupStepView | null,
  number: number,
  total: number,
): string {
  const head = number > 0 && total > 0 ? `Step ${number} of ${total}` : "Setup";
  return step ? `${head} · ${step.op}` : head;
}

/** The runner's status in one short line. */
export function runnerLabel(setup: SetupState | undefined): string {
  if (!setup) return "";
  if (!setup.runner) return "No runner paired";
  const label = stripBidi(setup.runner.label);
  return setup.runner.connected
    ? `Runner ${label} paired`
    : `Runner ${label} lost`;
}

/** The bar's "View steps" target: opens the mobile sheet, else scrolls to the stepper. */
export function openSetupStepper(): void {
  const el = document.getElementById(SETUP_STEPPER_ID);
  if (!el) return;
  if (el instanceof HTMLButtonElement) {
    el.click();
    return;
  }
  el.scrollIntoView?.({ block: "start" });
  const focusable =
    el.querySelector<HTMLElement>("button, a, [tabindex]") ?? el;
  focusable.focus?.();
}

/** The bar's "Pairing command" target: scrolls to and focuses the pairing card. */
export function focusSetupPairing(): void {
  const el = document.getElementById(SETUP_PAIRING_ID);
  if (!el) return;
  el.scrollIntoView?.({ block: "start" });
  const focusable =
    el.querySelector<HTMLElement>("button, a, [tabindex]") ?? el;
  focusable.focus?.();
}

export interface SetupPairing {
  command: string;
  expires_at?: string;
  /** The server's fingerprint of this pairing (POST /api/setup `fingerprint`). */
  fingerprint?: string;
  /** The setup request's target (compose, strimzi, ...). */
  target?: string;
  /** For target strimzi: what the in-cluster commands are built from. */
  strimzi?: StrimziPairing;
}

/**
 * What the in-cluster pairing commands of docs/design/setup-strimzi.md §3.1–§3.2 are built from:
 * every field is the person's own SetupRequest or the server's answer, and each was checked
 * against a plain name shape, so no command needs quoting.
 */
export interface StrimziPairing {
  context: string;
  namespace: string;
  kafka: string;
  pipeline: string;
  connectors: string[];
  /** The room id without its r_ prefix, lowercased as the chart does. */
  room: string;
  /** The server origin the Job dials and checks the Welcome against. */
  server: string;
  code: string;
  /** host[:port] of the room's image_registry target, when it has one. */
  registry?: string;
}

/** What the browser keeps of POST /api/setup's answer for the room's pairing card. */
export function pairingOf(
  req: Parameters<typeof strimziPairingFrom>[0],
  res: Parameters<typeof strimziPairingFrom>[1] & {
    expires_at?: string;
    fingerprint?: string;
  },
): SetupPairing {
  return {
    command: res.command,
    expires_at: res.expires_at,
    fingerprint: res.fingerprint,
    target: req.target,
    strimzi: strimziPairingFrom(req, res),
  };
}

/** One labelled command of the in-cluster pairing. */
export interface PairingStep {
  label: string;
  command: string;
}

// A DNS-1123 label: a namespace, a Kafka CR, a pipeline id, a connector, the room's chart name.
const dnsLabel = /^[a-z0-9]([-a-z0-9]{0,61}[a-z0-9])?$/;
// A kubectl context name as people write them (EKS ARNs carry ':' and '/'); none needs quoting.
const kubeContext = /^[A-Za-z0-9._@:/-]{1,253}$/;
const origin = /^https?:\/\/[A-Za-z0-9.-]+(:[0-9]{1,5})?$/;
const pairingCode = /^[A-Z0-9]{8}$/;
const registryHost = /^[a-z0-9.-]+(:[0-9]{1,5})?$/;
const fingerprintShape = /^[0-9a-f]{12}$/;

/** The server origin a pairing command names (`--server <origin>`), or "" when it names none. */
export function commandOrigin(command: string): string {
  const m = /--server (\S+)/.exec(command);
  return m ? m[1] : "";
}

/**
 * The facts of the in-cluster commands for a Strimzi setup, or undefined when any of them is
 * missing or not a plain name (the card then points at the pilot guide instead of printing a
 * command it cannot vouch for).
 */
export function strimziPairingFrom(
  req: {
    pipeline_id: string;
    target: string;
    connectors?: string[];
    targets: { kind: string; address: string }[];
  },
  res: { room: { id: string }; code: string; command: string },
): StrimziPairing | undefined {
  if (req.target !== "strimzi") return undefined;
  const kube = req.targets.find((t) => t.kind === "kubernetes")?.address ?? "";
  const cut = kube.lastIndexOf("/");
  const context = cut > 0 ? kube.slice(0, cut) : "";
  const namespace = cut >= 0 ? kube.slice(cut + 1) : "";
  const kafkaAddr = req.targets.find((t) => t.kind === "kafka")?.address ?? "";
  const kafka = kafkaAddr.startsWith("strimzi://")
    ? kafkaAddr.slice("strimzi://".length)
    : "";
  const registry = req.targets.find((t) => t.kind === "image_registry")?.address;
  const room = res.room.id.replace(/^r_/, "").toLowerCase();
  const server = commandOrigin(res.command);
  const connectors = req.connectors ?? [];
  const ok =
    kubeContext.test(context) &&
    dnsLabel.test(namespace) &&
    dnsLabel.test(kafka) &&
    dnsLabel.test(req.pipeline_id) &&
    dnsLabel.test(room) &&
    connectors.length > 0 &&
    connectors.every((c) => dnsLabel.test(c)) &&
    origin.test(server) &&
    pairingCode.test(res.code);
  if (!ok) return undefined;
  return checkedStrimzi({
    context,
    namespace,
    kafka,
    pipeline: req.pipeline_id,
    connectors,
    room,
    server,
    code: res.code,
    registry:
      registry && registryHost.test(registry.toLowerCase())
        ? registry.toLowerCase()
        : undefined,
  });
}

/**
 * The Strimzi facts when every one is a plain name, else undefined. Run again on what the tab's
 * storage hands back, so no stored value reaches a copyable command line unchecked.
 */
export function checkedStrimzi(v: unknown): StrimziPairing | undefined {
  if (typeof v !== "object" || v === null) return undefined;
  const p = v as Record<string, unknown>;
  const str = (x: unknown): x is string => typeof x === "string";
  const connectors = p.connectors;
  const ok =
    str(p.context) &&
    kubeContext.test(p.context) &&
    str(p.namespace) &&
    dnsLabel.test(p.namespace) &&
    str(p.kafka) &&
    dnsLabel.test(p.kafka) &&
    str(p.pipeline) &&
    dnsLabel.test(p.pipeline) &&
    str(p.room) &&
    dnsLabel.test(p.room) &&
    Array.isArray(connectors) &&
    connectors.length > 0 &&
    connectors.every((c) => str(c) && dnsLabel.test(c)) &&
    str(p.server) &&
    origin.test(p.server) &&
    str(p.code) &&
    pairingCode.test(p.code) &&
    (p.registry === undefined || (str(p.registry) && registryHost.test(p.registry)));
  if (!ok) return undefined;
  return {
    context: p.context as string,
    namespace: p.namespace as string,
    kafka: p.kafka as string,
    pipeline: p.pipeline as string,
    connectors: [...(connectors as string[])],
    room: p.room as string,
    server: p.server as string,
    code: p.code as string,
    registry: p.registry as string | undefined,
  };
}

/**
 * The commands of docs/design/setup-strimzi.md §3.1–§3.2 (docs/pilot/strimzi-target.md §2–§3),
 * in the order the person runs them with their own kubectl and helm: the registry push Secret
 * and the pipeline release (skipped when that release exists), the pairing Secret, the room
 * release, the Job's log, and the confirm Secret carrying the fingerprint. The chart is the
 * checkout's deploy/helm/piperoom, as the pilot guide installs it.
 */
export function strimziPairingSteps(
  p: StrimziPairing,
  fingerprint: string,
): PairingStep[] {
  const kubectl = `kubectl --context ${p.context} -n ${p.namespace}`;
  const helm = `helm upgrade --install --kube-context ${p.context}`;
  const connectors = p.connectors.join(",");
  const job = `piperoom-setup-${p.room}`;
  const confirm = fingerprintShape.test(fingerprint)
    ? fingerprint
    : '"$FINGERPRINT"';
  return [
    {
      label: "Registry push Secret (skip when the pipeline release exists)",
      command:
        `${kubectl} create secret docker-registry ${p.pipeline}-registry-push ` +
        `--docker-server=${p.registry ?? '"$REGISTRY_SERVER"'} --docker-username="$REGISTRY_USER" --docker-password="$REGISTRY_PASSWORD"`,
    },
    {
      label: "Pipeline release (skip when it exists)",
      command:
        `${helm} piperoom-pipeline-${p.pipeline} deploy/helm/piperoom -n ${p.namespace} ` +
        `--set setup.pipeline.enabled=true --set setup.pipeline.id=${p.pipeline} ` +
        `--set setup.pipeline.kafka=${p.kafka} --set 'setup.pipeline.connectors={${connectors}}'`,
    },
    {
      label: "Pairing Secret (the single-use code)",
      command: `${kubectl} create secret generic ${job}-pairing --from-literal=code=${p.code}`,
    },
    {
      label: "Room release (the runner Job)",
      command:
        `${helm} ${job} deploy/helm/piperoom -n ${p.namespace} --set setup.runner.enabled=true ` +
        `--set setup.runner.room=${p.room} --set setup.runner.pipeline=${p.pipeline} ` +
        `--set setup.runner.kafka=${p.kafka} --set 'setup.runner.connectors={${connectors}}' ` +
        `--set setup.runner.serverURL=${p.server}`,
    },
    {
      label: "Read the Job's Welcome block and compare its fingerprint",
      command: `${kubectl} logs job/${job}`,
    },
    {
      label: "Confirm: only when the fingerprints match",
      command: `${kubectl} create secret generic ${job}-confirm --from-literal=fingerprint=${confirm}`,
    },
  ];
}

const pairingKey = (roomId: string) => `piperoom.setup.pairing.${roomId}`;

/**
 * The pairing command POST /api/setup answered with. The server never returns it again and
 * keeps no copy (the code is a credential), so the browser holds it for this tab only.
 */
export function rememberSetupPairing(
  roomId: string,
  pairing: SetupPairing,
): void {
  try {
    sessionStorage.setItem(pairingKey(roomId), JSON.stringify(pairing));
  } catch {
    // storage blocked: the pairing card says the command was shown once
  }
}

export function setupPairing(roomId: string): SetupPairing | null {
  try {
    const raw = sessionStorage.getItem(pairingKey(roomId));
    if (!raw) return null;
    const parsed = JSON.parse(raw) as Partial<SetupPairing>;
    if (typeof parsed.command !== "string" || !parsed.command) return null;
    return {
      command: parsed.command,
      expires_at: parsed.expires_at,
      fingerprint:
        typeof parsed.fingerprint === "string" &&
        fingerprintShape.test(parsed.fingerprint)
          ? parsed.fingerprint
          : undefined,
      target: typeof parsed.target === "string" ? parsed.target : undefined,
      strimzi: checkedStrimzi(parsed.strimzi),
    };
  } catch {
    return null;
  }
}

export function clearSetupPairing(roomId: string): void {
  try {
    sessionStorage.removeItem(pairingKey(roomId));
  } catch {
    // nothing to clear
  }
}

import { describe, expect, it } from "vitest";
import {
  checkedStrimzi,
  commandOrigin,
  pairingOf,
  setupPairing,
  strimziPairingFrom,
  strimziPairingSteps,
} from "./setup";

const req = {
  pipeline_id: "orders",
  target: "strimzi",
  connectors: ["orders-src"],
  targets: [
    { kind: "kubernetes", address: "arn:aws:eks:us-east-1:000000000000:cluster/prod/data" },
    { kind: "kafka", address: "strimzi://prod-kafka" },
    { kind: "image_registry", address: "Registry.Example.com:5000" },
  ],
};
const res = {
  room: { id: "r_01JBX0ABCDEF" },
  code: "ABCD2345",
  command: "piperoom-setup --server https://piperoom.example.com --code ABCD2345",
  expires_at: "2026-10-22T10:10:00Z",
  fingerprint: "3fa9c1d2e4b5",
};

describe("strimziPairingFrom", () => {
  it("takes the namespace after the last / and keeps the context whole", () => {
    const p = strimziPairingFrom(req, res);
    expect(p).toMatchObject({
      context: "arn:aws:eks:us-east-1:000000000000:cluster/prod",
      namespace: "data",
      kafka: "prod-kafka",
      room: "01jbx0abcdef",
      server: "https://piperoom.example.com",
      registry: "registry.example.com:5000",
    });
  });

  it("builds nothing for another target", () => {
    expect(strimziPairingFrom({ ...req, target: "compose" }, res)).toBeUndefined();
  });

  // A value that is not a plain name never reaches a command line.
  it.each([
    ["a namespace with a space", { targets: [{ kind: "kubernetes", address: "ctx/da ta" }, req.targets[1]] }],
    ["a context with a quote", { targets: [{ kind: "kubernetes", address: "c'tx/data" }, req.targets[1]] }],
    ["a kafka target that is not strimzi://", { targets: [req.targets[0], { kind: "kafka", address: "kafka:9092" }] }],
    ["no connector", { connectors: [] }],
    ["a connector with a semicolon", { connectors: ["a;b"] }],
    ["an upper-case pipeline id", { pipeline_id: "Orders" }],
  ])("refuses %s", (_, change) => {
    expect(strimziPairingFrom({ ...req, ...change }, res)).toBeUndefined();
  });

  it("refuses a command without a --server origin or with a bad code", () => {
    expect(strimziPairingFrom(req, { ...res, command: "piperoom-setup --code ABCD2345" })).toBeUndefined();
    expect(strimziPairingFrom(req, { ...res, code: "abcd$(x)" })).toBeUndefined();
  });
});

describe("strimziPairingSteps", () => {
  it("puts a placeholder in the confirm Secret when the fingerprint is not 12 hex", () => {
    const p = strimziPairingFrom(req, res)!;
    const steps = strimziPairingSteps(p, "nope");
    expect(steps[5].command).toContain('--from-literal=fingerprint="$FINGERPRINT"');
    expect(strimziPairingSteps({ ...p, registry: undefined }, "3fa9c1d2e4b5")[0].command).toContain(
      '--docker-server="$REGISTRY_SERVER"',
    );
  });

  // A placeholder the shell reads as a redirection (<user>, <registry>) would turn an unedited
  // paste into `< user > --docker-password=...`; every command is safe to paste as printed.
  it("prints no bare shell metacharacter outside quotes", () => {
    const p = strimziPairingFrom(req, res)!;
    for (const fp of ["3fa9c1d2e4b5", "nope"]) {
      for (const reg of [p.registry, undefined]) {
        for (const step of strimziPairingSteps({ ...p, registry: reg }, fp)) {
          const bare = step.command.replace(/'[^']*'|"[^"]*"/g, "");
          expect(bare, step.label).not.toMatch(/[<>|;&`$(){}]/);
        }
      }
    }
  });
});

describe("checkedStrimzi", () => {
  it("keeps facts that are all plain names", () => {
    const p = strimziPairingFrom(req, res)!;
    expect(checkedStrimzi(JSON.parse(JSON.stringify(p)))).toEqual(p);
  });

  it.each([
    ["a namespace with a command", { namespace: "data; rm -rf ~" }],
    ["a connector list that is not an array", { connectors: "a,b" }],
    ["a connector with a space", { connectors: ["a b"] }],
    ["a server that is not an origin", { server: "https://x/$(id)" }],
    ["a code with a dollar", { code: "ABCD$(x)" }],
    ["a registry with a quote", { registry: "r'eg" }],
    ["a missing context", { context: undefined }],
  ])("refuses stored facts with %s", (_, change) => {
    const p = strimziPairingFrom(req, res)!;
    expect(checkedStrimzi({ ...p, ...change })).toBeUndefined();
  });

  it("is applied to what the tab's storage hands back", () => {
    const p = strimziPairingFrom(req, res)!;
    sessionStorage.setItem(
      "piperoom.setup.pairing.r_x",
      JSON.stringify({ command: res.command, strimzi: { ...p, namespace: "a b" } }),
    );
    expect(setupPairing("r_x")?.strimzi).toBeUndefined();
    sessionStorage.setItem("piperoom.setup.pairing.r_x", JSON.stringify({ command: res.command, strimzi: p }));
    expect(setupPairing("r_x")?.strimzi).toEqual(p);
    sessionStorage.removeItem("piperoom.setup.pairing.r_x");
  });

  it("refuses what is not an object", () => {
    expect(checkedStrimzi(null)).toBeUndefined();
    expect(checkedStrimzi("x")).toBeUndefined();
  });
});

describe("pairingOf and commandOrigin", () => {
  it("keeps the fingerprint, the target and the Strimzi facts", () => {
    const p = pairingOf(req, res);
    expect(p.fingerprint).toBe("3fa9c1d2e4b5");
    expect(p.target).toBe("strimzi");
    expect(p.strimzi?.namespace).toBe("data");
    expect(commandOrigin("piperoom-setup --code X")).toBe("");
  });
});

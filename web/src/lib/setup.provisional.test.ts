import { describe, expect, it } from "vitest";
import { isProvisionalSetup } from "./setup";

// KAN-601: only an unregistered setup room has no topology or agent stats on the server.
describe("isProvisionalSetup", () => {
  it("is false for a normal room", () => {
    expect(isProvisionalSetup(undefined)).toBe(false);
    expect(isProvisionalSetup(null)).toBe(false);
  });

  it("is true for a setup room before registration, including a list row (no watched_by)", () => {
    expect(isProvisionalSetup({ watched_by: "" })).toBe(true);
    expect(isProvisionalSetup({ mode: true })).toBe(true);
  });

  it("is false once the pipeline is registered", () => {
    expect(isProvisionalSetup({ watched_by: "server" })).toBe(false);
    expect(isProvisionalSetup({ watched_by: "local" })).toBe(false);
  });

  it("is false when the snapshot says it is not a setup room", () => {
    expect(isProvisionalSetup({ mode: false })).toBe(false);
  });
});

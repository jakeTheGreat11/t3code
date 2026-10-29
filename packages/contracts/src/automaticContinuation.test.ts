import { describe, expect, it } from "vite-plus/test";
import * as Schema from "effect/Schema";
import { NewRunTrigger } from "./automaticContinuation.ts";
import { OrchestrationV2RunJson } from "./orchestrationV2.ts";
const decodeRun = Schema.decodeUnknownSync(OrchestrationV2RunJson);
const encodeRunJson = Schema.encodeSync(Schema.fromJsonString(OrchestrationV2RunJson));
const decodeRunJson = Schema.decodeSync(Schema.fromJsonString(OrchestrationV2RunJson));
const decodeTrigger = Schema.decodeUnknownSync(NewRunTrigger);
const reference = { threadId: "worker", runId: "source" };
const binding = {
  providerInstanceId: "instance",
  providerThreadId: "provider-thread",
  providerSessionId: "session",
};
const legacy = {
  id: "run",
  threadId: "worker",
  ordinal: 1,
  providerInstanceId: "instance",
  modelSelection: { instanceId: "instance", model: "catalog-model" },
  providerThreadId: null,
  userMessageId: "message",
  rootNodeId: null,
  activeAttemptId: null,
  status: "queued",
  requestedAt: "2026-10-01T00:00:00Z",
  startedAt: null,
  completedAt: null,
  checkpointId: null,
  contextHandoffId: null,
};
describe("durable new-run trigger contracts", () => {
  it.each(["exact", "unknown", "ambiguous"] as const)(
    "roundtrips %s provenance in a queued run",
    (type) => {
      const provenance =
        type === "exact"
          ? { type, originatingRun: reference }
          : type === "ambiguous"
            ? { type, candidates: [reference] }
            : { type };
      const decoded = decodeRun({
        ...legacy,
        newRunTrigger: {
          type: "automatic",
          kind: "provider_background",
          provenance,
          providerBinding: binding,
        },
      });
      const encoded = encodeRunJson(decoded);
      expect(decodeRunJson(encoded).newRunTrigger).toEqual(decoded.newRunTrigger);
    },
  );
  it("keeps legacy runs absent, without inventing manual provenance", () => {
    expect(decodeRun(legacy).newRunTrigger).toBeUndefined();
  });
  it.each([
    {},
    { type: "automatic" },
    { type: "automatic", kind: "provider_background", provenance: { type: "exact" } },
    {
      type: "automatic",
      kind: "provider_background",
      provenance: { type: "unknown" },
      providerBinding: { providerInstanceId: "instance" },
    },
    { type: "automatic", kind: "unsupported", provenance: { type: "unknown" } },
  ])("rejects malformed trigger %j", (value) => {
    expect(() => decodeTrigger(value)).toThrow();
  });
  it("does not accept model-supplied authorization or automatic metadata on manual input", () => {
    for (const value of [
      { type: "manual", provenance: { type: "unknown" } },
      { type: "automatic", kind: "restart", provenance: { type: "unknown" }, authorized: true },
    ])
      expect(() => decodeTrigger(value, { onExcessProperty: "error" })).toThrow();
  });
  it("bounds ambiguous diagnostic candidates", () => {
    expect(() =>
      decodeTrigger({
        type: "automatic",
        kind: "restart",
        provenance: { type: "ambiguous", candidates: Array.from({ length: 17 }, () => reference) },
      }),
    ).toThrow();
  });
});

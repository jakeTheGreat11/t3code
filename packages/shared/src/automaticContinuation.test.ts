import { describe, expect, it } from "vite-plus/test";
import {
  ThreadId,
  RunId,
  ProviderInstanceId,
  ProviderThreadId,
  ProviderSessionId,
  type NewRunTrigger,
} from "@t3tools/contracts";
import {
  admitAutomaticContinuation,
  readNewRunTrigger,
  type AutomaticContinuationContext,
} from "./automaticContinuation.ts";
const threadId = ThreadId.make("worker-conversation");
const reference = { threadId, runId: RunId.make("original-run") };
const binding = {
  providerInstanceId: ProviderInstanceId.make("instance"),
  providerThreadId: ProviderThreadId.make("native-binding"),
  providerSessionId: ProviderSessionId.make("runtime"),
};
const exact: NewRunTrigger = {
  type: "automatic",
  kind: "provider_background",
  provenance: { type: "exact", originatingRun: reference },
  providerBinding: binding,
};
const context: AutomaticContinuationContext = {
  threadId,
  providerBinding: binding,
  bindingExposure: "controlled_worker",
  originEvidence: { reference, classification: "controlled_worker", providerBinding: binding },
};

describe("automatic new-run admission", () => {
  it.each(["provider_background", "delegated_completion", "steer_follow_up", "restart"] as const)(
    "requires separate authorization for a controlled %s origin",
    (kind) => {
      expect(admitAutomaticContinuation({ ...exact, kind }, context)).toEqual({
        decision: "deny",
        reason: "separate_authorization_required",
      });
    },
  );
  it.each(["unexposed", "controlled_worker", "unknown"] as const)(
    "allows a verified manual-origin wake on a %s binding",
    (bindingExposure) => {
      expect(
        admitAutomaticContinuation(exact, {
          ...context,
          bindingExposure,
          originEvidence: { ...context.originEvidence!, classification: "ordinary" },
        }),
      ).toEqual({ decision: "allow", reason: "ordinary_origin" });
    },
  );
  it.each(["manual", "ordinary"] as const)(
    "preserves explicit %s input independent of the old controller",
    (type) => {
      expect(admitAutomaticContinuation({ type }, context)).toEqual({
        decision: "allow",
        reason: type,
      });
    },
  );
  const uncertainProvenances = [
    { type: "unknown" as const },
    { type: "ambiguous" as const },
    {
      type: "ambiguous" as const,
      candidates: [reference, { ...reference, runId: RunId.make("later-manual") }],
    },
  ];
  it.each(
    uncertainProvenances.flatMap((provenance) =>
      (["controlled_worker", "unknown"] as const).map((bindingExposure) => ({
        provenance,
        bindingExposure,
      })),
    ),
  )(
    "rejects uncertain origin on exposed or unknown binding: %j",
    ({ provenance, bindingExposure }) => {
      expect(
        admitAutomaticContinuation({ ...exact, provenance }, { ...context, bindingExposure }),
      ).toEqual({ decision: "deny", reason: "unsafe_origin" });
    },
  );
  it.each(uncertainProvenances)(
    "preserves ordinary unexposed uncertain behavior: %j",
    (provenance) => {
      expect(
        admitAutomaticContinuation(
          { ...exact, provenance },
          { ...context, bindingExposure: "unexposed" },
        ).decision,
      ).toBe("allow");
    },
  );
  it.each(["providerInstanceId", "providerThreadId", "providerSessionId"] as const)(
    "rejects stale %s even for an ordinary origin",
    (field) => {
      expect(
        admitAutomaticContinuation(exact, {
          ...context,
          providerBinding: { ...binding, [field]: "changed" },
          originEvidence: { ...context.originEvidence!, classification: "ordinary" },
        }),
      ).toEqual({ decision: "deny", reason: "stale_provider_binding" });
    },
  );
  it("does not substitute a later manual run's evidence for an earlier controlled run", () => {
    expect(
      admitAutomaticContinuation(exact, {
        ...context,
        originEvidence: {
          ...context.originEvidence!,
          reference: { ...reference, runId: RunId.make("later-manual") },
          classification: "ordinary",
        },
      }),
    ).toEqual({ decision: "deny", reason: "origin_unverified" });
  });
  it.each([undefined, { reference, classification: "unresolved" as const }])(
    "rejects missing or unresolved exact origin evidence",
    (originEvidence) => {
      const { originEvidence: _previous, ...withoutEvidence } = context;
      expect(
        admitAutomaticContinuation(
          exact,
          originEvidence ? { ...withoutEvidence, originEvidence } : withoutEvidence,
        ),
      ).toEqual({ decision: "deny", reason: "origin_unverified" });
    },
  );
  it("rejects cross-thread origins and stale source bindings", () => {
    expect(
      admitAutomaticContinuation(
        {
          ...exact,
          provenance: {
            type: "exact",
            originatingRun: { ...reference, threadId: ThreadId.make("other") },
          },
        },
        context,
      ).reason,
    ).toBe("origin_thread_mismatch");
    expect(
      admitAutomaticContinuation(exact, {
        ...context,
        originEvidence: {
          ...context.originEvidence!,
          providerBinding: { ...binding, providerSessionId: ProviderSessionId.make("other") },
        },
      }).reason,
    ).toBe("stale_provider_binding");
  });
  it("requires a binding for provider wakes", () => {
    const { providerBinding: _binding, ...unbound } = exact;
    expect(admitAutomaticContinuation(unbound, context).reason).toBe("missing_provider_binding");
    const { providerBinding: _current, ...withoutBinding } = context;
    expect(admitAutomaticContinuation(exact, withoutBinding).reason).toBe("stale_provider_binding");
  });
  it("allows an exact ordinary app-owned completion without a native binding", () => {
    expect(
      admitAutomaticContinuation(
        {
          type: "automatic",
          kind: "delegated_completion",
          provenance: { type: "exact", originatingRun: reference },
        },
        {
          threadId,
          bindingExposure: "unknown",
          originEvidence: { reference, classification: "ordinary" },
        },
      ).decision,
    ).toBe("allow");
  });
  it("never silently classifies missing legacy provenance as manual", () => {
    expect(readNewRunTrigger({})).toEqual({ type: "legacy_unknown" });
    expect(admitAutomaticContinuation(undefined, context).reason).toBe(
      "missing_trigger_classification",
    );
    expect(
      admitAutomaticContinuation(undefined, { ...context, legacyIntent: "automatic" }).reason,
    ).toBe("unsafe_origin");
    expect(
      admitAutomaticContinuation(undefined, {
        ...context,
        legacyIntent: "automatic",
        bindingExposure: "unexposed",
      }).decision,
    ).toBe("allow");
  });
  it.each(["manual", "ordinary"] as const)(
    "allows explicitly classified legacy %s intake",
    (legacyIntent) => {
      expect(admitAutomaticContinuation(undefined, { ...context, legacyIntent })).toEqual({
        decision: "allow",
        reason: legacyIntent,
      });
    },
  );
  it("does not mutate stored provenance or evidence", () => {
    const before = JSON.stringify({ exact, context });
    admitAutomaticContinuation(exact, context);
    expect(JSON.stringify({ exact, context })).toBe(before);
    expect(readNewRunTrigger({ newRunTrigger: exact })).toEqual(exact);
  });
});

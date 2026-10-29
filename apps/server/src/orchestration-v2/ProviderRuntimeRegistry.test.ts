import { describe, expect, it } from "vite-plus/test";
import {
  CommandId,
  NodeId,
  MessageId,
  RunId,
  RunAttemptId,
  ThreadId,
  ProviderThreadId,
  ProviderInstanceId,
  ProviderSessionId,
  ProviderInstanceGenerationId,
  ProviderSessionGenerationId,
  type ContinuationEmitterIdentity,
  type MultiAgentWorkerOwnership,
} from "@t3tools/contracts";
import type { ProviderAdapterV2SessionRuntime } from "@t3tools/provider-core/server/ProviderAdapter";
import { ProviderRuntimeRegistry } from "./ProviderRuntimeRegistry.ts";

function identity(
  session = "session-one",
  generation = "runtime-one",
  adapter = "adapter-one",
  instance = "instance-one",
) {
  return {
    type: "known" as const,
    providerInstanceId: ProviderInstanceId.make(instance),
    providerInstanceGenerationId: ProviderInstanceGenerationId.make(adapter),
    providerSessionId: ProviderSessionId.make(session),
    providerSessionGenerationId: ProviderSessionGenerationId.make(generation),
  };
}
function runtime(emitter: ContinuationEmitterIdentity | undefined = identity()) {
  return {
    instanceId:
      emitter?.type === "known"
        ? emitter.providerInstanceId
        : ProviderInstanceId.make("instance-one"),
    providerSessionId:
      emitter?.type === "known" ? emitter.providerSessionId : ProviderSessionId.make("session-one"),
    ...(emitter ? { continuationEmitterIdentity: emitter } : {}),
  } as ProviderAdapterV2SessionRuntime;
}
function register(
  registry: ProviderRuntimeRegistry,
  session: ProviderAdapterV2SessionRuntime,
  historicalState: "fresh" | "unknown" = "fresh",
) {
  registry.register({
    runtime: session,
    providerInstanceId: session.instanceId,
    providerSessionId: session.providerSessionId,
    historicalState,
  });
}
function owner(session: ProviderAdapterV2SessionRuntime): MultiAgentWorkerOwnership {
  return {
    coordinatorThreadId: ThreadId.make("coordinator"),
    coordinatorRunId: RunId.make("coordinator-run"),
    taskId: NodeId.make("task"),
    workerThreadId: ThreadId.make("worker"),
    workerRunId: RunId.make("worker-run"),
    workerAttemptId: RunAttemptId.make("worker-attempt"),
    launchCommandId: CommandId.make("launch"),
    launchMessageId: MessageId.make("message"),
    providerThreadId: ProviderThreadId.make("provider-thread"),
    providerInstanceId: session.instanceId,
    providerSessionId: session.providerSessionId,
  };
}
describe("provider runtime registry", () => {
  it("conflicting repeat registration cannot silently retain clean evidence", () => {
    const registry = new ProviderRuntimeRegistry();
    const session = runtime();
    register(registry, session);
    registry.register({
      runtime: session,
      providerInstanceId: session.instanceId,
      providerSessionId: ProviderSessionId.make("different-binding"),
      historicalState: "fresh",
    });
    expect(registry.validate(session.continuationEmitterIdentity)).toEqual({
      status: "conflicting",
      bindingExposure: "unknown",
    });
    register(registry, session);
    expect(registry.validate(session.continuationEmitterIdentity).bindingExposure).toBe("unknown");
  });
  it("positively registers clean construction and does not register from an offer", () => {
    const registry = new ProviderRuntimeRegistry();
    const session = runtime();
    expect(registry.validate(session.continuationEmitterIdentity)).toEqual({
      status: "unknown",
      bindingExposure: "unknown",
    });
    register(registry, session);
    expect(registry.validate(session.continuationEmitterIdentity)).toEqual({
      status: "verified",
      bindingExposure: "unexposed",
    });
  });
  it("marks a shared runtime monotonically, including later manual use and duplicate registration", () => {
    const registry = new ProviderRuntimeRegistry();
    const shared = runtime();
    register(registry, shared);
    expect(registry.exposeControlledWorker(shared, owner(shared))).toBeUndefined();
    expect(registry.exposeControlledWorker(shared, owner(shared))).toBeUndefined();
    register(registry, shared);
    expect(registry.validate(shared.continuationEmitterIdentity)).toEqual({
      status: "verified",
      bindingExposure: "controlled_worker",
    });
    const independent = runtime(identity("independent", "independent-runtime"));
    register(registry, independent);
    expect(registry.validate(independent.continuationEmitterIdentity).bindingExposure).toBe(
      "unexposed",
    );
  });
  it("requires the exact registered object and exact immutable ownership binding", () => {
    const registry = new ProviderRuntimeRegistry();
    const session = runtime();
    register(registry, session);
    expect(registry.exposeControlledWorker({ ...session }, owner(session))).toBe(
      "unregistered_runtime",
    );
    expect(
      registry.exposeControlledWorker(session, {
        ...owner(session),
        providerSessionId: ProviderSessionId.make("other"),
      }),
    ).toBe("ownership_mismatch");
    expect(
      registry.exposeControlledWorker(session, {
        ...owner(session),
        providerInstanceId: ProviderInstanceId.make("other"),
      }),
    ).toBe("ownership_mismatch");
    expect(registry.validate(session.continuationEmitterIdentity).bindingExposure).toBe(
      "unexposed",
    );
  });
  it("offer metadata cannot overwrite a registered tuple", () => {
    const registry = new ProviderRuntimeRegistry();
    const session = runtime();
    register(registry, session);
    for (const forged of [
      { ...identity(), providerSessionId: ProviderSessionId.make("other") },
      { ...identity(), providerInstanceId: ProviderInstanceId.make("other") },
      { ...identity(), providerInstanceGenerationId: ProviderInstanceGenerationId.make("other") },
      { ...identity(), providerSessionGenerationId: ProviderSessionGenerationId.make("other") },
    ])
      expect(registry.validate(forged).bindingExposure).toBe("unknown");
    expect(registry.validate(identity())).toEqual({
      status: "verified",
      bindingExposure: "unexposed",
    });
  });
  it("keeps delayed retired offers separate from replacement adapter and session generations", () => {
    const registry = new ProviderRuntimeRegistry();
    const old = runtime();
    register(registry, old);
    registry.exposeControlledWorker(old, owner(old));
    registry.retire(old);
    const replacement = runtime(identity("other-session", "replacement", "adapter-two"));
    register(registry, replacement);
    expect(registry.validate(old.continuationEmitterIdentity)).toEqual({
      status: "retired",
      bindingExposure: "controlled_worker",
    });
    expect(registry.validate(replacement.continuationEmitterIdentity)).toEqual({
      status: "verified",
      bindingExposure: "unexposed",
    });
    expect(registry.exposeControlledWorker(old, owner(old))).toBe("retired_runtime");
  });
  it("cannot turn a reopened exposed logical session into a clean generation", () => {
    const registry = new ProviderRuntimeRegistry();
    const old = runtime();
    register(registry, old);
    registry.exposeControlledWorker(old, owner(old));
    registry.retire(old);
    const reopened = runtime(identity("session-one", "reopened", "replacement-adapter"));
    register(registry, reopened);
    expect(registry.validate(reopened.continuationEmitterIdentity).bindingExposure).toBe(
      "controlled_worker",
    );
  });
  it("retired clean generations are stale rather than live clean evidence", () => {
    const registry = new ProviderRuntimeRegistry();
    const old = runtime();
    register(registry, old);
    registry.retire(old);
    expect(registry.validate(old.continuationEmitterIdentity)).toEqual({
      status: "retired",
      bindingExposure: "unknown",
    });
  });
  it("fails closed on generation collisions instead of replacing history", () => {
    const registry = new ProviderRuntimeRegistry();
    const first = runtime();
    register(registry, first);
    registry.exposeControlledWorker(first, owner(first));
    const conflicting = runtime(identity("another-session"));
    register(registry, conflicting);
    expect(registry.validate(first.continuationEmitterIdentity)).toEqual({
      status: "conflicting",
      bindingExposure: "unknown",
    });
    expect(registry.validate(conflicting.continuationEmitterIdentity).bindingExposure).toBe(
      "unknown",
    );
    expect(registry.exposeControlledWorker(conflicting, owner(conflicting))).toBe(
      "conflicting_identity",
    );
    const reopened = runtime(identity("session-one", "fresh-generation"));
    register(registry, reopened);
    expect(registry.validate(reopened.continuationEmitterIdentity).bindingExposure).toBe(
      "controlled_worker",
    );
  });
  it("does not let a copied runtime reclaim the same generation", () => {
    const registry = new ProviderRuntimeRegistry();
    const first = runtime();
    register(registry, first);
    register(registry, { ...first });
    expect(registry.validate(first.continuationEmitterIdentity).status).toBe("conflicting");
  });
  it("rejects an adapter generation reused across configured provider instances", () => {
    const registry = new ProviderRuntimeRegistry();
    const first = runtime();
    const second = runtime(identity("session-two", "runtime-two", "adapter-one", "instance-two"));
    register(registry, first);
    register(registry, second);
    expect(registry.validate(first.continuationEmitterIdentity).status).toBe("conflicting");
    expect(registry.validate(second.continuationEmitterIdentity).bindingExposure).toBe("unknown");
  });
  it("checks authoritative open bindings instead of trusting runtime metadata", () => {
    const registry = new ProviderRuntimeRegistry();
    const session = runtime();
    registry.register({
      runtime: session,
      providerInstanceId: session.instanceId,
      providerSessionId: ProviderSessionId.make("actual-session"),
      historicalState: "fresh",
    });
    expect(registry.validate(session.continuationEmitterIdentity)).toEqual({
      status: "conflicting",
      bindingExposure: "unknown",
    });
    expect(registry.exposeControlledWorker(session, owner(session))).toBe("conflicting_identity");
  });
  it("lost history after restart is unknown, including a recovered wrapper with a fresh generation", () => {
    const oldRegistry = new ProviderRuntimeRegistry();
    const old = runtime();
    register(oldRegistry, old);
    oldRegistry.exposeControlledWorker(old, owner(old));
    const restarted = new ProviderRuntimeRegistry();
    expect(restarted.validate(old.continuationEmitterIdentity).bindingExposure).toBe("unknown");
    const recovered = runtime(identity("session-one", "restart-generation", "restart-adapter"));
    register(restarted, recovered, "unknown");
    expect(restarted.validate(recovered.continuationEmitterIdentity)).toEqual({
      status: "verified",
      bindingExposure: "unknown",
    });
    restarted.exposeControlledWorker(recovered, owner(recovered));
    expect(restarted.validate(recovered.continuationEmitterIdentity).bindingExposure).toBe(
      "controlled_worker",
    );
  });
  it("unknown historical state survives another reopening", () => {
    const registry = new ProviderRuntimeRegistry();
    const recovered = runtime();
    register(registry, recovered, "unknown");
    registry.retire(recovered);
    const next = runtime(identity("session-one", "next-generation"));
    register(registry, next);
    expect(registry.validate(next.continuationEmitterIdentity).bindingExposure).toBe("unknown");
  });
  it.each([
    undefined,
    { type: "unknown", reason: "legacy_request" },
    { type: "unknown", reason: "not_runtime_owned" },
  ] as const)("never treats missing or explicit unknown identity as clean: %j", (emitter) => {
    const registry = new ProviderRuntimeRegistry();
    const session =
      emitter === undefined
        ? ({
            instanceId: ProviderInstanceId.make("instance-one"),
            providerSessionId: ProviderSessionId.make("session-one"),
          } as ProviderAdapterV2SessionRuntime)
        : runtime(emitter);
    register(registry, session);
    expect(registry.validate(emitter)).toEqual({ status: "unknown", bindingExposure: "unknown" });
    expect(registry.exposeControlledWorker(session, owner(session))).toBeUndefined();
    expect(registry.validate(emitter).bindingExposure).toBe("unknown");
  });
});

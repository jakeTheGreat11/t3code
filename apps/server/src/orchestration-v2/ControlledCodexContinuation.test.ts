import { assert, describe, it } from "@effect/vitest";
import {
  MessageId,
  RunId,
  ThreadId,
  type OrchestrationV2ThreadProjection,
} from "@t3tools/contracts";
import {
  controlledCodexDispatchBlocked,
  controlledCodexOriginBlocked,
  controlledCodexQueuedStartBlocked,
} from "./ControlledCodexContinuation.ts";

const workerId = RunId.make("controlled-initial");
const manualId = RunId.make("later-manual");
const threadId = ThreadId.make("worker-thread");
function fixture() {
  return {
    runs: [
      { id: workerId, threadId, controlledWorkerOwnership: { providerThreadId: "owned-binding" } },
      { id: manualId, threadId, userMessageId: MessageId.make("manual-message") },
    ],
    providerThreads: [{ id: "owned-binding", driver: "codex" }],
    messages: [{ id: "manual-message", createdBy: "user", creationSource: "web" }],
  } as unknown as {
    runs: Array<OrchestrationV2ThreadProjection["runs"][number]>;
    providerThreads: Array<OrchestrationV2ThreadProjection["providerThreads"][number]>;
    messages: Array<OrchestrationV2ThreadProjection["messages"][number]>;
  };
}

describe("controlled Codex automatic new-run guard", () => {
  it("blocks the exact controlled origin and permits a later explicit manual origin", () => {
    const projection = fixture();
    assert.isTrue(controlledCodexOriginBlocked(projection, workerId));
    assert.isFalse(controlledCodexOriginBlocked(projection, manualId));
  });
  it.each(["unknown", "stale", "missing-binding"])("fails closed for %s origin", (kind) => {
    const projection = fixture();
    if (kind === "missing-binding") projection.providerThreads = [];
    assert.isTrue(
      controlledCodexOriginBlocked(projection, kind === "stale" ? RunId.make("stale") : undefined),
    );
  });
  it("does not attribute a delayed wake to a newer manual run", () => {
    assert.isTrue(
      controlledCodexDispatchBlocked(fixture(), {
        createdBy: "agent",
        creationSource: "provider",
      }),
    );
    assert.isFalse(
      controlledCodexDispatchBlocked(fixture(), {
        createdBy: "user",
        creationSource: "server",
      }),
    );
  });
  it.each(["restart", "limit", "delegated", "scheduled", "notification"])(
    "blocks %s automatic dispatch",
    (kind) => {
      assert.isTrue(
        controlledCodexDispatchBlocked(fixture(), {
          createdBy: "user",
          creationSource: "server",
          ...(kind === "restart" ? { restartContinuationOfRunId: workerId } : {}),
          ...(kind === "limit" ? { usageLimitContinuationOfRunId: workerId } : {}),
          ...(kind === "delegated"
            ? { delegatedCompletion: { parentRunId: workerId, generation: 1, taskIds: [] } }
            : {}),
          ...(kind === "scheduled" ? { scheduledTaskId: "scheduled" as never } : {}),
          ...(kind === "notification"
            ? { notification: { type: "background_task", detail: null } as never }
            : {}),
        }),
      );
    },
  );
  it("follows restart chains and protects against cycles", () => {
    const projection = fixture();
    projection.runs[1] = { ...projection.runs[1]!, restartContinuationOfRunId: workerId };
    assert.isTrue(controlledCodexOriginBlocked(projection, manualId));
    projection.runs[1] = { ...projection.runs[1]!, restartContinuationOfRunId: manualId };
    assert.isTrue(controlledCodexOriginBlocked(projection, manualId));
  });
  it("does not choose a safe-looking source from conflicting trigger identities", () => {
    assert.isTrue(
      controlledCodexDispatchBlocked(fixture(), {
        createdBy: "user",
        creationSource: "server",
        restartContinuationOfRunId: manualId,
        usageLimitContinuationOfRunId: workerId,
      }),
    );
  });
  it("keeps legacy unowned automatic runs from laundering ownership", () => {
    const projection = fixture();
    projection.messages[0] = {
      ...projection.messages[0]!,
      createdBy: "agent",
      creationSource: "server",
    };
    assert.isTrue(controlledCodexOriginBlocked(projection, manualId));
    assert.isTrue(controlledCodexQueuedStartBlocked(projection, projection.runs[1]!));
  });
  it("fails closed for legacy server/user wakes until manual intake is durably classified", () => {
    const projection = fixture();
    projection.messages[0] = { ...projection.messages[0]!, creationSource: "server" };
    assert.isTrue(controlledCodexQueuedStartBlocked(projection, projection.runs[1]!));
    projection.runs[1] = { ...projection.runs[1]!, newRunTrigger: { type: "manual" } };
    assert.isFalse(controlledCodexQueuedStartBlocked(projection, projection.runs[1]!));
    assert.isFalse(controlledCodexOriginBlocked(projection, manualId));
  });
  it("leaves the owned initial run to its one-use commitment guard", () => {
    assert.isFalse(controlledCodexQueuedStartBlocked(fixture(), fixture().runs[0]!));
    assert.isFalse(controlledCodexQueuedStartBlocked(fixture(), fixture().runs[1]!));
  });
  it("leaves ordinary Codex and other-provider worker behavior unchanged", () => {
    const projection = fixture();
    projection.runs = [projection.runs[1]!];
    assert.isFalse(controlledCodexOriginBlocked(projection));
    const other = fixture();
    other.providerThreads[0] = { ...other.providerThreads[0]!, driver: "claude-code" as never };
    assert.isFalse(controlledCodexOriginBlocked(other));
  });
});

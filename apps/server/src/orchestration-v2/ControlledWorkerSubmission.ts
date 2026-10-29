import { CommandId, type OrchestrationV2Run } from "@t3tools/contracts";
import { modelSelectionsEqual } from "@t3tools/shared/model";
import * as Effect from "effect/Effect";
import * as Context from "effect/Context";
import * as ProviderAdapter from "@t3tools/provider-core/server/ProviderAdapter";
import { randomUuidV4 } from "@t3tools/provider-core/server/randomUuid";
import type { OrchestratorV2Shape } from "./Orchestrator.ts";
import type { ProjectionStoreV2Shape } from "./ProjectionStore.ts";
import type { ProviderSessionManagerV2Shape } from "./ProviderSessionManager.ts";

/** Tests can close the live boundary; this reference cannot grant eligibility. */
export class ControlledWorkerExecutionDisabled extends Context.Reference<boolean>(
  "t3/orchestration-v2/ControlledWorkerExecutionDisabled",
  { defaultValue: () => false },
) {}

/** Every evaluation needs a fresh durable grant. Never cache or replay permission. */
export function withControlledWorkerSubmission(
  session: ProviderAdapter.ProviderAdapterV2SessionRuntime,
  run: OrchestrationV2Run,
  store: Pick<ProjectionStoreV2Shape, "controlledWorkerStartAllowed">,
  commands: Pick<OrchestratorV2Shape, "dispatch">,
  sessions: Pick<ProviderSessionManagerV2Shape, "markControlledWorkerExposure">,
): ProviderAdapter.ProviderAdapterV2SessionRuntime {
  const owner = run.controlledWorkerOwnership;
  if (!owner) return session;
  const submit = (input: ProviderAdapter.ProviderAdapterV2TurnInput, compact: boolean) =>
    Effect.gen(function* () {
      if (
        session.driver !== "codex" ||
        session.instanceId !== owner.providerInstanceId ||
        session.providerSessionId !== owner.providerSessionId ||
        input.appThread.id !== owner.workerThreadId ||
        input.threadId !== owner.workerThreadId ||
        input.runId !== owner.workerRunId ||
        input.attemptId !== owner.workerAttemptId ||
        input.providerThread.id !== owner.providerThreadId ||
        input.providerThread.providerSessionId !== owner.providerSessionId ||
        input.providerThread.providerInstanceId !== owner.providerInstanceId ||
        !modelSelectionsEqual(input.modelSelection, run.modelSelection) ||
        !(yield* store.controlledWorkerStartAllowed(input.threadId, input.runId, input.attemptId))
      )
        return yield* Effect.fail("Controlled worker submission ownership is stale or mismatched.");
      // Mark the captured manager-owned runtime before granting permission. Even
      // a denied or lost commitment response cannot later imply clean exposure.
      yield* sessions.markControlledWorkerExposure(session, owner);
      const invocationClaimId = CommandId.make(`controlled-submission:${yield* randomUuidV4}`);
      const result = yield* commands.dispatch({
        type: "multi-agent.controller.command",
        commandId: invocationClaimId,
        threadId: owner.coordinatorThreadId,
        runId: owner.coordinatorRunId,
        action: {
          type: "commit_submission",
          claim: {
            coordinatorThreadId: owner.coordinatorThreadId,
            coordinatorRunId: owner.coordinatorRunId,
            taskId: owner.taskId,
            workerThreadId: owner.workerThreadId,
            workerRunId: owner.workerRunId,
            workerAttemptId: owner.workerAttemptId,
            providerInstanceId: owner.providerInstanceId,
            providerThreadId: owner.providerThreadId,
            providerSessionId: owner.providerSessionId,
            invocationClaimId,
          },
        },
      });
      if (result.submissionCommitmentGranted !== true)
        return yield* Effect.fail(
          "Controlled worker submission requires a fresh commitment grant.",
        );
      // The command lock has been released. This executor alone owns one call;
      // a crash here leaves uncertainty, never permission for another executor.
      yield* Effect.suspend(() =>
        compact ? session.compactThread!(input) : session.startTurn(input),
      );
    }).pipe(
      Effect.mapError(
        (cause) =>
          new ProviderAdapter.ProviderAdapterTurnStartError({
            driver: session.driver,
            threadId: input.threadId,
            runId: input.runId,
            providerThreadId: input.providerThread.id,
            cause,
          }),
      ),
    );
  return {
    ...session,
    startTurn: (input) => submit(input, false),
    ...(session.compactThread === undefined
      ? {}
      : {
          compactThread: (input: ProviderAdapter.ProviderAdapterV2TurnInput) => submit(input, true),
        }),
  };
}

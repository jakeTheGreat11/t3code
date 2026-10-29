import { CommandId, type NodeId, type RunId, type ThreadId } from "@t3tools/contracts";
import { resolvePersistedMultiAgentTask } from "@t3tools/shared/multiAgentRouting";
import { modelSelectionsEqual } from "@t3tools/shared/model";
import * as KeyedLock from "@t3tools/shared/KeyedLock";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as ProviderRegistry from "../provider/ProviderRegistry.ts";
import * as CommandReceiptStore from "./CommandReceiptStore.ts";
import * as ThreadLaunchService from "./ThreadLaunchService.ts";
import * as ThreadManagementService from "./ThreadManagementService.ts";
import { controlledWorkerFinishedCommand } from "./Orchestrator.ts";

export class MultiAgentWorkerLaunchError extends Schema.TaggedError<MultiAgentWorkerLaunchError>()(
  "MultiAgentWorkerLaunchError",
  {
    coordinatorRunId: Schema.String,
    taskId: Schema.String,
    reason: Schema.String,
    cause: Schema.optional(Schema.Defect()),
  },
) {}

const isMultiAgentWorkerLaunchError = Schema.is(MultiAgentWorkerLaunchError);

export class MultiAgentWorkerLaunchService extends Context.Service<
  MultiAgentWorkerLaunchService,
  {
    readonly execute: (input: {
      readonly threadId: ThreadId;
      readonly coordinatorRunId: RunId;
      readonly taskId: NodeId;
      readonly launchCommandId: CommandId;
    }) => Effect.Effect<void, MultiAgentWorkerLaunchError>;
  }
>()("t3/orchestration-v2/MultiAgentWorkerLaunchService") {}

const make = Effect.gen(function* () {
  const threads = yield* ThreadManagementService.ThreadManagementService;
  const launcher = yield* ThreadLaunchService.ThreadLaunchService;
  const providers = yield* ProviderRegistry.ProviderRegistry;
  const receipts = yield* CommandReceiptStore.CommandReceiptStoreV2;
  // Duplicate/expired leases in one process share the same launch boundary. Persisted
  // receipts provide the cross-restart identity; this lock never replaces them.
  const launches = yield* KeyedLock.make<CommandId>();
  const execute: MultiAgentWorkerLaunchService["Service"]["execute"] = (input) =>
    launches.withLock(
      input.launchCommandId,
      Effect.gen(function* () {
        const reject = (reason: string) =>
          new MultiAgentWorkerLaunchError({
            coordinatorRunId: input.coordinatorRunId,
            taskId: input.taskId,
            reason,
          });
        const owner = yield* threads.getThreadRecords(input.threadId, ["runs"], {
          runIds: [input.coordinatorRunId],
        });
        const run = owner.runs.find((candidate) => candidate.id === input.coordinatorRunId);
        const controller = run?.multiAgentController;
        const task = controller?.tasks.find((candidate) => candidate.taskId === input.taskId);
        if (
          !run ||
          controller?.coordinatorRunId !== run.id ||
          controller.coordinatorThreadId !== input.threadId ||
          !task ||
          task.launchCommandId !== input.launchCommandId ||
          task.workerThreadId === input.threadId
        )
          return yield* reject("identity_mismatch");
        if (
          task.status === "running" ||
          task.status === "completed" ||
          task.status === "failed" ||
          task.status === "cancelled"
        )
          return;
        if (task.status !== "launching" && task.status !== "cancellation_pending")
          return yield* reject("reservation_not_launching");

        const created = yield* receipts.getByCommandId(task.launchCommandId);
        const messageCommandId = CommandId.make(`${task.launchCommandId}:initial-message`);
        const message = yield* receipts.getByCommandId(messageCommandId);
        for (const [receipt, commandType] of [
          [created, "thread.create"],
          [message, "message.dispatch"],
        ] as const) {
          if (
            Option.isSome(receipt) &&
            (receipt.value.threadId !== task.workerThreadId ||
              receipt.value.commandType !== commandType ||
              receipt.value.status !== "accepted")
          )
            return yield* reject("launch_receipt_mismatch");
        }
        const finish = (workerRunId: RunId) =>
          Effect.gen(function* () {
            yield* threads.dispatch({
              type: "multi-agent.controller.command",
              commandId: CommandId.make(`${task.launchCommandId}:controller-started`),
              threadId: input.threadId,
              runId: run.id,
              action: {
                type: "started",
                taskId: task.taskId,
                launchCommandId: task.launchCommandId,
                workerThreadId: task.workerThreadId,
                workerRunId,
              },
            });
            const worker = yield* threads.getThreadRecords(
              task.workerThreadId,
              ["runs", "attempts"],
              { runIds: [workerRunId] },
            );
            const initial = worker.runs.find((candidate) => candidate.id === workerRunId);
            const owner = initial?.controlledWorkerOwnership;
            const attempt = worker.attempts.find(
              (candidate) => candidate.id === owner?.workerAttemptId,
            );
            if (
              initial &&
              owner &&
              owner.coordinatorThreadId === input.threadId &&
              owner.coordinatorRunId === input.coordinatorRunId &&
              owner.taskId === input.taskId &&
              owner.launchCommandId === input.launchCommandId &&
              owner.workerThreadId === task.workerThreadId &&
              owner.workerRunId === workerRunId &&
              initial.completedAt !== null &&
              ["completed", "failed", "interrupted", "cancelled"].includes(initial.status) &&
              attempt?.runId === workerRunId &&
              initial.activeAttemptId === owner.workerAttemptId &&
              attempt.completedAt !== null &&
              (initial.status !== "completed" || attempt.status === "completed") &&
              ["completed", "failed", "interrupted", "cancelled"].includes(attempt.status)
            )
              yield* threads.dispatch(controlledWorkerFinishedCommand(owner));
          });
        if (Option.isSome(message)) {
          if (Option.isNone(created)) return yield* reject("launch_receipt_mismatch");
          // Never use activeRunId/latest run: later turns may already exist here.
          const worker = yield* threads.getThreadRecords(task.workerThreadId, ["runs"]);
          const initial = worker.runs.filter(
            (candidate) => candidate.userMessageId === task.launchMessageId,
          );
          if (initial.length !== 1) return yield* reject("initial_run_missing_or_ambiguous");
          const owner = initial[0]!.controlledWorkerOwnership;
          if (
            !owner ||
            owner.coordinatorThreadId !== input.threadId ||
            owner.coordinatorRunId !== run.id ||
            owner.taskId !== task.taskId ||
            owner.workerThreadId !== task.workerThreadId ||
            owner.workerRunId !== initial[0]!.id ||
            owner.launchCommandId !== task.launchCommandId ||
            owner.launchMessageId !== task.launchMessageId
          )
            return yield* reject("initial_run_ownership_mismatch");
          yield* finish(initial[0]!.id);
          return;
        }

        const failBeforeCreation = (reason: string) =>
          Effect.gen(function* () {
            // An accepted create with no message is an incomplete launch, not a proof
            // that nothing happened. Keep its slot for repair instead of guessing.
            if (Option.isSome(created)) return yield* reject(reason);
            yield* threads.dispatch({
              type: "multi-agent.controller.command",
              commandId: CommandId.make(`${task.launchCommandId}:controller-launch-failed`),
              threadId: input.threadId,
              runId: run.id,
              action: {
                type: "launch_failed",
                taskId: task.taskId,
                launchCommandId: task.launchCommandId,
              },
            });
            yield* Effect.logWarning("Controlled worker launch rejected before creation", {
              coordinatorRunId: run.id,
              taskId: task.taskId,
              reason,
            });
          });
        if (
          controller.status !== "open" ||
          (controller.coordinatorStoppedAt !== undefined && !task.submissionCommitment) ||
          task.status === "cancellation_pending" ||
          owner.thread.archivedAt !== null ||
          owner.thread.deletedAt !== null
        ) {
          yield* failBeforeCreation("controller_closed");
          return;
        }
        if (run.multiAgent?.mode !== "custom" && run.multiAgent?.mode !== "auto") {
          yield* failBeforeCreation("multi_agent_policy_required");
          return;
        }
        const routing = resolvePersistedMultiAgentTask(run, task, {
          providers: yield* providers.getProviders,
        });
        if (
          routing.status !== "resolved" ||
          !modelSelectionsEqual(routing.modelSelection, task.modelSelection)
        ) {
          yield* failBeforeCreation(
            routing.status === "unavailable"
              ? routing.issue.reason
              : routing.status === "resolved"
                ? "assignment_changed"
                : routing.reason,
          );
          return;
        }
        const launchAttempt = yield* launcher
          .launch({
            controlledWorkerLaunchOwner: {
              coordinatorThreadId: input.threadId,
              coordinatorRunId: run.id,
              taskId: task.taskId,
              workerThreadId: task.workerThreadId,
              launchCommandId: task.launchCommandId,
              launchMessageId: task.launchMessageId,
            },
            commandId: task.launchCommandId,
            threadId: task.workerThreadId,
            coordinatorThreadId: input.threadId,
            projectId: owner.thread.projectId,
            title: task.title ?? task.key,
            modelSelection: routing.modelSelection,
            validateExplicitModelSelection: true,
            runtimeMode: owner.thread.runtimeMode,
            interactionMode: owner.thread.interactionMode,
            workspaceStrategy:
              owner.thread.worktreePath === null
                ? { type: "root", branch: owner.thread.branch ?? undefined }
                : {
                    type: "existing_worktree",
                    worktreePath: owner.thread.worktreePath,
                    branch: owner.thread.branch ?? undefined,
                  },
            initialMessage: {
              messageId: task.launchMessageId,
              text: task.prompt,
              attachments: [],
              multiAgent: { mode: "off" },
              senderThreadId: input.threadId,
            },
            createdBy: "system",
            creationSource: "server",
          })
          .pipe(
            Effect.map(Option.some),
            Effect.catchTags({
              ThreadLaunchError: (cause) =>
                cause.operation === "validate-model-selection"
                  ? failBeforeCreation(cause.selectionIssue?.reason ?? "selection_rejected").pipe(
                      Effect.as(Option.none()),
                    )
                  : Effect.fail(cause),
              ThreadManagementProviderInstanceDisabledError: () =>
                failBeforeCreation("provider_not_allowed").pipe(Effect.as(Option.none())),
            }),
          );
        if (Option.isNone(launchAttempt)) return;
        const launched = launchAttempt.value;
        const initial = launched.projection.runs.filter(
          (candidate) => candidate.userMessageId === task.launchMessageId,
        );
        if (launched.threadId !== task.workerThreadId || initial.length !== 1)
          return yield* reject("launch_result_mismatch");
        yield* finish(initial[0]!.id);
      }).pipe(
        Effect.mapError((cause) =>
          isMultiAgentWorkerLaunchError(cause)
            ? cause
            : new MultiAgentWorkerLaunchError({
                coordinatorRunId: input.coordinatorRunId,
                taskId: input.taskId,
                reason: "launch_or_persistence_failed",
                cause,
              }),
        ),
      ),
    );
  return MultiAgentWorkerLaunchService.of({ execute });
});

export const layer = Layer.effect(MultiAgentWorkerLaunchService, make);

import * as Schema from "effect/Schema";
import {
  CommandId,
  IsoDateTime,
  MessageId,
  NodeId,
  ProviderSessionId,
  ProviderThreadId,
  RunAttemptId,
  RunId,
  ThreadId,
  TrimmedNonEmptyString,
} from "./baseSchemas.ts";
import { ModelSelection } from "./modelSelection.ts";
import { ProviderOptionSelections } from "./model.ts";
import { ServerProviderModel } from "./server.ts";
import { ProviderInstanceId } from "./providerInstance.ts";

/** Model output may select a server-issued candidate, never a provider/run identity. */
export const MultiAgentAutoRoutingRequest = Schema.Struct({
  candidateId: TrimmedNonEmptyString.check(Schema.isMaxLength(128)),
  options: Schema.optional(ProviderOptionSelections),
  reason: Schema.optional(TrimmedNonEmptyString.check(Schema.isMaxLength(512))),
});
export type MultiAgentAutoRoutingRequest = typeof MultiAgentAutoRoutingRequest.Type;
export const MultiAgentAutoCatalog = Schema.Struct({
  providerInstanceId: ProviderInstanceId,
  fallback: ModelSelection,
  candidates: Schema.Array(
    Schema.Struct({ id: TrimmedNonEmptyString, model: ServerProviderModel }),
  ),
});
export type MultiAgentAutoCatalog = typeof MultiAgentAutoCatalog.Type;

export const MULTI_AGENT_CONTROLLER_LIMITS = {
  concurrentPerController: 2,
  concurrentPerEnvironment: 4,
  totalTasksPerController: 4,
  automaticReasoningRetries: 0,
} as const;

/** Existing Custom roles that can execute independent worker tasks; planner is reserved for coordinator planning. */
export const MultiAgentTaskRole = Schema.Literals(["worker", "rescuer", "reviewer"]);
export type MultiAgentTaskRole = typeof MultiAgentTaskRole.Type;
const TaskKey = TrimmedNonEmptyString.check(Schema.isMaxLength(128));

/** Model output contains intentions only. Decode untrusted input with onExcessProperty: "error".
 * All ownership and execution configuration comes from T3.
 */
export const MultiAgentTaskProposal = Schema.Struct({
  routing: Schema.optional(MultiAgentAutoRoutingRequest),
  key: TaskKey,
  role: MultiAgentTaskRole,
  prompt: TrimmedNonEmptyString.check(Schema.isMaxLength(16_384)),
  title: Schema.optional(TrimmedNonEmptyString.check(Schema.isMaxLength(256))),
});
export type MultiAgentTaskProposal = typeof MultiAgentTaskProposal.Type;
export const MultiAgentProposal = Schema.Struct({
  tasks: Schema.Array(MultiAgentTaskProposal).check(
    Schema.isMinLength(1),
    Schema.isMaxLength(MULTI_AGENT_CONTROLLER_LIMITS.totalTasksPerController),
  ),
});
export type MultiAgentProposal = typeof MultiAgentProposal.Type;

/** Allocated by T3 before admission and retained across infrastructure retries. */
export const MultiAgentTaskIdentity = Schema.Struct({
  key: TaskKey,
  taskId: NodeId,
  launchCommandId: CommandId,
  launchMessageId: MessageId,
  workerThreadId: ThreadId,
});
export type MultiAgentTaskIdentity = typeof MultiAgentTaskIdentity.Type;
export const MultiAgentResultReference = Schema.Struct({
  workerThreadId: ThreadId,
  workerRunId: RunId,
  messageId: Schema.optional(MessageId),
});
export type MultiAgentResultReference = typeof MultiAgentResultReference.Type;

/** T3-owned invocation identity. A commitment permits one attempt; it is not delivery evidence. */
export const MultiAgentSubmissionClaim = Schema.Struct({
  coordinatorThreadId: ThreadId,
  coordinatorRunId: RunId,
  taskId: NodeId,
  workerThreadId: ThreadId,
  workerRunId: RunId,
  workerAttemptId: RunAttemptId,
  providerInstanceId: ProviderInstanceId,
  providerThreadId: ProviderThreadId,
  providerSessionId: ProviderSessionId,
  invocationClaimId: CommandId,
});
export type MultiAgentSubmissionClaim = typeof MultiAgentSubmissionClaim.Type;
export const MultiAgentSubmissionCommitment = Schema.Struct({
  ...MultiAgentSubmissionClaim.fields,
  committedAt: IsoDateTime,
});
export type MultiAgentSubmissionCommitment = typeof MultiAgentSubmissionCommitment.Type;

/** Supplied by the T3 launch service only; run/attempt/provider identities are allocated during run creation. */
export const MultiAgentWorkerLaunchOwner = Schema.Struct({
  coordinatorThreadId: ThreadId,
  coordinatorRunId: RunId,
  taskId: NodeId,
  workerThreadId: ThreadId,
  launchCommandId: CommandId,
  launchMessageId: MessageId,
});
export type MultiAgentWorkerLaunchOwner = typeof MultiAgentWorkerLaunchOwner.Type;
/** Immutable initial binding, not evidence of a submitted or executing provider request. */
export const MultiAgentWorkerOwnership = Schema.Struct({
  ...MultiAgentWorkerLaunchOwner.fields,
  workerRunId: RunId,
  workerAttemptId: RunAttemptId,
  providerInstanceId: ProviderInstanceId,
  providerThreadId: ProviderThreadId,
  providerSessionId: ProviderSessionId,
});
export type MultiAgentWorkerOwnership = typeof MultiAgentWorkerOwnership.Type;

export const MultiAgentTaskStatus = Schema.Literals([
  "queued",
  "reserved",
  "launching",
  "running",
  "cancellation_pending",
  "completed",
  "failed",
  "cancelled",
]);
export type MultiAgentTaskStatus = typeof MultiAgentTaskStatus.Type;
const TaskFields = {
  routing: Schema.optional(MultiAgentAutoRoutingRequest),
  routingFallback: Schema.optional(Schema.Literal("coordinator")),
  ...MultiAgentTaskIdentity.fields,
  /** The persisted proposal message and execution attempt, supplied by T3 rather than the model. */
  proposalId: MessageId,
  proposalAttemptId: RunAttemptId,
  role: MultiAgentTaskRole,
  prompt: MultiAgentTaskProposal.fields.prompt,
  title: MultiAgentTaskProposal.fields.title,
  modelSelection: ModelSelection,
  submissionCommitment: Schema.optional(MultiAgentSubmissionCommitment),
};
export const MultiAgentControllerTask = Schema.Union([
  Schema.Struct({ ...TaskFields, status: Schema.Literals(["queued", "reserved"]) }),
  Schema.Struct({
    ...TaskFields,
    status: Schema.Literal("launching"),
    workerRunId: Schema.optional(RunId),
  }),
  Schema.Struct({ ...TaskFields, status: Schema.Literal("running"), workerRunId: RunId }),
  Schema.Struct({
    ...TaskFields,
    status: Schema.Literal("cancellation_pending"),
    workerRunId: Schema.optional(RunId),
  }),
  Schema.Struct({
    ...TaskFields,
    status: Schema.Literal("completed"),
    workerRunId: RunId,
    result: MultiAgentResultReference,
  }),
  Schema.Struct({
    ...TaskFields,
    status: Schema.Literal("failed"),
    workerRunId: Schema.optional(RunId),
    result: Schema.optional(MultiAgentResultReference),
  }),
  Schema.Struct({
    ...TaskFields,
    status: Schema.Literal("cancelled"),
    workerRunId: Schema.optional(RunId),
    result: Schema.optional(MultiAgentResultReference),
  }),
]).check(
  Schema.makeFilter(
    (task) => {
      if (
        task.submissionCommitment &&
        (task.submissionCommitment.taskId !== task.taskId ||
          task.submissionCommitment.workerThreadId !== task.workerThreadId ||
          task.submissionCommitment.providerInstanceId !== task.modelSelection.instanceId ||
          !("workerRunId" in task) ||
          task.workerRunId !== task.submissionCommitment.workerRunId)
      )
        return false;
      if (!("result" in task) || task.result === undefined) return true;
      return (
        task.workerRunId === task.result.workerRunId &&
        task.workerThreadId === task.result.workerThreadId
      );
    },
    { expected: "result references the task's worker thread and run" },
  ),
);
export type MultiAgentControllerTask = typeof MultiAgentControllerTask.Type;
export const MultiAgentControllerStatus = Schema.Literals([
  "open",
  "cancelling",
  "settled",
  "cancelled",
]);
export type MultiAgentControllerStatus = typeof MultiAgentControllerStatus.Type;
export const MultiAgentControllerState = Schema.Struct({
  /** Durable delivery ledger. Each batch authorizes at most one initial review submission. */
  reviews: Schema.optional(
    Schema.Array(
      Schema.Struct({
        runId: RunId,
        attemptId: RunAttemptId,
        taskIds: Schema.Array(NodeId),
        submissionClaimedAt: Schema.optional(IsoDateTime),
      }),
    ),
  ),
  /** Frozen choices shown to this Auto coordinator; legacy/Custom controllers omit this. */
  autoRoutingCatalog: Schema.optional(MultiAgentAutoCatalog),
  coordinatorThreadId: ThreadId,
  coordinatorRunId: RunId,
  /** Initial coordinator attempt allocated by T3; absent on legacy controllers. */
  coordinatorAttemptId: Schema.optional(RunAttemptId),
  /** Closes coordinator admissions only; does not imply worker cancellation or provider-start suppression. */
  coordinatorStoppedAt: Schema.optional(IsoDateTime),
  status: MultiAgentControllerStatus,
  tasks: Schema.Array(MultiAgentControllerTask).check(
    Schema.isMaxLength(MULTI_AGENT_CONTROLLER_LIMITS.totalTasksPerController),
  ),
}).check(
  Schema.makeFilter(
    (state) => {
      const claims = state.tasks.flatMap((task) =>
        task.submissionCommitment ? [task.submissionCommitment.invocationClaimId] : [],
      );
      if (new Set(claims).size !== claims.length) return false;
      if (
        state.tasks.some(
          (task) =>
            task.submissionCommitment &&
            (task.submissionCommitment.coordinatorThreadId !== state.coordinatorThreadId ||
              task.submissionCommitment.coordinatorRunId !== state.coordinatorRunId),
        )
      )
        return false;
      const terminal = (task: MultiAgentControllerTask) =>
        task.status === "completed" || task.status === "failed" || task.status === "cancelled";
      if (state.status === "settled" || state.status === "cancelled")
        return state.tasks.every(terminal);
      if (state.status === "cancelling")
        return (
          state.tasks.some((task) => task.status === "cancellation_pending") &&
          state.tasks.every((task) => terminal(task) || task.status === "cancellation_pending")
        );
      return !state.tasks.some((task) => task.status === "cancellation_pending");
    },
    { expected: "controller status agrees with its task lifecycle" },
  ),
);
export type MultiAgentControllerState = typeof MultiAgentControllerState.Type;

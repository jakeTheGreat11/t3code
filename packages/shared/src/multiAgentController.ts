import {
  MULTI_AGENT_CONTROLLER_LIMITS,
  MultiAgentProposal,
  type MultiAgentControllerState,
  type MultiAgentControllerTask,
  type MultiAgentResultReference,
  type MultiAgentTaskIdentity,
  type NodeId,
  type OrchestrationV2Run,
  type MessageId,
  type RunAttemptId,
  type RunId,
  type ThreadId,
  type MultiAgentSubmissionCommitment,
  type OrchestrationV2AppThread,
  type OrchestrationV2RunAttempt,
  type OrchestrationV2ProviderThread,
} from "@t3tools/contracts";
import { modelSelectionsEqual } from "./model.ts";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import {
  resolveMultiAgentRouting,
  resolveMultiAgentAutoTask,
  type MultiAgentRoutingContext,
  type MultiAgentRoutingResult,
} from "./multiAgentRouting.ts";

const decodeProposal = Schema.decodeUnknownOption(MultiAgentProposal);
/** A review is a delivery of new results, never a timer or a retry of an uncertain submission. */
type ReviewRun = Pick<OrchestrationV2Run, "id" | "ordinal" | "status" | "multiAgentController">;
export function multiAgentReviewDecision(run: ReviewRun, runs: readonly ReviewRun[]) {
  const state = run.multiAgentController;
  if (!state || state.status !== "open" || state.coordinatorStoppedAt) return "none";
  if (state.tasks.some((task) => !terminal(task))) return "none";
  const last = state.reviews?.at(-1);
  const execution = last ? runs.find((row) => row.id === last.runId) : run;
  if (!execution || !["completed", "failed", "cancelled", "interrupted"].includes(execution.status))
    return "none";
  // A later explicit user turn owns the conversation; never queue a stale automatic review behind it.
  if (runs.some((row) => row.ordinal > execution.ordinal)) return "settle";
  const delivered = new Set(state.reviews?.flatMap((review) => review.taskIds) ?? []);
  return state.tasks.some((task) => !delivered.has(task.taskId)) ? "review" : "settle";
}
const terminal = (task: MultiAgentControllerTask) =>
  task.status === "completed" || task.status === "failed" || task.status === "cancelled";

export function multiAgentTaskOccupiesCapacity(task: MultiAgentControllerTask): boolean {
  return (
    task.status === "reserved" ||
    task.status === "launching" ||
    task.status === "running" ||
    task.status === "cancellation_pending"
  );
}

export type MultiAgentAdmissionResult =
  | { status: "admitted" | "duplicate"; state: MultiAgentControllerState }
  | {
      status: "rejected";
      reason:
        | "off"
        | "auto_unsupported"
        | "malformed_proposal"
        | "duplicate_task_key"
        | "controller_origin_mismatch"
        | "controller_closed"
        | "proposal_conflict"
        | "task_limit"
        | "invalid_task_identities"
        | "role_ineligible";
      taskKey?: string;
      routing?: MultiAgentRoutingResult;
    };

/** Caller supplies a persisted run and its verified proposal source, not identities extracted from model output. */
export function admitMultiAgentProposal(input: {
  run: Pick<OrchestrationV2Run, "id" | "threadId" | "multiAgent">;
  source: { proposalId: MessageId; attemptId: RunAttemptId };
  state?: MultiAgentControllerState;
  proposal: unknown;
  identities: readonly MultiAgentTaskIdentity[];
  catalog: MultiAgentRoutingContext;
}): MultiAgentAdmissionResult {
  const { run, source, identities } = input;
  const state = input.state;
  if (state && (state.coordinatorRunId !== run.id || state.coordinatorThreadId !== run.threadId)) {
    return { status: "rejected", reason: "controller_origin_mismatch" };
  }
  if (!run.multiAgent || run.multiAgent.mode === "off")
    return { status: "rejected", reason: "off" };
  if (run.multiAgent.mode === "auto" && !state?.autoRoutingCatalog)
    return { status: "rejected", reason: "auto_unsupported" };
  const decoded = decodeProposal(input.proposal, { onExcessProperty: "error" });
  if (Option.isNone(decoded)) return { status: "rejected", reason: "malformed_proposal" };
  const proposals = decoded.value.tasks;
  if (new Set(proposals.map((task) => task.key)).size !== proposals.length) {
    return { status: "rejected", reason: "duplicate_task_key" };
  }
  const previous = state?.tasks.filter((task) => task.proposalId === source.proposalId) ?? [];
  if (previous.length > 0) {
    const identical =
      previous.length === proposals.length &&
      previous.every((task) => {
        const proposed = proposals.find((candidate) => candidate.key === task.key);
        const identity = identities.find((candidate) => candidate.key === task.key);
        return (
          task.proposalAttemptId === source.attemptId &&
          proposed?.role === task.role &&
          proposed.prompt === task.prompt &&
          proposed.title === task.title &&
          JSON.stringify(proposed.routing) === JSON.stringify(task.routing) &&
          identity?.taskId === task.taskId &&
          identity.launchCommandId === task.launchCommandId &&
          identity.launchMessageId === task.launchMessageId &&
          identity.workerThreadId === task.workerThreadId
        );
      });
    return identical && identities.length === previous.length
      ? { status: "duplicate", state: state! }
      : { status: "rejected", reason: "proposal_conflict" };
  }
  if (state && (state.status !== "open" || state.coordinatorStoppedAt !== undefined))
    return { status: "rejected", reason: "controller_closed" };
  if (
    (state?.tasks.length ?? 0) + proposals.length >
    MULTI_AGENT_CONTROLLER_LIMITS.totalTasksPerController
  ) {
    return { status: "rejected", reason: "task_limit" };
  }
  if (
    identities.length !== proposals.length ||
    new Set(identities.map((identity) => identity.key)).size !== identities.length ||
    proposals.some((task) => !identities.some((identity) => identity.key === task.key))
  ) {
    return { status: "rejected", reason: "invalid_task_identities" };
  }
  const all = [...(state?.tasks ?? []), ...identities];
  for (const field of ["taskId", "launchCommandId", "launchMessageId", "workerThreadId"] as const) {
    if (new Set(all.map((task) => task[field])).size !== all.length) {
      return { status: "rejected", reason: "invalid_task_identities" };
    }
  }
  if (identities.some((identity) => identity.workerThreadId === run.threadId)) {
    return { status: "rejected", reason: "invalid_task_identities" };
  }
  const tasks: MultiAgentControllerTask[] = [];
  for (const proposed of proposals) {
    if (run.multiAgent.mode === "custom" && proposed.routing !== undefined)
      return { status: "rejected", reason: "malformed_proposal" };
    const routing =
      run.multiAgent.mode === "auto"
        ? resolveMultiAgentAutoTask(
            state?.autoRoutingCatalog,
            proposed.routing,
            proposed.role,
            input.catalog,
          )
        : resolveMultiAgentRouting(run.multiAgent, proposed.role, input.catalog);
    if (routing.status !== "resolved") {
      return { status: "rejected", reason: "role_ineligible", taskKey: proposed.key, routing };
    }
    const identity = identities.find((candidate) => candidate.key === proposed.key)!;
    tasks.push({
      ...structuredClone(identity),
      ...proposed,
      proposalId: source.proposalId,
      proposalAttemptId: source.attemptId,
      status: "queued",
      modelSelection: routing.modelSelection,
      ...(routing.fallback ? { routingFallback: routing.fallback } : {}),
    });
  }
  return {
    status: "admitted",
    state: {
      ...state,
      coordinatorRunId: run.id,
      coordinatorThreadId: run.threadId,
      status: "open",
      tasks: [...(state?.tasks ?? []), ...tasks],
    },
  };
}

export type MultiAgentTaskAction =
  | { type: "reserve" }
  | { type: "launch" }
  | { type: "started"; workerRunId: RunId }
  /** Confirmed launch failure with no executing worker; an ambiguous transport failure keeps its slot. */
  | { type: "launch_failed" }
  | { type: "completed" | "worker_failed"; result: MultiAgentResultReference }
  /** Supplied only after the future service verifies termination of this exact worker execution. */
  | { type: "stop_confirmed"; workerThreadId: ThreadId; workerRunId?: RunId };

export type MultiAgentTransitionResult =
  | { status: "changed" | "unchanged"; state: MultiAgentControllerState }
  | {
      status: "rejected";
      reason:
        | "task_missing"
        | "invalid_transition"
        | "controller_closed"
        | "controller_capacity"
        | "environment_capacity"
        | "invalid_environment_occupancy"
        | "worker_identity_mismatch";
    };

function replaceTask(
  state: MultiAgentControllerState,
  task: MultiAgentControllerTask,
): MultiAgentControllerState {
  const tasks = state.tasks.map((existing) => (existing.taskId === task.taskId ? task : existing));
  return {
    ...state,
    tasks,
    status: state.status === "cancelling" && tasks.every(terminal) ? "cancelled" : state.status,
  };
}

export interface MultiAgentSubmissionWorkerContext {
  readonly thread: Pick<
    OrchestrationV2AppThread,
    "id" | "coordinatorThreadId" | "archivedAt" | "deletedAt"
  >;
  readonly run: Pick<
    OrchestrationV2Run,
    | "id"
    | "threadId"
    | "userMessageId"
    | "modelSelection"
    | "multiAgent"
    | "status"
    | "activeAttemptId"
    | "providerInstanceId"
    | "providerThreadId"
  >;
  readonly attempt: Pick<
    OrchestrationV2RunAttempt,
    "id" | "runId" | "status" | "providerInstanceId" | "providerThreadId"
  >;
  readonly providerThread: Pick<
    OrchestrationV2ProviderThread,
    "id" | "appThreadId" | "providerInstanceId" | "providerSessionId" | "status"
  >;
}

/** Pure decision only. The server must use durable context and commit before returning a fresh grant. */
export function grantMultiAgentSubmission(
  state: MultiAgentControllerState,
  commitment: MultiAgentSubmissionCommitment,
  worker: MultiAgentSubmissionWorkerContext,
):
  | { status: "granted"; state: MultiAgentControllerState }
  | {
      status: "rejected";
      reason:
        | "controller_closed"
        | "already_committed"
        | "identity_mismatch"
        | "ineligible_task"
        | "stale_worker";
    } {
  if (state.status !== "open" || state.coordinatorStoppedAt !== undefined)
    return { status: "rejected", reason: "controller_closed" };
  const task = state.tasks.find((candidate) => candidate.taskId === commitment.taskId);
  if (
    !task ||
    commitment.coordinatorRunId !== state.coordinatorRunId ||
    commitment.coordinatorThreadId !== state.coordinatorThreadId ||
    commitment.workerThreadId !== task.workerThreadId ||
    commitment.workerRunId === state.coordinatorRunId
  )
    return { status: "rejected", reason: "identity_mismatch" };
  if (
    task.submissionCommitment ||
    state.tasks.some(
      (candidate) =>
        candidate.submissionCommitment?.invocationClaimId === commitment.invocationClaimId,
    )
  )
    return { status: "rejected", reason: "already_committed" };
  if (task.status !== "launching" && task.status !== "running")
    return { status: "rejected", reason: "ineligible_task" };
  const { thread, run, attempt, providerThread } = worker;
  if (
    thread.id !== task.workerThreadId ||
    thread.coordinatorThreadId !== state.coordinatorThreadId ||
    run.threadId !== thread.id ||
    run.id !== commitment.workerRunId ||
    run.userMessageId !== task.launchMessageId ||
    ("workerRunId" in task && task.workerRunId !== undefined && task.workerRunId !== run.id) ||
    !modelSelectionsEqual(run.modelSelection, task.modelSelection) ||
    (run.multiAgent?.mode ?? "off") !== "off"
  )
    return { status: "rejected", reason: "identity_mismatch" };
  if (
    thread.archivedAt !== null ||
    thread.deletedAt !== null ||
    (run.status !== "starting" && run.status !== "running") ||
    run.activeAttemptId !== commitment.workerAttemptId ||
    attempt.id !== commitment.workerAttemptId ||
    attempt.runId !== run.id ||
    (attempt.status !== "pending" && attempt.status !== "running") ||
    run.providerInstanceId !== commitment.providerInstanceId ||
    attempt.providerInstanceId !== commitment.providerInstanceId ||
    run.providerThreadId !== commitment.providerThreadId ||
    attempt.providerThreadId !== commitment.providerThreadId ||
    providerThread.id !== commitment.providerThreadId ||
    providerThread.appThreadId !== thread.id ||
    providerThread.providerInstanceId !== commitment.providerInstanceId ||
    providerThread.providerSessionId !== commitment.providerSessionId ||
    providerThread.status === "closed" ||
    providerThread.status === "archived" ||
    providerThread.status === "error"
  )
    return { status: "rejected", reason: "stale_worker" };
  return {
    status: "granted",
    state: replaceTask(state, {
      ...task,
      workerRunId: run.id,
      submissionCommitment: structuredClone(commitment),
    }),
  };
}

/** Occupancy must include this controller and all other controllers in the destination environment.
 * The future service must serialize reservation decisions and commit them before launching anything.
 */
export function transitionMultiAgentTask(
  state: MultiAgentControllerState,
  taskId: NodeId,
  action: MultiAgentTaskAction,
  environmentOccupancy: number,
): MultiAgentTransitionResult {
  const task = state.tasks.find((candidate) => candidate.taskId === taskId);
  if (!task) return { status: "rejected", reason: "task_missing" };
  const changed = (next: MultiAgentControllerTask): MultiAgentTransitionResult => ({
    status: "changed",
    state: replaceTask(state, next),
  });
  const unchanged = (): MultiAgentTransitionResult => ({ status: "unchanged", state });
  const invalid = (): MultiAgentTransitionResult => ({
    status: "rejected",
    reason: "invalid_transition",
  });
  const mismatch = (): MultiAgentTransitionResult => ({
    status: "rejected",
    reason: "worker_identity_mismatch",
  });
  switch (action.type) {
    case "reserve": {
      if (state.coordinatorStoppedAt !== undefined)
        return { status: "rejected", reason: "controller_closed" };
      if (task.status === "reserved") return unchanged();
      if (state.status !== "open") return { status: "rejected", reason: "controller_closed" };
      if (task.status !== "queued") return invalid();
      const occupied = state.tasks.filter(multiAgentTaskOccupiesCapacity).length;
      if (!Number.isSafeInteger(environmentOccupancy) || environmentOccupancy < occupied) {
        return { status: "rejected", reason: "invalid_environment_occupancy" };
      }
      if (occupied >= MULTI_AGENT_CONTROLLER_LIMITS.concurrentPerController)
        return { status: "rejected", reason: "controller_capacity" };
      if (environmentOccupancy >= MULTI_AGENT_CONTROLLER_LIMITS.concurrentPerEnvironment)
        return { status: "rejected", reason: "environment_capacity" };
      return changed({ ...task, status: "reserved" });
    }
    case "launch":
      if (state.coordinatorStoppedAt !== undefined)
        return { status: "rejected", reason: "controller_closed" };
      if (task.status === "launching") return unchanged();
      if (state.status !== "open") return { status: "rejected", reason: "controller_closed" };
      return task.status === "reserved" ? changed({ ...task, status: "launching" }) : invalid();
    case "started":
      if (task.submissionCommitment && task.submissionCommitment.workerRunId !== action.workerRunId)
        return mismatch();
      if (action.workerRunId === state.coordinatorRunId) return mismatch();
      if (task.status === "running" || task.status === "cancellation_pending") {
        if (task.workerRunId !== undefined)
          return task.workerRunId === action.workerRunId ? unchanged() : mismatch();
        return changed({ ...task, workerRunId: action.workerRunId });
      }
      return task.status === "launching"
        ? changed({ ...task, status: "running", workerRunId: action.workerRunId })
        : invalid();
    case "launch_failed":
      if (task.submissionCommitment) return invalid();
      if (
        (task.status === "failed" || task.status === "cancelled") &&
        task.workerRunId === undefined
      )
        return unchanged();
      if (task.status === "cancellation_pending" && task.workerRunId === undefined)
        return changed({ ...task, status: "cancelled" });
      return task.status === "launching" || task.status === "reserved"
        ? changed({ ...task, status: "failed" })
        : invalid();
    case "completed":
    case "worker_failed": {
      if (
        !("workerRunId" in task) ||
        task.workerRunId === undefined ||
        task.workerRunId !== action.result.workerRunId ||
        task.workerThreadId !== action.result.workerThreadId
      )
        return mismatch();
      const status =
        task.status === "cancellation_pending"
          ? "cancelled"
          : action.type === "completed"
            ? "completed"
            : "failed";
      if (terminal(task)) {
        if (task.status === "cancelled" && task.result === undefined) {
          return changed({ ...task, result: structuredClone(action.result) });
        }
        if (
          (task.status === status || task.status === "cancelled") &&
          "result" in task &&
          task.result?.messageId === action.result.messageId &&
          task.result !== undefined
        )
          return unchanged();
        return invalid();
      }
      return task.status === "running" || task.status === "cancellation_pending"
        ? changed({
            ...task,
            status,
            workerRunId: task.workerRunId,
            result: structuredClone(action.result),
          })
        : invalid();
    }
    case "stop_confirmed":
      if (
        task.workerThreadId !== action.workerThreadId ||
        ("workerRunId" in task ? task.workerRunId : undefined) !== action.workerRunId
      )
        return mismatch();
      if (task.status === "cancelled") return unchanged();
      return task.status === "cancellation_pending"
        ? changed({ ...task, status: "cancelled" })
        : invalid();
  }
}

/** Durable admission closure only. Existing task states are not proof of provider execution. */
export function stopMultiAgentCoordinator(
  state: MultiAgentControllerState,
  stoppedAt: NonNullable<MultiAgentControllerState["coordinatorStoppedAt"]>,
): MultiAgentControllerState {
  return state.coordinatorStoppedAt === undefined
    ? { ...state, coordinatorStoppedAt: stoppedAt }
    : state;
}

export function cancelMultiAgentController(
  state: MultiAgentControllerState,
): MultiAgentControllerState {
  if (state.status === "cancelled" || state.status === "settled") return state;
  const tasks = state.tasks.map((task): MultiAgentControllerTask => {
    if (terminal(task) || task.status === "cancellation_pending") return task;
    return {
      ...task,
      status:
        task.status === "queued" || task.status === "reserved"
          ? "cancelled"
          : "cancellation_pending",
    };
  });
  return { ...state, tasks, status: tasks.every(terminal) ? "cancelled" : "cancelling" };
}

export function settleMultiAgentController(
  state: MultiAgentControllerState,
):
  | { status: "settled"; state: MultiAgentControllerState }
  | { status: "rejected"; reason: "controller_closed" | "tasks_not_terminal" } {
  if (state.status === "settled") return { status: "settled", state };
  if (state.status !== "open") return { status: "rejected", reason: "controller_closed" };
  if (!state.tasks.every(terminal)) return { status: "rejected", reason: "tasks_not_terminal" };
  return { status: "settled", state: { ...state, status: "settled" } };
}

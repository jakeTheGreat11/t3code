import type {
  MultiAgentWorkerLaunchOwner,
  OrchestrationV2Run,
  OrchestrationV2AppThread,
} from "@t3tools/contracts";
import { modelSelectionsEqual } from "@t3tools/shared/model";
import * as Context from "effect/Context";

/** Scoped around the internal launch service's initial dispatch, never accepted from message/MCP payloads. */
export const ControlledWorkerLaunchOwner = Context.Reference<
  MultiAgentWorkerLaunchOwner | undefined
>("t3/orchestration-v2/ControlledWorkerLaunchOwner", { defaultValue: () => undefined });

export function controlledWorkerOwnershipProblem(
  coordinator: OrchestrationV2Run,
  thread: OrchestrationV2AppThread,
  worker: OrchestrationV2Run,
  creating = false,
): string | undefined {
  const owner = worker.controlledWorkerOwnership;
  if (
    !owner ||
    owner.coordinatorRunId !== coordinator.id ||
    owner.coordinatorThreadId !== coordinator.threadId ||
    owner.workerThreadId !== thread.id ||
    owner.workerRunId !== worker.id ||
    worker.threadId !== thread.id ||
    thread.coordinatorThreadId !== coordinator.threadId ||
    worker.userMessageId !== owner.launchMessageId ||
    worker.ordinal !== 1 ||
    (worker.multiAgent?.mode ?? "off") !== "off"
  )
    return "worker_ownership_mismatch";
  const controller = coordinator.multiAgentController;
  const tasks = controller?.tasks.filter((task) => task.taskId === owner.taskId) ?? [];
  const task = tasks[0];
  if (
    controller?.coordinatorThreadId !== coordinator.threadId ||
    controller.coordinatorRunId !== coordinator.id ||
    tasks.length !== 1 ||
    !task ||
    task.workerThreadId !== thread.id ||
    task.launchMessageId !== owner.launchMessageId ||
    task.launchCommandId !== owner.launchCommandId ||
    !modelSelectionsEqual(task.modelSelection, worker.modelSelection) ||
    ("workerRunId" in task && task.workerRunId !== undefined && task.workerRunId !== worker.id)
  )
    return "controller_task_ownership_mismatch";
  if (creating && task.status !== "launching") return "worker_ownership_not_authorized";
  if (controller.tasks.filter((row) => row.workerThreadId === thread.id).length !== 1)
    return "controller_task_ownership_ambiguous";
  if (
    worker.activeAttemptId !== owner.workerAttemptId ||
    worker.providerInstanceId !== owner.providerInstanceId ||
    worker.providerThreadId !== owner.providerThreadId
  )
    return "worker_ownership_stale";
  return undefined;
}

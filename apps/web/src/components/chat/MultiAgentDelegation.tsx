import type { MessageId, OrchestrationV2Run, ThreadId } from "@t3tools/contracts";
import type { ChatMessage } from "../../types";

// Provider input remains a user-role message. Only the durable, server-owned
// review association changes its presentation; ordinary prompts stay untouched.
export function automaticCoordinatorReview(
  runs: readonly OrchestrationV2Run[],
  message: ChatMessage,
) {
  if (
    message.role !== "user" ||
    message.createdBy !== "agent" ||
    message.creationSource !== "server"
  )
    return undefined;
  const review = runs.find((run) => run.id === message.runId && run.userMessageId === message.id);
  if (!review?.multiAgentReview) return undefined;
  const coordinatorRunId = review.multiAgentReview.coordinatorRunId;
  const coordinator = runs.find((run) => run.id === coordinatorRunId);
  const controller = coordinator?.multiAgentController;
  if (
    !coordinator ||
    !controller ||
    coordinator.threadId !== review.threadId ||
    controller.coordinatorThreadId !== review.threadId ||
    controller.coordinatorRunId !== coordinator.id ||
    !controller.reviews?.some((entry) => entry.runId === review.id)
  )
    return undefined;
  return { review, coordinator };
}

export function AutomaticCoordinatorReview({
  review,
  coordinator,
  message,
}: NonNullable<ReturnType<typeof automaticCoordinatorReview>> & { message: ChatMessage }) {
  return (
    <details className="text-xs text-muted-foreground">
      <summary>Automatic coordinator review · Technical details</summary>
      <p>Controller run: {coordinator.id}</p>
      <p>Review run: {review.id}</p>
      <p>Message: {message.id}</p>
      <pre className="max-h-64 overflow-auto whitespace-pre-wrap">{message.text}</pre>
    </details>
  );
}

export function acceptedDelegation(runs: readonly OrchestrationV2Run[], messageId: MessageId) {
  return runs.find((run) =>
    run.multiAgentController?.tasks.some((task) => task.proposalId === messageId),
  );
}

export function coordinatorPhase(run: OrchestrationV2Run, runs: readonly OrchestrationV2Run[]) {
  const state = run.multiAgentController;
  if (state?.coordinatorStoppedAt) return "Stopped — committed workers may finish";
  const review = state?.reviews?.at(-1);
  const execution = review ? runs.find((row) => row.id === review.runId) : run;
  if (
    execution?.status === "failed" ||
    execution?.status === "interrupted" ||
    execution?.status === "cancelled"
  )
    return "Coordinator ended without a final review";
  if (state?.status === "settled") return "Finished";
  if (state?.tasks.some((task) => !["completed", "failed", "cancelled"].includes(task.status)))
    return "Waiting for workers";
  if (
    review &&
    execution &&
    ["preparing", "queued", "starting", "running", "waiting"].includes(execution.status)
  )
    return "Reviewing results";
  return state?.tasks.length ? "Workers finished — review pending" : "Working";
}

export function MultiAgentDelegation({
  run,
  runs,
  raw,
  onOpenThread,
}: {
  run: OrchestrationV2Run;
  runs: readonly OrchestrationV2Run[];
  raw: string;
  onOpenThread: (threadId: ThreadId) => void;
}) {
  const tasks = run.multiAgentController?.tasks ?? [];
  const complete = tasks.filter((task) =>
    ["completed", "failed", "cancelled"].includes(task.status),
  ).length;
  return (
    <section aria-label="Delegated work" className="rounded-lg border border-border p-3 text-sm">
      <p className="font-medium">
        Delegated work — {complete}/{tasks.length} settled
      </p>
      <ul className="mt-2 space-y-2">
        {tasks.map((task) => (
          <li key={task.taskId}>
            <button
              type="button"
              className="text-left underline underline-offset-2"
              onClick={() => onOpenThread(task.workerThreadId)}
            >
              {task.title ?? task.key}
            </button>
            <span>
              {" "}
              — {task.status === "running" && !task.submissionCommitment ? "Starting" : task.status}
            </span>
            <p className="text-xs text-muted-foreground">
              {task.modelSelection.model}
              {task.modelSelection.options
                ?.map((option) => ` · ${option.id}: ${option.value}`)
                .join("")}
            </p>
            {task.routing?.reason && (
              <p className="text-xs text-muted-foreground">{task.routing.reason}</p>
            )}
          </li>
        ))}
      </ul>
      <p className="mt-3" role="status">
        Coordinator: {coordinatorPhase(run, runs)}
      </p>
      <p className="text-xs text-muted-foreground">
        Worker completion does not confirm integration; see the coordinator’s final review.
      </p>
      <details className="mt-2 text-xs">
        <summary>Technical details</summary>
        <pre className="max-h-64 overflow-auto whitespace-pre-wrap">{raw}</pre>
      </details>
    </section>
  );
}

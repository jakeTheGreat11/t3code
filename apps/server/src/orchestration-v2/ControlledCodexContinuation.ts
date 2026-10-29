import type {
  OrchestrationV2Command,
  OrchestrationV2Run,
  OrchestrationV2ThreadProjection,
  RunId,
} from "@t3tools/contracts";

type OwnershipProjection = Pick<OrchestrationV2ThreadProjection, "runs" | "providerThreads"> &
  Partial<Pick<OrchestrationV2ThreadProjection, "messages">>;

function ownedCodexRun(projection: OwnershipProjection, run: OrchestrationV2Run): boolean {
  const owner = run.controlledWorkerOwnership;
  if (run.multiAgentReview) return true;
  if (!owner) return false;
  const binding = projection.providerThreads.find((row) => row.id === owner.providerThreadId);
  // Missing saved binding is not evidence that an owned run is safe to restart.
  return binding === undefined || binding.driver === "codex";
}

/** Unknown wakes remain ambiguous even after a later manual turn. Never use the
 * latest run or current provider routing to attribute them to that manual turn.
 */
export function controlledCodexOriginBlocked(
  projection: OwnershipProjection,
  sourceRunId?: RunId,
): boolean {
  const exposed = projection.runs.some((run) => ownedCodexRun(projection, run));
  if (!exposed) return false;
  const visited = new Set<RunId>();
  let source = sourceRunId;
  while (source !== undefined) {
    if (visited.has(source)) return true;
    visited.add(source);
    const run = projection.runs.find((row) => row.id === source);
    if (!run) return true;
    if (ownedCodexRun(projection, run)) return true;
    if (run.restartContinuationOfRunId !== undefined) {
      source = run.restartContinuationOfRunId;
      continue;
    }
    const trigger = run.newRunTrigger;
    if (trigger?.type === "automatic") {
      if (trigger.provenance.type !== "exact") return true;
      if (trigger.provenance.originatingRun.threadId !== run.threadId) return true;
      source = trigger.provenance.originatingRun.runId;
      continue;
    }
    if (trigger?.type === "manual" || trigger?.type === "ordinary") return false;
    const message = projection.messages?.find((row) => row.id === run.userMessageId);
    // Legacy automatic follow-ups may not yet carry newRunTrigger. Absence of
    // ownership on such a run must not launder its original worker provenance.
    if (message?.delegatedCompletion) {
      source = message.delegatedCompletion.parentRunId;
      continue;
    }
    return (
      !message ||
      message.notification !== undefined ||
      message.scheduledTaskId !== undefined ||
      message.creationSource === "server" ||
      (message.createdBy !== "user" && message.creationSource !== "mcp")
    );
  }
  return true;
}

/** Explicit user turns and MCP sends stay independent. Server-generated wakes,
 * scheduled deliveries and late automatic steers cannot become manual turns.
 */
export function controlledCodexDispatchBlocked(
  projection: OwnershipProjection,
  command: Pick<
    Extract<OrchestrationV2Command, { type: "message.dispatch" }>,
    | "createdBy"
    | "creationSource"
    | "notification"
    | "delegatedCompletion"
    | "restartContinuationOfRunId"
    | "usageLimitContinuationOfRunId"
    | "scheduledTaskId"
  >,
): boolean {
  const sources = [
    command.restartContinuationOfRunId,
    command.usageLimitContinuationOfRunId,
    command.delegatedCompletion?.parentRunId,
  ].filter((id) => id !== undefined);
  if (new Set(sources).size > 1 && controlledCodexOriginBlocked(projection)) return true;
  const source = sources[0];
  const automatic =
    source !== undefined ||
    command.notification !== undefined ||
    command.scheduledTaskId !== undefined ||
    (command.createdBy === "agent" &&
      (command.creationSource === "server" || command.creationSource === "provider"));
  return automatic && controlledCodexOriginBlocked(projection, source);
}

/** Re-check persisted queued/legacy work before opening any runtime. The owned
 * initial run is governed separately by its exact, one-use submission grant.
 */
export function controlledCodexQueuedStartBlocked(
  projection: OwnershipProjection & Pick<OrchestrationV2ThreadProjection, "messages">,
  run: OrchestrationV2Run,
): boolean {
  if (run.controlledWorkerOwnership || run.multiAgentReview) return false;
  if (run.newRunTrigger?.type === "automatic")
    return controlledCodexOriginBlocked(projection, run.id);
  if (run.newRunTrigger?.type === "manual" || run.newRunTrigger?.type === "ordinary") return false;
  const message = projection.messages.find((row) => row.id === run.userMessageId);
  if (!message) return controlledCodexOriginBlocked(projection);
  if (
    message.createdBy === "user" &&
    message.creationSource === "server" &&
    controlledCodexOriginBlocked(projection)
  )
    return true;
  return controlledCodexDispatchBlocked(projection, {
    ...message,
    restartContinuationOfRunId: run.restartContinuationOfRunId,
  });
}

import type { OrchestrationV2Run, ServerProvider } from "@t3tools/contracts";
import {
  resolveMultiAgentRouting,
  validateModelSelection,
} from "@t3tools/shared/multiAgentRouting";

/** First live MVP supports only catalog-validated Codex assignments. This is
 * eligibility, never a start permit; Stop and one-use commitments remain authoritative.
 */
export function multiAgentCodexEligibility(
  run: OrchestrationV2Run,
  providers: readonly ServerProvider[],
): { eligible: true } | { eligible: false; reason: string } {
  const controller = run.multiAgentController;
  if (run.multiAgent?.mode !== "custom" && run.multiAgent?.mode !== "auto")
    return { eligible: false, reason: "multi_agent_policy_required" };
  if (
    run.controlledWorkerOwnership ||
    !controller?.coordinatorAttemptId ||
    controller.coordinatorThreadId !== run.threadId ||
    controller.coordinatorRunId !== run.id
  )
    return { eligible: false, reason: "controller_identity_unverified" };
  const context = { providers };
  const coordinator = validateModelSelection(run.modelSelection, context);
  if ("issue" in coordinator) return { eligible: false, reason: coordinator.issue.reason };
  if (
    run.modelSelection.instanceId !== run.providerInstanceId ||
    providers.find((provider) => provider.instanceId === run.providerInstanceId)?.driver !== "codex"
  )
    return { eligible: false, reason: "codex_coordinator_required" };
  if (run.multiAgent.mode === "auto") {
    const catalog = controller.autoRoutingCatalog;
    return catalog?.providerInstanceId === run.providerInstanceId && catalog.candidates.length > 0
      ? { eligible: true }
      : { eligible: false, reason: "auto_catalog_missing" };
  }
  for (const role of ["planner", "worker", "rescuer", "reviewer"] as const) {
    if (role !== "worker" && !run.multiAgent.roles[role]) continue;
    const selection = resolveMultiAgentRouting(run.multiAgent, role, context);
    if (selection.status !== "resolved")
      return { eligible: false, reason: `${role}_${selection.status}` };
    if (
      providers.find((provider) => provider.instanceId === selection.modelSelection.instanceId)
        ?.driver !== "codex"
    )
      return { eligible: false, reason: `${role}_codex_required` };
  }
  return { eligible: true };
}

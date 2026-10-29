import type {
  AutomaticTriggerProviderBinding,
  AutomaticTriggerRunReference,
  NewRunTrigger,
  ThreadId,
} from "@t3tools/contracts";

/** Supplied by trusted server reads. Ordinary means positively classified,
 * not simply missing controlledWorkerOwnership. Derived automatic runs must
 * resolve their provenance chain before supplying this evidence.
 */
export interface AutomaticContinuationContext {
  readonly threadId: ThreadId;
  readonly providerBinding?: AutomaticTriggerProviderBinding;
  readonly bindingExposure: "unexposed" | "controlled_worker" | "unknown";
  readonly originEvidence?: {
    readonly reference: AutomaticTriggerRunReference;
    readonly classification: "ordinary" | "controlled_worker" | "unresolved";
    readonly providerBinding?: AutomaticTriggerProviderBinding;
  };
  /** Trusted intake classification for legacy records; absence never means manual. */
  readonly legacyIntent?: "manual" | "ordinary" | "automatic";
}
export type AutomaticContinuationDecision =
  | { decision: "allow"; reason: "manual" | "ordinary" | "ordinary_origin" | "unexposed_binding" }
  | {
      decision: "deny";
      reason:
        | "missing_trigger_classification"
        | "missing_provider_binding"
        | "stale_provider_binding"
        | "origin_thread_mismatch"
        | "origin_unverified"
        | "separate_authorization_required"
        | "unsafe_origin";
    };

function bindingsEqual(a: AutomaticTriggerProviderBinding, b: AutomaticTriggerProviderBinding) {
  return (
    a.providerInstanceId === b.providerInstanceId &&
    a.providerThreadId === b.providerThreadId &&
    a.providerSessionId === b.providerSessionId
  );
}
/** Admission of a NEW run only. No progress/terminal events, callbacks, or execution side effects. */
export function admitAutomaticContinuation(
  trigger: NewRunTrigger | undefined,
  context: AutomaticContinuationContext,
): AutomaticContinuationDecision {
  if (!trigger) {
    if (context.legacyIntent === "manual") return { decision: "allow", reason: "manual" };
    if (context.legacyIntent === "ordinary") return { decision: "allow", reason: "ordinary" };
    if (context.legacyIntent !== "automatic")
      return { decision: "deny", reason: "missing_trigger_classification" };
    // Legacy automatic requests remain automatic with unknown provenance.
    return context.bindingExposure === "unexposed"
      ? { decision: "allow", reason: "unexposed_binding" }
      : { decision: "deny", reason: "unsafe_origin" };
  }
  if (trigger.type !== "automatic") return { decision: "allow", reason: trigger.type };
  const binding = trigger.providerBinding;
  if (binding && (!context.providerBinding || !bindingsEqual(binding, context.providerBinding)))
    return { decision: "deny", reason: "stale_provider_binding" };
  if (trigger.kind === "provider_background" && !binding)
    return { decision: "deny", reason: "missing_provider_binding" };
  const origin = trigger.provenance;
  if (origin.type !== "exact")
    return context.bindingExposure === "unexposed"
      ? { decision: "allow", reason: "unexposed_binding" }
      : { decision: "deny", reason: "unsafe_origin" };
  if (origin.originatingRun.threadId !== context.threadId)
    return { decision: "deny", reason: "origin_thread_mismatch" };
  const evidence = context.originEvidence;
  if (
    !evidence ||
    evidence.reference.threadId !== origin.originatingRun.threadId ||
    evidence.reference.runId !== origin.originatingRun.runId ||
    evidence.classification === "unresolved"
  )
    return { decision: "deny", reason: "origin_unverified" };
  if (binding && (!evidence.providerBinding || !bindingsEqual(binding, evidence.providerBinding)))
    return { decision: "deny", reason: "stale_provider_binding" };
  // No authorization mechanism exists yet. Provenance alone can never grant it.
  return evidence.classification === "controlled_worker"
    ? { decision: "deny", reason: "separate_authorization_required" }
    : { decision: "allow", reason: "ordinary_origin" };
}

export function readNewRunTrigger(run: {
  readonly newRunTrigger?: NewRunTrigger;
}): NewRunTrigger | { readonly type: "legacy_unknown" } {
  return run.newRunTrigger ?? { type: "legacy_unknown" };
}

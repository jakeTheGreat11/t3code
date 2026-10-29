import type {
  ModelSelection,
  MultiAgentConfig,
  ProviderInstanceId,
  ServerProvider,
  MultiAgentAutoCatalog,
  MultiAgentAutoRoutingRequest,
  MultiAgentControllerTask,
  OrchestrationV2Run,
} from "@t3tools/contracts";
import {
  createModelSelection,
  buildProviderOptionSelectionsFromDescriptors,
  modelSelectionsEqual,
} from "./model.ts";
import { DEFAULT_MULTI_AGENT_CONFIG } from "@t3tools/contracts";

/** Capture preferences before asynchronous dispatch or persistence can outlive the composer draft. */
export function snapshotMultiAgentConfig(config: MultiAgentConfig | undefined): MultiAgentConfig {
  return structuredClone(config ?? DEFAULT_MULTI_AGENT_CONFIG);
}

export type MultiAgentRole = keyof Extract<MultiAgentConfig, { mode: "custom" }>["roles"];
export type MultiAgentPreset = Extract<MultiAgentConfig, { mode: "auto" }>["preset"];

/** Explicit policy supplied by the caller; catalogs do not advertise price or quality ranks. */
export type MultiAgentAutoPolicy = Partial<
  Record<MultiAgentPreset, Partial<Record<MultiAgentRole, ModelSelection>>>
>;

export interface MultiAgentRoutingContext {
  /** The destination environment's catalog, never a merged catalog from multiple environments. */
  readonly providers: readonly ServerProvider[];
  /** Optional project restrictions, applied to both Custom and Auto. */
  readonly allowedInstanceIds?: readonly ProviderInstanceId[];
  readonly autoPolicy?: MultiAgentAutoPolicy;
}

export type MultiAgentRoutingIssue =
  | { reason: "provider_unsupported" }
  | {
      reason:
        | "auto_catalog_missing"
        | "auto_candidate_missing"
        | "auto_selection_required"
        | "auto_option_required"
        | "selection_conflict";
    }
  | { reason: "provider_missing" | "provider_ambiguous" | "provider_not_allowed" }
  | {
      reason:
        | "provider_disabled"
        | "provider_not_installed"
        | "provider_unavailable"
        | "provider_error"
        | "provider_unauthenticated";
    }
  | { reason: "model_missing" | "model_ambiguous" | "option_metadata_missing" }
  | { reason: "duplicate_option" | "unknown_option" | "invalid_option_value"; optionId: string };

export type MultiAgentRoutingResult =
  | { status: "disabled"; reason: "mode_off" }
  | { status: "unassigned"; reason: "role_unassigned"; role: MultiAgentRole }
  | { status: "unavailable"; role: MultiAgentRole; issue: MultiAgentRoutingIssue }
  | {
      status: "unresolved";
      role: MultiAgentRole;
      preset: MultiAgentPreset;
      reason: "policy_unassigned" | "policy_selection_unavailable";
      issue?: MultiAgentRoutingIssue;
    }
  | {
      status: "resolved";
      role: MultiAgentRole;
      source: "auto" | "custom";
      modelSelection: ModelSelection;
      fallback?: "coordinator";
    };

/** Only this coordinator's Codex account is eligible. Snapshot names/options, not invented rankings. */
export function createMultiAgentAutoCatalog(
  selection: ModelSelection,
  providers: readonly ServerProvider[],
): MultiAgentAutoCatalog | undefined {
  const validated = validateModelSelection(selection, { providers });
  const provider = providers.find((p) => p.instanceId === selection.instanceId);
  if ("issue" in validated || provider?.driver !== "codex") return;
  const candidates = provider.models
    .filter(
      (model) =>
        "modelSelection" in
        validateModelSelection(
          { instanceId: provider.instanceId, model: model.slug },
          { providers },
        ),
    )
    .sort((a, b) => a.slug.localeCompare(b.slug, "en"))
    .map((model, index) => ({
      id: `candidate-${index + 1}`,
      model: structuredClone({
        ...model,
        capabilities: model.capabilities && {
          ...model.capabilities,
          optionDescriptors: model.capabilities.optionDescriptors?.map((descriptor) => {
            if (descriptor.type !== "select") return descriptor;
            const options = descriptor.options.filter(
              (option) => !descriptor.promptInjectedValues?.includes(option.id),
            );
            const { currentValue, ...rest } = descriptor;
            return {
              ...rest,
              options,
              ...(options.some((option) => option.id === currentValue) ? { currentValue } : {}),
            };
          }),
        },
      }),
    }));
  if (candidates.length === 0) return;
  return {
    providerInstanceId: provider.instanceId,
    fallback: validated.modelSelection,
    candidates,
  };
}

function requestedAutoSelection(
  catalog: MultiAgentAutoCatalog,
  request: MultiAgentAutoRoutingRequest | undefined,
) {
  const candidate = catalog.candidates.find((c) => c.id === request?.candidateId);
  if (!candidate) return;
  const defaults =
    buildProviderOptionSelectionsFromDescriptors(candidate.model.capabilities?.optionDescriptors) ??
    [];
  const explicit = request?.options ?? [];
  return createModelSelection(catalog.providerInstanceId, candidate.model.slug, [
    ...defaults.filter((option) => !explicit.some((o) => o.id === option.id)),
    ...explicit,
  ]);
}

/** Admission chooses once. Unknown candidate/option IDs fail; a removed, previously advertised
 * model falls back only to the coordinator's frozen, still-valid selection. No fallback at launch. */
export function resolveMultiAgentAutoTask(
  catalog: MultiAgentAutoCatalog | undefined,
  request: MultiAgentAutoRoutingRequest | undefined,
  role: MultiAgentRole,
  context: MultiAgentRoutingContext,
): MultiAgentRoutingResult {
  const unavailable = (issue: MultiAgentRoutingIssue): MultiAgentRoutingResult => ({
    status: "unavailable",
    role,
    issue,
  });
  if (!catalog) return unavailable({ reason: "auto_catalog_missing" });
  if (!request) return unavailable({ reason: "auto_selection_required" });
  const candidate = catalog.candidates.find((c) => c.id === request.candidateId);
  const selection = requestedAutoSelection(catalog, request);
  if (!candidate || !selection) return unavailable({ reason: "auto_candidate_missing" });
  if (
    candidate.model.capabilities?.optionDescriptors?.some(
      (descriptor) =>
        descriptor.type === "select" &&
        descriptor.options.length > 0 &&
        !request.options?.some((option) => option.id === descriptor.id),
    )
  )
    return unavailable({ reason: "auto_option_required" });
  const providers = context.providers.filter((p) => p.instanceId === catalog.providerInstanceId);
  if (providers.some((provider) => provider.driver !== "codex"))
    return unavailable({ reason: "provider_unsupported" });
  // This metadata-only check validates the originally offered options. Actual availability
  // is checked against the untouched destination catalog immediately below.
  const offered = validateModelSelection(selection, {
    ...context,
    providers: providers.map((p) => ({ ...p, models: [candidate.model] })),
  });
  if ("issue" in offered) return unavailable(offered.issue);
  const validated = validateModelSelection(selection, context);
  if ("modelSelection" in validated)
    return { status: "resolved", source: "auto", role, ...validated };
  if (validated.issue.reason !== "model_missing") return unavailable(validated.issue);
  const fallback = validateModelSelection(catalog.fallback, context);
  return "issue" in fallback
    ? unavailable(fallback.issue)
    : { status: "resolved", source: "auto", role, ...fallback, fallback: "coordinator" };
}

/** Revalidate the persisted decision, never reroute a reserved or launched task. */
export function resolvePersistedMultiAgentTask(
  run: Pick<OrchestrationV2Run, "multiAgent" | "multiAgentController" | "providerInstanceId">,
  task: Pick<MultiAgentControllerTask, "role" | "modelSelection" | "routing" | "routingFallback">,
  context: MultiAgentRoutingContext,
): MultiAgentRoutingResult {
  if (run.multiAgent?.mode !== "auto")
    return resolveMultiAgentRouting(run.multiAgent, task.role, context);
  const catalog = run.multiAgentController?.autoRoutingCatalog;
  const expected =
    catalog &&
    (task.routingFallback === "coordinator"
      ? catalog.fallback
      : requestedAutoSelection(catalog, task.routing));
  if (
    !catalog ||
    catalog.providerInstanceId !== run.providerInstanceId ||
    !expected ||
    !modelSelectionsEqual(expected, task.modelSelection)
  )
    return { status: "unavailable", role: task.role, issue: { reason: "selection_conflict" } };
  if (
    context.providers.some(
      (provider) => provider.instanceId === run.providerInstanceId && provider.driver !== "codex",
    )
  )
    return { status: "unavailable", role: task.role, issue: { reason: "provider_unsupported" } };
  const validated = validateModelSelection(task.modelSelection, {
    ...context,
    allowedInstanceIds: context.allowedInstanceIds?.filter(
      (id) => id === run.providerInstanceId,
    ) ?? [run.providerInstanceId],
  });
  return "issue" in validated
    ? { status: "unavailable", role: task.role, issue: validated.issue }
    : { status: "resolved", source: "auto", role: task.role, ...validated };
}

/** Validate an explicit selection against the destination catalog without applying routing preferences. */
export function validateModelSelection(
  selection: ModelSelection,
  context: MultiAgentRoutingContext,
): { modelSelection: ModelSelection } | { issue: MultiAgentRoutingIssue } {
  if (context.allowedInstanceIds && !context.allowedInstanceIds.includes(selection.instanceId)) {
    return { issue: { reason: "provider_not_allowed" } };
  }
  const providers = context.providers.filter(
    (provider) => provider.instanceId === selection.instanceId,
  );
  const provider = providers[0];
  if (!provider) return { issue: { reason: "provider_missing" } };
  if (providers.length > 1) return { issue: { reason: "provider_ambiguous" } };
  if (!provider.enabled || provider.status === "disabled") {
    return { issue: { reason: "provider_disabled" } };
  }
  if (!provider.installed) return { issue: { reason: "provider_not_installed" } };
  if (provider.availability === "unavailable") return { issue: { reason: "provider_unavailable" } };
  if (provider.status === "error") return { issue: { reason: "provider_error" } };
  if (provider.auth.status === "unauthenticated") {
    return { issue: { reason: "provider_unauthenticated" } };
  }

  // Exact advertised slugs take precedence over aliases; ambiguous aliases never select arbitrarily.
  const exact = provider.models.filter((model) => model.slug === selection.model);
  const models = exact.length
    ? exact
    : provider.models.filter((model) => model.aliases?.includes(selection.model));
  const model = models[0];
  if (!model) return { issue: { reason: "model_missing" } };
  if (models.length > 1) return { issue: { reason: "model_ambiguous" } };

  const descriptors = model.capabilities?.optionDescriptors;
  const seen = new Set<string>();
  for (const option of selection.options ?? []) {
    if (seen.has(option.id)) return { issue: { reason: "duplicate_option", optionId: option.id } };
    seen.add(option.id);
    if (!descriptors) return { issue: { reason: "option_metadata_missing" } };
    const descriptor = descriptors.find((candidate) => candidate.id === option.id);
    if (!descriptor) return { issue: { reason: "unknown_option", optionId: option.id } };
    const valid =
      descriptor.type === "boolean"
        ? typeof option.value === "boolean"
        : descriptor.options.some((choice) => choice.id === option.value);
    if (!valid) return { issue: { reason: "invalid_option_value", optionId: option.id } };
  }
  return {
    modelSelection: createModelSelection(selection.instanceId, model.slug, selection.options),
  };
}

/** Pure preference resolution. This does not authorize or launch work, or infer policy from model names. */
export function resolveMultiAgentRouting(
  preferences: MultiAgentConfig | null | undefined,
  role: MultiAgentRole,
  context: MultiAgentRoutingContext,
): MultiAgentRoutingResult {
  if (!preferences || preferences.mode === "off") return { status: "disabled", reason: "mode_off" };
  const selection =
    preferences.mode === "custom"
      ? preferences.roles[role]
      : context.autoPolicy?.[preferences.preset]?.[role];
  if (!selection) {
    return preferences.mode === "custom"
      ? { status: "unassigned", reason: "role_unassigned", role }
      : { status: "unresolved", reason: "policy_unassigned", role, preset: preferences.preset };
  }
  const validated = validateModelSelection(selection, context);
  if ("issue" in validated) {
    return preferences.mode === "custom"
      ? { status: "unavailable", role, issue: validated.issue }
      : {
          status: "unresolved",
          reason: "policy_selection_unavailable",
          role,
          preset: preferences.preset,
          issue: validated.issue,
        };
  }
  return { status: "resolved", role, source: preferences.mode, ...validated };
}

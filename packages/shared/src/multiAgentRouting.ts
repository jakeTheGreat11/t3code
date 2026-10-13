import type {
  ModelSelection,
  MultiAgentConfig,
  ProviderInstanceId,
  ServerProvider,
} from "@t3tools/contracts";
import { createModelSelection } from "./model.ts";
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
    };

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

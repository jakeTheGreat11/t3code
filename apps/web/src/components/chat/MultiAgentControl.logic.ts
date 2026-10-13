import type {
  ModelSelection,
  MultiAgentConfig,
  ProviderInstanceId,
  ProviderOptionDescriptor,
} from "@t3tools/contracts";
import { createModelSelection, getProviderOptionDescriptors } from "@t3tools/shared/model";
import { isProviderInstancePickerReady, type ProviderInstanceEntry } from "../../providerInstances";

export type MultiAgentRole = keyof Extract<MultiAgentConfig, { mode: "custom" }>["roles"];
export const MULTI_AGENT_ROLES = ["planner", "worker", "rescuer", "reviewer"] as const;
export const MULTI_AGENT_EXECUTION_NOTICE =
  "Preferences only. Multi-Agent execution is not enabled; sending a message will not launch workers from these settings.";

export function multiAgentControlLabel(config: MultiAgentConfig) {
  if (config.mode === "off") return "Multi-Agent: Off";
  if (config.mode === "custom") return "Multi-Agent: Custom (saved)";
  const preset = config.preset[0]!.toUpperCase() + config.preset.slice(1);
  return `Multi-Agent: ${preset} (saved)`;
}

export function changeMultiAgentMode(
  config: MultiAgentConfig,
  mode: MultiAgentConfig["mode"],
): MultiAgentConfig {
  if (config.mode === mode) return config;
  if (mode === "auto") return { mode, preset: "balanced" };
  if (mode === "custom") return { mode, roles: {} };
  return { mode: "off" };
}

export function setMultiAgentRoleModel(
  config: MultiAgentConfig,
  role: MultiAgentRole,
  instanceId: ProviderInstanceId,
  model: string,
): MultiAgentConfig {
  if (config.mode !== "custom") return config;
  const previous = config.roles[role];
  if (previous?.instanceId === instanceId && previous.model === model) return config;
  return { ...config, roles: { ...config.roles, [role]: createModelSelection(instanceId, model) } };
}

export function clearMultiAgentRole(
  config: MultiAgentConfig,
  role: MultiAgentRole,
): MultiAgentConfig {
  if (config.mode !== "custom" || config.roles[role] === undefined) return config;
  const roles = { ...config.roles };
  delete roles[role];
  return { ...config, roles };
}

/** Read exact catalog metadata; unavailable assignments remain saved without invented options. */
export function multiAgentRoleDescriptors(
  selection: ModelSelection,
  entry: ProviderInstanceEntry | undefined,
): ReadonlyArray<ProviderOptionDescriptor> {
  if (!entry || entry.instanceId !== selection.instanceId || !isProviderInstancePickerReady(entry))
    return [];
  const model = entry.models.find(
    (candidate) =>
      candidate.slug === selection.model || candidate.aliases?.includes(selection.model),
  );
  if (!model) return [];
  return getProviderOptionDescriptors({
    caps: model.capabilities ?? {},
    selections: selection.options,
  })
    .map((descriptor) =>
      descriptor.type === "select"
        ? {
            ...descriptor,
            options: descriptor.options.filter(
              (option) => !descriptor.promptInjectedValues?.includes(option.id),
            ),
          }
        : descriptor,
    )
    .filter((descriptor) => descriptor.type !== "select" || descriptor.options.length > 0);
}

export function setMultiAgentRoleOption(
  config: MultiAgentConfig,
  role: MultiAgentRole,
  descriptor: ProviderOptionDescriptor,
  value: string | boolean,
): MultiAgentConfig {
  if (config.mode !== "custom") return config;
  const selection = config.roles[role];
  if (
    !selection ||
    (descriptor.type === "boolean"
      ? typeof value !== "boolean"
      : !descriptor.options.some((option) => option.id === value) ||
        descriptor.promptInjectedValues?.includes(String(value)))
  )
    return config;
  const options = [
    ...(selection.options ?? []).filter((option) => option.id !== descriptor.id),
    { id: descriptor.id, value },
  ];
  return {
    ...config,
    roles: {
      ...config.roles,
      [role]: createModelSelection(selection.instanceId, selection.model, options),
    },
  };
}

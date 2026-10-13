import {
  ProviderDriverKind,
  ProviderInstanceId,
  type ModelSelection,
  type ServerProvider,
} from "@t3tools/contracts";
import { describe, expect, it } from "vite-plus/test";
import {
  resolveMultiAgentRouting,
  type MultiAgentPreset,
  type MultiAgentRole,
  type MultiAgentRoutingContext,
} from "./multiAgentRouting.ts";

const instanceId = ProviderInstanceId.make("configured-a");
const otherId = ProviderInstanceId.make("configured-b");
const selection: ModelSelection = { instanceId, model: "advertised-model" };
const provider: ServerProvider = {
  instanceId,
  driver: ProviderDriverKind.make("test-driver"),
  enabled: true,
  installed: true,
  version: null,
  status: "ready",
  auth: { status: "authenticated" },
  checkedAt: "2026-10-01T00:00:00Z",
  slashCommands: [],
  skills: [],
  models: [
    {
      slug: selection.model,
      name: "Advertised model",
      isCustom: true,
      aliases: ["advertised-alias"],
      capabilities: {
        optionDescriptors: [
          {
            id: "effort",
            label: "Effort",
            type: "select",
            options: [{ id: "supported", label: "Supported" }],
            promptInjectedValues: ["supported"],
          },
          { id: "feature", label: "Feature", type: "boolean" },
        ],
      },
    },
  ],
};
const context: MultiAgentRoutingContext = { providers: [provider] };
function custom(
  value: ModelSelection = selection,
  catalog = context,
  role: MultiAgentRole = "worker",
) {
  return resolveMultiAgentRouting({ mode: "custom", roles: { [role]: value } }, role, catalog);
}

describe("resolveMultiAgentRouting", () => {
  it.each([undefined, null, { mode: "off" } as const])(
    "disables absent or Off preferences: %j",
    (config) => {
      expect(resolveMultiAgentRouting(config, "worker", context)).toEqual({
        status: "disabled",
        reason: "mode_off",
      });
    },
  );

  it.each<MultiAgentRole>(["planner", "worker", "rescuer", "reviewer"])(
    "resolves only the assigned %s role",
    (role) => {
      expect(custom(selection, context, role)).toEqual({
        status: "resolved",
        source: "custom",
        role,
        modelSelection: selection,
      });
      expect(resolveMultiAgentRouting({ mode: "custom", roles: {} }, role, context)).toEqual({
        status: "unassigned",
        reason: "role_unassigned",
        role,
      });
    },
  );

  it.each<MultiAgentPreset>(["budget", "balanced", "maximum"])(
    "requires explicit policy for %s",
    (preset) => {
      const config = { mode: "auto", preset } as const;
      expect(resolveMultiAgentRouting(config, "worker", context)).toEqual({
        status: "unresolved",
        reason: "policy_unassigned",
        role: "worker",
        preset,
      });
      const catalog = { ...context, autoPolicy: { [preset]: { worker: selection } } };
      expect(resolveMultiAgentRouting(config, "worker", catalog)).toEqual({
        status: "resolved",
        source: "auto",
        role: "worker",
        modelSelection: selection,
      });
      expect(resolveMultiAgentRouting(config, "planner", catalog)).toMatchObject({
        status: "unresolved",
        reason: "policy_unassigned",
      });
      expect(resolveMultiAgentRouting(config, "worker", { ...catalog, providers: [] })).toEqual({
        status: "unresolved",
        reason: "policy_selection_unavailable",
        preset,
        role: "worker",
        issue: { reason: "provider_missing" },
      });
    },
  );

  it("does not substitute another preset's policy or a catalog default", () => {
    expect(
      resolveMultiAgentRouting({ mode: "auto", preset: "budget" }, "worker", {
        providers: [
          { ...provider, models: provider.models.map((model) => ({ ...model, isDefault: true })) },
        ],
        autoPolicy: { maximum: { worker: selection } },
      }),
    ).toMatchObject({ status: "unresolved", reason: "policy_unassigned" });
  });

  it.each<MultiAgentPreset>(["budget", "balanced", "maximum"])(
    "validates and preserves the explicit %s policy's options",
    (preset) => {
      const configured = { ...selection, options: [{ id: "effort", value: "supported" }] };
      const config = { mode: "auto", preset } as const;
      expect(
        resolveMultiAgentRouting(config, "worker", {
          ...context,
          autoPolicy: { [preset]: { worker: configured } },
        }),
      ).toMatchObject({ status: "resolved", modelSelection: configured });
      expect(
        resolveMultiAgentRouting(config, "worker", {
          ...context,
          autoPolicy: {
            [preset]: {
              worker: { ...selection, options: [{ id: "effort", value: "unsupported" }] },
            },
          },
        }),
      ).toMatchObject({
        status: "unresolved",
        reason: "policy_selection_unavailable",
        issue: { reason: "invalid_option_value", optionId: "effort" },
      });
    },
  );

  it("preserves all advertised options without adding defaults or mutating input", () => {
    const options = Object.freeze([
      Object.freeze({ id: "effort", value: "supported" }),
      Object.freeze({ id: "feature", value: false }),
    ]);
    const configured = Object.freeze({ ...selection, options });
    const result = custom(configured);
    expect(result).toEqual({
      status: "resolved",
      source: "custom",
      role: "worker",
      modelSelection: configured,
    });
    if (result.status === "resolved") expect(result.modelSelection.options).not.toBe(options);
  });

  it("canonicalizes only advertised aliases and rejects ambiguous aliases", () => {
    expect(custom({ ...selection, model: "advertised-alias" })).toMatchObject({
      status: "resolved",
      modelSelection: selection,
    });
    const models = [...provider.models, { ...provider.models[0]!, slug: "second-model" }];
    expect(
      custom({ ...selection, model: "advertised-alias" }, { providers: [{ ...provider, models }] }),
    ).toMatchObject({ issue: { reason: "model_ambiguous" } });
    expect(custom(selection, { providers: [{ ...provider, models }] })).toMatchObject({
      status: "resolved",
    });
  });

  it.each([
    { models: [] },
    { models: [{ ...provider.models[0]!, slug: "another-model", aliases: [] }] },
  ])("rejects missing models without fallback: %j", ({ models }) => {
    expect(custom(selection, { providers: [{ ...provider, models }] })).toMatchObject({
      status: "unavailable",
      issue: { reason: "model_missing" },
    });
  });

  it("keeps provider instances and destination catalogs isolated", () => {
    expect(custom(selection, { providers: [{ ...provider, instanceId: otherId }] })).toMatchObject({
      issue: { reason: "provider_missing" },
    });
    expect(custom(selection, { ...context, allowedInstanceIds: [otherId] })).toMatchObject({
      issue: { reason: "provider_not_allowed" },
    });
    expect(custom(selection, { providers: [provider, provider] })).toMatchObject({
      issue: { reason: "provider_ambiguous" },
    });
    expect(
      custom(selection, {
        providers: [
          { ...provider, models: [] },
          { ...provider, instanceId: otherId },
        ],
      }),
    ).toMatchObject({ issue: { reason: "model_missing" } });
  });

  it.each([
    [{ enabled: false }, "provider_disabled"],
    [{ status: "disabled" }, "provider_disabled"],
    [{ installed: false }, "provider_not_installed"],
    [{ availability: "unavailable" }, "provider_unavailable"],
    [{ status: "error" }, "provider_error"],
    [{ auth: { status: "unauthenticated" } }, "provider_unauthenticated"],
  ] as const)("rejects unusable provider state %j", (patch, reason) => {
    expect(custom(selection, { providers: [{ ...provider, ...patch }] })).toMatchObject({
      status: "unavailable",
      issue: { reason },
    });
  });

  it("accepts legacy availability and non-failing warning/unknown auth states", () => {
    expect(
      custom(selection, {
        providers: [{ ...provider, status: "warning", auth: { status: "unknown" } }],
      }),
    ).toMatchObject({ status: "resolved" });
  });

  it.each([
    [[{ id: "effort", value: "invented" }], "invalid_option_value", "effort"],
    [[{ id: "effort", value: true }], "invalid_option_value", "effort"],
    [[{ id: "feature", value: "true" }], "invalid_option_value", "feature"],
    [[{ id: "unknown", value: false }], "unknown_option", "unknown"],
    [
      [
        { id: "feature", value: true },
        { id: "feature", value: false },
      ],
      "duplicate_option",
      "feature",
    ],
  ] as const)("rejects invalid options without dropping them: %j", (options, reason, optionId) => {
    expect(custom({ ...selection, options })).toEqual({
      status: "unavailable",
      role: "worker",
      issue: { reason, optionId },
    });
  });

  it("requires option metadata only when options are selected, and uses the selected model's descriptors", () => {
    const catalog = {
      providers: [{ ...provider, models: [{ ...provider.models[0]!, capabilities: null }] }],
    };
    expect(custom(selection, catalog)).toMatchObject({ status: "resolved" });
    expect(
      custom({ ...selection, options: [{ id: "effort", value: "supported" }] }, catalog),
    ).toMatchObject({ issue: { reason: "option_metadata_missing" } });
    expect(
      custom(
        { ...selection, options: [{ id: "effort", value: "supported" }] },
        {
          providers: [
            {
              ...provider,
              models: [{ ...provider.models[0]!, capabilities: { optionDescriptors: [] } }],
            },
          ],
        },
      ),
    ).toMatchObject({ issue: { reason: "unknown_option" } });
  });
});

import { describe, expect, it } from "vite-plus/test";
import {
  ProviderDriverKind,
  ProviderInstanceId,
  type MultiAgentConfig,
  type ProviderOptionDescriptor,
  type ServerProvider,
} from "@t3tools/contracts";
import { deriveProviderInstanceEntries } from "../../providerInstances";
import {
  changeMultiAgentMode,
  clearMultiAgentRole,
  multiAgentRoleDescriptors,
  setMultiAgentRoleModel,
  setMultiAgentRoleOption,
} from "./MultiAgentControl.logic";

const instanceId = ProviderInstanceId.make("configured-instance");
const reasoning: ProviderOptionDescriptor = {
  id: "catalog-option",
  label: "Reasoning",
  type: "select",
  options: [
    { id: "economy", label: "Economy" },
    { id: "thorough", label: "Thorough" },
    { id: "prompt-only", label: "Prompt only" },
  ],
  promptInjectedValues: ["prompt-only"],
};
const provider: ServerProvider = {
  instanceId,
  driver: ProviderDriverKind.make("codex"),
  enabled: true,
  installed: true,
  status: "ready",
  version: null,
  auth: { status: "authenticated" },
  checkedAt: "2026-10-01T00:00:00Z",
  models: [
    {
      slug: "user-model",
      name: "User model",
      isCustom: true,
      aliases: ["model-alias"],
      capabilities: { optionDescriptors: [reasoning] },
    },
  ],
  slashCommands: [],
  skills: [],
};

describe("Multi-Agent preference editing", () => {
  it("switches modes without inventing role models or changing an existing mode", () => {
    expect(changeMultiAgentMode({ mode: "off" }, "auto")).toEqual({
      mode: "auto",
      preset: "balanced",
    });
    expect(changeMultiAgentMode({ mode: "auto", preset: "maximum" }, "custom")).toEqual({
      mode: "custom",
      roles: {},
    });
    const config: MultiAgentConfig = { mode: "auto", preset: "budget" };
    expect(changeMultiAgentMode(config, "auto")).toBe(config);
    expect(changeMultiAgentMode(config, "off")).toEqual({ mode: "off" });
  });

  it("retains same-model options and clears them when the role model or instance changes", () => {
    const config: MultiAgentConfig = {
      mode: "custom",
      roles: {
        worker: {
          instanceId,
          model: "user-model",
          options: [{ id: reasoning.id, value: "thorough" }],
        },
        reviewer: { instanceId, model: "review-model" },
      },
    };
    expect(setMultiAgentRoleModel(config, "worker", instanceId, "user-model")).toBe(config);
    for (const [nextInstance, model] of [
      [instanceId, "another-model"],
      [ProviderInstanceId.make("another-instance"), "user-model"],
    ] as const) {
      const changed = setMultiAgentRoleModel(config, "worker", nextInstance, model);
      expect(changed.mode === "custom" && changed.roles.worker).toEqual({
        instanceId: nextInstance,
        model,
      });
      expect(changed.mode === "custom" && changed.roles.reviewer).toEqual(config.roles.reviewer);
    }
  });

  it("reads supported choices from the exact provider instance and excludes prompt-injected choices", () => {
    const entry = deriveProviderInstanceEntries([provider])[0]!;
    const descriptors = multiAgentRoleDescriptors({ instanceId, model: "model-alias" }, entry);
    expect(
      descriptors[0]?.type === "select" && descriptors[0].options.map((option) => option.id),
    ).toEqual(["economy", "thorough"]);
    expect(multiAgentRoleDescriptors({ instanceId, model: "missing-model" }, entry)).toEqual([]);
    expect(multiAgentRoleDescriptors({ instanceId, model: "user-model" }, undefined)).toEqual([]);
    expect(
      multiAgentRoleDescriptors({ instanceId, model: "user-model" }, { ...entry, enabled: false }),
    ).toEqual([]);
  });

  it("accepts supported option values without losing other role or option preferences", () => {
    const config: MultiAgentConfig = {
      mode: "custom",
      roles: {
        worker: { instanceId, model: "user-model", options: [{ id: "other-option", value: true }] },
        planner: { instanceId, model: "planner-model" },
      },
    };
    const next = setMultiAgentRoleOption(config, "worker", reasoning, "thorough");
    expect(next.mode === "custom" && next.roles.worker?.options).toEqual([
      { id: "other-option", value: true },
      { id: reasoning.id, value: "thorough" },
    ]);
    expect(next.mode === "custom" && next.roles.planner).toEqual(config.roles.planner);
    expect(setMultiAgentRoleOption(config, "worker", reasoning, "unknown")).toBe(config);
    expect(setMultiAgentRoleOption(config, "worker", reasoning, "prompt-only")).toBe(config);
    expect(setMultiAgentRoleOption(config, "rescuer", reasoning, "economy")).toBe(config);
    expect(clearMultiAgentRole(config, "worker")).toEqual({
      mode: "custom",
      roles: { planner: config.roles.planner },
    });
  });

  it("keeps boolean options typed and prevents edits from enabling Off mode", () => {
    const descriptor: ProviderOptionDescriptor = {
      id: "catalog-toggle",
      label: "Toggle",
      type: "boolean",
    };
    const config: MultiAgentConfig = {
      mode: "custom",
      roles: { reviewer: { instanceId, model: "user-model" } },
    };
    const next = setMultiAgentRoleOption(config, "reviewer", descriptor, false);
    expect(next.mode === "custom" && next.roles.reviewer?.options).toEqual([
      { id: descriptor.id, value: false },
    ]);
    expect(setMultiAgentRoleOption(config, "reviewer", descriptor, "false")).toBe(config);
    const off: MultiAgentConfig = { mode: "off" };
    expect(setMultiAgentRoleModel(off, "worker", instanceId, "user-model")).toBe(off);
  });
});

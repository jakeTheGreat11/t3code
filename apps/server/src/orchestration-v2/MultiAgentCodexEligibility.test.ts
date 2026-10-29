import { assert, describe, it } from "@effect/vitest";
import {
  ProviderInstanceId,
  type OrchestrationV2Run,
  type ServerProvider,
} from "@t3tools/contracts";
import { multiAgentCodexEligibility } from "./MultiAgentCodexEligibility.ts";

const instanceId = ProviderInstanceId.make("scripted-codex");
const selection = {
  instanceId,
  model: "scripted-model",
  options: [{ id: "reasoning", value: "supported" }],
};
const provider = {
  instanceId,
  driver: "codex",
  enabled: true,
  installed: true,
  status: "ready",
  auth: { status: "authenticated" },
  models: [
    {
      slug: "scripted-model",
      capabilities: {
        optionDescriptors: [{ id: "reasoning", type: "select", options: [{ id: "supported" }] }],
      },
    },
  ],
} as unknown as ServerProvider;
const run = {
  id: "coordinator-run",
  threadId: "coordinator-thread",
  providerInstanceId: instanceId,
  modelSelection: selection,
  multiAgent: { mode: "custom", roles: { worker: selection } },
  multiAgentController: {
    coordinatorRunId: "coordinator-run",
    coordinatorThreadId: "coordinator-thread",
    coordinatorAttemptId: "initial-attempt",
    tasks: [],
    status: "open",
  },
} as unknown as OrchestrationV2Run;

describe("production Custom/Codex eligibility", () => {
  it("permits valid saved selections and preserves options", () => {
    assert.deepEqual(multiAgentCodexEligibility(run, [provider]), { eligible: true });
    assert.deepEqual(run.modelSelection.options, selection.options);
  });
  it.each(["off", "auto"] as const)("does not activate %s", (mode) => {
    const candidate = {
      ...run,
      multiAgent: mode === "off" ? { mode } : { mode, preset: "balanced" },
    } as OrchestrationV2Run;
    assert.isFalse(multiAgentCodexEligibility(candidate, [provider]).eligible);
  });
  it.each(["missing", "wrong-thread", "wrong-run", "missing-attempt"])(
    "rejects %s controller identity",
    (kind) => {
      const controller = { ...run.multiAgentController! };
      const candidate = {
        ...run,
        multiAgentController:
          kind === "missing"
            ? undefined
            : {
                ...controller,
                ...(kind === "wrong-thread" ? { coordinatorThreadId: "other" } : {}),
                ...(kind === "wrong-run" ? { coordinatorRunId: "other" } : {}),
                ...(kind === "missing-attempt" ? { coordinatorAttemptId: undefined } : {}),
              },
      } as OrchestrationV2Run;
      assert.isFalse(multiAgentCodexEligibility(candidate, [provider]).eligible);
    },
  );
  it.each(["planner", "worker", "rescuer", "reviewer"] as const)(
    "does not enable a non-Codex %s assignment",
    (role) => {
      const otherId = ProviderInstanceId.make("scripted-other");
      const other = { ...provider, instanceId: otherId, driver: "claude-code" } as ServerProvider;
      const candidate = {
        ...run,
        multiAgent: {
          mode: "custom",
          roles: { worker: selection, [role]: { ...selection, instanceId: otherId } },
        },
      } as OrchestrationV2Run;
      assert.isFalse(multiAgentCodexEligibility(candidate, [provider, other]).eligible);
    },
  );
  it.each([
    "missing-provider",
    "missing-model",
    "invalid-option",
    "disabled-provider",
    "duplicate-provider",
  ])("rejects %s", (kind) => {
    const catalog =
      kind === "missing-provider"
        ? []
        : kind === "duplicate-provider"
          ? [provider, provider]
          : [
              kind === "missing-model"
                ? { ...provider, models: [] }
                : kind === "disabled-provider"
                  ? { ...provider, enabled: false }
                  : provider,
            ];
    const candidate =
      kind === "invalid-option"
        ? ({
            ...run,
            multiAgent: {
              mode: "custom",
              roles: {
                worker: { ...selection, options: [{ id: "reasoning", value: "unsupported" }] },
              },
            },
          } as OrchestrationV2Run)
        : run;
    assert.isFalse(multiAgentCodexEligibility(candidate, catalog).eligible);
  });
  it("allows independent Codex instances without crossing provider boundaries", () => {
    const workerId = ProviderInstanceId.make("second-codex");
    const candidate = {
      ...run,
      multiAgent: { mode: "custom", roles: { worker: { ...selection, instanceId: workerId } } },
    } as OrchestrationV2Run;
    assert.isTrue(
      multiAgentCodexEligibility(candidate, [provider, { ...provider, instanceId: workerId }])
        .eligible,
    );
    assert.isFalse(multiAgentCodexEligibility(candidate, [provider]).eligible);
  });
  it("requires a Codex coordinator and an assigned worker role", () => {
    assert.isFalse(
      multiAgentCodexEligibility(run, [{ ...provider, driver: "scripted-other" } as ServerProvider])
        .eligible,
    );
    assert.isFalse(
      multiAgentCodexEligibility({ ...run, multiAgent: { mode: "custom", roles: {} } }, [provider])
        .eligible,
    );
  });
});

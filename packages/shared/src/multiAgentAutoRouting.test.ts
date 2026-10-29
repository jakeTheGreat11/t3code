import { describe, expect, it } from "vite-plus/test";
import * as Schema from "effect/Schema";
import {
  CommandId,
  MessageId,
  NodeId,
  ProviderDriverKind,
  ProviderInstanceId,
  RunAttemptId,
  RunId,
  ThreadId,
  MultiAgentControllerState,
  MultiAgentProposal,
  type ServerProvider,
} from "@t3tools/contracts";
import {
  createMultiAgentAutoCatalog,
  resolveMultiAgentAutoTask,
  resolvePersistedMultiAgentTask,
} from "./multiAgentRouting.ts";
import { admitMultiAgentProposal } from "./multiAgentController.ts";

const decodeController = Schema.decodeUnknownSync(Schema.fromJsonString(MultiAgentControllerState));
const encodeController = Schema.encodeSync(Schema.fromJsonString(MultiAgentControllerState));
const decodeProposal = Schema.decodeUnknownSync(MultiAgentProposal);

const instanceId = ProviderInstanceId.make("connected-codex-account");
const fallback = { instanceId, model: "coordinator" };
const provider: ServerProvider = {
  instanceId,
  driver: ProviderDriverKind.make("codex"),
  enabled: true,
  installed: true,
  status: "ready",
  version: null,
  auth: { status: "authenticated" },
  checkedAt: "2026-10-15T00:00:00Z",
  slashCommands: [],
  skills: [],
  models: [
    { slug: "coordinator", name: "Coordinator", isCustom: true, capabilities: null },
    ...["economy", "reasoning"].map((slug) => ({
      slug,
      name: slug,
      isCustom: true,
      capabilities: {
        optionDescriptors: [
          {
            id: "provider-depth",
            label: "Reasoning",
            type: "select" as const,
            currentValue: "light",
            options: [
              { id: "light", label: "Light" },
              { id: "deep", label: "Deep" },
            ],
          },
        ],
      },
    })),
  ],
};
const catalog = createMultiAgentAutoCatalog(fallback, [provider])!;
const request = (model = "economy", value = "light") => ({
  candidateId: catalog.candidates.find((c) => c.model.slug === model)!.id,
  options: [{ id: "provider-depth", value }],
  reason: model === "economy" ? "Routine button" : "Ambiguous compatibility engine",
});
const context = { providers: [provider] };
const run = {
  id: RunId.make("coordinator-run"),
  threadId: ThreadId.make("coordinator-thread"),
  providerInstanceId: instanceId,
  multiAgent: { mode: "auto" as const, preset: "balanced" as const },
};
const state: MultiAgentControllerState = {
  coordinatorRunId: run.id,
  coordinatorThreadId: run.threadId,
  coordinatorAttemptId: RunAttemptId.make("initial"),
  status: "open",
  tasks: [],
  autoRoutingCatalog: catalog,
};
const identity = (key: string) => ({
  key,
  taskId: NodeId.make(`task:${key}`),
  workerThreadId: ThreadId.make(`worker:${key}`),
  launchCommandId: CommandId.make(`launch:${key}`),
  launchMessageId: MessageId.make(`message:${key}`),
});

describe("catalog-bound task-aware Auto routing", () => {
  it.each(["budget", "balanced", "maximum"] as const)(
    "admits per-task decisions for %s without assigning model rankings",
    (preset) => {
      const tasks = [
        { key: "button", role: "worker", prompt: "Build a button", routing: request() },
        {
          key: "engine",
          role: "worker",
          prompt: "Design a compatibility engine",
          routing: request("reasoning", "deep"),
        },
      ];
      const input = {
        run: { ...run, multiAgent: { mode: "auto" as const, preset } },
        state,
        source: { proposalId: MessageId.make("proposal"), attemptId: state.coordinatorAttemptId! },
        proposal: { tasks },
        identities: tasks.map((t) => identity(t.key)),
        catalog: context,
      };
      const result = admitMultiAgentProposal(input);
      expect(result.status).toBe("admitted");
      if (result.status !== "admitted") throw new Error("Admission failed");
      expect(result.state.tasks.map((t) => t.modelSelection)).toEqual([
        { instanceId, model: "economy", options: [{ id: "provider-depth", value: "light" }] },
        { instanceId, model: "reasoning", options: [{ id: "provider-depth", value: "deep" }] },
      ]);
      const restored = decodeController(encodeController(result.state));
      expect(restored).toEqual(result.state);
      const duplicate = admitMultiAgentProposal({
        ...input,
        state: restored,
        catalog: { providers: [] },
      });
      expect(duplicate).toEqual({ status: "duplicate", state: restored });
    },
  );
  it("rejects unknown candidates and missing selections rather than guessing model IDs", () => {
    expect(
      resolveMultiAgentAutoTask(catalog, { candidateId: "invented-model" }, "worker", context),
    ).toMatchObject({ status: "unavailable", issue: { reason: "auto_candidate_missing" } });
    expect(resolveMultiAgentAutoTask(catalog, undefined, "worker", context)).toMatchObject({
      status: "unavailable",
      issue: { reason: "auto_selection_required" },
    });
    expect(
      resolveMultiAgentAutoTask(catalog, { candidateId: request().candidateId }, "worker", context),
    ).toMatchObject({ status: "unavailable", issue: { reason: "auto_option_required" } });
  });
  it.each(
    [
      [{ id: "provider-depth", value: "invented-effort" }],
      [{ id: "unknown-option", value: "light" }],
      [
        { id: "provider-depth", value: "light" },
        { id: "provider-depth", value: "deep" },
      ],
    ].map((options) => ({ options })),
  )("rejects invalid model-specific options $options", ({ options }) => {
    expect(
      resolveMultiAgentAutoTask(catalog, { ...request(), options }, "worker", context).status,
    ).toBe("unavailable");
  });
  it("rejects provider/run identities and model overrides in a proposal", () => {
    for (const field of ["modelSelection", "providerInstanceId", "runId", "workerThreadId"]) {
      expect(() =>
        decodeProposal(
          {
            tasks: [
              {
                key: "task",
                role: "worker",
                prompt: "Do useful work",
                routing: request(),
                [field]: "injected",
              },
            ],
          },
          { onExcessProperty: "error" },
        ),
      ).toThrow();
    }
  });
  it("falls back once for a removed offered model, then preserves that choice if it returns", () => {
    const changed = {
      providers: [{ ...provider, models: provider.models.filter((m) => m.slug !== "economy") }],
    };
    const resolved = resolveMultiAgentAutoTask(catalog, request(), "worker", changed);
    expect(resolved).toMatchObject({
      status: "resolved",
      modelSelection: fallback,
      fallback: "coordinator",
    });
    const task = {
      role: "worker" as const,
      routing: request(),
      routingFallback: "coordinator" as const,
      modelSelection: fallback,
    };
    expect(
      resolvePersistedMultiAgentTask({ ...run, multiAgentController: state }, task, context),
    ).toMatchObject({ status: "resolved", modelSelection: fallback });
  });
  it("never reroutes an admitted selection when its model disappears", () => {
    const task = {
      role: "worker" as const,
      routing: request(),
      modelSelection: { instanceId, model: "economy", options: request().options },
    };
    expect(
      resolvePersistedMultiAgentTask({ ...run, multiAgentController: state }, task, {
        providers: [{ ...provider, models: provider.models.filter((m) => m.slug !== "economy") }],
      }),
    ).toMatchObject({ status: "unavailable", issue: { reason: "model_missing" } });
  });
  it("fails clearly when fallback is unavailable, and respects instance boundaries", () => {
    expect(
      resolveMultiAgentAutoTask(catalog, request(), "worker", {
        providers: [{ ...provider, models: [] }],
      }).status,
    ).toBe("unavailable");
    expect(
      resolveMultiAgentAutoTask(catalog, request(), "worker", {
        ...context,
        allowedInstanceIds: [],
      }),
    ).toMatchObject({ status: "unavailable", issue: { reason: "provider_not_allowed" } });
    expect(
      createMultiAgentAutoCatalog(fallback, [{ ...provider, enabled: false }]),
    ).toBeUndefined();
    expect(
      createMultiAgentAutoCatalog(fallback, [
        { ...provider, driver: ProviderDriverKind.make("claudeAgent") },
      ]),
    ).toBeUndefined();
  });
  it("does not admit Auto without its authoritative catalog or let Custom override assignments", () => {
    const source = {
      proposalId: MessageId.make("proposal"),
      attemptId: state.coordinatorAttemptId!,
    };
    const input = {
      run,
      source,
      proposal: { tasks: [{ key: "task", role: "worker", prompt: "Work", routing: request() }] },
      identities: [identity("task")],
      catalog: context,
    };
    expect(admitMultiAgentProposal(input)).toMatchObject({
      status: "rejected",
      reason: "auto_unsupported",
    });
    expect(
      admitMultiAgentProposal({
        ...input,
        run: { ...run, multiAgent: { mode: "custom", roles: { worker: fallback } } },
      }),
    ).toMatchObject({ status: "rejected", reason: "malformed_proposal" });
  });
});

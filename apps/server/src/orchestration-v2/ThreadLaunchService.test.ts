import * as Scheduler from "../scheduling/Scheduler.ts";
import * as WorktreeSetupTracker from "../project/WorktreeSetupTracker.ts";
import * as ProjectCloneTracker from "../project/ProjectCloneTracker.ts";
import * as TerminalManager from "../terminal/Manager.ts";
import * as NodeServices from "@effect/platform-node/NodeServices";
import * as FileSystem from "effect/FileSystem";
import * as ServerConfig from "../config.ts";
import { createPendingAttachmentId, resolveAttachmentPath } from "../attachmentStore.ts";
import * as ThreadMessageIntake from "./ThreadMessageIntake.ts";
import { assert, it, vi } from "@effect/vitest";
import * as NodeCrypto from "@effect/platform-node/NodeCrypto";
import {
  ORCHESTRATION_V2_WORKSPACE_PREPARATION_FAILURE_CODE,
  ChatAttachmentId,
  ComposerContextId,
  type ChatAttachment,
  CommandId,
  EventId,
  NodeId,
  RunAttemptId,
  RunId,
  DEFAULT_SERVER_SETTINGS,
  GitCommandError,
  MessageId,
  ProjectId,
  ProviderDriverKind,
  ProviderInstanceId,
  OrchestrationV2ThreadProjectionJson,
  ScheduledTaskId,
  type ServerProvider,
  type MultiAgentConfig,
  type MultiAgentControllerTask,
  type OrchestrationV2Run,
  type ModelSelection,
  orchestrationV2RunMultiAgentConfig,
  ThreadId,
} from "@t3tools/contracts";
import * as Cause from "effect/Cause";
import * as Deferred from "effect/Deferred";
import * as DateTime from "effect/DateTime";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Ref from "effect/Ref";
import * as Stream from "effect/Stream";
import * as Schema from "effect/Schema";
import * as TestClock from "effect/testing/TestClock";

import * as GitWorkflow from "../git/GitWorkflowService.ts";
import * as SqlitePersistence from "../persistence/Sqlite.ts";
import * as ProjectStore from "./ProjectStore.ts";
import * as ProjectService from "../project/ProjectService.ts";
import * as ProjectSetupScriptRunner from "../project/ProjectSetupScriptRunner.ts";
import * as ManagedProjectFolders from "../project/ManagedProjectFolders.ts";
import * as ProviderRegistryMock from "../provider/testUtils/providerRegistryMock.ts";
import * as ServerSettings from "../serverSettings.ts";
import * as ScheduledTasks from "../scheduledTasks/ScheduledTaskService.ts";
import * as SecretRequests from "../secrets/SecretRequests.ts";
import * as TextGeneration from "../textGeneration/TextGeneration.ts";
import { CodexProviderCapabilitiesV2 } from "./Adapters/CodexAdapterV2.ts";
import * as CommandReceiptStore from "./CommandReceiptStore.ts";
import * as EffectOutbox from "./EffectOutbox.ts";
import * as EffectWorker from "./EffectWorker.ts";
import * as EventStore from "./EventStore.ts";
import * as ProjectionStore from "./ProjectionStore.ts";
import * as IdAllocator from "@t3tools/provider-core/server/IdAllocator";
import * as ProviderAdapterRegistry from "./ProviderAdapterRegistry.ts";
import * as ThreadLaunch from "./ThreadLaunchService.ts";
import * as MultiAgentWorkerLaunchService from "./MultiAgentWorkerLaunchService.ts";
import * as Orchestrator from "./Orchestrator.ts";
import * as EventSink from "./EventSink.ts";
import * as ThreadManagement from "./ThreadManagementService.ts";
import * as ThreadTitleRegeneration from "./ThreadTitleRegenerationService.ts";
import * as ProviderReplayHarness from "./testkit/ProviderReplayHarness.ts";
import type * as ProviderAdapter from "@t3tools/provider-core/server/ProviderAdapter";

const projectId = ProjectId.make("project:launch-test");
const otherProjectId = ProjectId.make("project:launch-other");
const encodeThreadProjection = Schema.encodeEffect(OrchestrationV2ThreadProjectionJson);
const modelSelection = {
  instanceId: ProviderInstanceId.make("codex"),
  model: "gpt-5.1-codex",
} as const;
const launchCatalogProvider: ServerProvider = {
  instanceId: modelSelection.instanceId,
  driver: ProviderDriverKind.make("codex"),
  enabled: true,
  installed: true,
  status: "ready",
  version: null,
  auth: { status: "authenticated" },
  checkedAt: "2026-06-20T00:00:00.000Z",
  slashCommands: [],
  skills: [],
  models: [
    {
      slug: modelSelection.model,
      name: "Launch test model",
      aliases: ["launch-model-alias"],
      isCustom: false,
      capabilities: {
        optionDescriptors: [
          {
            id: "effort",
            label: "Effort",
            type: "select",
            options: [{ id: "supported", label: "Supported" }],
          },
          { id: "feature", label: "Feature", type: "boolean" },
        ],
      },
    },
  ],
};
const project = {
  id: projectId,
  title: "Project",
  workspaceRoot: "/repo",
  repositoryIdentity: null,
  faviconPath: null,
  defaultModelSelection: modelSelection,
  defaultThreadEnvMode: null,
  scripts: [],
  createdAt: "2026-06-20T00:00:00.000Z",
  updatedAt: "2026-06-20T00:00:00.000Z",
  deletedAt: null,
} as const;

const otherProject = {
  ...project,
  id: otherProjectId,
  title: "Other",
} as const;

const adapter = {
  instanceId: modelSelection.instanceId,
  driver: ProviderDriverKind.make("codex"),
  getCapabilities: () => Effect.succeed(CodexProviderCapabilitiesV2),
  planSelectionTransition: () => Effect.succeed({ type: "apply_on_next_turn" as const }),
  openSession: () => Effect.die("provider execution is disabled in launch tests"),
} as ProviderAdapter.ProviderAdapterV2["Service"];

interface HarnessOptions {
  readonly managedFolders?: Layer.Layer<ManagedProjectFolders.ManagedProjectFolders>;
  readonly createWorktree?: GitWorkflow.GitWorkflowService["Service"]["createWorktree"];
  readonly fetchRemote?: GitWorkflow.GitWorkflowService["Service"]["fetchRemote"];
  readonly hasCommit?: GitWorkflow.GitWorkflowService["Service"]["hasCommit"];
  readonly renameBranch?: GitWorkflow.GitWorkflowService["Service"]["renameBranch"];
  readonly runSetup?: ProjectSetupScriptRunner.ProjectSetupScriptRunner["Service"]["runForThread"];
  readonly generateTitle?: TextGeneration.TextGeneration["Service"]["generateThreadTitle"];
  readonly generateBranchName?: TextGeneration.TextGeneration["Service"]["generateBranchName"];
  readonly serverSettings?: Parameters<typeof ServerSettings.layerTest>[0];
  readonly providers?: ReadonlyArray<ServerProvider>;
  readonly git?: Partial<GitWorkflow.GitWorkflowService["Service"]>;
}

function makeHarness(options: HarnessOptions = {}) {
  const layerDatabase = SqlitePersistence.layerMemory;
  const layerRegistry = ProviderAdapterRegistry.layerFromAdapters([adapter]);
  const layerOrchestrator = ProviderReplayHarness.layerWithRegistry(
    { name: "thread-launch" },
    layerRegistry,
    {
      databaseLayer: layerDatabase,
      runEffectWorker: false,
      providerCatalog: options.providers ?? [launchCatalogProvider],
    },
  );
  const layerServerSettings = ServerSettings.layerTest(options.serverSettings);
  const layerReceipts = CommandReceiptStore.layer.pipe(Layer.provide(layerDatabase));
  const layerThreadManagement = ThreadManagement.layer.pipe(
    Layer.provide(layerOrchestrator),
    Layer.provide(layerServerSettings),
    Layer.provide(layerReceipts),
  );
  const layerOutbox = EffectOutbox.layer.pipe(Layer.provide(layerDatabase));
  const createWorktree = vi.fn(
    options.createWorktree ??
      ((input) =>
        Effect.succeed({
          worktree: { path: "/repo-worktrees/feature", refName: input.newRefName, headSha: "abc" },
        } as never)),
  );
  const renameBranch = vi.fn(
    options.renameBranch ?? ((input) => Effect.succeed({ branch: input.newBranch })),
  );
  const removeWorktree = vi.fn(
    (_input: Parameters<GitWorkflow.GitWorkflowService["Service"]["removeWorktree"]>[0]) =>
      Effect.void,
  );
  const runSetup = vi.fn(
    options.runSetup ?? (() => Effect.succeed({ status: "no-script" as const })),
  );
  const generateBranchName = vi.fn(
    options.generateBranchName ?? (() => Effect.succeed({ branch: "generated-branch" })),
  );
  const generateThreadTitle = vi.fn(
    options.generateTitle ?? (() => Effect.succeed({ title: "Generated title" })),
  );
  const layerExternalServices = Layer.mergeAll(
    WorktreeSetupTracker.layer,
    Layer.mock(ProjectCloneTracker.ProjectCloneTracker)({ get: () => Effect.succeed(null) }),
    Layer.mock(TerminalManager.TerminalManager)({ close: () => Effect.void }),
    Layer.succeed(ProjectService.ProjectService, {
      create: () => Effect.die("unused"),
      bootstrap: () => Effect.die("unused"),
      update: () => Effect.die("unused"),
      delete: () => Effect.die("unused"),
      getById: (id) =>
        Effect.succeed(
          id === projectId
            ? Option.some(project)
            : id === otherProjectId
              ? Option.some(otherProject)
              : Option.none(),
        ),
      getByWorkspaceRoot: () => Effect.succeed(Option.some(project)),
      snapshot: Effect.die("unused"),
      getShell: () => Effect.die("unused"),
      listShells: () => Effect.die("unused"),
    }),
    Layer.mock(GitWorkflow.GitWorkflowService)({
      createWorktree,
      renameBranch,
      fetchRemote: options.fetchRemote ?? (() => Effect.void),
      hasCommit: options.hasCommit ?? (() => Effect.succeed(false)),
      remoteExists: () => Effect.succeed(true),
      remoteBranchExists: () => Effect.succeed(true),
      removeWorktree,
      resolveRemoteTrackingCommit: () =>
        Effect.succeed({ commitSha: "remote-main-sha", remoteRefName: "origin/main" }),
      ...options.git,
    }),
    Layer.succeed(ProjectSetupScriptRunner.ProjectSetupScriptRunner, {
      runForThread: runSetup,
    }),
    Layer.mock(TextGeneration.TextGeneration)({
      generateThreadTitle,
      generateBranchName,
    }),
    layerServerSettings,
    ProviderRegistryMock.layer(options.providers),
    options.managedFolders ??
      Layer.mock(ManagedProjectFolders.ManagedProjectFolders)({
        namedProjectsRoot: "/projects",
        folderForThread: () => Effect.succeed(Option.none()),
      }),
  );
  const layerLaunch = ThreadLaunch.layer.pipe(
    Layer.provide(
      Layer.mergeAll(
        layerExternalServices,
        layerThreadManagement,
        layerReceipts,
        IdAllocator.layer,
      ),
    ),
  );
  const layerProjectedProjects = Layer.mock(ProjectStore.ProjectStoreV2)({
    get: (requestedProjectId) =>
      Effect.succeed(
        requestedProjectId === projectId
          ? Option.some({
              projectId,
              title: project.title,
              workspaceRoot: project.workspaceRoot,
              defaultModelSelection: project.defaultModelSelection,
              defaultThreadEnvMode: null,
              autoPull: false,
              faviconPath: null,
              projectIcon: null,
              scripts: project.scripts,
              createdAt: project.createdAt,
              updatedAt: project.updatedAt,
              deletedAt: project.deletedAt,
            })
          : Option.none(),
      ),
  });
  const layerTitleRegeneration = ThreadTitleRegeneration.layer.pipe(
    Layer.provide(
      Layer.mergeAll(layerThreadManagement, layerProjectedProjects, layerExternalServices),
    ),
  );
  return {
    layer: Layer.mergeAll(
      MultiAgentWorkerLaunchService.layer.pipe(
        Layer.provide(
          Layer.mergeAll(layerLaunch, layerThreadManagement, layerReceipts, layerExternalServices),
        ),
      ),
      layerReceipts,
      layerOrchestrator,
      layerLaunch,
      layerThreadManagement,
      layerTitleRegeneration,
      layerOutbox,
      ProjectionStore.layer.pipe(Layer.provide(layerDatabase)),
      layerDatabase,
      layerExternalServices,
    ),
    createWorktree,
    removeWorktree,
    renameBranch,
    generateBranchName,
    generateThreadTitle,
    runSetup,
  };
}

it.effect.each([false, true])(
  "validates explicit selections without changing grouping (%s)",
  (grouped) => {
    const harness = makeHarness({ providers: [launchCatalogProvider] });
    return Effect.gen(function* () {
      const launches = yield* ThreadLaunch.ThreadLaunchService;
      const coordinator = yield* launches.launch(
        launchInput({ command: "explicit:coordinator", thread: "explicit:coordinator" }),
      );
      const options = [
        { id: "effort", value: "supported" },
        { id: "feature", value: false },
      ];
      const input = {
        ...launchInput({ command: "explicit:worker", thread: "explicit:worker" }),
        modelSelection: { ...modelSelection, model: "launch-model-alias", options },
        validateExplicitModelSelection: true,
        ...(grouped ? { coordinatorThreadId: coordinator.threadId } : {}),
      };
      const launched = yield* launches.launch(input);
      assert.deepEqual(launched.projection.thread.modelSelection, { ...modelSelection, options });
      assert.equal(
        launched.projection.thread.coordinatorThreadId,
        grouped ? coordinator.threadId : undefined,
      );
      assert.isNull(launched.projection.thread.lineage.parentThreadId);
      const replay = yield* launches.launch({
        ...input,
        modelSelection: { ...modelSelection, model: "no-longer-advertised" },
      });
      assert.equal(replay.threadId, launched.threadId);
      assert.deepEqual(
        replay.projection.thread.modelSelection,
        launched.projection.thread.modelSelection,
      );
    }).pipe(Effect.provide(harness.layer));
  },
);

it.effect.each([
  {
    selection: { ...modelSelection, model: "missing" },
    providers: [launchCatalogProvider],
    reason: "model_missing",
  },
  { selection: modelSelection, providers: [], reason: "provider_missing" },
  {
    selection: { ...modelSelection, instanceId: ProviderInstanceId.make("foreign-instance") },
    providers: [launchCatalogProvider],
    reason: "provider_missing",
  },
  {
    selection: modelSelection,
    providers: [
      {
        ...launchCatalogProvider,
        instanceId: ProviderInstanceId.make("same-driver-other-instance"),
      },
    ],
    reason: "provider_missing",
  },
  {
    selection: modelSelection,
    providers: [{ ...launchCatalogProvider, availability: "unavailable" as const }],
    reason: "provider_unavailable",
  },
  {
    selection: modelSelection,
    providers: [{ ...launchCatalogProvider, installed: false }],
    reason: "provider_not_installed",
  },
  {
    selection: { ...modelSelection, options: [{ id: "effort", value: "unsupported" }] },
    providers: [launchCatalogProvider],
    reason: "invalid_option_value",
    optionId: "effort",
  },
  {
    selection: { ...modelSelection, options: [{ id: "feature", value: "true" }] },
    providers: [launchCatalogProvider],
    reason: "invalid_option_value",
    optionId: "feature",
  },
  {
    selection: { ...modelSelection, options: [{ id: "unknown", value: false }] },
    providers: [launchCatalogProvider],
    reason: "unknown_option",
    optionId: "unknown",
  },
  {
    selection: {
      ...modelSelection,
      options: [
        { id: "feature", value: false },
        { id: "feature", value: true },
      ],
    },
    providers: [launchCatalogProvider],
    reason: "duplicate_option",
    optionId: "feature",
  },
  {
    selection: { ...modelSelection, options: [{ id: "effort", value: "supported" }] },
    providers: [
      {
        ...launchCatalogProvider,
        models: launchCatalogProvider.models.map((model) => ({ ...model, capabilities: null })),
      },
    ],
    reason: "option_metadata_missing",
  },
])("refuses explicit selections before creating a thread: $reason", (testCase) => {
  const harness = makeHarness({ providers: testCase.providers });
  return Effect.gen(function* () {
    const launches = yield* ThreadLaunch.ThreadLaunchService;
    const threads = yield* ThreadManagement.ThreadManagementService;
    const input = {
      ...launchInput({ command: "explicit:invalid", thread: "explicit:invalid" }),
      modelSelection: testCase.selection,
      validateExplicitModelSelection: true,
    };
    const error = yield* launches.launch(input).pipe(Effect.flip);
    if (error._tag !== "ThreadLaunchError") return assert.fail(`Unexpected error: ${error._tag}`);
    assert.equal(error.operation, "validate-model-selection");
    assert.deepEqual(error.selectionIssue, {
      reason: testCase.reason,
      ...("optionId" in testCase ? { optionId: testCase.optionId } : {}),
    });
    assert.isNull(yield* threads.getThreadShell(input.threadId));
    assert.isFalse(harness.createWorktree.mock.calls.length > 0);
  }).pipe(Effect.provide(harness.layer));
});

it.effect(
  "leaves inherited and ordinary launch selections unchanged without catalog validation",
  () => {
    const harness = makeHarness({ providers: [] });
    return Effect.gen(function* () {
      const launches = yield* ThreadLaunch.ThreadLaunchService;
      for (const validateExplicitModelSelection of [undefined, false]) {
        const input = {
          ...launchInput({
            command: `inherit:${validateExplicitModelSelection}`,
            thread: `inherit:${validateExplicitModelSelection}`,
          }),
          ...(validateExplicitModelSelection === undefined
            ? {}
            : { validateExplicitModelSelection }),
        };
        const launched = yield* launches.launch(input);
        assert.deepEqual(launched.projection.thread.modelSelection, modelSelection);
        assert.isUndefined(launched.projection.thread.coordinatorThreadId);
      }
    }).pipe(Effect.provide(harness.layer));
  },
);

function launchInput(input: {
  readonly command: string;
  readonly thread: string;
  readonly message?: string;
  readonly workspace?: ThreadLaunch.ThreadLaunchWorkspaceStrategy;
}) {
  return {
    commandId: CommandId.make(input.command),
    threadId: ThreadId.make(input.thread),
    projectId,
    title: "New thread",
    modelSelection,
    runtimeMode: "full-access" as const,
    interactionMode: "default" as const,
    workspaceStrategy: input.workspace ?? { type: "root" as const },
    ...(input.message === undefined
      ? {}
      : {
          initialMessage: {
            messageId: MessageId.make(`${input.message}:id`),
            text: input.message,
            attachments: [],
          },
        }),
    createdBy: "user" as const,
    creationSource: "web" as const,
  };
}

it.effect.each([
  { name: "legacy", config: undefined },
  { name: "off", config: { mode: "off" } },
  { name: "budget", config: { mode: "auto", preset: "budget" } },
  { name: "balanced", config: { mode: "auto", preset: "balanced" } },
  { name: "maximum", config: { mode: "auto", preset: "maximum" } },
  {
    name: "custom",
    config: {
      mode: "custom",
      roles: {
        worker: {
          instanceId: ProviderInstanceId.make("configured-worker"),
          model: "catalog-model",
          options: [
            { id: "catalog-option", value: "catalog-value" },
            { id: "catalog-boolean", value: false },
          ],
        },
      },
    },
  },
] satisfies Array<{ name: string; config: MultiAgentConfig | undefined }>)(
  "snapshots $name preferences through launch, queue, reload, and event replay",
  (testCase) => {
    const harness = makeHarness({
      runSetup: () => Effect.never,
      providers: [
        launchCatalogProvider,
        {
          ...launchCatalogProvider,
          instanceId: ProviderInstanceId.make("configured-worker"),
          models: [
            {
              slug: "catalog-model",
              name: "Policy fixture",
              isCustom: true,
              capabilities: {
                optionDescriptors: [
                  {
                    id: "catalog-option",
                    label: "Option",
                    type: "select",
                    options: [{ id: "catalog-value", label: "Value" }],
                  },
                  { id: "catalog-boolean", label: "Boolean", type: "boolean" },
                ],
              },
            },
          ],
        },
      ],
    });
    const layerFiles = ServerConfig.layerTest(process.cwd(), { prefix: "t3-run-policy-" }).pipe(
      Layer.provideMerge(NodeServices.layer),
    );
    return Effect.gen(function* () {
      const launches = yield* ThreadLaunch.ThreadLaunchService;
      const threads = yield* ThreadManagement.ThreadManagementService;
      const config = structuredClone(testCase.config);
      const expected = structuredClone(config ?? { mode: "off" });
      const launched = yield* launches.launch({
        ...launchInput({ command: "policy:first", thread: "policy:thread", message: "First" }),
        initialMessage: {
          messageId: MessageId.make("policy:first"),
          text: "First",
          attachments: [],
          ...(config === undefined ? {} : { multiAgent: config }),
        },
      });
      const firstRun = launched.projection.runs[0]!;
      assert.include(["preparing", "starting", "running"], firstRun.status);
      assert.deepEqual(firstRun.multiAgent, expected);
      // Preferences can be changed while the first run is active, but only the next run sees them.
      if (config?.mode === "custom" && config.roles.worker)
        config.roles.worker.options![0]!.value = "later-value";
      const nextPolicy: MultiAgentConfig =
        expected.mode === "off" ? { mode: "auto", preset: "maximum" } : { mode: "off" };
      const command = {
        type: "message.dispatch" as const,
        commandId: CommandId.make("policy:next"),
        threadId: launched.threadId,
        messageId: MessageId.make("policy:next"),
        text: "Next",
        attachments: [],
        multiAgent: nextPolicy,
        dispatchMode: { type: "queue_after_active" as const },
        createdBy: "user" as const,
        creationSource: "web" as const,
      };
      yield* ThreadMessageIntake.dispatchCommand(command);
      yield* ThreadMessageIntake.dispatchCommand({
        ...command,
        multiAgent: { mode: "custom", roles: {} },
      });
      const reloaded = yield* threads.getThreadProjection(launched.threadId);
      assert.lengthOf(reloaded.runs, 2);
      assert.deepEqual(reloaded.runs[0]?.multiAgent, expected);
      assert.deepEqual(reloaded.runs[1]?.multiAgent, nextPolicy);
      const eventStore = yield* EventStore.EventStoreV2.pipe(Effect.provide(EventStore.layer));
      const events = yield* eventStore
        .read({ threadId: launched.threadId })
        .pipe(Stream.runCollect);
      for (const layer of [ProjectionStore.layerMemory, ProjectionStore.layer]) {
        yield* Effect.gen(function* () {
          const projections = yield* ProjectionStore.ProjectionStoreV2;
          if (layer === ProjectionStore.layerMemory)
            for (const stored of events) yield* projections.apply(stored.event);
          const restored = yield* projections.getThreadProjection(launched.threadId);
          assert.deepEqual(restored.runs[0]?.multiAgent, expected);
          assert.deepEqual(restored.runs[1]?.multiAgent, nextPolicy);
          // Legacy updates and even conflicting lifecycle snapshots cannot replace the policy.
          const created = events.find((stored) => stored.event.type === "run.created");
          assert.ok(created?.event.type === "run.created");
          const { multiAgent: _multiAgent, ...legacyRun } = created.event.payload;
          for (const payload of [legacyRun, { ...legacyRun, multiAgent: nextPolicy }]) {
            yield* projections.apply({ ...created.event, type: "run.updated", payload });
            assert.deepEqual(
              (yield* projections.getThreadProjection(launched.threadId)).runs[0]?.multiAgent,
              expected,
            );
          }
        }).pipe(Effect.provide(layer));
      }
      assert.deepEqual(orchestrationV2RunMultiAgentConfig({}), { mode: "off" });
    }).pipe(Effect.provide(Layer.mergeAll(harness.layer, layerFiles)));
  },
);

function waitUntil<E, R>(predicate: () => Effect.Effect<boolean, E, R>): Effect.Effect<void, E, R> {
  return Effect.gen(function* () {
    for (let attempt = 0; attempt < 200; attempt += 1) {
      if (yield* predicate()) return;
      yield* Effect.promise(
        () =>
          new Promise<void>((resolve) => {
            setImmediate(resolve);
          }),
      );
    }
    assert.fail("Condition was not reached before timeout.");
  });
}

it.effect.each(
  (["new", "existing"] as const).flatMap((target) =>
    (["user", "agent"] as const).map((createdBy) => ({ target, createdBy })),
  ),
)(
  "attributes $createdBy-configured automations in $target threads without changing their prompt",
  ({ target, createdBy }) => {
    const harness = makeHarness();
    const layerScheduledTasks = ScheduledTasks.layer.pipe(
      Layer.provide(
        Layer.mergeAll(
          harness.layer,
          NodeCrypto.layer,
          Scheduler.layer,
          Layer.mock(SecretRequests.SecretRequests)({}),
        ),
      ),
    );
    return Effect.gen(function* () {
      const tasks = yield* ScheduledTasks.ScheduledTaskService;
      const launches = yield* ThreadLaunch.ThreadLaunchService;
      const threads = yield* ThreadManagement.ThreadManagementService;
      const existing =
        target === "existing"
          ? yield* launches.launch(
              launchInput({ command: "command:existing", thread: "thread:existing" }),
            )
          : null;
      const { task } = yield* tasks.upsert({
        id: ScheduledTaskId.make("scheduled-task:attribution"),
        title: "Daily audit",
        prompt: "Audit performance and crashes.",
        enabled: false,
        schedule: { type: "interval", everyMs: 60_000 },
        projectId,
        threadId: existing?.threadId ?? null,
        workspaceStrategy: { type: "root" },
        modelSelection,
        runtimeMode: "full-access",
        interactionMode: "default",
        createdBy,
        creationSource: createdBy === "agent" ? "mcp" : "web",
      });
      const result = yield* tasks.runNow({ id: task.id });
      assert.equal(result.task.lastRunStatus, "succeeded");
      const projectThreads = yield* threads.listProjectThreads({
        projectId,
        includeSubagents: false,
      });
      const thread =
        projectThreads.find((candidate) => candidate.id === existing?.threadId) ??
        projectThreads[0];
      assert.isDefined(thread);
      const projection = yield* threads.getThreadProjection(thread!.id);
      // Encoding the persisted projection exercises both message and turn-item wire schemas.
      const wire = yield* encodeThreadProjection(projection);
      assert.equal(wire.messages[0]?.text, task.prompt);
      assert.equal(wire.messages[0]?.scheduledTaskId, task.id);
      assert.equal(wire.messages[0]?.createdBy, createdBy);
      const turnItem = wire.turnItems.find(
        (item): item is Extract<typeof item, { type: "user_message" }> =>
          item.type === "user_message",
      );
      assert.equal(turnItem?.text, task.prompt);
      assert.equal(turnItem?.scheduledTaskId, task.id);
    }).pipe(Effect.provide(Layer.mergeAll(harness.layer, layerScheduledTasks)));
  },
);

it.effect("retains automation and sender attribution while a message waits in the queue", () => {
  const harness = makeHarness({ runSetup: () => Effect.never });
  return Effect.gen(function* () {
    const launches = yield* ThreadLaunch.ThreadLaunchService;
    const threads = yield* ThreadManagement.ThreadManagementService;
    const launched = yield* launches.launch(
      launchInput({
        command: "command:automation:queue",
        thread: "thread:automation:queue",
        message: "First message",
      }),
    );
    const scheduledTaskId = ScheduledTaskId.make("scheduled-task:queued");
    const senderThreadId = ThreadId.make("thread:agent-sender");
    const queued = yield* threads.sendToThread({
      projectId,
      commandId: CommandId.make("command:automation:queued"),
      threadId: launched.threadId,
      messageId: MessageId.make("message:automation:queued"),
      scheduledTaskId,
      senderThreadId,
      text: "Run the audit",
      attachments: [],
      mode: "queue",
      createdBy: "agent",
      creationSource: "mcp",
    });
    assert.equal(queued.delivery, "queued");
    const projection = yield* threads.getThreadProjection(launched.threadId);
    const message = projection.messages.find((item) => item.id === queued.message.id);
    assert.equal(message?.scheduledTaskId, scheduledTaskId);
    assert.equal(message?.senderThreadId, senderThreadId);
    assert.equal(message?.text, "Run the audit");
  }).pipe(Effect.provide(harness.layer));
});

it.effect("returns a visible preparing message while provisioning is still blocked", () =>
  Effect.gen(function* () {
    const worktreeEntered = yield* Deferred.make<void>();
    const allowWorktree = yield* Deferred.make<void>();
    const setupEntered = yield* Deferred.make<void>();
    const allowSetup = yield* Deferred.make<void>();
    const harness = makeHarness({
      createWorktree: () =>
        Deferred.succeed(worktreeEntered, undefined).pipe(
          Effect.andThen(Deferred.await(allowWorktree)),
          Effect.as({
            worktree: { path: "/repo-worktrees/feature", refName: "feature", headSha: "abc" },
          } as never),
        ),
      runSetup: () =>
        Deferred.succeed(setupEntered, undefined).pipe(
          Effect.andThen(Deferred.await(allowSetup)),
          Effect.as({ status: "no-script" as const }),
        ),
    });
    yield* Effect.gen(function* () {
      const launches = yield* ThreadLaunch.ThreadLaunchService;
      const outbox = yield* EffectOutbox.EffectOutboxV2;
      const threads = yield* ThreadManagement.ThreadManagementService;
      const input = launchInput({
        command: "command:launch:blocked",
        thread: "thread:launch:blocked",
        message: "Build the feature",
        workspace: { type: "worktree", baseRef: "main" },
      });
      const launched = yield* launches.launch(input);
      assert.equal(launched.projection.messages[0]?.text, "Build the feature");
      assert.equal(launched.projection.runs[0]?.status, "preparing");
      assert.equal(
        launched.projection.turnItems.find((item) => item.type === "command_execution")?.status,
        "running",
      );
      yield* Deferred.await(worktreeEntered);
      let current = yield* threads.getThreadProjection(launched.threadId);
      assert.equal(
        current.turnItems.find((item) => item.type === "command_execution")?.title,
        "Preparing worktree",
      );
      yield* Deferred.succeed(allowWorktree, undefined);
      const entered = yield* Deferred.await(setupEntered).pipe(
        Effect.timeoutOption(Duration.seconds(2)),
      );
      if (Option.isNone(entered)) {
        current = yield* threads.getThreadProjection(launched.threadId);
        assert.fail(
          `Setup was not reached; run=${current.runs[0]?.status ?? "missing"}, worklog=${current.turnItems.find((item) => item.type === "command_execution")?.title ?? "missing"}.`,
        );
      }
      current = yield* threads.getThreadProjection(launched.threadId);
      assert.equal(
        current.turnItems.find((item) => item.type === "command_execution")?.title,
        "Starting setup script",
      );
      const prematureEffects = yield* outbox.listByCommandId(
        CommandId.make("command:launch:blocked:initial-message"),
      );
      assert.isEmpty(prematureEffects);
      yield* Deferred.succeed(allowSetup, undefined);
    }).pipe(Effect.provide(harness.layer));
  }),
);

it.effect("provisions independent launches concurrently instead of behind a global semaphore", () =>
  Effect.gen(function* () {
    const setupCount = yield* Ref.make(0);
    const bothEntered = yield* Deferred.make<void>();
    const allowSetup = yield* Deferred.make<void>();
    const harness = makeHarness({
      runSetup: () =>
        Ref.updateAndGet(setupCount, (count) => count + 1).pipe(
          Effect.tap((count) =>
            count === 2 ? Deferred.succeed(bothEntered, undefined) : Effect.void,
          ),
          Effect.andThen(Deferred.await(allowSetup)),
          Effect.as({ status: "no-script" as const }),
        ),
    });
    yield* Effect.gen(function* () {
      const launches = yield* ThreadLaunch.ThreadLaunchService;
      const results = yield* Effect.all(
        [
          launches.launch(
            launchInput({
              command: "command:launch:concurrent-a",
              thread: "thread:launch:concurrent-a",
              message: "First",
            }),
          ),
          launches.launch(
            launchInput({
              command: "command:launch:concurrent-b",
              thread: "thread:launch:concurrent-b",
              message: "Second",
            }),
          ),
        ],
        { concurrency: "unbounded" },
      );
      assert.deepEqual(
        results.map((result) => result.projection.runs[0]?.status),
        ["preparing", "preparing"],
      );
      yield* Deferred.await(bothEntered);
      assert.equal(yield* Ref.get(setupCount), 2);
      yield* Deferred.succeed(allowSetup, undefined);
    }).pipe(Effect.provide(harness.layer));
  }),
);

it.effect("enqueues provider work only after setup has been initiated", () =>
  Effect.gen(function* () {
    const setupEntered = yield* Deferred.make<void>();
    const allowSetup = yield* Deferred.make<void>();
    const harness = makeHarness({
      runSetup: () =>
        Deferred.succeed(setupEntered, undefined).pipe(
          Effect.andThen(Deferred.await(allowSetup)),
          Effect.as({
            status: "started" as const,
            async: false,
            scriptId: "setup",
            scriptName: "Setup",
            scriptCommand: "vp install",
            terminalId: "setup",
            cwd: "/repo",
          }),
        ),
    });
    yield* Effect.gen(function* () {
      const launches = yield* ThreadLaunch.ThreadLaunchService;
      const outbox = yield* EffectOutbox.EffectOutboxV2;
      const threads = yield* ThreadManagement.ThreadManagementService;
      const input = launchInput({
        command: "command:launch:release",
        thread: "thread:launch:release",
        message: "Start after setup",
        workspace: { type: "worktree", baseRef: "main" },
      });
      const launched = yield* launches.launch(input);
      yield* Deferred.await(setupEntered);
      assert.isEmpty(
        yield* outbox.listByCommandId(CommandId.make("command:launch:release:release")),
      );
      yield* Deferred.succeed(allowSetup, undefined);
      yield* waitUntil(() =>
        outbox
          .listByCommandId(CommandId.make("command:launch:release:release"))
          .pipe(Effect.map((effects) => effects.length === 1)),
      );
      const projection = yield* threads.getThreadProjection(launched.threadId);
      assert.equal(projection.runs[0]?.status, "starting");
      assert.equal(projection.checkpointScopes[0]?.cwd, "/repo-worktrees/feature");
      assert.equal(
        projection.turnItems.find((item) => item.type === "command_execution")?.status,
        "completed",
      );
    }).pipe(Effect.provide(harness.layer));
  }),
);

it.effect(
  "queues follow-up messages behind preparation and checkpoints them in the final workspace",
  () =>
    Effect.gen(function* () {
      const setupEntered = yield* Deferred.make<void>();
      const failSetup = yield* Deferred.make<void>();
      const harness = makeHarness({
        runSetup: () =>
          Deferred.succeed(setupEntered, undefined).pipe(
            Effect.andThen(Deferred.await(failSetup)),
            Effect.andThen(Effect.fail(new Error("setup failed") as never)),
          ),
      });
      yield* Effect.gen(function* () {
        const launches = yield* ThreadLaunch.ThreadLaunchService;
        const threads = yield* ThreadManagement.ThreadManagementService;
        const launched = yield* launches.launch(
          launchInput({
            command: "command:launch:queued-during-preparation",
            thread: "thread:launch:queued-during-preparation",
            message: "Prepare the workspace",
            workspace: { type: "worktree", baseRef: "main" },
          }),
        );
        yield* Deferred.await(setupEntered);

        const followUp = yield* threads.sendToThread({
          projectId,
          commandId: CommandId.make("command:launch:queued-follow-up"),
          threadId: launched.threadId,
          messageId: MessageId.make("message:launch:queued-follow-up"),
          text: "Run after preparation",
          attachments: [],
          mode: "auto",
          createdBy: "user",
          creationSource: "web",
        });
        assert.equal(followUp.delivery, "queued");
        assert.equal(followUp.run.status, "queued");
        assert.equal(
          (yield* threads.getThreadRecords(launched.threadId, ["nodes"])).nodes.find(
            (node) => node.runId === followUp.run.id && node.kind === "root_turn",
          )?.checkpointScopeId,
          null,
        );

        yield* Deferred.succeed(failSetup, undefined);
        yield* waitUntil(() =>
          threads
            .getThreadProjection(launched.threadId)
            .pipe(
              Effect.map(
                (projection) =>
                  projection.runs.find((run) => run.id === followUp.run.id)?.status === "starting",
              ),
            ),
        );

        const projection = yield* threads.getThreadProjection(launched.threadId);
        const rootNode = projection.nodes.find(
          (node) => node.runId === followUp.run.id && node.kind === "root_turn",
        );
        assert.isNotNull(rootNode?.checkpointScopeId);
        assert.equal(
          projection.checkpointScopes.find((scope) => scope.id === rootNode?.checkpointScopeId)
            ?.cwd,
          "/repo-worktrees/feature",
        );
      }).pipe(Effect.provide(harness.layer));
    }),
);

it.effect.each([" /COMPACT ", "/logout"])(
  "uses the first conversation message for a title after %s",
  (nativeCommand) =>
    Effect.gen(function* () {
      const harness = makeHarness();
      yield* Effect.gen(function* () {
        const launches = yield* ThreadLaunch.ThreadLaunchService;
        const threads = yield* ThreadManagement.ThreadManagementService;
        const outbox = yield* EffectOutbox.EffectOutboxV2;
        const launched = yield* launches.launch({
          ...launchInput({
            command: "compact-title",
            thread: "compact-title-thread",
            message: nativeCommand,
          }),
          generateTitle: true,
        });
        assert.isUndefined(
          (yield* threads.getThreadProjection(launched.threadId)).thread.titleRegeneration,
        );
        assert.isFalse(
          (yield* outbox.listByCommandId(CommandId.make("compact-title:initial-message"))).some(
            (effect) => effect.request.type === "thread-title.generate",
          ),
        );
        const commandId = CommandId.make("compact-title-conversation");
        const messageId = MessageId.make("compact-title-conversation-message");
        yield* threads.dispatch({
          type: "message.dispatch",
          commandId,
          threadId: launched.threadId,
          messageId,
          createdBy: "user",
          creationSource: "web",
          text: "Fix the failing parser",
          attachments: [],
          dispatchMode: { type: "defer_start" },
        });
        assert.equal(
          (yield* threads.getThreadProjection(launched.threadId)).thread.titleRegeneration
            ?.requestId,
          commandId,
        );
        assert.deepEqual(
          (yield* outbox.listByCommandId(commandId))
            .filter((effect) => effect.request.type === "thread-title.generate")
            .map((effect) => effect.request),
          [{ type: "thread-title.generate", kind: { type: "initial", messageId } }],
        );
      }).pipe(Effect.provide(harness.layer));
    }),
);

it.effect("keeps native maintenance commands out of steering and restart messages", () =>
  Effect.gen(function* () {
    const harness = makeHarness();
    yield* Effect.gen(function* () {
      const launches = yield* ThreadLaunch.ThreadLaunchService;
      const threads = yield* ThreadManagement.ThreadManagementService;
      const outbox = yield* EffectOutbox.EffectOutboxV2;
      for (const scenario of [
        {
          name: "compact-steer",
          first: "Fix the parser",
          next: " /COMPACT ",
          mode: "steer_active",
        },
        {
          name: "compact-restart",
          first: "Fix the parser",
          next: "/compact",
          mode: "restart_active",
        },
        {
          name: "logout-steer",
          first: "Fix the parser",
          next: "/logout",
          mode: "steer_active",
        },
        {
          name: "logout-restart",
          first: "Fix the parser",
          next: "/logout",
          mode: "restart_active",
        },
        {
          name: "steer-logout",
          first: "/logout",
          next: "Continue with the parser",
          mode: "steer_active",
        },
        {
          name: "steer-compaction",
          first: "/compact",
          next: "Continue with the parser",
          mode: "steer_active",
        },
      ] as const) {
        const launched = yield* launches.launch(
          launchInput({
            command: `${scenario.name}:launch`,
            thread: scenario.name,
            message: scenario.first,
          }),
        );
        const before = yield* threads.getThreadProjection(launched.threadId);
        const targetRun = before.runs[0];
        if (targetRun === undefined) return yield* Effect.die("Launch must create a run");
        const commandId = CommandId.make(`${scenario.name}:message`);
        const failure = yield* threads
          .dispatch({
            type: "message.dispatch",
            commandId,
            threadId: launched.threadId,
            messageId: MessageId.make(`${scenario.name}:message`),
            createdBy: "user",
            creationSource: "web",
            text: scenario.next,
            attachments: [],
            dispatchMode: { type: scenario.mode, targetRunId: targetRun.id },
          })
          .pipe(Effect.flip);
        assert.include(
          String(failure.cause).toLowerCase(),
          scenario.name.includes("logout") ? "sign" : "context compaction",
        );
        const after = yield* threads.getThreadProjection(launched.threadId);
        assert.deepEqual(after.messages, before.messages);
        assert.deepEqual(after.thread.titleRegeneration, before.thread.titleRegeneration);
        assert.deepEqual(yield* outbox.listByCommandId(commandId), []);
      }
    }).pipe(Effect.provide(harness.layer));
  }),
);

it.effect("arms durable title generation after accepting the first message", () =>
  Effect.gen(function* () {
    const harness = makeHarness({
      generateTitle: (input) =>
        Effect.succeed({
          title: input.previousTitle === undefined ? "Generated title" : "Regenerated title",
        }),
    });
    yield* Effect.gen(function* () {
      const launches = yield* ThreadLaunch.ThreadLaunchService;
      const threads = yield* ThreadManagement.ThreadManagementService;
      const outbox = yield* EffectOutbox.EffectOutboxV2;
      const titleRegeneration = yield* ThreadTitleRegeneration.ThreadTitleRegenerationService;
      const input = {
        ...launchInput({
          command: "command:launch:title-generation",
          thread: "thread:launch:title-generation",
          message: "Generate my title",
        }),
        title: "Generate my title",
        generateTitle: true,
      };
      const launched = yield* launches.launch(input);
      const generationCommandId = CommandId.make("command:launch:title-generation:initial-message");

      const projection = yield* threads.getThreadProjection(launched.threadId);
      assert.equal(projection.thread.title, "Generate my title");
      assert.equal(projection.thread.titleRegeneration?.requestId, generationCommandId);
      assert.deepEqual(
        (yield* outbox.listByCommandId(generationCommandId)).map((effect) => effect.request),
        [
          {
            type: "thread-title.generate",
            kind: { type: "initial", messageId: MessageId.make("Generate my title:id") },
          },
        ],
      );
      yield* titleRegeneration.execute({
        threadId: launched.threadId,
        requestId: generationCommandId,
        kind: { type: "initial", messageId: MessageId.make("Generate my title:id") },
      });
      const generated = yield* threads.getThreadProjection(launched.threadId);
      assert.equal(generated.thread.title, "Generated title");
      assert.deepEqual(
        harness.generateThreadTitle.mock.calls[0]?.[0]?.modelSelection,
        DEFAULT_SERVER_SETTINGS.textGenerationModelSelection,
      );

      const manualRequestId = CommandId.make("command:title-generation:manual");
      yield* threads.dispatch({
        type: "thread.metadata.update",
        commandId: manualRequestId,
        threadId: launched.threadId,
        regenerateTitle: true,
      });
      assert.deepEqual(
        (yield* outbox.listByCommandId(manualRequestId)).map((effect) => effect.request),
        [{ type: "thread-title.generate", kind: { type: "regenerate" } }],
      );
      yield* titleRegeneration.execute({
        threadId: launched.threadId,
        requestId: manualRequestId,
        kind: { type: "regenerate" },
      });
      const regenerated = yield* threads.getThreadProjection(launched.threadId);
      assert.equal(regenerated.thread.title, "Regenerated title");
      assert.equal(
        harness.generateThreadTitle.mock.calls[1]?.[0]?.previousTitle,
        "Generated title",
      );

      yield* threads.dispatch({
        type: "thread.metadata.update",
        commandId: CommandId.make("command:title-generation:user-rename"),
        threadId: launched.threadId,
        title: "Keep my title",
      });
      const renamed = yield* threads.getThreadProjection(launched.threadId);
      yield* TestClock.adjust(Duration.seconds(1));
      yield* threads.dispatch({
        type: "thread.title.regeneration.complete",
        commandId: CommandId.make("command:title-generation:stale-completion"),
        threadId: launched.threadId,
        requestId: generationCommandId,
        title: "Stale generated title",
      });
      const afterStaleCompletion = yield* threads.getThreadProjection(launched.threadId);
      assert.equal(afterStaleCompletion.thread.title, "Keep my title");
      assert.equal(
        DateTime.toEpochMillis(afterStaleCompletion.thread.updatedAt),
        DateTime.toEpochMillis(renamed.thread.updatedAt),
      );
    }).pipe(Effect.provide(harness.layer));
  }),
);

it.effect("does not update a reused thread title when the initial message is rejected", () =>
  Effect.gen(function* () {
    const harness = makeHarness();
    yield* Effect.gen(function* () {
      const launches = yield* ThreadLaunch.ThreadLaunchService;
      const threads = yield* ThreadManagement.ThreadManagementService;
      const outbox = yield* EffectOutbox.EffectOutboxV2;
      const threadId = ThreadId.make("thread:launch:reused-title-failure");
      yield* threads.dispatch({
        type: "thread.create",
        commandId: CommandId.make("command:launch:reused-title-failure:create"),
        threadId,
        projectId,
        title: "Original title",
        modelSelection,
        runtimeMode: "full-access",
        interactionMode: "default",
        branch: null,
        worktreePath: null,
        createdBy: "user",
        creationSource: "web",
      });

      const commandId = CommandId.make("command:launch:reused-title-failure");
      const failed = yield* launches
        .launch({
          ...launchInput({
            command: commandId,
            thread: threadId,
            message: "Generate a provisional title",
          }),
          reuseExistingThread: true,
          title: "Generate a provisional title",
          generateTitle: true,
          modelSelection: {
            instanceId: ProviderInstanceId.make("missing-provider"),
            model: "missing-model",
          },
        })
        .pipe(Effect.exit);

      assert.isTrue(Exit.isFailure(failed));
      const projection = yield* threads.getThreadProjection(threadId);
      assert.equal(projection.thread.title, "Original title");
      assert.isUndefined(projection.thread.titleRegeneration);
      assert.isEmpty(projection.messages);
      assert.isEmpty(yield* outbox.listByCommandId(CommandId.make(`${commandId}:initial-message`)));
    }).pipe(Effect.provide(harness.layer));
  }),
);

it.effect("generates an initial title for an attachment-only message", () =>
  Effect.gen(function* () {
    const harness = makeHarness();
    yield* Effect.gen(function* () {
      const launches = yield* ThreadLaunch.ThreadLaunchService;
      const titleRegeneration = yield* ThreadTitleRegeneration.ThreadTitleRegenerationService;
      const messageId = MessageId.make("message:image-only");
      const input = {
        ...launchInput({
          command: "command:launch:image-only",
          thread: "thread:launch:image-only",
        }),
        title: "Image: screenshot.png",
        generateTitle: true,
        initialMessage: {
          messageId,
          text: "",
          attachments: [
            {
              type: "image" as const,
              id: "attachment-image-only",
              name: "screenshot.png",
              mimeType: "image/png",
              sizeBytes: 128,
            },
          ],
        },
      };

      const launched = yield* launches.launch(input);
      yield* titleRegeneration.execute({
        threadId: launched.threadId,
        requestId: CommandId.make("command:launch:image-only:initial-message"),
        kind: { type: "initial", messageId },
      });

      assert.equal(harness.generateThreadTitle.mock.calls[0]?.[0]?.message, "");
      assert.equal(
        harness.generateThreadTitle.mock.calls[0]?.[0]?.attachments?.[0]?.name,
        "screenshot.png",
      );
    }).pipe(Effect.provide(harness.layer));
  }),
);

it.effect("uses the available source control writer for generated worktree branches", () =>
  Effect.gen(function* () {
    const writerInstanceId = ProviderInstanceId.make("source-control-writer");
    const writerModelSelection = {
      instanceId: writerInstanceId,
      model: "branch-writer-model",
    } as const;
    const harness = makeHarness({
      serverSettings: {
        providerInstances: {
          [writerInstanceId]: {
            driver: ProviderDriverKind.make("codex"),
            config: {},
          },
        },
        sourceControlWriterModelSelection: writerModelSelection,
      },
      providers: [
        {
          instanceId: writerInstanceId,
          driver: ProviderDriverKind.make("codex"),
          enabled: true,
          installed: true,
          version: null,
          status: "ready",
          auth: { status: "authenticated" },
          checkedAt: "2026-07-28T00:00:00.000Z",
          availability: "available",
          models: [],
          slashCommands: [],
          skills: [],
        },
      ],
    });

    yield* Effect.gen(function* () {
      const launches = yield* ThreadLaunch.ThreadLaunchService;
      yield* launches.launch(
        launchInput({
          command: "command:launch:source-control-writer",
          thread: "thread:launch:source-control-writer",
          message: "Generate a branch with the configured writer",
          workspace: { type: "worktree", baseRef: "main" },
        }),
      );
      yield* waitUntil(() => Effect.sync(() => harness.generateBranchName.mock.calls.length === 1));
      assert.deepEqual(
        harness.generateBranchName.mock.calls[0]?.[0]?.modelSelection,
        writerModelSelection,
      );
    }).pipe(Effect.provide(harness.layer));
  }),
);

it.effect("falls back when the source control writer is unavailable", () =>
  Effect.gen(function* () {
    const writerInstanceId = ProviderInstanceId.make("missing-source-control-writer");
    const harness = makeHarness({
      serverSettings: {
        providerInstances: {
          [writerInstanceId]: {
            driver: ProviderDriverKind.make("missing-driver"),
            config: {},
          },
        },
        sourceControlWriterModelSelection: {
          instanceId: writerInstanceId,
          model: "missing-branch-writer-model",
        },
      },
      providers: [],
    });

    yield* Effect.gen(function* () {
      const launches = yield* ThreadLaunch.ThreadLaunchService;
      yield* launches.launch(
        launchInput({
          command: "command:launch:source-control-writer-fallback",
          thread: "thread:launch:source-control-writer-fallback",
          message: "Generate a branch with the available writer",
          workspace: { type: "worktree", baseRef: "main" },
        }),
      );
      yield* waitUntil(() => Effect.sync(() => harness.generateBranchName.mock.calls.length === 1));
      assert.deepEqual(
        harness.generateBranchName.mock.calls[0]?.[0]?.modelSelection,
        DEFAULT_SERVER_SETTINGS.textGenerationModelSelection,
      );
    }).pipe(Effect.provide(harness.layer));
  }),
);

it.effect("runs a Scratch thread launched at the root in its own folder", () =>
  Effect.gen(function* () {
    // Only `projectId` stands in for the Scratch project here.
    const claimed: Array<{ readonly threadId: ThreadId; readonly text: string }> = [];
    const harness = makeHarness({
      managedFolders: Layer.mock(ManagedProjectFolders.ManagedProjectFolders)({
        namedProjectsRoot: "/projects",
        folderForThread: (input) =>
          Effect.sync(() => {
            if (input.projectId !== projectId) return Option.none();
            claimed.push({ threadId: input.threadId, text: input.text });
            return Option.some(`/scratch/folder-${claimed.length}`);
          }),
      }),
    });
    yield* Effect.gen(function* () {
      const launches = yield* ThreadLaunch.ThreadLaunchService;
      const threads = yield* ThreadManagement.ThreadManagementService;
      const input = launchInput({
        command: "command:launch:scratch",
        thread: "thread:launch:scratch",
        message: "Convert these PNGs",
      });
      const launched = yield* launches.launch(input);
      assert.deepEqual(claimed, [{ threadId: launched.threadId, text: "Convert these PNGs" }]);
      assert.equal(launched.projection.thread.worktreePath, "/scratch/folder-1");
      yield* waitUntil(() => Effect.sync(() => harness.runSetup.mock.calls.length === 1));
      assert.equal(harness.runSetup.mock.calls[0]?.[0]?.worktreePath, "/scratch/folder-1");
      assert.equal(harness.createWorktree.mock.calls.length, 0);

      // A retry replays the first attempt and claims no second folder.
      const retried = yield* launches.launch(input);
      assert.isTrue(retried.resumed);
      assert.lengthOf(claimed, 1);
      assert.equal(
        (yield* threads.getThreadProjection(launched.threadId)).thread.worktreePath,
        "/scratch/folder-1",
      );

      const other = yield* launches.launch({
        ...launchInput({
          command: "command:launch:scratch-other",
          thread: "thread:launch:scratch-other",
          message: "Elsewhere",
        }),
        projectId: otherProjectId,
      });
      assert.lengthOf(claimed, 1);
      assert.isNull(other.projection.thread.worktreePath);
    }).pipe(Effect.provide(harness.layer));
  }),
);

it.effect("names the worktree itself when the client provides no branch", () =>
  Effect.gen(function* () {
    const harness = makeHarness();
    yield* Effect.gen(function* () {
      const launches = yield* ThreadLaunch.ThreadLaunchService;
      const threads = yield* ThreadManagement.ThreadManagementService;
      const launched = yield* launches.launch(
        launchInput({
          command: "command:launch:server-named-branch",
          thread: "thread:launch:server-named-branch",
          message: "Build the feature",
          workspace: { type: "worktree", baseRef: "main" },
        }),
      );
      yield* waitUntil(() => Effect.sync(() => harness.createWorktree.mock.calls.length === 1));
      assert.match(
        harness.createWorktree.mock.calls[0]?.[0]?.newRefName ?? "",
        /^t3\/[0-9a-f]{8}$/u,
      );
      yield* waitUntil(() =>
        threads
          .getThreadProjection(launched.threadId)
          .pipe(Effect.map((projection) => projection.thread.branch === "generated-branch")),
      );
    }).pipe(Effect.provide(harness.layer));
  }),
);

it.effect("renames a temporary t3/<hash> branch off the provisioning critical path", () =>
  Effect.gen(function* () {
    const branchNameStarted = yield* Deferred.make<void>();
    const allowBranchName = yield* Deferred.make<void>();
    const harness = makeHarness({
      createWorktree: (input) =>
        Effect.succeed({
          worktree: { path: "/repo-worktrees/temp", refName: input.newRefName, headSha: "abc" },
        } as never),
      generateBranchName: () =>
        Deferred.succeed(branchNameStarted, undefined).pipe(
          Effect.andThen(Deferred.await(allowBranchName)),
          Effect.as({ branch: "generated-branch" }),
        ),
    });
    yield* Effect.gen(function* () {
      const launches = yield* ThreadLaunch.ThreadLaunchService;
      const threads = yield* ThreadManagement.ThreadManagementService;
      const launched = yield* launches.launch(
        launchInput({
          command: "command:launch:temp-branch",
          thread: "thread:launch:temp-branch",
          message: "Build the feature",
          workspace: { type: "worktree", baseRef: "main", branch: "t3/abcd1234" },
        }),
      );
      yield* Deferred.await(branchNameStarted);
      assert.equal(harness.createWorktree.mock.calls[0]?.[0]?.newRefName, "t3/abcd1234");
      yield* waitUntil(() =>
        threads
          .getThreadProjection(launched.threadId)
          .pipe(Effect.map((projection) => projection.runs[0]?.status === "starting")),
      );
      assert.equal(
        (yield* threads.getThreadProjection(launched.threadId)).thread.branch,
        "t3/abcd1234",
      );
      yield* Deferred.succeed(allowBranchName, undefined);
      yield* waitUntil(() =>
        threads
          .getThreadProjection(launched.threadId)
          .pipe(Effect.map((projection) => projection.thread.branch === "generated-branch")),
      );
      assert.deepEqual(harness.renameBranch.mock.calls[0]?.[0], {
        cwd: "/repo-worktrees/temp",
        oldBranch: "t3/abcd1234",
        newBranch: "generated-branch",
      });
    }).pipe(Effect.provide(harness.layer));
  }),
);

it.effect("provisions under t3-<hash> when a plain t3 branch blocks t3/*", () =>
  Effect.gen(function* () {
    const harness = makeHarness({
      hasCommit: (input) => Effect.succeed(input.refName === "refs/heads/t3"),
      createWorktree: (input) =>
        Effect.succeed({
          worktree: { path: "/repo-worktrees/temp", refName: input.newRefName, headSha: "abc" },
        } as never),
    });
    yield* Effect.gen(function* () {
      const launches = yield* ThreadLaunch.ThreadLaunchService;
      const threads = yield* ThreadManagement.ThreadManagementService;
      const launched = yield* launches.launch(
        launchInput({
          command: "command:launch:blocked-namespace",
          thread: "thread:launch:blocked-namespace",
          message: "Build the feature",
          workspace: { type: "worktree", baseRef: "main", branch: "t3/abcd1234" },
        }),
      );
      yield* waitUntil(() =>
        threads
          .getThreadProjection(launched.threadId)
          .pipe(Effect.map((projection) => projection.thread.branch === "generated-branch")),
      );
      assert.equal(harness.createWorktree.mock.calls[0]?.[0]?.newRefName, "t3-abcd1234");
      assert.equal(harness.renameBranch.mock.calls[0]?.[0]?.oldBranch, "t3-abcd1234");
    }).pipe(Effect.provide(harness.layer));
  }),
);

it.effect("keeps an explicit branch name instead of generating one", () =>
  Effect.gen(function* () {
    const harness = makeHarness();
    yield* Effect.gen(function* () {
      const launches = yield* ThreadLaunch.ThreadLaunchService;
      yield* launches.launch(
        launchInput({
          command: "command:launch:explicit-branch",
          thread: "thread:launch:explicit-branch",
          message: "Build the feature",
          workspace: { type: "worktree", baseRef: "main", branch: "my-feature" },
        }),
      );
      yield* waitUntil(() => Effect.sync(() => harness.createWorktree.mock.calls.length === 1));
      assert.equal(harness.generateBranchName.mock.calls.length, 0);
      assert.equal(harness.createWorktree.mock.calls[0]?.[0]?.newRefName, "my-feature");
    }).pipe(Effect.provide(harness.layer));
  }),
);

it.effect("keeps the temporary branch when branch generation fails", () =>
  Effect.gen(function* () {
    const harness = makeHarness({
      createWorktree: (input) =>
        Effect.succeed({
          worktree: { path: "/repo-worktrees/temp", refName: input.newRefName, headSha: "abc" },
        } as never),
      generateBranchName: () => Effect.die("branch generation is down"),
    });
    yield* Effect.gen(function* () {
      const launches = yield* ThreadLaunch.ThreadLaunchService;
      const threads = yield* ThreadManagement.ThreadManagementService;
      const launched = yield* launches.launch(
        launchInput({
          command: "command:launch:branch-fallback",
          thread: "thread:launch:branch-fallback",
          message: "Build the feature",
          workspace: { type: "worktree", baseRef: "main", branch: "t3/abcd1234" },
        }),
      );
      yield* waitUntil(() => Effect.sync(() => harness.generateBranchName.mock.calls.length === 1));
      assert.equal(harness.createWorktree.mock.calls[0]?.[0]?.newRefName, "t3/abcd1234");
      yield* waitUntil(() =>
        threads
          .getThreadProjection(launched.threadId)
          .pipe(Effect.map((projection) => projection.runs[0]?.status === "starting")),
      );
      assert.equal(harness.renameBranch.mock.calls.length, 0);
      assert.equal(
        (yield* threads.getThreadProjection(launched.threadId)).thread.branch,
        "t3/abcd1234",
      );
    }).pipe(Effect.provide(harness.layer));
  }),
);

it.effect("renames a temporary branch on an existing worktree to a generated name", () =>
  Effect.gen(function* () {
    const harness = makeHarness();
    yield* Effect.gen(function* () {
      const launches = yield* ThreadLaunch.ThreadLaunchService;
      const threads = yield* ThreadManagement.ThreadManagementService;
      const launched = yield* launches.launch(
        launchInput({
          command: "command:launch:existing-worktree-rename",
          thread: "thread:launch:existing-worktree-rename",
          message: "Build the feature",
          workspace: {
            type: "existing_worktree",
            worktreePath: "/repo-worktrees/t3-abcd1234",
            branch: "t3/abcd1234",
          },
        }),
      );
      yield* waitUntil(() =>
        threads
          .getThreadProjection(launched.threadId)
          .pipe(Effect.map((projection) => projection.thread.branch === "generated-branch")),
      );
      assert.deepEqual(harness.renameBranch.mock.calls[0]?.[0], {
        cwd: "/repo-worktrees/t3-abcd1234",
        oldBranch: "t3/abcd1234",
        newBranch: "generated-branch",
      });
    }).pipe(Effect.provide(harness.layer));
  }),
);

it.effect("shows the fetch diagnosis when preparing a worktree from origin fails", () => {
  const detail =
    "Git could not authenticate with the remote. Check Git credentials or SSH access on the server, then retry.";
  const harness = makeHarness({
    fetchRemote: () =>
      Effect.fail(
        new GitCommandError({
          operation: "GitVcsDriver.fetchRemote",
          command: "git",
          cwd: project.workspaceRoot,
          detail,
          exitCode: 128,
        }),
      ),
  });
  return Effect.gen(function* () {
    const launches = yield* ThreadLaunch.ThreadLaunchService;
    const threads = yield* ThreadManagement.ThreadManagementService;
    const launched = yield* launches.launch(
      launchInput({
        command: "command:launch:fetch-failure",
        thread: "thread:launch:fetch-failure",
        message: "Start from origin",
        workspace: { type: "worktree", baseRef: "main", startFromOrigin: true },
      }),
    );
    yield* threads.streamStoredEventsFrom({ threadId: launched.threadId }).pipe(
      Stream.filter(
        (stored) => stored.event.type === "run.updated" && stored.event.payload.status === "failed",
      ),
      Stream.runHead,
    );
    const projection = yield* threads.getThreadProjection(launched.threadId);
    assert.equal(projection.messages[0]?.text, "Start from origin");
    assert.equal(projection.runs[0]?.status, "failed");
    assert.equal(projection.thread.worktreePath, null);
    assert.equal(
      projection.turnItems.find((item) => item.type === "command_execution")?.status,
      "failed",
    );
    assert.include(
      projection.turnItems.find((item) => item.type === "error")?.failure.message ?? "",
      detail,
    );
    assert.equal(harness.createWorktree.mock.calls.length, 0);
    assert.equal(harness.runSetup.mock.calls.length, 0);
  }).pipe(Effect.provide(harness.layer));
});

it.effect("retries a failed workspace preparation on the same run", () => {
  let fetchFailures = 1;
  const harness = makeHarness({
    fetchRemote: () =>
      fetchFailures-- > 0
        ? Effect.fail(
            new GitCommandError({
              operation: "GitVcsDriver.fetchRemote",
              command: "git",
              cwd: project.workspaceRoot,
              detail: "Git could not update a local reference.",
              exitCode: 1,
            }),
          )
        : Effect.void,
  });
  return Effect.gen(function* () {
    const launches = yield* ThreadLaunch.ThreadLaunchService;
    const outbox = yield* EffectOutbox.EffectOutboxV2;
    const threads = yield* ThreadManagement.ThreadManagementService;
    const launched = yield* launches.launch(
      launchInput({
        command: "command:launch:retry",
        thread: "thread:launch:retry",
        message: "Retry me",
        workspace: { type: "worktree", baseRef: "main", startFromOrigin: true },
      }),
    );
    yield* waitUntil(() =>
      threads
        .getThreadProjection(launched.threadId)
        .pipe(Effect.map((projection) => projection.runs[0]?.status === "failed")),
    );
    const failed = yield* threads.getThreadProjection(launched.threadId);
    const runId = failed.runs[0]!.id;
    assert.equal(
      failed.turnItems.find((item) => item.type === "error")?.failure.code,
      ORCHESTRATION_V2_WORKSPACE_PREPARATION_FAILURE_CODE,
    );

    const retry = {
      commandId: CommandId.make("command:launch:retry:1"),
      threadId: launched.threadId,
      runId,
    };
    yield* launches.retryPreparation(retry);
    yield* waitUntil(() =>
      outbox
        .listByCommandId(CommandId.make("command:launch:retry:1:release"))
        .pipe(Effect.map((effects) => effects.length === 1)),
    );
    const retried = yield* threads.getThreadProjection(launched.threadId);
    assert.equal(retried.runs.length, 1);
    assert.equal(retried.runs[0]?.status, "starting");
    assert.equal(retried.thread.worktreePath, "/repo-worktrees/feature");
    assert.equal(retried.turnItems.find((item) => item.type === "error")?.status, "cancelled");
    assert.equal(
      retried.turnItems.find((item) => item.type === "command_execution")?.status,
      "completed",
    );
    assert.equal(harness.createWorktree.mock.calls.length, 1);

    // The run left preparation, so a second retry has nothing to do.
    const rejected = yield* launches
      .retryPreparation({ ...retry, commandId: CommandId.make("command:launch:retry:2") })
      .pipe(Effect.flip);
    assert.equal(rejected._tag, "OrchestratorDispatchError");
  }).pipe(Effect.provide(harness.layer));
});

it.effect.each(["fails", "is interrupted"] as const)(
  "holds a message queued during setup when the workspace preparation %s",
  (ending) =>
    Effect.gen(function* () {
      const fetchEntered = yield* Deferred.make<void>();
      const allowFetch = yield* Deferred.make<void>();
      let fetchFailures = 1;
      const harness = makeHarness({
        fetchRemote: () =>
          fetchFailures-- <= 0
            ? Effect.void
            : Deferred.succeed(fetchEntered, undefined).pipe(
                Effect.andThen(Deferred.await(allowFetch)),
                Effect.andThen(
                  Effect.fail(
                    new GitCommandError({
                      operation: "GitVcsDriver.fetchRemote",
                      command: "git",
                      cwd: project.workspaceRoot,
                      detail: "Git could not reach the remote.",
                      exitCode: 128,
                    }),
                  ),
                ),
              ),
      });
      yield* Effect.gen(function* () {
        const launches = yield* ThreadLaunch.ThreadLaunchService;
        const threads = yield* ThreadManagement.ThreadManagementService;
        const launched = yield* launches.launch(
          launchInput({
            command: `command:launch:queued-setup-${ending}`,
            thread: `thread:launch:queued-setup-${ending}`,
            message: "First message",
            workspace: { type: "worktree", baseRef: "main", startFromOrigin: true },
          }),
        );
        yield* Deferred.await(fetchEntered);
        const queued = yield* threads.sendToThread({
          projectId,
          commandId: CommandId.make(`command:launch:queued-setup-${ending}:follow-up`),
          threadId: launched.threadId,
          messageId: MessageId.make(`message:launch:queued-setup-${ending}:follow-up`),
          text: "Sent during setup",
          attachments: [],
          mode: "queue",
          createdBy: "user",
          creationSource: "web",
        });
        assert.equal(queued.delivery, "queued");
        if (ending === "fails") {
          yield* Deferred.succeed(allowFetch, undefined);
        } else {
          // An agent stop does not ask to hold the queue.
          yield* threads.dispatch({
            type: "run.interrupt",
            commandId: CommandId.make("command:launch:queued-setup-interrupt"),
            threadId: launched.threadId,
            runId: launched.projection.runs[0]!.id,
          });
        }

        // Without its worktree the follow-up would start in the project checkout.
        yield* threads.streamStoredEventsFrom({ threadId: launched.threadId }).pipe(
          Stream.filter(
            (stored) =>
              stored.event.type === "run.updated" && stored.event.payload.queueHeld === true,
          ),
          Stream.runHead,
        );
        const projection = yield* threads.getThreadProjection(launched.threadId);
        assert.equal(projection.runs.at(-1)?.status, "queued");
        assert.equal(projection.thread.worktreePath, null);

        if (ending === "fails") {
          // A successful retry lets the held message follow the first turn again.
          yield* launches.retryPreparation({
            commandId: CommandId.make("command:launch:queued-setup-retry"),
            threadId: launched.threadId,
            runId: launched.projection.runs[0]!.id,
          });
          const retried = yield* threads.getThreadProjection(launched.threadId);
          assert.equal(retried.runs.at(-1)?.queueHeld, false);
        }
      }).pipe(Effect.provide(harness.layer));
    }),
);

it.effect.each(["failed", "interrupted"] as const)(
  "holds a message queued during setup that arrives after the setup %s",
  (ending) => {
    let fetchEntered: Deferred.Deferred<void> | null = null;
    const harness = makeHarness({
      fetchRemote: () =>
        ending === "interrupted"
          ? Deferred.succeed(fetchEntered!, undefined).pipe(Effect.andThen(Effect.never))
          : Effect.fail(
              new GitCommandError({
                operation: "GitVcsDriver.fetchRemote",
                command: "git",
                cwd: project.workspaceRoot,
                detail: "Git could not reach the remote.",
                exitCode: 128,
              }),
            ),
    });
    return Effect.gen(function* () {
      fetchEntered = yield* Deferred.make<void>();
      const launches = yield* ThreadLaunch.ThreadLaunchService;
      const threads = yield* ThreadManagement.ThreadManagementService;
      const launched = yield* launches.launch(
        launchInput({
          command: `command:launch:late-queued-follow-up-${ending}`,
          thread: `thread:launch:late-queued-follow-up-${ending}`,
          message: "First message",
          workspace: { type: "worktree", baseRef: "main", startFromOrigin: true },
        }),
      );
      if (ending === "interrupted") {
        yield* Deferred.await(fetchEntered);
        // An agent stop does not ask to hold the queue.
        yield* threads.dispatch({
          type: "run.interrupt",
          commandId: CommandId.make("command:launch:late-queued-follow-up:interrupt"),
          threadId: launched.threadId,
          runId: launched.projection.runs[0]!.id,
        });
      }
      yield* threads.streamStoredEventsFrom({ threadId: launched.threadId }).pipe(
        Stream.filter(
          (stored) => stored.event.type === "run.updated" && stored.event.payload.status === ending,
        ),
        Stream.runHead,
      );

      // Queued while setup ran, delivered only after it failed: it must not start
      // in the project checkout.
      const queued = yield* threads.sendToThread({
        projectId,
        commandId: CommandId.make(`command:launch:late-queued-follow-up-${ending}:send`),
        threadId: launched.threadId,
        messageId: MessageId.make(`message:launch:late-queued-follow-up-${ending}:send`),
        text: "Sent during setup",
        attachments: [],
        mode: "queue",
        createdBy: "user",
        creationSource: "web",
      });
      assert.equal(queued.delivery, "queued");
      const projection = yield* threads.getThreadProjection(launched.threadId);
      assert.equal(projection.runs.at(-1)?.status, "queued");
      assert.equal(projection.runs.at(-1)?.queueHeld, true);
    }).pipe(Effect.provide(harness.layer));
  },
);

it.effect("a retry reuses a recorded worktree without undoing its branch rename", () => {
  let setupFailures = 1;
  const harness = makeHarness({
    runSetup: () =>
      setupFailures-- > 0
        ? Effect.fail(new Error("setup failed") as never)
        : Effect.succeed({ status: "no-script" as const }),
  });
  return Effect.gen(function* () {
    const launches = yield* ThreadLaunch.ThreadLaunchService;
    const outbox = yield* EffectOutbox.EffectOutboxV2;
    const threads = yield* ThreadManagement.ThreadManagementService;
    const tracker = yield* WorktreeSetupTracker.WorktreeSetupTracker;
    const launched = yield* launches.launch(
      launchInput({
        command: "command:launch:reuse",
        thread: "thread:launch:reuse",
        message: "Reuse the worktree",
        workspace: { type: "worktree", baseRef: "main" },
      }),
    );
    yield* waitUntil(() =>
      threads
        .getThreadProjection(launched.threadId)
        .pipe(
          Effect.map(
            (projection) =>
              projection.runs[0]?.status === "failed" &&
              projection.thread.branch === "generated-branch",
          ),
        ),
    );
    const failed = yield* threads.getThreadProjection(launched.threadId);
    assert.equal(failed.thread.worktreePath, "/repo-worktrees/feature");

    yield* launches.retryPreparation({
      commandId: CommandId.make("command:launch:reuse:retry"),
      threadId: launched.threadId,
      runId: failed.runs[0]!.id,
    });
    yield* waitUntil(() =>
      outbox
        .listByCommandId(CommandId.make("command:launch:reuse:retry:release"))
        .pipe(Effect.map((effects) => effects.length === 1)),
    );
    const retried = yield* threads.getThreadProjection(launched.threadId);
    assert.equal(retried.runs[0]?.status, "starting");
    // The retry neither checks out again nor puts back the temporary branch.
    assert.equal(harness.createWorktree.mock.calls.length, 1);
    assert.equal(harness.renameBranch.mock.calls.length, 1);
    assert.equal(retried.thread.branch, "generated-branch");
    assert.equal(retried.thread.worktreePath, "/repo-worktrees/feature");
    // Clients see the retry's setup, not the failed one it replaced.
    const snapshot = yield* tracker.get(launched.threadId);
    assert.equal(snapshot?.phase, "done");
    assert.deepEqual(
      snapshot?.stages.map((stage) => stage.id),
      ["setup-script", "agent"],
    );
  }).pipe(Effect.provide(harness.layer));
});

it.effect("removes a worktree that failed before the thread recorded it", () => {
  const harness = makeHarness({
    // A checkout that dies after claiming its directory.
    createWorktree: (_input, options) =>
      (options?.progress?.onWorktreeClaimed?.("/repo-worktrees/partial") ?? Effect.void).pipe(
        Effect.andThen(Effect.fail(new Error("checkout failed") as never)),
      ),
  });
  return Effect.gen(function* () {
    const launches = yield* ThreadLaunch.ThreadLaunchService;
    const threads = yield* ThreadManagement.ThreadManagementService;
    const launched = yield* launches.launch(
      launchInput({
        command: "command:launch:partial-worktree",
        thread: "thread:launch:partial-worktree",
        message: "Partial checkout",
        workspace: { type: "worktree", baseRef: "main" },
      }),
    );
    yield* waitUntil(() =>
      threads
        .getThreadProjection(launched.threadId)
        .pipe(Effect.map((projection) => projection.runs[0]?.status === "failed")),
    );
    const projection = yield* threads.getThreadProjection(launched.threadId);
    // Unrecorded, so a retry would create a second checkout beside it.
    assert.equal(projection.thread.worktreePath, null);
    assert.deepEqual(
      harness.removeWorktree.mock.calls.map(([input]) => input.path),
      ["/repo-worktrees/partial"],
    );
  }).pipe(Effect.provide(harness.layer));
});

it.effect.each(["worktree", "setup"] as const)(
  "%s failure keeps the thread and message visible and emits failure items",
  (failurePoint) =>
    Effect.gen(function* () {
      const failure = new Error(`${failurePoint} failed`);
      const harness = makeHarness(
        failurePoint === "worktree"
          ? { createWorktree: () => Effect.fail(failure as never) }
          : { runSetup: () => Effect.fail(failure as never) },
      );
      yield* Effect.gen(function* () {
        const launches = yield* ThreadLaunch.ThreadLaunchService;
        const threads = yield* ThreadManagement.ThreadManagementService;
        const input = launchInput({
          command: `command:launch:${failurePoint}-failure`,
          thread: `thread:launch:${failurePoint}-failure`,
          message: `Fail during ${failurePoint}`,
          workspace: { type: "worktree", baseRef: "main" },
        });
        const launched = yield* launches.launch(input);
        yield* waitUntil(() =>
          threads
            .getThreadProjection(launched.threadId)
            .pipe(Effect.map((projection) => projection.runs[0]?.status === "failed")),
        );
        const projection = yield* threads.getThreadProjection(launched.threadId);
        assert.equal(projection.messages[0]?.text, `Fail during ${failurePoint}`);
        assert.equal(projection.runs[0]?.status, "failed");
        assert.equal(
          projection.turnItems.find((item) => item.type === "command_execution")?.status,
          "failed",
        );
        assert.match(
          projection.turnItems.find((item) => item.type === "error")?.failure.message ?? "",
          new RegExp(`${failurePoint} failed`, "u"),
        );
      }).pipe(Effect.provide(harness.layer));
    }),
);

it.effect("replays a server-allocated launch", () =>
  Effect.gen(function* () {
    const setupEntered = yield* Deferred.make<void>();
    const allowSetup = yield* Deferred.make<void>();
    const harness = makeHarness({
      runSetup: () =>
        Deferred.succeed(setupEntered, undefined).pipe(
          Effect.andThen(Deferred.await(allowSetup)),
          Effect.as({ status: "no-script" as const }),
        ),
    });
    yield* Effect.gen(function* () {
      const launches = yield* ThreadLaunch.ThreadLaunchService;
      const threads = yield* ThreadManagement.ThreadManagementService;
      const { threadId: _unusedThreadId, ...rest } = launchInput({
        command: "command:launch:allocated-retry",
        thread: "unused",
        message: "Only once",
      });
      const first = yield* launches.launch(rest);
      yield* Deferred.await(setupEntered);
      const retry = yield* launches.launch(rest);
      assert.equal(first.threadId, retry.threadId);
      assert.isFalse(first.resumed);
      assert.isTrue(retry.resumed);
      assert.equal(harness.runSetup.mock.calls.length, 1);
      assert.equal(retry.projection.messages.length, 1);
      assert.equal(retry.projection.runs.length, 1);
      assert.equal(retry.projection.messages[0]?.id, first.projection.messages[0]?.id);
      assert.equal(retry.projection.runs[0]?.id, first.projection.runs[0]?.id);
      yield* Deferred.succeed(allowSetup, undefined);
      yield* threads.streamStoredEventsFrom({ threadId: first.threadId }).pipe(
        Stream.filter(
          (stored) =>
            stored.commandId === CommandId.make(`${rest.commandId}:release`) &&
            stored.event.type === "run.updated",
        ),
        Stream.runHead,
      );
      const settled = yield* launches.launch(rest);
      assert.equal(settled.threadId, first.threadId);
      assert.isTrue(settled.resumed);
      assert.equal(settled.projection.messages[0]?.id, first.projection.messages[0]?.id);
      assert.equal(settled.projection.runs[0]?.id, first.projection.runs[0]?.id);
      assert.equal(harness.runSetup.mock.calls.length, 1);
    }).pipe(Effect.provide(harness.layer));
  }),
);

it.effect("refuses a launch when the project disables the model's provider instance", () => {
  const harness = makeHarness({
    serverSettings: {
      projectSettingsOverrides: {
        [projectId]: { providerInstances: { [modelSelection.instanceId]: { enabled: false } } },
      },
    },
  });
  return Effect.gen(function* () {
    const launches = yield* ThreadLaunch.ThreadLaunchService;
    const failed = yield* launches
      .launch(
        launchInput({
          command: "command:launch:project-disabled-instance",
          thread: "thread:launch:project-disabled-instance",
          message: "Hello",
        }),
      )
      .pipe(Effect.flip);
    if (failed._tag !== "ThreadManagementProviderInstanceDisabledError") {
      assert.fail(`expected ThreadManagementProviderInstanceDisabledError, got ${failed._tag}`);
    }
    assert.equal(failed.projectId, projectId);
    assert.equal(failed.instanceId, modelSelection.instanceId);
  }).pipe(Effect.provide(harness.layer));
});

it.effect("replays an accepted launch after the project disables the instance", () => {
  const harness = makeHarness();
  return Effect.gen(function* () {
    const launches = yield* ThreadLaunch.ThreadLaunchService;
    const settings = yield* ServerSettings.ServerSettingsService;
    const input = launchInput({
      command: "command:launch:retry-after-disable",
      thread: "thread:launch:retry-after-disable",
      message: "Hello",
    });
    const first = yield* launches.launch(input);

    yield* settings.updateSettings({
      projectSettingsOverrides: {
        [projectId]: { providerInstances: { [modelSelection.instanceId]: { enabled: false } } },
      },
    });

    const retry = yield* launches.launch(input);
    assert.equal(retry.threadId, first.threadId);
    assert.isTrue(retry.resumed);

    const fresh = yield* launches
      .launch(
        launchInput({
          command: "command:launch:fresh-after-disable",
          thread: "thread:launch:fresh-after-disable",
          message: "Hello",
        }),
      )
      .pipe(Effect.flip);
    if (fresh._tag !== "ThreadManagementProviderInstanceDisabledError") {
      assert.fail(`expected ThreadManagementProviderInstanceDisabledError, got ${fresh._tag}`);
    }
  }).pipe(Effect.provide(harness.layer));
});

it.effect("rejects a server-allocated launch replay with a mismatching thread id", () => {
  const harness = makeHarness();
  return Effect.gen(function* () {
    const launches = yield* ThreadLaunch.ThreadLaunchService;
    const { threadId: _unusedThreadId, ...rest } = launchInput({
      command: "command:launch:allocated-mismatch",
      thread: "unused",
      message: "Mismatch",
    });
    const first = yield* launches.launch(rest);
    const failed = yield* launches
      .launch({
        ...rest,
        threadId: ThreadId.make("thread:launch:allocated-mismatch"),
      })
      .pipe(Effect.flip);
    assert.notEqual(first.threadId, ThreadId.make("thread:launch:allocated-mismatch"));
    if (failed._tag !== "ThreadLaunchError") {
      assert.fail(`expected ThreadLaunchError, got ${failed._tag}`);
    }
    assert.equal(failed.operation, "create-thread");
    assert.include(String(failed.cause), "cannot be replayed");
  }).pipe(Effect.provide(harness.layer));
});

it.effect("rejects a server-allocated launch receipt from another project", () => {
  const harness = makeHarness();
  return Effect.gen(function* () {
    const launches = yield* ThreadLaunch.ThreadLaunchService;
    const { threadId: _unusedThreadId, ...rest } = launchInput({
      command: "command:launch:allocated-wrong-project",
      thread: "unused",
      message: "Wrong project",
    });
    const first = yield* launches.launch(rest);
    const failed = yield* launches
      .launch({
        ...rest,
        projectId: otherProjectId,
      })
      .pipe(Effect.flip);
    if (failed._tag !== "ThreadLaunchError") {
      assert.fail(`expected ThreadLaunchError, got ${failed._tag}`);
    }
    assert.equal(failed.operation, "resolve-project");
    assert.equal(failed.threadId, first.threadId);
    assert.equal(failed.cause, "Project identity changed.");
  }).pipe(Effect.provide(harness.layer));
});

it.effect("rejects a server-allocated launch retry after the thread is deleted", () => {
  const harness = makeHarness();
  return Effect.gen(function* () {
    const launches = yield* ThreadLaunch.ThreadLaunchService;
    const threads = yield* ThreadManagement.ThreadManagementService;
    const { threadId: _unusedThreadId, ...rest } = launchInput({
      command: "command:launch:allocated-deleted",
      thread: "unused",
      message: "Deleted before retry",
    });
    const first = yield* launches.launch(rest);
    yield* threads.dispatch({
      type: "thread.delete",
      commandId: CommandId.make("command:launch:allocated-deleted:delete"),
      threadId: first.threadId,
    });
    const failed = yield* launches.launch(rest).pipe(Effect.flip);
    if (failed._tag !== "ThreadLaunchError") {
      assert.fail(`expected ThreadLaunchError, got ${failed._tag}`);
    }
    assert.equal(failed.operation, "create-thread");
    assert.equal(failed.threadId, first.threadId);
    assert.equal(failed.cause, "Thread not found.");
    const shells = yield* threads.listProjectThreads({ projectId, includeSubagents: true });
    assert.equal(shells.length, 0);
  }).pipe(Effect.provide(harness.layer));
});

it.effect("does not treat an unrelated accepted command receipt as a launch", () => {
  const harness = makeHarness();
  return Effect.gen(function* () {
    const launches = yield* ThreadLaunch.ThreadLaunchService;
    const threads = yield* ThreadManagement.ThreadManagementService;
    const threadId = ThreadId.make("thread:launch:unrelated-receipt");
    yield* threads.dispatch({
      type: "thread.create",
      commandId: CommandId.make("command:launch:unrelated-receipt:create"),
      threadId,
      projectId,
      title: "Existing",
      modelSelection,
      runtimeMode: "full-access",
      interactionMode: "default",
      branch: null,
      worktreePath: null,
      createdBy: "user",
      creationSource: "web",
    });
    yield* threads.dispatch({
      type: "thread.metadata.update",
      commandId: CommandId.make("command:launch:unrelated-receipt"),
      threadId,
      expectedEmpty: true,
    });
    const { threadId: _unusedThreadId, ...rest } = launchInput({
      command: "command:launch:unrelated-receipt",
      thread: "unused",
      message: "Should not become a launch",
    });
    const failed = yield* launches.launch(rest).pipe(Effect.flip);
    if (failed._tag !== "ThreadLaunchError") {
      assert.fail(`expected ThreadLaunchError, got ${failed._tag}`);
    }
    assert.equal(failed.operation, "create-thread");
    assert.include(String(failed.cause), "cannot be replayed");
    const projection = yield* threads.getThreadProjection(threadId);
    assert.equal(projection.messages.length, 0);
    assert.equal(projection.runs.length, 0);
  }).pipe(Effect.provide(harness.layer));
});

it.effect("bounds concurrent first launches to one thread per command", () =>
  Effect.gen(function* () {
    const setupEntered = yield* Deferred.make<void>();
    const allowSetup = yield* Deferred.make<void>();
    const harness = makeHarness({
      runSetup: () =>
        Deferred.succeed(setupEntered, undefined).pipe(
          Effect.andThen(Deferred.await(allowSetup)),
          Effect.as({ status: "no-script" as const }),
        ),
    });
    yield* Effect.gen(function* () {
      const launches = yield* ThreadLaunch.ThreadLaunchService;
      const threads = yield* ThreadManagement.ThreadManagementService;
      const { threadId: _unusedThreadId, ...rest } = launchInput({
        command: "command:launch:concurrent-allocated",
        thread: "unused",
        message: "Race me",
      });
      // The command receipt is reserved atomically with the winning create, so
      // a loser either replays the winner's stored events or surfaces a
      // transient replay conflict that the next attempt resolves — the race
      // can never persist a second thread.
      const results = yield* Effect.all(
        [launches.launch(rest).pipe(Effect.exit), launches.launch(rest).pipe(Effect.exit)],
        { concurrency: "unbounded" },
      );
      const winner = results.find(Exit.isSuccess);
      assert.isDefined(winner);
      const threadId = winner!.value.threadId;
      for (const result of results) {
        if (Exit.isSuccess(result)) {
          assert.equal(result.value.threadId, threadId);
          continue;
        }
        const error = Cause.findErrorOption(result.cause).pipe(Option.getOrThrow);
        if (error._tag !== "ThreadLaunchError") {
          assert.fail(`expected ThreadLaunchError, got ${error._tag}`);
        }
        assert.equal(error.operation, "create-thread");
        assert.include(String(error.cause), "cannot be replayed");
        const retried = yield* launches.launch(rest);
        assert.equal(retried.threadId, threadId);
      }
      const projectThreads = yield* threads.listProjectThreads({
        projectId,
        includeSubagents: false,
      });
      assert.equal(projectThreads.length, 1);
      const projection = yield* threads.getThreadProjection(threadId);
      assert.equal(projection.messages.length, 1);
      assert.equal(projection.runs.length, 1);
      yield* Deferred.await(setupEntered);
      assert.equal(harness.runSetup.mock.calls.length, 1);
      yield* Deferred.succeed(allowSetup, undefined);
    }).pipe(Effect.provide(harness.layer));
  }),
);

it.effect("deduplicates retried launch side effects in-process", () =>
  Effect.gen(function* () {
    const setupEntered = yield* Deferred.make<void>();
    const allowSetup = yield* Deferred.make<void>();
    const harness = makeHarness({
      runSetup: () =>
        Deferred.succeed(setupEntered, undefined).pipe(
          Effect.andThen(Deferred.await(allowSetup)),
          Effect.as({ status: "no-script" as const }),
        ),
    });
    yield* Effect.gen(function* () {
      const launches = yield* ThreadLaunch.ThreadLaunchService;
      const input = launchInput({
        command: "command:launch:retry",
        thread: "thread:launch:retry",
        message: "Only once",
      });
      const [first, retry] = yield* Effect.all([launches.launch(input), launches.launch(input)], {
        concurrency: "unbounded",
      });
      yield* Deferred.await(setupEntered);
      assert.equal(first.threadId, retry.threadId);
      assert.isFalse(first.resumed);
      assert.isTrue(retry.resumed);
      assert.equal(harness.runSetup.mock.calls.length, 1);
      yield* Deferred.succeed(allowSetup, undefined);
    }).pipe(Effect.provide(harness.layer));
  }),
);

it.effect("does not let a failing same-command caller strand a concurrent durable launch", () =>
  Effect.gen(function* () {
    const setupEntered = yield* Deferred.make<void>();
    const allowSetup = yield* Deferred.make<void>();
    const harness = makeHarness({
      runSetup: () =>
        Deferred.succeed(setupEntered, undefined).pipe(
          Effect.andThen(Deferred.await(allowSetup)),
          Effect.as({ status: "no-script" as const }),
        ),
    });
    yield* Effect.gen(function* () {
      const launches = yield* ThreadLaunch.ThreadLaunchService;
      const command = "command:launch:failed-owner-race";
      const [failed, launched] = yield* Effect.all(
        [
          launches
            .launch({
              ...launchInput({
                command,
                thread: "thread:launch:failed-owner-race",
                message: "This invalid reuse fails",
              }),
              reuseExistingThread: true,
            })
            .pipe(Effect.exit),
          launches.launch(
            launchInput({
              command,
              thread: "thread:launch:successful-peer",
              message: "This peer persists",
            }),
          ),
        ],
        { concurrency: "unbounded" },
      );
      assert.isTrue(Exit.isFailure(failed));
      assert.equal(launched.projection.runs[0]?.status, "preparing");
      const entered = yield* Deferred.await(setupEntered).pipe(
        Effect.timeoutOption(Duration.seconds(2)),
      );
      assert.isTrue(Option.isSome(entered));
      assert.equal(harness.runSetup.mock.calls.length, 1);
      yield* Deferred.succeed(allowSetup, undefined);
    }).pipe(Effect.provide(harness.layer));
  }),
);

it.effect("schedules an accepted preparing message exactly once across concurrent retries", () =>
  Effect.gen(function* () {
    const setupEntered = yield* Deferred.make<void>();
    const allowSetup = yield* Deferred.make<void>();
    const harness = makeHarness({
      runSetup: () =>
        Deferred.succeed(setupEntered, undefined).pipe(
          Effect.andThen(Deferred.await(allowSetup)),
          Effect.as({ status: "no-script" as const }),
        ),
    });
    yield* Effect.gen(function* () {
      const launches = yield* ThreadLaunch.ThreadLaunchService;
      const threads = yield* ThreadManagement.ThreadManagementService;
      const input = launchInput({
        command: "command:launch:accepted-before-fork",
        thread: "thread:launch:accepted-before-fork",
        message: "Resume preparation",
      });
      const messageId = MessageId.make("message:launch:accepted-before-fork");

      yield* threads.dispatch({
        type: "thread.create",
        commandId: input.commandId,
        threadId: input.threadId,
        projectId: input.projectId,
        title: input.title,
        modelSelection: input.modelSelection,
        runtimeMode: input.runtimeMode,
        interactionMode: input.interactionMode,
        branch: null,
        worktreePath: null,
        createdBy: input.createdBy,
        creationSource: input.creationSource,
      });
      yield* threads.dispatch({
        type: "message.dispatch",
        commandId: CommandId.make(`${input.commandId}:initial-message`),
        threadId: input.threadId,
        messageId,
        text: "Resume preparation",
        attachments: [],
        modelSelection: input.modelSelection,
        dispatchMode: { type: "defer_start" },
        createdBy: input.createdBy,
        creationSource: input.creationSource,
      });
      const preparing = yield* threads.getThreadProjection(input.threadId);
      assert.equal(preparing.runs[0]?.status, "preparing");

      const [first, second] = yield* Effect.all([launches.launch(input), launches.launch(input)], {
        concurrency: "unbounded",
      });
      yield* Deferred.await(setupEntered);
      assert.isTrue(first.resumed);
      assert.isTrue(second.resumed);
      assert.equal(harness.runSetup.mock.calls.length, 1);
      yield* Deferred.succeed(allowSetup, undefined);
    }).pipe(Effect.provide(harness.layer));
  }),
);

it.effect("creates a strong provider-thread mapping for an imported native session", () => {
  const harness = makeHarness();
  return Effect.gen(function* () {
    const launches = yield* ThreadLaunch.ThreadLaunchService;
    const input = {
      ...launchInput({
        command: "command:launch:imported-native-session",
        thread: "thread:launch:imported-native-session",
      }),
      importedNativeThread: {
        ref: {
          driver: ProviderDriverKind.make("codex"),
          nativeId: "native-session-42",
          strength: "strong" as const,
        },
        metadata: {
          title: "Native session",
          updatedAt: "2026-08-23T00:00:00Z",
        },
      },
    };

    const launched = yield* launches.launch(input);

    assert.deepInclude(launched.projection.providerThreads[0], {
      id: IdAllocator.deriveProviderThread({
        driver: input.importedNativeThread.ref.driver,
        providerInstanceId: modelSelection.instanceId,
        nativeThreadId: input.importedNativeThread.ref.nativeId,
      }),
      driver: input.importedNativeThread.ref.driver,
      providerInstanceId: modelSelection.instanceId,
      appThreadId: input.threadId,
      nativeThreadRef: input.importedNativeThread.ref,
      status: "not_loaded",
      nativeMetadata: input.importedNativeThread.metadata,
    });
    assert.equal(
      launched.projection.thread.activeProviderThreadId,
      launched.projection.providerThreads[0]?.id,
    );
  }).pipe(Effect.provide(harness.layer));
});

it.effect("shared intake preserves durable attachment bytes after a lost launch result", () => {
  const harness = makeHarness();
  const layerFiles = ServerConfig.layerTest(process.cwd(), { prefix: "t3-message-intake-" }).pipe(
    Layer.provideMerge(NodeServices.layer),
  );
  return Effect.gen(function* () {
    const config = yield* ServerConfig.ServerConfig;
    const fs = yield* FileSystem.FileSystem;
    const launches = yield* ThreadLaunch.ThreadLaunchService;
    const threads = yield* ThreadManagement.ThreadManagementService;
    const pendingId = createPendingAttachmentId();
    assert.isNotNull(pendingId);
    const attachment: ChatAttachment = {
      type: "image",
      id: ChatAttachmentId.make(pendingId),
      name: "image.png",
      mimeType: "image/png",
      sizeBytes: 4,
    };
    const pendingPath = resolveAttachmentPath({
      attachmentsDir: config.attachmentsDir,
      attachment,
    });
    assert.isNotNull(pendingPath);
    yield* fs.makeDirectory(config.attachmentsDir, { recursive: true });
    yield* fs.writeFile(pendingPath, new Uint8Array([1, 2, 3, 4]));
    const input = {
      ...launchInput({ command: "intake-launch", thread: "intake-thread" }),
      initialMessage: {
        messageId: MessageId.make("intake-first"),
        text: "First [file](t3-context://v1/file/intake-file)",
        context: {
          version: 1 as const,
          records: [
            {
              version: 1 as const,
              contextId: ComposerContextId.make("intake-file"),
              kind: "file" as const,
              label: attachment.name,
              attachmentId: attachment.id,
              name: attachment.name,
              mimeType: attachment.mimeType,
              sizeBytes: attachment.sizeBytes,
            },
          ],
        },
        attachments: [attachment],
      },
    };
    const failed = yield* ThreadMessageIntake.launchThread(input).pipe(
      Effect.provideService(ThreadLaunch.ThreadLaunchService, {
        launch: (request) =>
          launches.launch(request).pipe(
            Effect.andThen(
              new ThreadLaunch.ThreadLaunchError({
                operation: "create-thread",
                commandId: request.commandId,
                projectId,
                cause: "lost result after acceptance",
              }),
            ),
          ),
        checkWorktreeBase: launches.checkWorktreeBase,
        retryPreparation: launches.retryPreparation,
      }),
      Effect.flip,
    );
    assert.equal(failed._tag, "ThreadLaunchError");
    // The observer failed, but the real V2 message and its bytes were accepted.
    const accepted = yield* threads.getThreadProjection(input.threadId);
    const stored = accepted.messages.find(
      (message) => message.id === input.initialMessage.messageId,
    );
    assert.isDefined(stored);
    assert.notEqual(stored.attachments[0]?.id, attachment.id);
    assert.equal(
      (stored.context?.records[0] as { attachmentId: string }).attachmentId,
      stored.attachments[0]?.id,
    );
    const userItem = accepted.turnItems.find(
      (item) => item.type === "user_message" && item.messageId === stored.id,
    );
    assert.ok(userItem?.type === "user_message");
    assert.deepEqual(userItem.context, stored.context);
    const storedPath = resolveAttachmentPath({
      attachmentsDir: config.attachmentsDir,
      attachment: stored.attachments[0]!,
    });
    assert.isNotNull(storedPath);
    assert.deepEqual(yield* fs.readFile(storedPath), new Uint8Array([1, 2, 3, 4]));
    assert.deepEqual(yield* fs.readFile(pendingPath), new Uint8Array([1, 2, 3, 4]));

    const replayed = yield* ThreadMessageIntake.launchThread(input);
    assert.equal(replayed.projection.messages[0]?.id, stored.id);
    assert.deepEqual(replayed.projection.messages[0]?.attachments, stored.attachments);
    const claimedFiles = Effect.map(fs.readDirectory(config.attachmentsDir), (files) =>
      files.filter((name) => !name.startsWith("pending-")),
    );
    assert.equal((yield* claimedFiles).length, 1);

    const missingProject = yield* ThreadMessageIntake.launchThread({
      ...input,
      commandId: CommandId.make("intake-no-project"),
      projectId: ProjectId.make("missing-project"),
    }).pipe(Effect.flip);
    assert.equal(missingProject._tag, "ThreadLaunchError");
    assert.equal((yield* claimedFiles).length, 1);
    const invalidSelection = yield* ThreadMessageIntake.launchThread({
      ...input,
      commandId: CommandId.make("intake-invalid-selection"),
      threadId: ThreadId.make("intake-invalid-selection"),
      modelSelection: {
        ...modelSelection,
        instanceId: ProviderInstanceId.make("foreign-instance"),
      },
      validateExplicitModelSelection: true,
    }).pipe(Effect.flip);
    assert.equal(invalidSelection._tag, "ThreadLaunchError");
    if (invalidSelection._tag === "ThreadLaunchError") {
      assert.equal(invalidSelection.operation, "validate-model-selection");
    }
    assert.equal((yield* claimedFiles).length, 1);
    assert.isNull(yield* threads.getThreadShell(ThreadId.make("intake-invalid-selection")));
    const missingThread = yield* ThreadMessageIntake.dispatchCommand({
      type: "message.dispatch",
      commandId: CommandId.make("intake-no-thread"),
      threadId: ThreadId.make("missing-thread"),
      messageId: MessageId.make("intake-missing"),
      text: "Missing",
      attachments: [attachment],
      dispatchMode: { type: "start_immediately" },
      createdBy: "user",
      creationSource: "web",
    }).pipe(Effect.flip);
    assert.equal(missingThread._tag, "OrchestratorProjectionError");
    assert.equal((yield* claimedFiles).length, 1);

    // Both ordinary command intake (RPC) and send intake (MCP) use the same store.
    const dispatch = ThreadMessageIntake.dispatchCommand({
      type: "message.dispatch",
      commandId: CommandId.make("intake-dispatch"),
      threadId: input.threadId,
      messageId: MessageId.make("intake-second"),
      text: "Second",
      attachments: [attachment],
      dispatchMode: { type: "queue_after_active" },
      createdBy: "user",
      creationSource: "web",
    });
    yield* dispatch;
    yield* dispatch;
    const queuedProjection = yield* threads.getThreadProjection(input.threadId);
    const queuedRun = queuedProjection.runs.find(
      (run) => run.userMessageId === MessageId.make("intake-second"),
    );
    assert.isDefined(queuedRun);
    assert.equal(queuedRun.status, "queued");
    const queuedMessage = queuedProjection.messages.find(
      (message) => message.id === queuedRun.userMessageId,
    );
    assert.isDefined(queuedMessage);
    const file: ChatAttachment = {
      type: "file",
      id: ChatAttachmentId.make(createPendingAttachmentId("pdf")),
      name: "queued.pdf",
      mimeType: "application/pdf",
      sizeBytes: 4,
    };
    const filePath = resolveAttachmentPath({
      attachmentsDir: config.attachmentsDir,
      attachment: file,
    });
    assert.isNotNull(filePath);
    yield* fs.writeFile(filePath, new Uint8Array([5, 6, 7, 8]));
    const edit = ThreadMessageIntake.dispatchCommand({
      type: "queued-run.edit",
      commandId: CommandId.make("intake-edit"),
      threadId: input.threadId,
      runId: queuedRun.id,
      text: "Edited with a file",
      attachments: [...queuedMessage.attachments, file],
    });
    yield* edit;
    yield* edit;
    const editedProjection = yield* threads.getThreadProjection(input.threadId);
    const editedMessage = editedProjection.messages.find(
      (message) => message.id === queuedRun.userMessageId,
    );
    assert.isDefined(editedMessage);
    assert.equal(editedMessage.attachments.length, 2);
    assert.deepEqual(editedMessage.attachments[0], queuedMessage.attachments[0]);
    assert.notEqual(editedMessage.attachments[1]?.id, file.id);
    const durableFilePath = resolveAttachmentPath({
      attachmentsDir: config.attachmentsDir,
      attachment: editedMessage.attachments[1]!,
    });
    assert.isNotNull(durableFilePath);
    assert.deepEqual(yield* fs.readFile(durableFilePath), new Uint8Array([5, 6, 7, 8]));
    assert.deepEqual(yield* fs.readFile(filePath), new Uint8Array([5, 6, 7, 8]));
    const beforeRejectedEdit = (yield* claimedFiles).length;
    const rejectedEdit = yield* ThreadMessageIntake.dispatchCommand({
      type: "queued-run.edit",
      commandId: CommandId.make("intake-edit-rejected"),
      threadId: input.threadId,
      runId: queuedRun.id,
      text: "",
      attachments: [file],
    }).pipe(Effect.flip);
    assert.equal(rejectedEdit._tag, "OrchestratorCommandRejectedError");
    assert.equal((yield* claimedFiles).length, beforeRejectedEdit);
    assert.deepEqual(yield* fs.readFile(filePath), new Uint8Array([5, 6, 7, 8]));
    const send = ThreadMessageIntake.sendToThread({
      commandId: CommandId.make("intake-send"),
      projectId,
      threadId: input.threadId,
      messageId: MessageId.make("intake-third"),
      text: "Third",
      attachments: [attachment],
      mode: "queue",
      createdBy: "agent",
      creationSource: "mcp",
    });
    yield* send;
    yield* send;
    assert.equal((yield* claimedFiles).length, 4);
    const final = yield* threads.getThreadProjection(input.threadId);
    assert.equal(final.messages.length, 3);
    for (const message of final.messages) {
      const path = resolveAttachmentPath({
        attachmentsDir: config.attachmentsDir,
        attachment: message.attachments[0]!,
      });
      assert.isNotNull(path);
      assert.deepEqual(yield* fs.readFile(path), new Uint8Array([1, 2, 3, 4]));
    }
  }).pipe(Effect.provide(Layer.mergeAll(harness.layer, layerFiles)));
});

it.effect("cancels tracked setup before provider work is released", () =>
  Effect.gen(function* () {
    const entered = yield* Deferred.make<void>();
    const harness = makeHarness({
      runSetup: () => Deferred.succeed(entered, undefined).pipe(Effect.andThen(Effect.never)),
    });
    yield* Effect.gen(function* () {
      const launches = yield* ThreadLaunch.ThreadLaunchService;
      const tracker = yield* WorktreeSetupTracker.WorktreeSetupTracker;
      const threads = yield* ThreadManagement.ThreadManagementService;
      const outbox = yield* EffectOutbox.EffectOutboxV2;
      const input = launchInput({
        command: "launch:cancel-tracked",
        thread: "thread:cancel-tracked",
        message: "Start",
        workspace: { type: "worktree", baseRef: "main" },
      });
      const launched = yield* launches.launch(input);
      yield* Deferred.await(entered);
      assert.equal((yield* tracker.get(launched.threadId))?.phase, "running");
      assert.isTrue(yield* tracker.cancel(launched.threadId));
      assert.equal((yield* tracker.get(launched.threadId))?.phase, "cancelled");
      const projection = yield* threads.getThreadProjection(launched.threadId);
      assert.equal(projection.runs[0]?.status, "failed");
      assert.isNull(projection.thread.worktreePath);
      assert.isEmpty(yield* outbox.listByCommandId(CommandId.make(`${input.commandId}:release`)));
    }).pipe(Effect.provide(harness.layer));
  }),
);

it.effect.each([0, 1])("releases an async setup before its completion with exit %s", (exitCode) =>
  Effect.gen(function* () {
    const completion = yield* Deferred.make<{ exitCode: number | null; durationMs: number }>();
    const harness = makeHarness({
      runSetup: () =>
        Effect.succeed({
          status: "started" as const,
          async: true,
          scriptId: "setup",
          scriptName: "Setup",
          scriptCommand: "vp install",
          terminalId: "setup",
          cwd: "/repo-worktrees/feature",
          completion: Deferred.await(completion),
        }),
    });
    yield* Effect.gen(function* () {
      const launches = yield* ThreadLaunch.ThreadLaunchService;
      const threads = yield* ThreadManagement.ThreadManagementService;
      const tracker = yield* WorktreeSetupTracker.WorktreeSetupTracker;
      const launched = yield* launches.launch(
        launchInput({
          command: `command:launch:async-${exitCode}`,
          thread: `thread:launch:async-${exitCode}`,
          message: "Start during setup",
          workspace: { type: "worktree", baseRef: "main" },
        }),
      );
      yield* tracker.stream(launched.threadId).pipe(
        Stream.filter(
          (snapshot) =>
            snapshot?.stages.some((stage) => stage.id === "agent" && stage.status === "done") ===
            true,
        ),
        Stream.runHead,
      );
      const running = yield* threads.getThreadProjection(launched.threadId);
      assert.equal(running.runs[0]?.status, "starting");
      assert.equal((yield* tracker.get(launched.threadId))?.phase, "running");
      yield* Deferred.succeed(completion, { exitCode, durationMs: 1 });
      yield* tracker.stream(launched.threadId).pipe(
        Stream.filter((snapshot) => snapshot?.phase === "done"),
        Stream.runHead,
      );
      const settled = yield* tracker.get(launched.threadId);
      assert.equal(
        settled?.stages.find((stage) => stage.id === "setup-script")?.status,
        exitCode === 0 ? "done" : "failed",
      );
      assert.equal(
        (yield* threads.getThreadProjection(launched.threadId)).runs[0]?.status,
        "starting",
      );
    }).pipe(Effect.provide(harness.layer));
  }),
);

it.effect.each([
  { name: "is missing", baseRef: "t3/renamed", startFromOrigin: false, origin: true },
  {
    name: "has no origin to come from",
    baseRef: "t3/renamed",
    startFromOrigin: true,
    origin: false,
  },
  {
    name: "is a previous checkout there never was",
    baseRef: "-",
    startFromOrigin: false,
    origin: true,
  },
])("refuses a new worktree whose base ref $name", (testCase) =>
  Effect.gen(function* () {
    const harness = makeHarness({
      hasCommit: ({ refName }) => Effect.succeed(refName === "HEAD"),
      git: {
        remoteExists: () => Effect.succeed(testCase.origin),
        hasRefNamed: () => Effect.succeed(false),
      },
    });
    yield* Effect.gen(function* () {
      const launches = yield* ThreadLaunch.ThreadLaunchService;
      const error = yield* launches
        .checkWorktreeBase({
          projectId,
          workspaceStrategy: {
            type: "worktree",
            baseRef: testCase.baseRef,
            startFromOrigin: testCase.startFromOrigin,
          },
        })
        .pipe(Effect.flip);
      assert.deepInclude(error, {
        _tag: "ThreadLaunchBaseRefError",
        projectId,
        baseRef: testCase.baseRef,
      });
      assert.equal(
        error.message,
        `Base ref "${testCase.baseRef}" does not resolve to a commit in project "Project" (/repo).`,
      );
    }).pipe(Effect.provide(harness.layer));
  }),
);

it.effect.each([
  { name: "a base ref that resolves", baseRef: "main", commits: ["main", "HEAD"] },
  // Provisioning fetches it; the ref may exist nowhere else yet.
  {
    name: "a base ref only origin has",
    baseRef: "remote-only",
    startFromOrigin: true,
    commits: ["HEAD"],
  },
  // `git worktree add` starts from a remote-tracking branch of that name.
  {
    name: "a base ref a remote branch carries",
    baseRef: "remote-only",
    commits: ["HEAD"],
    remoteBranch: true,
  },
  // Provisioning runs such a repository without a worktree.
  { name: "a repository with no commits yet", baseRef: "main", commits: [] },
  { name: "the previous checkout", baseRef: "-", commits: ["@{-1}", "HEAD"] },
  { name: "a commit search", baseRef: ":/fix", commits: ["HEAD"] },
  { name: "a folder that is not a repository", baseRef: "main", commits: null },
])("leaves $name to the launch", (testCase) =>
  Effect.gen(function* () {
    const { commits } = testCase;
    const harness = makeHarness({
      git: { hasRefNamed: () => Effect.succeed(testCase.remoteBranch === true) },
      hasCommit: ({ refName }) =>
        commits === null
          ? Effect.fail(
              new GitCommandError({
                operation: "GitWorkflowService.hasCommit",
                command: "git rev-parse",
                cwd: "/repo",
                detail: "Not a Git repository.",
              }),
            )
          : Effect.succeed(commits.includes(refName)),
    });
    yield* Effect.gen(function* () {
      const launches = yield* ThreadLaunch.ThreadLaunchService;
      yield* launches.checkWorktreeBase({
        projectId,
        workspaceStrategy: {
          type: "worktree",
          baseRef: testCase.baseRef,
          ...(testCase.startFromOrigin === undefined
            ? {}
            : { startFromOrigin: testCase.startFromOrigin }),
        },
      });
    }).pipe(Effect.provide(harness.layer));
  }),
);

it.effect("replays an accepted send after the project disables the instance", () => {
  const harness = makeHarness();
  const layerFiles = ServerConfig.layerTest(process.cwd(), { prefix: "t3-send-retry-" }).pipe(
    Layer.provideMerge(NodeServices.layer),
  );
  return Effect.gen(function* () {
    const launches = yield* ThreadLaunch.ThreadLaunchService;
    const settings = yield* ServerSettings.ServerSettingsService;
    const created = yield* launches.launch(
      launchInput({
        command: "command:send:retry-setup",
        thread: "thread:send:retry-after-disable",
      }),
    );

    const sendInput = {
      commandId: CommandId.make("command:send:retry-after-disable"),
      projectId,
      threadId: created.threadId,
      messageId: MessageId.make("message:send:retry-after-disable"),
      text: "Hello again",
      attachments: [],
      mode: "auto" as const,
      createdBy: "user" as const,
      creationSource: "web" as const,
    };
    const first = yield* ThreadMessageIntake.sendToThread(sendInput);

    yield* settings.updateSettings({
      projectSettingsOverrides: {
        [projectId]: { providerInstances: { [modelSelection.instanceId]: { enabled: false } } },
      },
    });

    const retry = yield* ThreadMessageIntake.sendToThread(sendInput);
    assert.equal(retry.message.id, first.message.id);

    const fresh = yield* ThreadMessageIntake.sendToThread({
      ...sendInput,
      commandId: CommandId.make("command:send:fresh-after-disable"),
      messageId: MessageId.make("message:send:fresh-after-disable"),
    }).pipe(Effect.flip);
    if (fresh._tag !== "ThreadManagementProviderInstanceDisabledError") {
      assert.fail(`expected ThreadManagementProviderInstanceDisabledError, got ${fresh._tag}`);
    }
  }).pipe(Effect.provide(Layer.mergeAll(harness.layer, layerFiles)));
});

it.effect("replays an accepted message.dispatch after the project disables the instance", () => {
  const harness = makeHarness();
  const layerFiles = ServerConfig.layerTest(process.cwd(), { prefix: "t3-dispatch-retry-" }).pipe(
    Layer.provideMerge(NodeServices.layer),
  );
  return Effect.gen(function* () {
    const launches = yield* ThreadLaunch.ThreadLaunchService;
    const settings = yield* ServerSettings.ServerSettingsService;
    const created = yield* launches.launch(
      launchInput({
        command: "command:dispatch:retry-setup",
        thread: "thread:dispatch:retry-after-disable",
      }),
    );

    const dispatchInput = {
      type: "message.dispatch" as const,
      commandId: CommandId.make("command:dispatch:retry-after-disable"),
      threadId: created.threadId,
      messageId: MessageId.make("message:dispatch:retry-after-disable"),
      text: "Hello again",
      attachments: [],
      dispatchMode: { type: "queue_after_active" as const },
      createdBy: "user" as const,
      creationSource: "web" as const,
    };
    const first = yield* ThreadMessageIntake.dispatchCommand(dispatchInput);

    yield* settings.updateSettings({
      projectSettingsOverrides: {
        [projectId]: { providerInstances: { [modelSelection.instanceId]: { enabled: false } } },
      },
    });

    const retry = yield* ThreadMessageIntake.dispatchCommand(dispatchInput);
    assert.deepEqual(retry, first);

    const fresh = yield* ThreadMessageIntake.dispatchCommand({
      ...dispatchInput,
      commandId: CommandId.make("command:dispatch:fresh-after-disable"),
      messageId: MessageId.make("message:dispatch:fresh-after-disable"),
    }).pipe(Effect.flip);
    if (fresh._tag !== "ThreadManagementProviderInstanceDisabledError") {
      assert.fail(`expected ThreadManagementProviderInstanceDisabledError, got ${fresh._tag}`);
    }
  }).pipe(Effect.provide(Layer.mergeAll(harness.layer, layerFiles)));
});

const seedControlledLaunch = Effect.fn("seedControlledLaunch")(function* (
  config: MultiAgentConfig = { mode: "custom", roles: { worker: modelSelection } },
  status: "queued" | "reserved" = "reserved",
  assignedSelection: ModelSelection = modelSelection,
  role: MultiAgentControllerTask["role"] = "worker",
) {
  const launches = yield* ThreadLaunch.ThreadLaunchService;
  const launched = yield* launches.launch({
    ...launchInput({ command: "controller:create", thread: "controller:thread" }),
  });
  // Legacy controllers can contain assignments that are no longer available.
  // Seed their persisted state directly; fresh dispatch now validates enrollment.
  const run: OrchestrationV2Run = {
    id: RunId.make("controller:run"),
    threadId: launched.threadId,
    ordinal: 1,
    providerInstanceId: modelSelection.instanceId,
    modelSelection,
    multiAgent: config,
    providerThreadId: null,
    userMessageId: MessageId.make("controller:input"),
    rootNodeId: NodeId.make("controller:root"),
    activeAttemptId: RunAttemptId.make("controller:attempt"),
    status: "completed",
    requestedAt: yield* DateTime.now,
    startedAt: null,
    completedAt: yield* DateTime.now,
    checkpointId: null,
    contextHandoffId: null,
  };
  const task: MultiAgentControllerTask = {
    key: "task",
    taskId: NodeId.make("controlled:task"),
    launchCommandId: CommandId.make("controlled:launch"),
    launchMessageId: MessageId.make("controlled:message"),
    workerThreadId: ThreadId.make("controlled:worker"),
    proposalId: MessageId.make("controller:proposal"),
    proposalAttemptId: RunAttemptId.make("controller:attempt"),
    role,
    prompt: "Do the authorized task",
    title: "Worker",
    modelSelection: assignedSelection,
    status,
  };
  const sink = yield* EventSink.EventSinkV2;
  yield* sink.write({
    events: [
      {
        id: EventId.make("controller:run"),
        type: "run.created",
        threadId: launched.threadId,
        runId: run.id,
        occurredAt: yield* DateTime.now,
        payload: run,
      },
      {
        id: EventId.make("controller:state"),
        type: "multi-agent.controller.updated",
        threadId: launched.threadId,
        runId: run.id,
        occurredAt: yield* DateTime.now,
        payload: {
          state: {
            coordinatorThreadId: launched.threadId,
            coordinatorRunId: run.id,
            status: "open",
            tasks: [task],
          },
          expectedRevision: 0,
          revision: 1,
        },
      },
    ],
  });
  const orchestrator = yield* Orchestrator.OrchestratorV2;
  const start = (commandKey = "controller:launch-request") =>
    orchestrator.dispatch({
      type: "multi-agent.controller.command",
      commandId: CommandId.make(commandKey),
      threadId: launched.threadId,
      runId: run.id,
      action: { type: "launch", taskId: task.taskId },
    });
  const input = {
    threadId: launched.threadId,
    coordinatorRunId: run.id,
    taskId: task.taskId,
    launchCommandId: task.launchCommandId,
  };
  const workerInput: ThreadLaunch.ThreadLaunchInput = {
    ...launchInput({ command: task.launchCommandId, thread: task.workerThreadId }),
    coordinatorThreadId: launched.threadId,
    controlledWorkerLaunchOwner: {
      coordinatorThreadId: launched.threadId,
      coordinatorRunId: run.id,
      taskId: task.taskId,
      workerThreadId: task.workerThreadId,
      launchCommandId: task.launchCommandId,
      launchMessageId: task.launchMessageId,
    },
    modelSelection: task.modelSelection,
    title: "Worker",
    validateExplicitModelSelection: true,
    initialMessage: {
      messageId: task.launchMessageId,
      text: task.prompt,
      attachments: [],
      multiAgent: { mode: "off" },
      senderThreadId: launched.threadId,
    },
    createdBy: "system",
    creationSource: "server",
  };
  const readTask = () =>
    orchestrator
      .getThreadRecords(launched.threadId, ["runs"], { runIds: [run.id] })
      .pipe(Effect.map((records) => records.runs[0]!.multiAgentController!.tasks[0]!));
  return { run, task, start, input, workerInput, readTask };
});

it.effect.each(["worker", "rescuer", "reviewer"] as const)(
  "launches the configured %s model with valid provider-specific options",
  (role) => {
    const harness = makeHarness({ providers: [launchCatalogProvider] });
    return Effect.gen(function* () {
      const selection = {
        ...modelSelection,
        options: [
          { id: "effort", value: "supported" },
          { id: "feature", value: false },
        ],
      };
      const owner = yield* seedControlledLaunch(
        { mode: "custom", roles: { [role]: selection } },
        "reserved",
        selection,
        role,
      );
      yield* owner.start();
      const executor = yield* MultiAgentWorkerLaunchService.MultiAgentWorkerLaunchService;
      yield* executor.execute(owner.input);
      const threads = yield* ThreadManagement.ThreadManagementService;
      const worker = yield* threads.getThreadProjection(owner.task.workerThreadId);
      assert.deepEqual(worker.runs[0]!.modelSelection, selection);
      assert.deepEqual(worker.runs[0]!.multiAgent, { mode: "off" });
      assert.equal((yield* owner.readTask()).status, "running");
    }).pipe(Effect.provide(harness.layer));
  },
);

it.effect.each(["returned", "lost-response"] as const)(
  "settles a worker that finished before launch registration (%s)",
  (boundary) => {
    const harness = makeHarness({ providers: [launchCatalogProvider] });
    return Effect.gen(function* () {
      const owner = yield* seedControlledLaunch();
      yield* owner.start();
      const launches = yield* ThreadLaunch.ThreadLaunchService;
      const projections = yield* ProjectionStore.ProjectionStoreV2;
      const sink = yield* EventSink.EventSinkV2;
      const receipts = yield* CommandReceiptStore.CommandReceiptStoreV2;
      const fastLaunch = {
        ...launches,
        launch: (input: ThreadLaunch.ThreadLaunchInput) =>
          Effect.gen(function* () {
            const launched = yield* launches.launch(input);
            const rows = yield* projections.getThreadRecords(owner.task.workerThreadId, [
              "runs",
              "attempts",
            ]);
            const run = rows.runs[0]!;
            const at = yield* DateTime.now;
            yield* sink.write({
              events: [
                {
                  id: EventId.make(`fast:${boundary}:run`),
                  type: "run.updated",
                  threadId: run.threadId,
                  runId: run.id,
                  occurredAt: at,
                  payload: { ...run, status: "completed", completedAt: at },
                },
                {
                  id: EventId.make(`fast:${boundary}:attempt`),
                  type: "run-attempt.updated",
                  threadId: run.threadId,
                  runId: run.id,
                  occurredAt: at,
                  payload: { ...rows.attempts[0]!, status: "completed", completedAt: at },
                },
              ],
            });
            assert.equal((yield* owner.readTask()).status, "launching");
            const completion = Orchestrator.controlledWorkerFinishedCommand(
              run.controlledWorkerOwnership!,
            );
            assert.isTrue(Option.isNone(yield* receipts.getByCommandId(completion.commandId)));
            if (boundary === "lost-response")
              return yield* new ThreadLaunch.ThreadLaunchError({
                commandId: input.commandId,
                operation: "dispatch-message",
                projectId: input.projectId,
                cause: "Lost terminal worker launch response",
              });
            return launched;
          }).pipe(
            Effect.mapError(
              (cause) =>
                new ThreadLaunch.ThreadLaunchError({
                  commandId: input.commandId,
                  operation: "dispatch-message",
                  projectId: input.projectId,
                  cause,
                }),
            ),
          ),
      };
      const first = yield* Effect.gen(function* () {
        const executor = yield* MultiAgentWorkerLaunchService.MultiAgentWorkerLaunchService;
        yield* executor.execute(owner.input);
      }).pipe(
        Effect.provide(
          Layer.fresh(MultiAgentWorkerLaunchService.layer).pipe(
            Layer.provide(Layer.succeed(ThreadLaunch.ThreadLaunchService, fastLaunch)),
          ),
        ),
        Effect.exit,
      );
      if (boundary === "lost-response") {
        assert.isTrue(Exit.isFailure(first));
        yield* (yield* MultiAgentWorkerLaunchService.MultiAgentWorkerLaunchService).execute(
          owner.input,
        );
      } else assert.isTrue(Exit.isSuccess(first));
      assert.equal((yield* owner.readTask()).status, "completed");
      const before = (yield* projections.getThreadRecords(owner.input.threadId, ["runs"])).runs[0]!;
      yield* (yield* MultiAgentWorkerLaunchService.MultiAgentWorkerLaunchService).execute(
        owner.input,
      );
      const after = (yield* projections.getThreadRecords(owner.input.threadId, ["runs"])).runs[0]!;
      assert.equal(after.multiAgentControllerRevision, before.multiAgentControllerRevision);
      assert.lengthOf(
        (yield* projections.getThreadRecords(owner.task.workerThreadId, ["runs"])).runs,
        1,
      );
    }).pipe(Effect.provide(harness.layer));
  },
);

it.effect("atomically queues only reserved launches and deduplicates repeated commands", () => {
  const harness = makeHarness({ providers: [launchCatalogProvider] });
  return Effect.gen(function* () {
    const owner = yield* seedControlledLaunch(undefined, "queued");
    assert.isTrue(
      Exit.isFailure(yield* owner.start("controller:invalid-launch").pipe(Effect.exit)),
    );
    const outbox = yield* EffectOutbox.EffectOutboxV2;
    assert.deepEqual(
      yield* outbox.listByCommandId(CommandId.make("controller:launch-request")),
      [],
    );
    const orchestrator = yield* Orchestrator.OrchestratorV2;
    yield* orchestrator.dispatch({
      type: "multi-agent.controller.command",
      commandId: CommandId.make("controller:reserve"),
      threadId: owner.input.threadId,
      runId: owner.run.id,
      action: { type: "reserve", taskId: owner.task.taskId },
    });
    yield* owner.start();
    yield* owner.start();
    assert.equal((yield* owner.readTask()).status, "launching");
    const effects = yield* outbox.listByCommandId(CommandId.make("controller:launch-request"));
    assert.equal(effects.length, 1);
    assert.deepEqual(effects[0]!.request, {
      type: "multi-agent.worker.launch",
      coordinatorRunId: owner.run.id,
      taskId: owner.task.taskId,
      launchCommandId: owner.task.launchCommandId,
    });
    const threads = yield* ThreadManagement.ThreadManagementService;
    assert.isNull(yield* threads.getThreadShell(owner.task.workerThreadId));
  }).pipe(Effect.provide(harness.layer));
});

it.effect.each(["before-create", "after-create", "after-message"] as const)(
  "recovers durable worker launch at crash boundary %s without duplicates",
  (boundary) => {
    const harness = makeHarness({ providers: [launchCatalogProvider] });
    return Effect.gen(function* () {
      const owner = yield* seedControlledLaunch();
      yield* owner.start();
      const threads = yield* ThreadManagement.ThreadManagementService;
      const launches = yield* ThreadLaunch.ThreadLaunchService;
      if (boundary === "after-create") {
        const { initialMessage: _message, ...createOnly } = owner.workerInput;
        yield* launches.launch(createOnly);
      } else if (boundary === "after-message") yield* launches.launch(owner.workerInput);
      const receipts = yield* CommandReceiptStore.CommandReceiptStoreV2;
      const previousMessage = yield* receipts.getByCommandId(
        CommandId.make(`${owner.task.launchCommandId}:initial-message`),
      );
      // Reacquire a fresh controller service, as after a process restart. The SQL
      // receipts and projections are authoritative, not its in-memory lock.
      yield* Effect.gen(function* () {
        const executor = yield* MultiAgentWorkerLaunchService.MultiAgentWorkerLaunchService;
        yield* Effect.all([executor.execute(owner.input), executor.execute(owner.input)], {
          concurrency: 2,
        });
      }).pipe(Effect.provide(Layer.fresh(MultiAgentWorkerLaunchService.layer)));
      const current = yield* owner.readTask();
      assert.equal(current.status, "running");
      assert.isTrue("workerRunId" in current && current.workerRunId !== undefined);
      const worker = yield* threads.getThreadProjection(owner.task.workerThreadId);
      assert.equal(worker.runs.length, 1);
      assert.deepEqual(worker.runs[0]!.multiAgent, { mode: "off" });
      assert.equal(worker.thread.coordinatorThreadId, owner.input.threadId);
      assert.isNull(worker.thread.lineage.parentThreadId);
      assert.equal(worker.runs[0]!.userMessageId, owner.task.launchMessageId);
      if (Option.isSome(previousMessage)) {
        const recovered = yield* receipts.getByCommandId(
          CommandId.make(`${owner.task.launchCommandId}:initial-message`),
        );
        assert.deepEqual(recovered, previousMessage);
      }
      const outbox = yield* EffectOutbox.EffectOutboxV2;
      const pending = (yield* outbox.listByCommandId(
        CommandId.make("controller:launch-request"),
      ))[0]!;
      assert.isTrue(
        EffectOutbox.REPLAY_SAFE_EFFECT_TYPES_AFTER_PROCESS_LOSS.includes(
          "multi-agent.worker.launch",
        ),
      );
      // Durable outbox reconciliation also retains the request across restart.
      yield* outbox.reconcileAfterProcessLoss;
      assert.isTrue(Option.isSome(yield* outbox.get(pending.id)));
    }).pipe(Effect.provide(harness.layer));
  },
);

it.effect.each([
  { name: "unassigned", config: { mode: "custom", roles: {} } },
  {
    name: "unavailable-model",
    config: { mode: "custom", roles: { worker: { ...modelSelection, model: "missing" } } },
  },
  {
    name: "foreign-provider",
    config: {
      mode: "custom",
      roles: { worker: { ...modelSelection, instanceId: ProviderInstanceId.make("foreign") } },
    },
  },
  {
    name: "invalid-option",
    config: {
      mode: "custom",
      roles: { worker: { ...modelSelection, options: [{ id: "effort", value: "invalid" }] } },
    },
  },
] satisfies ReadonlyArray<{ name: string; config: MultiAgentConfig }>)(
  "rejects Custom assignment %s before creation and releases its slot",
  ({ config }) => {
    const harness = makeHarness({ providers: [launchCatalogProvider] });
    return Effect.gen(function* () {
      const owner = yield* seedControlledLaunch(config);
      yield* owner.start();
      const executor = yield* MultiAgentWorkerLaunchService.MultiAgentWorkerLaunchService;
      yield* executor.execute(owner.input);
      assert.equal((yield* owner.readTask()).status, "failed");
      const threads = yield* ThreadManagement.ThreadManagementService;
      assert.isNull(yield* threads.getThreadShell(owner.task.workerThreadId));
      yield* executor.execute(owner.input);
      assert.isNull(yield* threads.getThreadShell(owner.task.workerThreadId));
    }).pipe(Effect.provide(harness.layer));
  },
);

it.effect.each(["before-create", "after-create", "after-message"] as const)(
  "retains uncertain capacity after a lost launch response (%s) and recovers safely",
  (boundary) => {
    const harness = makeHarness({ providers: [launchCatalogProvider] });
    return Effect.gen(function* () {
      const owner = yield* seedControlledLaunch();
      yield* owner.start();
      const launches = yield* ThreadLaunch.ThreadLaunchService;
      const lostResponse = {
        ...launches,
        launch: (input: ThreadLaunch.ThreadLaunchInput) =>
          Effect.gen(function* () {
            if (boundary === "after-create") {
              const { initialMessage: _message, ...createOnly } = input;
              yield* launches.launch(createOnly);
            } else if (boundary === "after-message") yield* launches.launch(input);
            return yield* new ThreadLaunch.ThreadLaunchError({
              commandId: input.commandId,
              projectId: input.projectId,
              operation: "dispatch-message",
              cause: "Lost response",
            });
          }),
      };
      const failed = yield* Effect.gen(function* () {
        const executor = yield* MultiAgentWorkerLaunchService.MultiAgentWorkerLaunchService;
        yield* executor.execute(owner.input);
      }).pipe(
        Effect.provide(
          Layer.fresh(MultiAgentWorkerLaunchService.layer).pipe(
            Layer.provide(Layer.succeed(ThreadLaunch.ThreadLaunchService, lostResponse)),
          ),
        ),
        Effect.exit,
      );
      assert.isTrue(Exit.isFailure(failed));
      assert.equal((yield* owner.readTask()).status, "launching");
      const executor = yield* MultiAgentWorkerLaunchService.MultiAgentWorkerLaunchService;
      yield* executor.execute(owner.input);
      assert.equal((yield* owner.readTask()).status, "running");
      const threads = yield* ThreadManagement.ThreadManagementService;
      assert.equal((yield* threads.getThreadProjection(owner.task.workerThreadId)).runs.length, 1);
    }).pipe(Effect.provide(harness.layer));
  },
);

it.effect("recovers the initial worker run even after later coordinator and worker turns", () => {
  const harness = makeHarness({ providers: [launchCatalogProvider] });
  return Effect.gen(function* () {
    const owner = yield* seedControlledLaunch();
    yield* owner.start();
    const launches = yield* ThreadLaunch.ThreadLaunchService;
    const launched = yield* launches.launch(owner.workerInput);
    const initialId = launched.projection.runs[0]!.id;
    const threads = yield* ThreadManagement.ThreadManagementService;
    for (const threadId of [owner.input.threadId, owner.task.workerThreadId]) {
      yield* threads.dispatch({
        type: "message.dispatch",
        commandId: CommandId.make(`later:${threadId}`),
        threadId,
        messageId: MessageId.make(`later:${threadId}`),
        text: "Later turn",
        attachments: [],
        modelSelection,
        multiAgent: { mode: "off" },
        createdBy: "user",
        creationSource: "web",
        dispatchMode: { type: "queue_after_active" },
      });
    }
    const executor = yield* MultiAgentWorkerLaunchService.MultiAgentWorkerLaunchService;
    yield* executor.execute(owner.input);
    const task = yield* owner.readTask();
    assert.equal(task.status, "running");
    assert.equal("workerRunId" in task ? task.workerRunId : undefined, initialId);
    const workerRuns = (yield* threads.getThreadProjection(owner.task.workerThreadId)).runs;
    assert.equal(workerRuns.length, 2);
    assert.isDefined(workerRuns[0]?.controlledWorkerOwnership);
    assert.isUndefined(workerRuns[1]?.controlledWorkerOwnership);
    const projections = yield* ProjectionStore.ProjectionStoreV2;
    assert.isTrue(
      yield* projections.controlledWorkerStartAllowed(
        owner.task.workerThreadId,
        workerRuns[1]!.id,
        workerRuns[1]!.activeAttemptId!,
      ),
    );
  }).pipe(Effect.provide(harness.layer));
});

it.effect(
  "replays an unsettled outbox launch after process loss without creating another worker",
  () => {
    const harness = makeHarness({ providers: [launchCatalogProvider] });
    return Effect.gen(function* () {
      const owner = yield* seedControlledLaunch();
      yield* owner.start();
      const outbox = yield* EffectOutbox.EffectOutboxV2;
      const executor = yield* MultiAgentWorkerLaunchService.MultiAgentWorkerLaunchService;
      let claimedLaunch = false;
      // Other effects are provider boundaries, deliberately stubbed in this test.
      while (!claimedLaunch) {
        const claim = yield* outbox.claimNext({
          workerId: "lost-process",
          leaseDurationMs: 30_000,
        });
        assert.isTrue(Option.isSome(claim));
        if (Option.isNone(claim)) return;
        if (claim.value.request.type === "multi-agent.worker.launch") {
          yield* executor.execute(owner.input);
          claimedLaunch = true;
          // Simulate process loss after acknowledgment, before outbox.succeed.
        } else yield* outbox.succeed({ effectId: claim.value.id, workerId: "lost-process" });
      }
      assert.isAtLeast((yield* outbox.reconcileAfterProcessLoss).requeued, 1);
      const layerWorker = EffectWorker.layerWithOptions({ workerId: "restarted-process" }).pipe(
        Layer.provide(
          Layer.mergeAll(
            Layer.succeed(EffectOutbox.EffectOutboxV2, outbox),
            Layer.succeed(EffectWorker.OrchestrationEffectExecutorV2, {
              execute: (effect) =>
                effect.request.type === "multi-agent.worker.launch"
                  ? executor
                      .execute({
                        threadId: effect.threadId,
                        coordinatorRunId: effect.request.coordinatorRunId,
                        taskId: effect.request.taskId,
                        launchCommandId: effect.request.launchCommandId,
                      })
                      .pipe(
                        Effect.mapError(
                          (cause) =>
                            new EffectWorker.OrchestrationEffectExecutionError({
                              effectId: effect.id,
                              effectType: effect.request.type,
                              cause,
                            }),
                        ),
                      )
                  : Effect.void,
            }),
          ),
        ),
      );
      yield* Effect.gen(function* () {
        const worker = yield* EffectWorker.OrchestrationEffectWorkerV2;
        yield* worker.drain();
      }).pipe(Effect.provide(layerWorker));
      const effect = (yield* outbox.listByCommandId(
        CommandId.make("controller:launch-request"),
      ))[0]!;
      assert.equal(effect.status, "succeeded");
      assert.isAtLeast(effect.attemptCount, 2);
      const threads = yield* ThreadManagement.ThreadManagementService;
      assert.equal((yield* threads.getThreadProjection(owner.task.workerThreadId)).runs.length, 1);
      assert.equal((yield* owner.readTask()).status, "running");
    }).pipe(Effect.provide(harness.layer));
  },
);

it.effect("retains cancellation capacity and records an already-created exact worker run", () => {
  const harness = makeHarness({ providers: [launchCatalogProvider] });
  return Effect.gen(function* () {
    const owner = yield* seedControlledLaunch();
    yield* owner.start();
    const launches = yield* ThreadLaunch.ThreadLaunchService;
    const launched = yield* launches.launch(owner.workerInput);
    const orchestrator = yield* Orchestrator.OrchestratorV2;
    yield* orchestrator.dispatch({
      type: "multi-agent.controller.command",
      commandId: CommandId.make("controller:cancel"),
      threadId: owner.input.threadId,
      runId: owner.run.id,
      action: { type: "cancel" },
    });
    const executor = yield* MultiAgentWorkerLaunchService.MultiAgentWorkerLaunchService;
    yield* executor.execute(owner.input);
    const task = yield* owner.readTask();
    assert.equal(task.status, "cancellation_pending");
    assert.equal(
      "workerRunId" in task ? task.workerRunId : undefined,
      launched.projection.runs[0]!.id,
    );
    const rejected = yield* orchestrator
      .dispatch({
        type: "multi-agent.controller.command",
        commandId: CommandId.make("controller:unsafe-release"),
        threadId: owner.input.threadId,
        runId: owner.run.id,
        action: {
          type: "launch_failed",
          taskId: task.taskId,
          launchCommandId: task.launchCommandId,
        },
      })
      .pipe(Effect.exit);
    assert.isTrue(Exit.isFailure(rejected));
    assert.equal((yield* owner.readTask()).status, "cancellation_pending");
  }).pipe(Effect.provide(harness.layer));
});

it.effect(
  "releases capacity for a verified selection rejection at the thread launch boundary",
  () => {
    const harness = makeHarness({ providers: [launchCatalogProvider] });
    return Effect.gen(function* () {
      const owner = yield* seedControlledLaunch();
      yield* owner.start();
      const launches = yield* ThreadLaunch.ThreadLaunchService;
      const rejected = {
        ...launches,
        launch: (input: ThreadLaunch.ThreadLaunchInput) =>
          new ThreadLaunch.ThreadLaunchError({
            commandId: input.commandId,
            projectId: input.projectId,
            operation: "validate-model-selection",
            selectionIssue: { reason: "provider_unavailable" },
            cause: "Catalog changed before create",
          }),
      };
      yield* Effect.gen(function* () {
        const executor = yield* MultiAgentWorkerLaunchService.MultiAgentWorkerLaunchService;
        yield* executor.execute(owner.input);
      }).pipe(
        Effect.provide(
          Layer.fresh(MultiAgentWorkerLaunchService.layer).pipe(
            Layer.provide(Layer.succeed(ThreadLaunch.ThreadLaunchService, rejected)),
          ),
        ),
      );
      assert.equal((yield* owner.readTask()).status, "failed");
      const threads = yield* ThreadManagement.ThreadManagementService;
      assert.isNull(yield* threads.getThreadShell(owner.task.workerThreadId));
    }).pipe(Effect.provide(harness.layer));
  },
);

it.effect("retains a partial launch slot when the catalog becomes unavailable", () => {
  const harness = makeHarness({ providers: [launchCatalogProvider] });
  return Effect.gen(function* () {
    const owner = yield* seedControlledLaunch();
    yield* owner.start();
    const launches = yield* ThreadLaunch.ThreadLaunchService;
    const { initialMessage: _message, ...createOnly } = owner.workerInput;
    yield* launches.launch(createOnly);
    const result = yield* Effect.gen(function* () {
      const executor = yield* MultiAgentWorkerLaunchService.MultiAgentWorkerLaunchService;
      yield* executor.execute(owner.input);
    }).pipe(
      Effect.provide(
        Layer.fresh(MultiAgentWorkerLaunchService.layer).pipe(
          Layer.provide(ProviderRegistryMock.layer([])),
        ),
      ),
      Effect.exit,
    );
    assert.isTrue(Exit.isFailure(result));
    assert.equal((yield* owner.readTask()).status, "launching");
    const threads = yield* ThreadManagement.ThreadManagementService;
    assert.equal((yield* threads.getThreadProjection(owner.task.workerThreadId)).runs.length, 0);
  }).pipe(Effect.provide(harness.layer));
});

it.effect(
  "binds worker acknowledgments to the completed originating run and exact launch identities",
  () => {
    const harness = makeHarness({ providers: [launchCatalogProvider] });
    return Effect.gen(function* () {
      const owner = yield* seedControlledLaunch();
      const sink = yield* EventSink.EventSinkV2;
      yield* sink.write({
        events: [
          {
            id: EventId.make("controller:completed"),
            type: "run.updated",
            threadId: owner.input.threadId,
            runId: owner.run.id,
            occurredAt: yield* DateTime.now,
            payload: { ...owner.run, status: "completed", completedAt: yield* DateTime.now },
          },
        ],
      });
      yield* owner.start();
      const launches = yield* ThreadLaunch.ThreadLaunchService;
      const launched = yield* launches.launch(owner.workerInput);
      const action = {
        type: "started" as const,
        taskId: owner.task.taskId,
        launchCommandId: owner.task.launchCommandId,
        workerThreadId: owner.task.workerThreadId,
        workerRunId: launched.projection.runs[0]!.id,
      };
      const orchestrator = yield* Orchestrator.OrchestratorV2;
      const wrong = [
        { ...action, launchCommandId: CommandId.make("foreign:launch") },
        { ...action, workerThreadId: ThreadId.make("foreign:thread") },
        { ...action, workerRunId: RunId.make("foreign:run") },
      ];
      for (const [index, attempt] of wrong.entries()) {
        const exit = yield* orchestrator
          .dispatch({
            type: "multi-agent.controller.command",
            commandId: CommandId.make(`wrong:ack:${index}`),
            threadId: owner.input.threadId,
            runId: owner.run.id,
            action: attempt,
          })
          .pipe(Effect.exit);
        assert.isTrue(Exit.isFailure(exit));
      }
      assert.equal((yield* owner.readTask()).status, "launching");
      const executor = yield* MultiAgentWorkerLaunchService.MultiAgentWorkerLaunchService;
      yield* executor.execute(owner.input);
      const task = yield* owner.readTask();
      assert.equal(task.status, "running");
      assert.equal("workerRunId" in task ? task.workerRunId : undefined, action.workerRunId);
    }).pipe(Effect.provide(harness.layer));
  },
);

it.effect("refuses a launch command receipt belonging to another thread", () => {
  const harness = makeHarness({ providers: [launchCatalogProvider] });
  return Effect.gen(function* () {
    const owner = yield* seedControlledLaunch();
    yield* owner.start();
    const launches = yield* ThreadLaunch.ThreadLaunchService;
    yield* launches.launch({
      ...launchInput({ command: owner.task.launchCommandId, thread: "foreign:worker" }),
    });
    const executor = yield* MultiAgentWorkerLaunchService.MultiAgentWorkerLaunchService;
    const exit = yield* executor.execute(owner.input).pipe(Effect.exit);
    assert.isTrue(Exit.isFailure(exit));
    assert.equal((yield* owner.readTask()).status, "launching");
    const threads = yield* ThreadManagement.ThreadManagementService;
    assert.isNull(yield* threads.getThreadShell(owner.task.workerThreadId));
  }).pipe(Effect.provide(harness.layer));
});

it.effect("rejects stale reservation and mismatched task/run/launch identity", () => {
  const harness = makeHarness({ providers: [launchCatalogProvider] });
  return Effect.gen(function* () {
    const owner = yield* seedControlledLaunch();
    const executor = yield* MultiAgentWorkerLaunchService.MultiAgentWorkerLaunchService;
    for (const input of [
      owner.input,
      { ...owner.input, coordinatorRunId: RunId.make("foreign") },
      { ...owner.input, taskId: NodeId.make("foreign") },
      { ...owner.input, launchCommandId: CommandId.make("foreign") },
    ])
      assert.isTrue(Exit.isFailure(yield* executor.execute(input).pipe(Effect.exit)));
    assert.equal((yield* owner.readTask()).status, "reserved");
    yield* owner.start();
    const orchestrator = yield* Orchestrator.OrchestratorV2;
    yield* orchestrator.dispatch({
      type: "multi-agent.controller.command",
      commandId: CommandId.make("controller:cancel"),
      threadId: owner.input.threadId,
      runId: owner.run.id,
      action: { type: "cancel" },
    });
    yield* executor.execute(owner.input);
    assert.equal((yield* owner.readTask()).status, "cancelled");
    const threads = yield* ThreadManagement.ThreadManagementService;
    assert.isNull(yield* threads.getThreadShell(owner.task.workerThreadId));
  }).pipe(Effect.provide(harness.layer));
});

it.effect("persists independent worker grouping without changing lineage", () => {
  const harness = makeHarness();
  return Effect.gen(function* () {
    const launches = yield* ThreadLaunch.ThreadLaunchService;
    const threads = yield* ThreadManagement.ThreadManagementService;
    const coordinator = yield* launches.launch(
      launchInput({ command: "command:coordinator", thread: "thread:coordinator" }),
    );
    assert.isUndefined(coordinator.projection.thread.coordinatorThreadId);
    const worker = yield* launches.launch({
      ...launchInput({ command: "command:worker", thread: "thread:worker" }),
      coordinatorThreadId: coordinator.threadId,
    });
    const reloaded = yield* threads.getThreadProjection(worker.threadId);
    assert.equal(reloaded.thread.coordinatorThreadId, coordinator.threadId);
    assert.deepEqual(reloaded.thread.lineage, {
      parentThreadId: null,
      relationshipToParent: null,
      rootThreadId: worker.threadId,
    });
    assert.isNull(reloaded.thread.forkedFrom);
    const shell = yield* threads.getThreadShell(worker.threadId);
    assert.equal(shell?.coordinatorThreadId, coordinator.threadId);
    const events = yield* EventStore.EventStoreV2.pipe(Effect.provide(EventStore.layer));
    const recorded = yield* events.read({ threadId: worker.threadId }).pipe(Stream.runCollect);
    const created = recorded.find((stored) => stored.event.type === "thread.created");
    assert.isDefined(created);
    if (created?.event.type === "thread.created") {
      assert.equal(created.event.payload.coordinatorThreadId, coordinator.threadId);
    }
    // Rebuild a fresh projection using events read back from persistent storage.
    yield* Effect.gen(function* () {
      const replay = yield* ProjectionStore.ProjectionStoreV2;
      for (const stored of recorded) yield* replay.apply(stored.event);
      const snapshot = yield* replay.getShellSnapshot();
      assert.equal(snapshot.threads[0]?.coordinatorThreadId, coordinator.threadId);
      assert.deepEqual(snapshot.threads[0]?.lineage, reloaded.thread.lineage);
    }).pipe(Effect.provide(ProjectionStore.layerMemory));
    // A new SQL projection service must recover the link from the same database.
    yield* Effect.gen(function* () {
      const projections = yield* ProjectionStore.ProjectionStoreV2;
      const snapshot = yield* projections.getShellSnapshot();
      assert.equal(
        snapshot.threads.find((thread) => thread.id === worker.threadId)?.coordinatorThreadId,
        coordinator.threadId,
      );
    }).pipe(Effect.provide(ProjectionStore.layer));
  }).pipe(Effect.provide(harness.layer));
});

it.effect(
  "persists exact initial ownership before deferred workspace preparation and preserves duplicate receipts",
  () =>
    Effect.gen(function* () {
      const entered = yield* Deferred.make<void>();
      const release = yield* Deferred.make<void>();
      const harness = makeHarness({
        providers: [launchCatalogProvider],
        runSetup: (input) =>
          input.threadId === ThreadId.make("controlled:worker")
            ? Deferred.succeed(entered, undefined).pipe(
                Effect.andThen(Deferred.await(release)),
                Effect.as({ status: "no-script" as const }),
              )
            : Effect.succeed({ status: "no-script" as const }),
      });
      yield* Effect.gen(function* () {
        const fixture = yield* seedControlledLaunch();
        yield* fixture.start();
        const launches = yield* ThreadLaunch.ThreadLaunchService;
        const first = yield* launches.launch(fixture.workerInput);
        yield* Deferred.await(entered);
        const threads = yield* ThreadManagement.ThreadManagementService;
        const initial = (yield* threads.getThreadProjection(first.threadId)).runs[0]!;
        const ownership = initial.controlledWorkerOwnership!;
        assert.equal(initial.status, "preparing");
        assert.deepEqual(ownership, {
          ...fixture.workerInput.controlledWorkerLaunchOwner!,
          workerRunId: initial.id,
          workerAttemptId: initial.activeAttemptId,
          providerInstanceId: initial.providerInstanceId,
          providerThreadId: initial.providerThreadId,
          providerSessionId: first.projection.providerThreads.find(
            (row) => row.id === initial.providerThreadId,
          )!.providerSessionId,
        });
        const retry = yield* launches.launch(fixture.workerInput);
        assert.equal(retry.projection.runs.length, 1);
        assert.deepEqual(retry.projection.runs[0]!.controlledWorkerOwnership, ownership);
        const projections = yield* ProjectionStore.ProjectionStoreV2;
        assert.isTrue(
          yield* projections.controlledWorkerStartAllowed(
            first.threadId,
            initial.id,
            initial.activeAttemptId!,
          ),
        );
        assert.isFalse(
          yield* projections.controlledWorkerStartAllowed(
            first.threadId,
            initial.id,
            RunAttemptId.make("stale-attempt"),
          ),
        );
        const conflict = yield* launches
          .launch({
            ...fixture.workerInput,
            controlledWorkerLaunchOwner: {
              ...fixture.workerInput.controlledWorkerLaunchOwner!,
              taskId: NodeId.make("other-task"),
            },
          })
          .pipe(Effect.exit);
        assert.isTrue(Exit.isFailure(conflict));
        yield* Deferred.succeed(release, undefined);
      }).pipe(Effect.provide(harness.layer));
    }),
);

it.effect.each(["coordinatorRunId", "taskId", "workerThreadId", "launchMessageId"] as const)(
  "rejects conflicting %s ownership before initial dispatch can start preparation",
  (field) => {
    const harness = makeHarness({ providers: [launchCatalogProvider] });
    return Effect.gen(function* () {
      const fixture = yield* seedControlledLaunch();
      yield* fixture.start();
      const launches = yield* ThreadLaunch.ThreadLaunchService;
      const owner = fixture.workerInput.controlledWorkerLaunchOwner!;
      const invalid = { ...owner, [field]: "mismatched-identity" };
      const result = yield* launches
        .launch({ ...fixture.workerInput, controlledWorkerLaunchOwner: invalid })
        .pipe(Effect.exit);
      assert.isTrue(Exit.isFailure(result));
      const threads = yield* ThreadManagement.ThreadManagementService;
      const projection = yield* threads.getThreadProjection(fixture.task.workerThreadId);
      assert.equal(projection.runs.length, 0);
    }).pipe(Effect.provide(harness.layer));
  },
);

it.effect(
  "fails closed for an older unowned controlled initial run without affecting ordinary launches",
  () => {
    const harness = makeHarness({ providers: [launchCatalogProvider] });
    return Effect.gen(function* () {
      const fixture = yield* seedControlledLaunch();
      yield* fixture.start();
      const launches = yield* ThreadLaunch.ThreadLaunchService;
      const { controlledWorkerLaunchOwner: _owner, ...legacy } = fixture.workerInput;
      const old = yield* launches.launch(legacy);
      const run = old.projection.runs[0]!;
      const projections = yield* ProjectionStore.ProjectionStoreV2;
      assert.isFalse(
        yield* projections.controlledWorkerStartAllowed(old.threadId, run.id, run.activeAttemptId!),
      );
      const recovery = yield* launches.launch(fixture.workerInput).pipe(Effect.exit);
      assert.isTrue(Exit.isFailure(recovery));
      // Grouping is presentation metadata, not execution authority.
      const sink = yield* EventSink.EventSinkV2;
      const { coordinatorThreadId: _group, ...ungrouped } = old.projection.thread;
      yield* sink.write({
        events: [
          {
            id: EventId.make("legacy:ungrouped"),
            type: "thread.metadata-updated",
            threadId: old.threadId,
            occurredAt: yield* DateTime.now,
            payload: ungrouped,
          },
        ],
      });
      assert.isFalse(
        yield* projections.controlledWorkerStartAllowed(old.threadId, run.id, run.activeAttemptId!),
      );
      const ordinary = yield* launches.launch(
        launchInput({
          command: "ordinary-owner-test",
          thread: "ordinary-owner-test",
          message: "Ordinary turn",
        }),
      );
      const ordinaryRun = ordinary.projection.runs[0]!;
      assert.isUndefined(ordinaryRun.controlledWorkerOwnership);
      assert.isTrue(
        yield* projections.controlledWorkerStartAllowed(
          ordinary.threadId,
          ordinaryRun.id,
          ordinaryRun.activeAttemptId!,
        ),
      );
    }).pipe(Effect.provide(harness.layer));
  },
);

it.effect(
  "preserves immutable worker ownership through stale updates, SQL reload and event replay",
  () => {
    const harness = makeHarness({ providers: [launchCatalogProvider] });
    return Effect.gen(function* () {
      const fixture = yield* seedControlledLaunch();
      yield* fixture.start();
      const launches = yield* ThreadLaunch.ThreadLaunchService;
      const worker = yield* launches.launch(fixture.workerInput);
      const initial = worker.projection.runs[0]!;
      const sink = yield* EventSink.EventSinkV2;
      const { controlledWorkerOwnership: ownership, ...stale } = initial;
      yield* sink.write({
        events: [
          {
            id: EventId.make("ownership:stale"),
            type: "run.updated",
            threadId: worker.threadId,
            runId: initial.id,
            occurredAt: yield* DateTime.now,
            payload: stale,
          },
        ],
      });
      const conflict = yield* sink
        .write({
          events: [
            {
              id: EventId.make("ownership:conflict"),
              type: "run.updated",
              threadId: worker.threadId,
              runId: initial.id,
              occurredAt: yield* DateTime.now,
              payload: {
                ...initial,
                controlledWorkerOwnership: {
                  ...ownership!,
                  workerAttemptId: RunAttemptId.make("different-attempt"),
                },
              },
            },
          ],
        })
        .pipe(Effect.exit);
      assert.isTrue(Exit.isFailure(conflict));
      const events = yield* EventStore.EventStoreV2.pipe(Effect.provide(EventStore.layer));
      const recorded = yield* events.read({}).pipe(Stream.runCollect);
      for (const layer of [ProjectionStore.layerMemory, ProjectionStore.layer]) {
        yield* Effect.gen(function* () {
          const projections = yield* ProjectionStore.ProjectionStoreV2;
          if (layer === ProjectionStore.layerMemory)
            for (const stored of recorded) yield* projections.apply(stored.event);
          const persisted = (yield* projections.getThreadProjection(worker.threadId)).runs[0]!;
          assert.deepEqual(persisted.controlledWorkerOwnership, ownership);
          assert.isTrue(
            yield* projections.controlledWorkerStartAllowed(
              worker.threadId,
              initial.id,
              initial.activeAttemptId!,
            ),
          );
          const conflictReplay = yield* projections
            .apply({
              id: EventId.make("ownership:replay-conflict"),
              type: "run.updated",
              threadId: worker.threadId,
              runId: initial.id,
              occurredAt: yield* DateTime.now,
              payload: {
                ...initial,
                controlledWorkerOwnership: { ...ownership!, taskId: NodeId.make("wrong-task") },
              },
            })
            .pipe(Effect.exit);
          assert.isTrue(Exit.isFailure(conflictReplay));
        }).pipe(Effect.provide(layer));
      }
    }).pipe(Effect.provide(harness.layer));
  },
);

it.effect("rejects ownership without a persisted launch authorization", () => {
  const harness = makeHarness({ providers: [launchCatalogProvider] });
  return Effect.gen(function* () {
    const fixture = yield* seedControlledLaunch();
    // Still reserved: no launch command has committed.
    const launches = yield* ThreadLaunch.ThreadLaunchService;
    const result = yield* launches.launch(fixture.workerInput).pipe(Effect.exit);
    assert.isTrue(Exit.isFailure(result));
    const threads = yield* ThreadManagement.ThreadManagementService;
    assert.equal((yield* threads.getThreadProjection(fixture.task.workerThreadId)).runs.length, 0);
  }).pipe(Effect.provide(harness.layer));
});

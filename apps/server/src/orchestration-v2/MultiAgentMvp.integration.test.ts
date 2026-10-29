import { makeProviderFailure } from "@t3tools/provider-core/server/failure";
import { assert, it } from "@effect/vitest";
import * as NodeServices from "@effect/platform-node/NodeServices";
import * as NodeCrypto from "@effect/platform-node/NodeCrypto";
import {
  CommandId,
  EnvironmentId,
  MessageId,
  NodeId,
  ProjectId,
  ProviderDriverKind,
  ProviderInstanceId,
  ProviderTurnId,
  RunId,
  ThreadId,
  TurnItemId,
  type Project,
  type ServerProvider,
  type OrchestrationV2Run,
} from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import * as FileSystem from "effect/FileSystem";
import * as Path from "effect/Path";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Queue from "effect/Queue";
import * as Ref from "effect/Ref";
import * as Stream from "effect/Stream";
import * as TestClock from "effect/testing/TestClock";
import { MULTI_AGENT_COORDINATOR_INSTRUCTIONS } from "./MultiAgentCoordinatorInstructions.ts";
import { multiAgentCoordinatorInstructions } from "./MultiAgentCoordinatorInstructions.ts";
import { createMultiAgentAutoCatalog } from "@t3tools/shared/multiAgentRouting";
import { controlledCodexOriginBlocked } from "./ControlledCodexContinuation.ts";
import * as ProviderAdapter from "@t3tools/provider-core/server/ProviderAdapter";
import { makeContinuationEmitter } from "@t3tools/provider-core/server/ProviderContinuationRequests";
import * as IdAllocator from "@t3tools/provider-core/server/IdAllocator";
import * as CheckpointStore from "../checkpointing/CheckpointStore.ts";
import * as GitWorkflow from "../git/GitWorkflowService.ts";
import * as OrchestratorMcpService from "../mcp/OrchestratorMcpService.ts";
import type { McpInvocationScope } from "../mcp/McpInvocationContext.ts";
import * as ManagedProjectFolders from "../project/ManagedProjectFolders.ts";
import * as ProjectCloneTracker from "../project/ProjectCloneTracker.ts";
import * as ProjectService from "../project/ProjectService.ts";
import * as ProjectSetupScriptRunner from "../project/ProjectSetupScriptRunner.ts";
import * as WorktreeSetupTracker from "../project/WorktreeSetupTracker.ts";
import * as ScheduledTaskService from "../scheduledTasks/ScheduledTaskService.ts";
import * as SecretRequests from "../secrets/SecretRequests.ts";
import * as TerminalManager from "../terminal/Manager.ts";
import * as TextGeneration from "../textGeneration/TextGeneration.ts";
import * as SqlitePersistence from "../persistence/Sqlite.ts";
import * as ServerSettings from "../serverSettings.ts";
import * as ProviderRegistryMock from "../provider/testUtils/providerRegistryMock.ts";
import { CodexProviderCapabilitiesV2 } from "./Adapters/CodexAdapterV2.ts";
import * as CommandReceiptStore from "./CommandReceiptStore.ts";
import * as EffectOutbox from "./EffectOutbox.ts";
import * as EffectWorker from "./EffectWorker.ts";
import * as EventStore from "./EventStore.ts";
import * as MultiAgentWorkerLaunchService from "./MultiAgentWorkerLaunchService.ts";
import * as Orchestrator from "./Orchestrator.ts";
import * as ProjectionStore from "./ProjectionStore.ts";
import * as ProviderAdapterRegistry from "./ProviderAdapterRegistry.ts";
import * as ThreadLaunchService from "./ThreadLaunchService.ts";
import * as ThreadManagementService from "./ThreadManagementService.ts";
import * as ProviderReplayHarness from "./testkit/ProviderReplayHarness.ts";

const instanceId = ProviderInstanceId.make("codex");
const driver = ProviderDriverKind.make("codex");
const coordinatorId = ThreadId.make("dj:coordinator");
const projectId = ProjectId.make("dj:project");
const workerSelection = {
  instanceId,
  model: "scripted-worker",
  options: [{ id: "scripted-effort", value: "deliberate" }],
};
const coordinatorSelection = { instanceId, model: "scripted-coordinator" };
const autoSimpleSelection = {
  instanceId,
  model: "scripted-economy",
  options: [{ id: "depth", value: "light" }],
};
const autoComplexSelection = {
  instanceId,
  model: "scripted-reasoning",
  options: [{ id: "depth", value: "deep" }],
};
const tasks = [
  {
    key: "frontend",
    role: "worker",
    title: "DJ frontend",
    prompt: "Build a DJ song-selection interface.",
  },
  {
    key: "backend",
    role: "worker",
    title: "DJ backend",
    prompt: "Build a song-recommendation API.",
  },
] as const;
const outputs = {
  frontend: "Frontend complete: DJ song-selection interface with a searchable song list.",
  backend: "Backend complete: song-recommendation API with a recommendations endpoint.",
};
const catalog: ServerProvider = {
  instanceId,
  driver,
  enabled: true,
  installed: true,
  version: null,
  status: "ready",
  auth: { status: "authenticated" },
  checkedAt: "2026-10-14T00:00:00Z",
  slashCommands: [],
  skills: [],
  models: [
    ...[autoSimpleSelection, autoComplexSelection].map((selection) => ({
      slug: selection.model,
      name: selection.model,
      isCustom: true,
      capabilities: {
        optionDescriptors: [
          {
            id: "depth",
            label: "Reasoning",
            type: "select" as const,
            options: [
              { id: "light", label: "Light" },
              { id: "deep", label: "Deep" },
            ],
            currentValue: "light",
          },
        ],
      },
    })),
    {
      slug: coordinatorSelection.model,
      name: "Scripted coordinator",
      isCustom: true,
      capabilities: null,
    },
    {
      slug: workerSelection.model,
      name: "Scripted worker",
      isCustom: true,
      capabilities: {
        optionDescriptors: [
          {
            id: "scripted-effort",
            label: "Scripted effort",
            type: "select",
            options: [{ id: "deliberate", label: "Deliberate" }],
          },
        ],
      },
    },
  ],
};
const project: Project = {
  id: projectId,
  title: "Scripted DJ project",
  workspaceRoot: process.cwd(),
  repositoryIdentity: null,
  faviconPath: null,
  projectIcon: null,
  defaultModelSelection: coordinatorSelection,
  defaultThreadEnvMode: null,
  autoPull: false,
  scripts: [],
  createdAt: "2026-10-14T00:00:00Z",
  updatedAt: "2026-10-14T00:00:00Z",
  deletedAt: null,
};

it.effect.each([
  "normal",
  "staggered-review",
  "partial-failure-review",
  "review-response-lost",
  "review-additional-tasks",
  "stop-and-replay",
  "stop-before-schedule",
  "submission-gated",
  "preauthorized-start-gated",
  "four-task-capacity",
  "unsupported-coordinator-runtime",
  "native-goal-proposal",
  "simple-no-delegation",
  "one-worker",
  "off-no-instructions",
  "auto-zero",
  "auto-one",
  "auto-two",
  "auto-stop-and-replay",
  "auto-invalid-model",
  "auto-invalid-option",
] as const)("DJ coordinator and two committed workers: %s", (scenario) =>
  Effect.gen(function* () {
    const auto = scenario.startsWith("auto-");
    const stopping = scenario === "stop-and-replay" || scenario === "auto-stop-and-replay";
    const proposedTasks =
      scenario === "simple-no-delegation" ||
      scenario === "off-no-instructions" ||
      scenario === "auto-zero"
        ? []
        : scenario === "one-worker" || scenario === "auto-one"
          ? tasks.slice(0, 1)
          : scenario === "native-goal-proposal"
            ? tasks.map((task) => ({ ...task, prompt: `/goal ${task.prompt}` }))
            : scenario === "four-task-capacity"
              ? [
                  ...tasks,
                  { ...tasks[0], key: "frontend-review" },
                  { ...tasks[1], key: "backend-review" },
                ]
              : tasks;
    const expectedWorkers = Math.min(2, proposedTasks.length);
    const expectedSubmissions = 1 + expectedWorkers;
    const userGoal =
      scenario === "simple-no-delegation"
        ? "Explain what this application does."
        : "Plan a DJ interface and recommendation API.";
    const coordinatorResponse =
      proposedTasks.length === 0
        ? "This application helps plan DJ sets."
        : JSON.stringify({
            tasks: proposedTasks.map((task, index) => {
              if (!auto) return task;
              const selection = index === 0 ? autoSimpleSelection : autoComplexSelection;
              const candidate = createMultiAgentAutoCatalog(coordinatorSelection, [
                catalog,
              ])!.candidates.find((c) => c.model.slug === selection.model)!;
              return {
                ...task,
                routing: {
                  candidateId: scenario === "auto-invalid-model" ? "invented" : candidate.id,
                  options:
                    scenario === "auto-invalid-option"
                      ? [{ id: "depth", value: "unsupported" }]
                      : selection.options,
                  reason:
                    index === 0
                      ? "Routine independent interface work"
                      : "Ambiguous recommendation algorithm requires deeper reasoning",
                },
              };
            }),
          });
    const finishWorkers = yield* Deferred.make<void>();
    const managementReady =
      yield* Deferred.make<ThreadManagementService.ThreadManagementService["Service"]>();
    const interruptions = yield* Ref.make<ReadonlyArray<string>>([]);
    const submissions = yield* Ref.make<ReadonlyArray<ProviderAdapter.ProviderAdapterV2TurnInput>>(
      [],
    );
    const launchExecutor =
      yield* Deferred.make<
        MultiAgentWorkerLaunchService.MultiAgentWorkerLaunchService["Service"]
      >();
    const fs = yield* FileSystem.FileSystem;
    const path = yield* Path.Path;
    const directory = yield* fs.makeTempDirectoryScoped({ prefix: "t3-dj-mvp-" });
    const filename = path.join(directory, "state.sqlite");
    const database = stopping
      ? SqlitePersistence.layerFromPath(filename).pipe(
          Layer.provide(NodeServices.layer),
          Layer.orDie,
        )
      : SqlitePersistence.layerMemory;
    const stores = ProjectionStore.layer.pipe(Layer.provide(database));
    // This is the external provider boundary. It only emits normal adapter events;
    // it cannot grant commitments, create runs, or write projections directly.
    const adapterLayer = Layer.effect(
      ProviderAdapterRegistry.ProviderAdapterRegistryV2,
      Effect.gen(function* () {
        const projections = yield* ProjectionStore.ProjectionStoreV2;
        const emitter = yield* makeContinuationEmitter(instanceId);
        const adapter = ProviderAdapter.ProviderAdapterV2.of({
          instanceId,
          driver,
          getCapabilities: () => Effect.succeed(CodexProviderCapabilitiesV2),
          planSelectionTransition: () => Effect.succeed({ type: "apply_on_next_turn" }),
          openSession: (open) =>
            Effect.gen(function* () {
              const sessionScope = yield* Effect.scope;
              const now = yield* DateTime.now;
              const identity = yield* emitter.forSession(open.providerSessionId);
              const events = yield* Queue.unbounded<ProviderAdapter.ProviderAdapterV2Event>();
              return {
                instanceId,
                driver:
                  scenario === "unsupported-coordinator-runtime"
                    ? ProviderDriverKind.make("scripted-unsupported")
                    : driver,
                providerSessionId: open.providerSessionId,
                continuationEmitterIdentity: identity.identity,
                providerSession: {
                  id: open.providerSessionId,
                  driver,
                  providerInstanceId: instanceId,
                  status: "ready",
                  cwd: process.cwd(),
                  model: open.modelSelection.model,
                  capabilities: CodexProviderCapabilitiesV2,
                  createdAt: now,
                  updatedAt: now,
                  lastError: null,
                },
                events: Stream.fromQueue(events),
                ensureThread: (input) => {
                  if (!input.existingProviderThread)
                    return Effect.die("Expected the T3-created provider thread");
                  return Effect.succeed({
                    ...input.existingProviderThread,
                    nativeThreadRef: {
                      driver,
                      nativeId: `native:${input.threadId}`,
                      strength: "strong" as const,
                    },
                  });
                },
                resumeThread: (input) => Effect.succeed(input.providerThread),
                startTurn: (input) =>
                  Effect.gen(function* () {
                    const worker = yield* projections.getThreadRecords(input.threadId, ["runs"]);
                    const run = worker.runs.find((row) => row.id === input.runId)!;
                    if (run.multiAgentReview) {
                      const history = yield* projections.getThreadRecords(input.threadId, [
                        "messages",
                      ]);
                      const instruction = history.messages.find(
                        (message) => message.id === run.userMessageId,
                      )!;
                      assert.equal(instruction.role, "user");
                      assert.equal(instruction.createdBy, "agent");
                      assert.equal(instruction.creationSource, "server");
                      assert.include(instruction.text, "Your delegated work has settled.");
                      assert.include(input.message.text, instruction.text);
                    }
                    let workerKey: string | undefined;
                    if (input.threadId !== coordinatorId) {
                      assert.notInclude(input.message.text, MULTI_AGENT_COORDINATOR_INSTRUCTIONS);
                      const owner = run.controlledWorkerOwnership!;
                      assert.isDefined(owner);
                      const source = yield* projections.getThreadRecords(
                        owner.coordinatorThreadId,
                        ["runs"],
                        { runIds: [owner.coordinatorRunId] },
                      );
                      const task = source.runs[0]!.multiAgentController!.tasks.find(
                        (row) => row.taskId === owner.taskId,
                      )!;
                      const commitment = task.submissionCommitment!;
                      workerKey = task.key;
                      assert.isDefined(commitment);
                      assert.equal(commitment.workerRunId, input.runId);
                      assert.equal(commitment.workerAttemptId, input.attemptId);
                      assert.equal(commitment.providerSessionId, open.providerSessionId);
                      assert.deepEqual(
                        input.modelSelection,
                        auto
                          ? workerKey === "frontend"
                            ? autoSimpleSelection
                            : autoComplexSelection
                          : workerSelection,
                      );
                    }
                    yield* Ref.update(submissions, (previous) => [...previous, input]);
                    if (run.multiAgentReview && scenario === "review-response-lost")
                      return yield* new ProviderAdapter.ProviderAdapterTurnStartError({
                        driver,
                        threadId: input.threadId,
                        providerThreadId: input.providerThread.id,
                        runId: input.runId,
                        cause: "Scripted lost submit response",
                      });
                    const at = yield* DateTime.now;
                    const turnId = ProviderTurnId.make(
                      `turn:${input.threadId}${run.multiAgentReview ? ":review:" + run.id : ""}`,
                    );
                    const messageId = MessageId.make(
                      `answer:${input.threadId}${run.multiAgentReview ? ":review:" + run.id : ""}`,
                    );
                    const text =
                      input.threadId === coordinatorId
                        ? run.multiAgentReview
                          ? scenario === "review-additional-tasks" &&
                            worker.runs[0]!.multiAgentController!.reviews!.length === 1
                            ? JSON.stringify({
                                tasks: tasks.map((task) => ({
                                  ...task,
                                  key: `review-${task.key}`,
                                })),
                              })
                            : "Reviewed both worker results, integrated and verified the work. Final answer."
                          : coordinatorResponse
                        : workerKey === "frontend"
                          ? outputs.frontend
                          : outputs.backend;
                    const turn = {
                      id: turnId,
                      providerThreadId: input.providerThread.id,
                      nodeId: input.rootNodeId,
                      runAttemptId: input.attemptId,
                      nativeTurnRef: {
                        driver,
                        nativeId: `native-turn:${input.threadId}`,
                        strength: "strong" as const,
                      },
                      ordinal: input.providerTurnOrdinal,
                      status: "running" as const,
                      startedAt: at,
                      completedAt: null,
                    };
                    const responseEvents: ReadonlyArray<ProviderAdapter.ProviderAdapterV2Event> = [
                      {
                        type: "provider_turn.updated",
                        driver,
                        threadId: input.threadId,
                        providerTurn: turn,
                      },
                      {
                        type: "message.updated",
                        driver,
                        message: {
                          id: messageId,
                          threadId: input.threadId,
                          runId: input.runId,
                          nodeId: input.rootNodeId,
                          role: "assistant",
                          text,
                          attachments: [],
                          streaming: false,
                          createdAt: at,
                          updatedAt: at,
                          createdBy: "agent",
                          creationSource: "provider",
                        },
                      },
                      {
                        type: "turn_item.updated",
                        driver,
                        turnItem: {
                          id: TurnItemId.make(
                            `answer-item:${input.threadId}${run.multiAgentReview ? ":review:" + run.id : ""}`,
                          ),
                          threadId: input.threadId,
                          runId: input.runId,
                          nodeId: input.rootNodeId,
                          providerThreadId: input.providerThread.id,
                          providerTurnId: turnId,
                          nativeItemRef: null,
                          parentItemId: null,
                          ordinal: 2,
                          status: "completed",
                          title: null,
                          startedAt: at,
                          completedAt: at,
                          updatedAt: at,
                          type: "assistant_message",
                          messageId,
                          text,
                          streaming: false,
                        },
                      },
                      {
                        type: "provider_turn.updated",
                        driver,
                        threadId: input.threadId,
                        providerTurn: { ...turn, status: "completed", completedAt: at },
                      },
                      {
                        type: "turn.terminal",
                        driver,
                        providerThreadId: input.providerThread.id,
                        providerTurnId: turnId,
                        runOrdinal: input.runOrdinal,
                        ...(scenario === "partial-failure-review" && workerKey === "backend"
                          ? {
                              status: "failed" as const,
                              failureItemOrdinal: 1000001,
                              failure: makeProviderFailure({
                                class: "provider_error",
                                message: "Scripted worker failure",
                                retryable: false,
                              }),
                            }
                          : { status: "completed" as const, failure: null }),
                        threadDisposition: "reusable",
                      },
                    ];
                    if (stopping && input.threadId === coordinatorId) {
                      yield* Queue.offer(events, {
                        type: "message.updated",
                        driver,
                        message: {
                          id: MessageId.make("dj:late-proposal"),
                          threadId: coordinatorId,
                          runId: input.runId,
                          nodeId: input.rootNodeId,
                          role: "assistant",
                          text: JSON.stringify({
                            tasks: [
                              {
                                key: "third",
                                role: "worker",
                                prompt: "Unwanted third assignment.",
                              },
                            ],
                          }),
                          attachments: [],
                          streaming: false,
                          createdAt: at,
                          updatedAt: at,
                          createdBy: "agent",
                          creationSource: "provider",
                        },
                      });
                    }
                    if (
                      (stopping || (scenario === "staggered-review" && workerKey === "backend")) &&
                      input.threadId !== coordinatorId
                    ) {
                      yield* Queue.offer(events, responseEvents[0]!);
                      // The provider accepted its single invocation. Completion is
                      // delayed independently; Stop must not interrupt this work.
                      yield* Deferred.await(finishWorkers).pipe(
                        Effect.andThen(Queue.offerAll(events, responseEvents.slice(1))),
                        Effect.forkIn(sessionScope),
                      );
                    } else yield* Queue.offerAll(events, responseEvents);
                  }).pipe(Effect.orDie),
                steerTurn: () => Effect.die("No steering in this simulation"),
                interruptTurn: (input) =>
                  Ref.update(interruptions, (previous) => [...previous, input.providerThread.id]),
                respondToRuntimeRequest: () => Effect.die("No approval requests"),
                readThreadSnapshot: () => Effect.die("No native history reads"),
                rollbackThread: () => Effect.die("No rollback"),
                forkThread: () => Effect.die("Workers are independent threads"),
              } satisfies ProviderAdapter.ProviderAdapterV2SessionRuntime;
            }),
        });
        return ProviderAdapterRegistry.ProviderAdapterRegistryV2.of({
          get: () => Effect.succeed(adapter),
          list: () => Effect.succeed([instanceId]),
        });
      }),
    ).pipe(Layer.provide(stores));
    const runtime = ProviderReplayHarness.layerWithRegistry(
      { name: "multi-agent-mvp" },
      adapterLayer,
      {
        databaseLayer: database,
        runEffectWorker: false,
        providerCatalog: [catalog],
        ...(scenario === "submission-gated" || scenario === "preauthorized-start-gated"
          ? { controlledWorkerExecutionDisabled: true }
          : {}),
        controlledWorkerLaunch: (input) =>
          Deferred.await(launchExecutor).pipe(Effect.flatMap((service) => service.execute(input))),
        stopDelegatedTasks: (input) =>
          Deferred.await(managementReady).pipe(
            Effect.flatMap((service) => service.stopDelegatedTasks(input)),
          ),
        checkpointStoreLayer: Layer.mock(CheckpointStore.CheckpointStore)({
          isGitRepository: () => Effect.succeed(false),
        }),
      },
    );
    const settings = ServerSettings.layerTest();
    const receipts = CommandReceiptStore.layer.pipe(Layer.provide(database));
    const external = Layer.mergeAll(
      NodeCrypto.layer,
      settings,
      ProviderRegistryMock.layer([catalog]),
      WorktreeSetupTracker.layer,
      Layer.mock(ProjectService.ProjectService)({
        getById: (id) => Effect.succeed(id === projectId ? Option.some(project) : Option.none()),
      }),
      Layer.mock(ProjectCloneTracker.ProjectCloneTracker)({ get: () => Effect.succeed(null) }),
      Layer.mock(TerminalManager.TerminalManager)({ close: () => Effect.void }),
      Layer.mock(GitWorkflow.GitWorkflowService)({}),
      Layer.mock(ProjectSetupScriptRunner.ProjectSetupScriptRunner)({
        runForThread: () => Effect.succeed({ status: "no-script" }),
      }),
      Layer.mock(TextGeneration.TextGeneration)({
        generateThreadTitle: () => Effect.succeed({ title: "Scripted DJ" }),
      }),
      Layer.mock(ManagedProjectFolders.ManagedProjectFolders)({
        namedProjectsRoot: process.cwd(),
        folderForThread: () => Effect.succeed(Option.none()),
      }),
    );
    const management = ThreadManagementService.layer.pipe(
      Layer.provide(Layer.mergeAll(runtime, receipts, external)),
    );
    const launches = ThreadLaunchService.layer.pipe(
      Layer.provide(Layer.mergeAll(management, receipts, external, IdAllocator.layer)),
    );
    const controlledLaunches = MultiAgentWorkerLaunchService.layer.pipe(
      Layer.provide(Layer.mergeAll(launches, management, receipts, external)),
    );
    const reads = OrchestratorMcpService.layer.pipe(
      Layer.provide(
        Layer.mergeAll(
          management,
          adapterLayer,
          external,
          Layer.mock(ScheduledTaskService.ScheduledTaskService)({}),
          Layer.mock(SecretRequests.SecretRequests)({}),
        ),
      ),
    );
    const services = Layer.mergeAll(
      runtime,
      controlledLaunches,
      launches,
      reads,
      management,
      stores,
      EffectOutbox.layer.pipe(Layer.provide(database)),
      EventStore.layer.pipe(Layer.provide(database)),
      external,
    ).pipe(Layer.provide(NodeServices.layer));
    const expected = yield* Effect.scoped(
      Effect.gen(function* () {
        yield* Deferred.succeed(
          launchExecutor,
          yield* MultiAgentWorkerLaunchService.MultiAgentWorkerLaunchService,
        );
        const launcher = yield* ThreadLaunchService.ThreadLaunchService;
        yield* Deferred.succeed(
          managementReady,
          yield* ThreadManagementService.ThreadManagementService,
        );
        const commands = yield* Orchestrator.OrchestratorV2;
        const worker = yield* EffectWorker.OrchestrationEffectWorkerV2;
        const projections = yield* ProjectionStore.ProjectionStoreV2;
        const outbox = yield* EffectOutbox.EffectOutboxV2;
        const awaitRunStatus = (run: OrchestrationV2Run, status: OrchestrationV2Run["status"]) =>
          commands.streamStoredEventsFrom({ threadId: run.threadId, afterSequence: 0 }).pipe(
            Stream.filter(
              (stored) =>
                (stored.event.type === "run.updated" || stored.event.type === "run.created") &&
                stored.event.payload.id === run.id &&
                stored.event.payload.status === status,
            ),
            Stream.take(1),
            Stream.runDrain,
          );
        const awaitSettledTasks = (count: number) =>
          commands.streamStoredEventsFrom({ threadId: coordinatorId, afterSequence: 0 }).pipe(
            Stream.filter(
              (stored) =>
                stored.event.type === "multi-agent.controller.updated" &&
                stored.event.payload.state.tasks.filter(
                  (task) => task.status === "completed" || task.status === "failed",
                ).length >= count,
            ),
            Stream.take(1),
            Stream.runDrain,
          );
        const awaitReview = (originRunId: RunId) =>
          Effect.gen(function* () {
            for (
              let generation = 0;
              generation < (scenario === "review-additional-tasks" ? 2 : 1);
              generation++
            ) {
              const reviewEvent = yield* commands
                .streamStoredEventsFrom({ threadId: coordinatorId, afterSequence: 0 })
                .pipe(
                  Stream.filter(
                    (stored) =>
                      stored.event.type === "run.created" &&
                      stored.event.payload.multiAgentReview?.coordinatorRunId === originRunId,
                  ),
                  Stream.drop(generation),
                  Stream.take(1),
                  Stream.runCollect,
                );
              const event = reviewEvent[0]!.event;
              assert.equal(event.type, "run.created");
              if (event.type !== "run.created") return yield* Effect.die("Missing review");
              yield* worker.drain();
              if (scenario === "review-response-lost")
                yield* awaitRunStatus(event.payload, "failed");
              else {
                yield* awaitRunStatus(event.payload, "waiting");
                yield* worker.drain();
                yield* awaitRunStatus(event.payload, "completed");
              }
              yield* outbox.enqueue([
                {
                  id: `duplicate-review:${event.payload.id}`,
                  commandId: CommandId.make("duplicate-review"),
                  threadId: coordinatorId,
                  request: { type: "provider-turn.start", runId: event.payload.id },
                },
              ]);
              yield* worker.drain();
              if (scenario === "review-additional-tasks" && generation === 0) {
                const controller = (yield* projections.getThreadRecords(coordinatorId, ["runs"]))
                  .runs[0]!.multiAgentController!;
                assert.lengthOf(controller.tasks, 4);
                assert.isEmpty(controller.reviews!.slice(1));
                for (const task of controller.tasks.slice(2)) {
                  const records = yield* projections.getThreadRecords(task.workerThreadId, [
                    "runs",
                  ]);
                  yield* awaitRunStatus(records.runs[0]!, "waiting");
                }
                yield* worker.drain();
                yield* awaitSettledTasks(4);
              }
            }
            yield* commands
              .streamStoredEventsFrom({ threadId: coordinatorId, afterSequence: 0 })
              .pipe(
                Stream.filter(
                  (stored) =>
                    stored.event.type === "multi-agent.controller.updated" &&
                    stored.event.payload.state.status === "settled",
                ),
                Stream.take(1),
                Stream.runDrain,
              );
            // Re-delivery and recovery may observe the batch repeatedly; they do not submit again.
            yield* commands.recoverDelegatedTasks;
            yield* worker.drain();
          });
        const coordinator = yield* launcher.launch({
          commandId: CommandId.make("dj:create-coordinator"),
          threadId: coordinatorId,
          projectId,
          title: "DJ coordinator",
          modelSelection: coordinatorSelection,
          runtimeMode: "full-access",
          interactionMode: "default",
          workspaceStrategy: { type: "root" },
          initialMessage: {
            messageId: MessageId.make("dj:request"),
            text: userGoal,
            attachments: [],
            multiAgent:
              scenario === "off-no-instructions"
                ? { mode: "off" }
                : auto
                  ? { mode: "auto", preset: "balanced" }
                  : { mode: "custom", roles: { worker: workerSelection } },
          },
          createdBy: "user",
          creationSource: "web",
        });
        const coordinatorRun = coordinator.projection.runs[0]!;
        yield* awaitRunStatus(coordinatorRun, "starting");
        yield* worker.drain(1);
        yield* awaitRunStatus(coordinatorRun, "waiting");
        const source = yield* projections.getThreadRecords(coordinatorId, [
          "runs",
          "attempts",
          "messages",
        ]);
        const proposal = source.messages.find(
          (message) =>
            message.id === MessageId.make(`answer:${coordinatorId}`) &&
            message.runId === coordinatorRun.id,
        )!;
        assert.equal(proposal.text, coordinatorResponse);
        assert.equal(
          source.messages.find((row) => row.id === coordinatorRun.userMessageId)!.text,
          userGoal,
        );
        const coordinatorSubmission = (yield* Ref.get(submissions))[0]!;
        const instructionCount =
          coordinatorSubmission.message.text.split(
            auto
              ? multiAgentCoordinatorInstructions(source.runs[0]!)
              : MULTI_AGENT_COORDINATOR_INSTRUCTIONS,
          ).length - 1;
        assert.equal(
          instructionCount,
          scenario === "off-no-instructions" || scenario === "unsupported-coordinator-runtime"
            ? 0
            : 1,
        );
        assert.include(coordinatorSubmission.message.text, userGoal);
        const admitted = source.runs[0]!.multiAgentController?.tasks ?? [];
        if (
          scenario === "simple-no-delegation" ||
          scenario === "off-no-instructions" ||
          scenario === "auto-zero"
        ) {
          assert.isEmpty(admitted);
          yield* outbox.enqueue([
            {
              id: `duplicate-coordinator:${scenario}`,
              commandId: CommandId.make(`duplicate-coordinator:${scenario}`),
              threadId: coordinatorId,
              request: { type: "provider-turn.start", runId: coordinatorRun.id },
            },
          ]);
          yield* worker.drain();
          assert.lengthOf(yield* Ref.get(submissions), 1);
          const controllerRun = (yield* projections.getThreadRecords(coordinatorId, ["runs"]))
            .runs[0]!;
          assert.equal(controllerRun.status, "completed");
          if (scenario === "off-no-instructions")
            assert.isUndefined(controllerRun.multiAgentController);
          const events = yield* EventStore.EventStoreV2.pipe(
            Effect.flatMap((store) => Stream.runCollect(store.read())),
          );
          assert.isFalse(
            events.some(
              (stored) =>
                stored.event.type === "run.created" && stored.event.threadId !== coordinatorId,
            ),
          );
          return { controllerRun, workerRuns: [] as OrchestrationV2Run[], events };
        }
        if (
          scenario === "unsupported-coordinator-runtime" ||
          scenario === "auto-invalid-model" ||
          scenario === "auto-invalid-option"
        ) {
          assert.isEmpty(admitted);
          yield* worker.drain();
          assert.lengthOf(yield* Ref.get(submissions), 1);
          assert.isEmpty(yield* Ref.get(interruptions));
          const controllerRun = (yield* projections.getThreadRecords(coordinatorId, ["runs"]))
            .runs[0]!;
          assert.isEmpty(controllerRun.multiAgentController!.tasks);
          const events = yield* EventStore.EventStoreV2.pipe(
            Effect.flatMap((store) => Stream.runCollect(store.read())),
          );
          assert.isFalse(
            events.some(
              (stored) =>
                stored.event.type === "run.created" && stored.event.threadId !== coordinatorId,
            ),
          );
          return { controllerRun, workerRuns: [] as OrchestrationV2Run[], events };
        }
        assert.lengthOf(admitted, proposedTasks.length);
        const identities = tasks.slice(0, expectedWorkers).map((proposal) => ({
          ...admitted.find((task) => task.key === proposal.key)!,
          key: proposal.key,
        }));
        assert.lengthOf(identities, expectedWorkers);
        assert.isTrue(
          identities.every(
            (task) =>
              task.status === "queued" &&
              task.proposalId === proposal.id &&
              task.proposalAttemptId === source.attempts[0]!.id,
          ),
        );
        assert.lengthOf(yield* Ref.get(submissions), 1);
        const intakeId = CommandId.make(
          `command:multi-agent:proposal:${[
            coordinatorId,
            coordinatorRun.id,
            source.attempts[0]!.id,
            TurnItemId.make(`answer-item:${coordinatorId}`),
          ]
            .map(encodeURIComponent)
            .join(":")}`,
        );
        const scheduleEffect = (yield* outbox.listByCommandId(intakeId))[0]!;
        assert.equal(scheduleEffect.request.type, "multi-agent.controller.schedule");
        yield* outbox.enqueue([
          {
            id: scheduleEffect.id,
            commandId: scheduleEffect.commandId,
            threadId: scheduleEffect.threadId,
            request: scheduleEffect.request,
          },
          {
            id: `${scheduleEffect.id}:duplicate-delivery`,
            commandId: scheduleEffect.commandId,
            threadId: scheduleEffect.threadId,
            request: scheduleEffect.request,
          },
        ]);
        if (
          scenario === "stop-before-schedule" ||
          scenario === "submission-gated" ||
          scenario === "native-goal-proposal"
        ) {
          const stop = {
            type: "multi-agent.controller.command" as const,
            commandId: CommandId.make("dj:stop-before-schedule"),
            threadId: coordinatorId,
            runId: coordinatorRun.id,
            action: { type: "stop_coordinator" as const },
          };
          if (scenario === "stop-before-schedule") {
            yield* commands.dispatch(stop);
            yield* commands.dispatch(stop);
          }
          yield* worker.drain();
          const row = (yield* projections.getThreadRecords(coordinatorId, ["runs"])).runs[0]!;
          if (scenario === "stop-before-schedule")
            assert.isDefined(row.multiAgentController!.coordinatorStoppedAt);
          assert.isTrue(
            row.multiAgentController!.tasks.every(
              (task) => task.status === "queued" && !task.submissionCommitment,
            ),
          );
          for (const identity of identities)
            assert.isNull(yield* projections.getThreadShell(identity.workerThreadId));
          assert.lengthOf(yield* Ref.get(submissions), 1);
          assert.isEmpty(yield* Ref.get(interruptions));
          const events = yield* EventStore.EventStoreV2.pipe(
            Effect.flatMap((store) => Stream.runCollect(store.read())),
          );
          return { controllerRun: row, workerRuns: [] as OrchestrationV2Run[], events };
        }
        if (scenario === "preauthorized-start-gated") {
          // Negative test only: prior authorization cannot bypass the default SDK gate.
          for (const identity of identities) {
            yield* commands.dispatch({
              type: "multi-agent.controller.command",
              commandId: CommandId.make(`negative:reserve:${identity.taskId}`),
              threadId: coordinatorId,
              runId: coordinatorRun.id,
              action: { type: "reserve", taskId: identity.taskId },
            });
            yield* commands.dispatch({
              type: "multi-agent.controller.command",
              commandId: CommandId.make(`negative:launch:${identity.taskId}`),
              threadId: coordinatorId,
              runId: coordinatorRun.id,
              action: { type: "launch", taskId: identity.taskId },
            });
          }
        }
        yield* worker.drain();
        yield* awaitRunStatus(coordinatorRun, "completed");
        const workerRuns = [];
        for (const identity of identities) {
          const records = yield* projections.getThreadRecords(identity.workerThreadId, ["runs"]);
          assert.equal(records.thread.coordinatorThreadId, coordinatorId);
          assert.equal(records.thread.lineage.parentThreadId, null);
          assert.lengthOf(records.runs, 1);
          const run = records.runs[0]!;
          assert.deepEqual(
            run.modelSelection,
            auto
              ? identity.key === "frontend"
                ? autoSimpleSelection
                : autoComplexSelection
              : workerSelection,
          );
          assert.deepEqual(run.multiAgent, { mode: "off" });
          assert.equal(run.controlledWorkerOwnership?.coordinatorRunId, coordinatorRun.id);
          assert.equal(run.controlledWorkerOwnership?.coordinatorThreadId, coordinatorId);
          assert.equal(run.controlledWorkerOwnership?.taskId, identity.taskId);
          assert.equal(run.controlledWorkerOwnership?.workerThreadId, identity.workerThreadId);
          assert.equal(run.controlledWorkerOwnership?.workerRunId, run.id);
          assert.equal(run.controlledWorkerOwnership?.workerAttemptId, run.activeAttemptId);
          assert.equal(run.controlledWorkerOwnership?.launchCommandId, identity.launchCommandId);
          assert.equal(run.controlledWorkerOwnership?.launchMessageId, identity.launchMessageId);
          workerRuns.push(run);
          yield* awaitRunStatus(run, "starting");
        }
        yield* worker.drain();
        if (scenario === "four-task-capacity") {
          yield* awaitSettledTasks(2);
          yield* worker.drain();
          const secondWave = (yield* projections.getThreadRecords(coordinatorId, [
            "runs",
          ])).runs[0]!.multiAgentController!.tasks.slice(2);
          assert.lengthOf(secondWave, 2);
          for (const task of secondWave) {
            assert.isTrue("workerRunId" in task);
            const records = yield* projections.getThreadRecords(task.workerThreadId, ["runs"]);
            yield* awaitRunStatus(records.runs[0]!, "waiting");
          }
          yield* worker.drain();
          yield* awaitSettledTasks(4);
          const row = (yield* projections.getThreadRecords(coordinatorId, ["runs"])).runs[0]!;
          assert.lengthOf(row.multiAgentController!.tasks, 4);
          assert.isTrue(
            row.multiAgentController!.tasks.every((task) => task.status === "completed"),
          );
          yield* awaitReview(coordinatorRun.id);
          assert.lengthOf(yield* Ref.get(submissions), 6);
          assert.isEmpty(yield* Ref.get(interruptions));
          const events = yield* EventStore.EventStoreV2.pipe(
            Effect.flatMap((store) => Stream.runCollect(store.read())),
          );
          for (const stored of events)
            if (stored.event.type === "multi-agent.controller.updated")
              assert.isAtMost(
                stored.event.payload.state.tasks.filter((task) =>
                  ["reserved", "launching", "running", "cancellation_pending"].includes(
                    task.status,
                  ),
                ).length,
                2,
              );
          return { controllerRun: row, workerRuns, events };
        }
        if (scenario === "preauthorized-start-gated") {
          const row = (yield* projections.getThreadRecords(coordinatorId, ["runs"])).runs[0]!;
          assert.equal(
            row.multiAgentController!.tasks.filter((task) => task.status === "running").length,
            2,
          );
          for (const run of workerRuns) {
            const task = row.multiAgentController!.tasks.find(
              (task) => task.workerThreadId === run.threadId,
            )!;
            if (scenario === "preauthorized-start-gated") {
              assert.isUndefined(task.submissionCommitment);
              assert.equal(
                (yield* projections.getThreadRecords(run.threadId, ["runs"])).runs[0]!.status,
                "starting",
              );
            } else assert.isDefined(task.submissionCommitment);
          }
          // Replayed schedule deliveries cannot spend another slot or bypass the real-start gate.
          yield* commands.scheduleMultiAgentWorkers({
            threadId: coordinatorId,
            runId: coordinatorRun.id,
            schedulingId: CommandId.make("dj:duplicate-schedule"),
          });
          for (const run of workerRuns)
            yield* outbox.enqueue([
              {
                id: `dj:duplicate-gated-start:${run.id}`,
                commandId: CommandId.make(`dj:duplicate-gated-start:${run.id}`),
                threadId: run.threadId,
                request: { type: "provider-turn.start", runId: run.id },
              },
            ]);
          yield* worker.drain();
          assert.lengthOf(
            yield* Ref.get(submissions),
            scenario === "preauthorized-start-gated" ? 1 : 3,
          );
          assert.isEmpty(yield* Ref.get(interruptions));
          const events = yield* EventStore.EventStoreV2.pipe(
            Effect.flatMap((store) => Stream.runCollect(store.read())),
          );
          return { controllerRun: row, workerRuns, events };
        }
        let stoppedAt: string | undefined;
        if (stopping) {
          for (const run of workerRuns) yield* awaitRunStatus(run, "running");
          const readControllerRun = () =>
            projections
              .getThreadRecords(coordinatorId, ["runs"])
              .pipe(Effect.map((rows) => rows.runs[0]!));
          const committed = yield* readControllerRun();
          assert.lengthOf(committed.multiAgentController!.tasks, 2);
          assert.isTrue(
            committed.multiAgentController!.tasks.every(
              (task) => task.status === "running" && task.submissionCommitment !== undefined,
            ),
          );
          const stop = {
            type: "multi-agent.controller.command" as const,
            commandId: CommandId.make("dj:stop"),
            threadId: coordinatorId,
            runId: coordinatorRun.id,
            action: { type: "stop_coordinator" as const },
          };
          const management = yield* ThreadManagementService.ThreadManagementService;
          const stopFromClient = () =>
            commands.dispatch({
              type: "run.interrupt",
              threadId: coordinatorId,
              runId: coordinatorRun.id,
              commandId: stop.commandId,
              holdQueue: true,
            });
          // The provider finished proposing before its workers; the existing Stop
          // service must still commit the barrier before its terminal early return.
          const firstStop = yield* stopFromClient();
          const stopped = yield* readControllerRun();
          stoppedAt = stopped.multiAgentController!.coordinatorStoppedAt;
          assert.isDefined(stoppedAt);
          assert.equal((yield* stopFromClient()).sequence, firstStop.sequence);
          assert.equal(
            (yield* management.interruptThread({
              projectId,
              threadId: coordinatorId,
              runId: coordinatorRun.id,
              commandId: CommandId.make("dj:stop-service"),
            })).type,
            "already_terminal",
          );
          yield* TestClock.adjust("1 second");
          yield* commands.dispatch({ ...stop, commandId: CommandId.make("dj:stop-again") });
          assert.deepEqual(
            (yield* readControllerRun()).multiAgentController,
            stopped.multiAgentController,
          );
          // A valid proposal from the original provider execution was recorded
          // before Stop; its delayed admission must encounter the durable barrier.
          const lateProposal = source.messages.find(
            (message) => message.id === MessageId.make("dj:late-proposal"),
          )!;
          assert.isDefined(lateProposal);
          const third = {
            key: "third",
            taskId: NodeId.make("dj:task:third"),
            launchCommandId: CommandId.make("dj:launch:third"),
            launchMessageId: MessageId.make("dj:request:third"),
            workerThreadId: ThreadId.make("dj:third"),
          };
          const denied = yield* Effect.result(
            commands.dispatch({
              type: "multi-agent.controller.command",
              commandId: CommandId.make("dj:late-admit"),
              threadId: coordinatorId,
              runId: coordinatorRun.id,
              action: {
                type: "admit",
                proposalId: lateProposal.id,
                attemptId: source.attempts[0]!.id,
                proposal: JSON.parse(lateProposal.text),
                identities: [third],
                providers: [catalog],
              },
            }),
          );
          assert.equal(denied._tag, "Failure");
          if (denied._tag === "Failure") {
            assert.equal(denied.failure._tag, "OrchestratorDispatchError");
            if (denied.failure._tag === "OrchestratorDispatchError")
              assert.include(String(denied.failure.cause), "controller_closed");
          }
          // Failed admission cannot be bypassed by reserve, launch authorization,
          // or the real launch executor inventing a third task.
          for (const type of ["reserve", "launch"] as const) {
            const commandId = CommandId.make(`dj:third:${type}`);
            assert.equal(
              (yield* commands
                .dispatch({
                  type: "multi-agent.controller.command",
                  commandId,
                  threadId: coordinatorId,
                  runId: coordinatorRun.id,
                  action: { type, taskId: third.taskId },
                })
                .pipe(Effect.exit))._tag,
              "Failure",
            );
            assert.isEmpty(yield* outbox.listByCommandId(commandId));
          }
          const executor = yield* MultiAgentWorkerLaunchService.MultiAgentWorkerLaunchService;
          assert.equal(
            (yield* executor
              .execute({
                threadId: coordinatorId,
                coordinatorRunId: coordinatorRun.id,
                taskId: third.taskId,
                launchCommandId: third.launchCommandId,
              })
              .pipe(Effect.exit))._tag,
            "Failure",
          );
          assert.isNull(yield* commands.getThreadShell(third.workerThreadId));
          assert.deepEqual(
            (yield* readControllerRun()).multiAgentController,
            stopped.multiAgentController,
          );
          for (const [index, identity] of identities.entries()) {
            const original = (yield* outbox.listByCommandId(
              CommandId.make(`${identity.launchCommandId}:scheduled-launch`),
            ))[0]!;
            yield* outbox.enqueue([
              {
                id: original.id,
                commandId: original.commandId,
                threadId: original.threadId,
                request: original.request,
              },
            ]);
            assert.lengthOf(yield* outbox.listByCommandId(original.commandId), 1);
            yield* outbox.enqueue([
              {
                id: `dj:duplicate-launch:${identity.key}`,
                commandId: CommandId.make(`dj:duplicate-launch:${identity.key}`),
                threadId: coordinatorId,
                request: original.request,
              },
              {
                id: `dj:duplicate-start:${identity.key}`,
                commandId: CommandId.make(`dj:duplicate-start:${identity.key}`),
                threadId: identity.workerThreadId,
                request: { type: "provider-turn.start", runId: workerRuns[index]!.id },
              },
            ]);
          }
          yield* worker.drain();
          assert.lengthOf(yield* Ref.get(submissions), 3);
          for (const identity of identities) {
            for (const kind of ["launch", "start"]) {
              const effect = (yield* outbox.listByCommandId(
                CommandId.make(`dj:duplicate-${kind}:${identity.key}`),
              ))[0]!;
              assert.equal(effect.status, "succeeded");
            }
          }
          assert.isEmpty(yield* Ref.get(interruptions));
          for (const run of workerRuns) {
            const records = yield* projections.getThreadRecords(run.threadId, ["runs", "messages"]);
            const current = records.runs[0]!;
            assert.equal(current.status, "running");
            assert.isNull(current.completedAt);
            assert.isEmpty(records.messages.filter((message) => message.role === "assistant"));
          }
          assert.deepEqual(
            (yield* readControllerRun()).multiAgentController,
            stopped.multiAgentController,
          );
          yield* Deferred.succeed(finishWorkers, undefined);
        }
        if (scenario === "staggered-review") {
          yield* awaitSettledTasks(1);
          const partial = (yield* projections.getThreadRecords(coordinatorId, ["runs"])).runs[0]!;
          assert.equal(
            partial.multiAgentController!.tasks.filter((task) => task.status === "completed")
              .length,
            1,
          );
          assert.isEmpty(partial.multiAgentController!.reviews ?? []);
          assert.lengthOf(yield* Ref.get(submissions), 3);
          yield* Deferred.succeed(finishWorkers, undefined);
        }
        for (const [index, run] of workerRuns.entries())
          yield* awaitRunStatus(
            run,
            scenario === "partial-failure-review" && identities[index]!.key === "backend"
              ? "failed"
              : "waiting",
          );
        yield* worker.drain();
        yield* awaitSettledTasks(expectedWorkers);
        for (const [index, run] of workerRuns.entries()) {
          yield* awaitRunStatus(
            run,
            scenario === "partial-failure-review" && identities[index]!.key === "backend"
              ? "failed"
              : "completed",
          );
          const identity = identities[index]!;
          const records = yield* projections.getThreadRecords(run.threadId, [
            "runs",
            "attempts",
            "messages",
          ]);
          const expectedStatus =
            scenario === "partial-failure-review" && identity.key === "backend"
              ? "failed"
              : "completed";
          assert.equal(records.runs[0]!.status, expectedStatus);
          assert.equal(records.attempts[0]!.status, expectedStatus);
          assert.equal(
            records.messages.find((message) => message.role === "assistant")?.text,
            outputs[identity.key],
          );
          const finished = {
            type: "multi-agent.controller.command",
            commandId: CommandId.make(`dj:finished:${identity.key}`),
            threadId: coordinatorId,
            runId: coordinatorRun.id,
            action: {
              type: "worker_finished",
              taskId: identity.taskId,
              launchCommandId: identity.launchCommandId,
              workerThreadId: run.threadId,
              workerRunId: run.id,
              workerAttemptId: run.controlledWorkerOwnership!.workerAttemptId,
            },
          } as const;
          if (stopping) {
            const beforeDuplicate = (yield* projections.getThreadRecords(coordinatorId, ["runs"]))
              .runs[0]!;
            yield* commands.dispatch(finished);
            yield* commands.dispatch({
              ...finished,
              commandId: CommandId.make(`dj:finished-again:${identity.key}`),
            });
            const afterDuplicate = (yield* projections.getThreadRecords(coordinatorId, ["runs"]))
              .runs[0]!;
            assert.deepEqual(
              afterDuplicate.multiAgentController,
              beforeDuplicate.multiAgentController,
            );
            assert.equal(
              afterDuplicate.multiAgentControllerRevision,
              beforeDuplicate.multiAgentControllerRevision,
            );
          }
        }
        const controller = (yield* projections.getThreadRecords(coordinatorId, ["runs"])).runs[0]!
          .multiAgentController!;
        assert.lengthOf(controller.tasks, expectedWorkers);
        assert.equal(controller.coordinatorStoppedAt, stoppedAt);
        assert.equal(
          new Set(controller.tasks.map((task) => task.submissionCommitment?.invocationClaimId))
            .size,
          expectedWorkers,
        );
        const read = yield* OrchestratorMcpService.OrchestratorMcpService;
        const coordinatorInput = (yield* Ref.get(submissions)).find(
          (input) => input.threadId === coordinatorId && input.runId === coordinatorRun.id,
        )!;
        assert.isDefined(coordinatorInput);
        const scope = {
          environmentId: EnvironmentId.make("scripted-dj-environment"),
          requestNamespace: "scripted-dj-coordinator",
          capabilities: new Set(["orchestration"] as const),
          issuedAt: 1,
          thread: {
            threadId: coordinatorId,
            providerInstanceId: instanceId,
            providerSessionId: coordinatorInput.providerThread.providerSessionId!,
          },
          client: undefined,
        } satisfies McpInvocationScope;
        for (const [index, task] of controller.tasks.entries()) {
          assert.equal(
            task.status,
            scenario === "partial-failure-review" && task.key === "backend"
              ? "failed"
              : "completed",
          );
          assert.deepEqual("result" in task ? task.result : undefined, {
            workerThreadId: identities[index]!.workerThreadId,
            workerRunId: workerRuns[index]!.id,
          });
          const result = yield* read.readThread(scope, { threadId: task.workerThreadId });
          assert.equal(result.thread.coordinatorThreadId, coordinatorId);
          assert.equal(result.recentRuns[0]!.runId, workerRuns[index]!.id);
          const response = result.items.filter((item) => item.type === "assistant_message");
          assert.lengthOf(response, 1);
          assert.equal(response[0]!.runId, workerRuns[index]!.id);
          assert.equal(response[0]!.sourceThreadId, identities[index]!.workerThreadId);
          assert.equal(
            response[0]!.messageId,
            MessageId.make(`answer:${identities[index]!.workerThreadId}`),
          );
          assert.equal(response[0]!.text, outputs[identities[index]!.key]);
        }
        const coordinatorTimeline = yield* projections.getThreadRecords(coordinatorId, [
          "turnItems",
        ]);
        const resultItems = coordinatorTimeline.turnItems.filter(
          (item) => item.type === "notification" && item.title === "Worker result",
        );
        assert.lengthOf(resultItems, expectedWorkers);
        for (const [index, task] of controller.tasks.entries()) {
          const item = resultItems.find(
            (item) => item.id === TurnItemId.make(`multi-agent:result:${task.taskId}`),
          );
          assert.isDefined(item);
          if (item?.type !== "notification")
            return yield* Effect.die("Missing worker notification");
          assert.equal(
            item.outcome,
            scenario === "partial-failure-review" && task.key === "backend"
              ? "failed"
              : "completed",
          );
          assert.equal(item.runId, coordinatorRun.id);
          assert.deepEqual(item.source, {
            kind: "delegated_task",
            taskIds: [task.taskId],
            childThreadId: task.workerThreadId,
          });
          assert.include(item.detail!, workerRuns[index]!.id);
          assert.notInclude(item.detail!, outputs[identities[index]!.key]);
        }
        yield* worker.drain();
        if (!stopping) yield* awaitReview(coordinatorRun.id);
        const calls = yield* Ref.get(submissions);
        assert.lengthOf(
          calls,
          expectedSubmissions + (stopping ? 0 : scenario === "review-additional-tasks" ? 4 : 1),
        );
        for (const id of [coordinatorId, ...identities.map((task) => task.workerThreadId)])
          assert.lengthOf(
            calls.filter((call) => call.threadId === id),
            id === coordinatorId && !stopping
              ? scenario === "review-additional-tasks"
                ? 3
                : 2
              : 1,
          );
        assert.isEmpty(yield* Ref.get(interruptions));
        const coordinatorRows = yield* projections.getThreadRecords(coordinatorId, [
          "runs",
          "providerThreads",
        ]);
        assert.lengthOf(
          coordinatorRows.runs,
          stopping ? 1 : scenario === "review-additional-tasks" ? 3 : 2,
        );
        if (!stopping) {
          assert.isUndefined(coordinatorRows.runs[1]!.multiAgentController);
          assert.equal(
            coordinatorRows.runs[1]!.multiAgentReview?.coordinatorRunId,
            coordinatorRun.id,
          );
          assert.lengthOf(
            coordinatorRows.runs[0]!.multiAgentController!.reviews!,
            scenario === "review-additional-tasks" ? 2 : 1,
          );
          assert.isDefined(
            coordinatorRows.runs[0]!.multiAgentController!.reviews![0]!.submissionClaimedAt,
          );
          assert.isTrue(controlledCodexOriginBlocked(coordinatorRows, coordinatorRows.runs[1]!.id));
          assert.isFalse(
            calls
              .filter((call) => call.threadId === coordinatorId)
              .some((call) => call.message.text.includes("not_runtime_owned")),
          );
          assert.equal(
            coordinatorRows.runs[1]!.status,
            scenario === "review-response-lost" ? "failed" : "completed",
          );
        }
        assert.equal(coordinatorRows.runs[0]!.status, "completed");
        for (const run of workerRuns) {
          for (const kind of ["background", "restart", "limit"] as const) {
            const command = {
              type: "message.dispatch",
              commandId: CommandId.make(`dj:denied-continuation:${run.id}:${kind}`),
              threadId: run.threadId,
              messageId: MessageId.make(`dj:denied-continuation:${run.id}:${kind}`),
              text: "Continue automatically.",
              attachments: [],
              dispatchMode: { type: "queue_after_active" },
              createdBy: kind === "limit" ? "user" : "agent",
              creationSource: "server",
              ...(kind === "restart" ? { restartContinuationOfRunId: run.id } : {}),
              ...(kind === "limit" ? { usageLimitContinuationOfRunId: run.id } : {}),
            } as const;
            for (let delivery = 0; delivery < 2; delivery++)
              assert.equal((yield* commands.dispatch(command).pipe(Effect.result))._tag, "Failure");
          }
          const unchanged = yield* projections.getThreadRecords(run.threadId, ["runs"]);
          assert.lengthOf(unchanged.runs, 1);
          assert.deepEqual(
            unchanged.runs[0]!.controlledWorkerOwnership,
            run.controlledWorkerOwnership,
          );
        }
        if (scenario === "normal") {
          const run = workerRuns[0]!;
          yield* commands.dispatch({
            type: "message.dispatch",
            commandId: CommandId.make("dj:manual-follow-up"),
            threadId: run.threadId,
            messageId: MessageId.make("dj:manual-follow-up"),
            text: "A separate manual assignment.",
            attachments: [],
            dispatchMode: { type: "defer_start" },
            createdBy: "user",
            creationSource: "server",
          });
          const later = yield* projections.getThreadRecords(run.threadId, [
            "runs",
            "messages",
            "providerThreads",
          ]);
          assert.lengthOf(later.runs, 2);
          assert.isUndefined(later.runs[1]!.controlledWorkerOwnership);
          assert.deepEqual(later.runs[1]!.newRunTrigger, { type: "manual" });
          assert.isFalse(controlledCodexOriginBlocked(later, later.runs[1]!.id));
          assert.isTrue(controlledCodexOriginBlocked(later));
        }
        assert.lengthOf(
          yield* Ref.get(submissions),
          expectedSubmissions + (stopping ? 0 : scenario === "review-additional-tasks" ? 4 : 1),
        );
        const events = yield* EventStore.EventStoreV2.pipe(
          Effect.flatMap((store) => Stream.runCollect(store.read())),
        );
        return { controllerRun: coordinatorRows.runs[0]!, workerRuns, events };
      }).pipe(Effect.provide(services)),
    );
    if (stopping) {
      const stopEvent = expected.events.find(
        (stored) =>
          stored.event.type === "multi-agent.controller.updated" &&
          stored.event.payload.state.coordinatorStoppedAt !== undefined,
      )!;
      assert.isDefined(stopEvent);
      for (const run of expected.workerRuns)
        assert.isTrue(
          expected.events.some(
            (stored) =>
              stored.sequence > stopEvent.sequence &&
              stored.event.type === "run.updated" &&
              stored.event.payload.id === run.id &&
              stored.event.payload.status === "completed",
          ),
        );
      for (const restoredLayer of [
        ProjectionStore.layerMemory,
        ProjectionStore.layer.pipe(Layer.provide(SqlitePersistence.layerFromPath(filename))),
      ]) {
        yield* Effect.scoped(
          Effect.gen(function* () {
            const restored = yield* ProjectionStore.ProjectionStoreV2;
            if (restoredLayer === ProjectionStore.layerMemory)
              for (const stored of expected.events) yield* restored.apply(stored.event);
            const coordinator = yield* restored.getThreadRecords(coordinatorId, [
              "runs",
              "messages",
              "turnItems",
            ]);
            assert.equal(
              coordinator.messages.find((row) => row.id === expected.controllerRun.userMessageId)!
                .text,
              userGoal,
            );
            assert.lengthOf(coordinator.runs, 1);
            assert.deepEqual(
              coordinator.runs[0]!.multiAgentController,
              expected.controllerRun.multiAgentController,
            );
            assert.equal(
              coordinator.runs[0]!.multiAgentControllerRevision,
              expected.controllerRun.multiAgentControllerRevision,
            );
            assert.deepEqual(coordinator.runs[0]!.multiAgent, expected.controllerRun.multiAgent);
            assert.lengthOf(
              coordinator.turnItems.filter(
                (item) => item.type === "notification" && item.title === "Worker result",
              ),
              2,
            );
            for (const run of expected.workerRuns) {
              const rows = yield* restored.getThreadRecords(run.threadId, [
                "runs",
                "messages",
                "providerThreads",
              ]);
              assert.lengthOf(rows.runs, 1);
              assert.isTrue(controlledCodexOriginBlocked(rows, run.id));
              assert.isTrue(controlledCodexOriginBlocked(rows));
              assert.equal(rows.runs[0]!.status, "completed");
              assert.deepEqual(
                rows.runs[0]!.controlledWorkerOwnership,
                run.controlledWorkerOwnership,
              );
              assert.equal(
                rows.messages.find((message) => message.role === "assistant")?.runId,
                run.id,
              );
              assert.equal(
                rows.messages.find((message) => message.role === "assistant")?.text,
                expected.controllerRun.multiAgentController!.tasks.find(
                  (task) => task.workerThreadId === run.threadId,
                )!.key === "frontend"
                  ? outputs.frontend
                  : outputs.backend,
              );
            }
          }).pipe(Effect.provide(restoredLayer)),
        );
      }
      assert.lengthOf(yield* Ref.get(submissions), 3);
      assert.isEmpty(yield* Ref.get(interruptions));
    }
  }).pipe(Effect.provide(NodeServices.layer)),
);

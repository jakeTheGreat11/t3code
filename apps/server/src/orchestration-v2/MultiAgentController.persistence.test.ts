import { assert, it } from "@effect/vitest";
import {
  CommandId,
  EventId,
  MessageId,
  NodeId,
  ProjectId,
  ProviderDriverKind,
  ProviderInstanceId,
  ProviderSessionId,
  ProviderThreadId,
  RunAttemptId,
  RunId,
  ThreadId,
  type OrchestrationV2AppThread,
  type OrchestrationV2Run,
  type OrchestrationV2ServerCommand,
  type ServerProvider,
  type MultiAgentControllerState,
  type MultiAgentConfig,
  type OrchestrationV2Command,
  type NewRunTrigger,
  type OrchestrationV2ProviderThread,
} from "@t3tools/contracts";
import {
  transitionMultiAgentTask,
  grantMultiAgentSubmission,
} from "@t3tools/shared/multiAgentController";
import { withControlledWorkerSubmission } from "./ControlledWorkerSubmission.ts";
import { ProviderRuntimeRegistry } from "./ProviderRuntimeRegistry.ts";
import { ProviderSessionActivityError } from "./ProviderSessionManager.ts";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Layer from "effect/Layer";
import * as Stream from "effect/Stream";
import * as Option from "effect/Option";
import * as SqlClient from "effect/sql/SqlClient";
import * as NodeServices from "@effect/platform-node/NodeServices";
import * as FileSystem from "effect/FileSystem";
import * as Path from "effect/Path";
import * as SqlitePersistence from "../persistence/Sqlite.ts";
import * as ProjectionStore from "./ProjectionStore.ts";
import * as EventStore from "./EventStore.ts";
import * as EventSink from "./EventSink.ts";
import * as CommandReceiptStore from "./CommandReceiptStore.ts";
import * as EffectOutbox from "./EffectOutbox.ts";
import * as Orchestrator from "./Orchestrator.ts";
import * as ProviderAdapterRegistry from "./ProviderAdapterRegistry.ts";
import * as ProviderReplayHarness from "./testkit/ProviderReplayHarness.ts";
import * as ProviderAdapter from "@t3tools/provider-core/server/ProviderAdapter";
import { CodexProviderCapabilitiesV2 } from "./Adapters/CodexAdapterV2.ts";

const instanceId = ProviderInstanceId.make("configured-instance");
const selection = { instanceId, model: "catalog-model" };
const provider: ServerProvider = {
  instanceId,
  driver: ProviderDriverKind.make("codex"),
  enabled: true,
  installed: true,
  version: null,
  status: "ready",
  auth: { status: "authenticated" },
  checkedAt: "2026-10-01T00:00:00Z",
  slashCommands: [],
  skills: [],
  models: [{ slug: selection.model, name: "Catalog model", isCustom: true, capabilities: null }],
};
const adapter = {
  instanceId,
  driver: provider.driver,
  getCapabilities: () => Effect.succeed(CodexProviderCapabilitiesV2),
  planSelectionTransition: () => Effect.succeed({ type: "apply_on_next_turn" as const }),
  openSession: () => Effect.die("No provider execution in persistence tests"),
} as ProviderAdapter.ProviderAdapterV2["Service"];
const database = SqlitePersistence.layerMemory;
const makeLayers = (
  databaseLayer: typeof database,
  catalog: readonly ServerProvider[] = [provider],
) =>
  Layer.mergeAll(
    databaseLayer,
    ProjectionStore.layer.pipe(Layer.provide(databaseLayer)),
    EventStore.layer.pipe(Layer.provide(databaseLayer)),
    CommandReceiptStore.layer.pipe(Layer.provide(databaseLayer)),
    EffectOutbox.layer.pipe(Layer.provide(databaseLayer)),
    ProviderReplayHarness.layerWithRegistry(
      { name: "multi-agent-persistence" },
      ProviderAdapterRegistry.layerFromAdapters([adapter]),
      { databaseLayer, runEffectWorker: false, providerCatalog: catalog },
    ),
  );
const layers = makeLayers(database);
const now = DateTime.makeUnsafe("2026-10-01T00:00:00Z");

type ControllerCommand = Extract<
  OrchestrationV2ServerCommand,
  { type: "multi-agent.controller.command" }
>;
const seed = Effect.fn("seedControllerRun")(function* (
  key: string,
  status: OrchestrationV2Run["status"] = "completed",
  newRunTrigger?: NewRunTrigger,
) {
  const sink = yield* EventSink.EventSinkV2;
  const threadId = ThreadId.make(`coordinator:${key}`);
  const runId = RunId.make(`run:${key}`);
  const attemptId = RunAttemptId.make(`attempt:${key}`);
  const proposalId = MessageId.make(`proposal:${key}`);
  const rootNodeId = NodeId.make(`root:${key}`);
  const thread: OrchestrationV2AppThread = {
    id: threadId,
    projectId: ProjectId.make("project"),
    title: "Coordinator",
    modelSelection: selection,
    providerInstanceId: instanceId,
    runtimeMode: "full-access",
    interactionMode: "default",
    branch: null,
    worktreePath: null,
    activeProviderThreadId: null,
    lineage: { parentThreadId: null, relationshipToParent: null, rootThreadId: threadId },
    forkedFrom: null,
    createdAt: now,
    updatedAt: now,
    archivedAt: null,
    deletedAt: null,
    lastVisitedAt: null,
    settledOverride: null,
    settledAt: null,
    createdBy: "user",
    creationSource: "web",
  };
  const run: OrchestrationV2Run = {
    id: runId,
    threadId,
    ordinal: 1,
    providerInstanceId: instanceId,
    modelSelection: selection,
    multiAgent: { mode: "custom", roles: { worker: selection } },
    ...(newRunTrigger === undefined ? {} : { newRunTrigger }),
    providerThreadId: null,
    userMessageId: MessageId.make(`user:${key}`),
    rootNodeId,
    activeAttemptId: attemptId,
    status,
    requestedAt: now,
    startedAt: now,
    completedAt: status === "completed" ? now : null,
    checkpointId: null,
    contextHandoffId: null,
  };
  const events = yield* sink.write({
    events: [
      {
        id: EventId.make(`thread:${key}`),
        type: "thread.created",
        threadId,
        occurredAt: now,
        payload: thread,
      },
      {
        id: EventId.make(`run:${key}`),
        type: "run.created",
        threadId,
        runId,
        occurredAt: now,
        payload: run,
      },
      {
        id: EventId.make(`attempt:${key}`),
        type: "run-attempt.created",
        threadId,
        runId,
        occurredAt: now,
        payload: {
          id: attemptId,
          runId,
          rootNodeId,
          attemptOrdinal: 1,
          providerInstanceId: instanceId,
          providerThreadId: ProviderThreadId.make(`provider-thread:${key}`),
          providerTurnId: null,
          reason: "initial",
          status: "completed",
          startedAt: now,
          completedAt: now,
        },
      },
      {
        id: EventId.make(`message:${key}`),
        type: "message.updated",
        threadId,
        runId,
        occurredAt: now,
        payload: {
          id: proposalId,
          threadId,
          runId,
          nodeId: rootNodeId,
          role: "assistant",
          text: "Proposal",
          attachments: [],
          streaming: false,
          createdAt: now,
          updatedAt: now,
          createdBy: "agent",
          creationSource: "provider",
        },
      },
    ],
  });
  const identities = ["a", "b", "c", "d"].map((task) => ({
    key: task,
    taskId: NodeId.make(`task:${key}:${task}`),
    launchCommandId: CommandId.make(`launch:${key}:${task}`),
    launchMessageId: MessageId.make(`launch-message:${key}:${task}`),
    workerThreadId: ThreadId.make(`worker:${key}:${task}`),
  }));
  const admit = (commandKey: string, count = 4): ControllerCommand => ({
    type: "multi-agent.controller.command",
    commandId: CommandId.make(commandKey),
    threadId,
    runId,
    action: {
      type: "admit",
      attemptId,
      proposalId,
      proposal: {
        tasks: identities
          .slice(0, count)
          .map((identity) => ({ key: identity.key, role: "worker", prompt: `Do ${identity.key}` })),
      },
      identities: identities.slice(0, count),
      providers: [provider],
    },
  });
  const reserve = (task: number, commandKey: string): ControllerCommand => ({
    type: "multi-agent.controller.command",
    commandId: CommandId.make(commandKey),
    threadId,
    runId,
    action: { type: "reserve", taskId: identities[task]!.taskId },
  });
  return { threadId, runId, run, events, identities, admit, reserve };
});

const initializationDispatch = (
  threadId: ThreadId,
  key: string,
  config?: MultiAgentConfig,
  dispatchMode: Extract<OrchestrationV2Command, { type: "message.dispatch" }>["dispatchMode"] = {
    type: "defer_start",
  },
): Extract<OrchestrationV2Command, { type: "message.dispatch" }> => ({
  type: "message.dispatch",
  commandId: CommandId.make(`initialize:${key}`),
  threadId,
  messageId: MessageId.make(`initialize:${key}`),
  text: "Coordinate the DJ frontend and backend",
  attachments: [],
  createdBy: "user",
  creationSource: "web",
  dispatchMode,
  ...(config === undefined ? {} : { multiAgent: config }),
});
const customPolicy: MultiAgentConfig = { mode: "custom", roles: { worker: selection } };

it.effect.each(["budget", "balanced", "maximum"] as const)(
  "initializes Auto %s with a frozen Codex catalog and rejects catalog replacement",
  (preset) =>
    Effect.gen(function* () {
      const fixture = yield* seed(`auto-init-${preset}`);
      const commands = yield* Orchestrator.OrchestratorV2;
      const dispatch = initializationDispatch(fixture.threadId, `auto-${preset}`, {
        mode: "auto",
        preset,
      });
      yield* commands.dispatch(dispatch);
      const projections = yield* ProjectionStore.ProjectionStoreV2;
      const read = () =>
        projections
          .getThreadRecords(fixture.threadId, ["runs"])
          .pipe(Effect.map((rows) => rows.runs[1]!));
      const run = yield* read();
      assert.deepEqual(run.multiAgent, { mode: "auto", preset });
      const state = run.multiAgentController!;
      assert.equal(state.coordinatorAttemptId, run.activeAttemptId);
      assert.equal(state.autoRoutingCatalog?.providerInstanceId, run.providerInstanceId);
      assert.isAbove(state.autoRoutingCatalog!.candidates.length, 0);
      yield* commands.dispatch(dispatch);
      assert.deepEqual((yield* read()).multiAgentController, state);
      assert.equal((yield* read()).multiAgentControllerRevision, 1);
      const sink = yield* EventSink.EventSinkV2;
      const outcome = yield* sink
        .write({
          events: [
            {
              id: EventId.make(`auto-tamper:${preset}`),
              type: "multi-agent.controller.updated",
              threadId: run.threadId,
              runId: run.id,
              occurredAt: yield* DateTime.now,
              payload: {
                expectedRevision: 1,
                revision: 2,
                state: {
                  ...state,
                  autoRoutingCatalog: { ...state.autoRoutingCatalog!, candidates: [] },
                },
              },
            },
          ],
        })
        .pipe(Effect.exit);
      assert.isTrue(Exit.isFailure(outcome));
      assert.deepEqual((yield* read()).multiAgentController, state);
    }).pipe(Effect.provide(layers)),
);

it.effect.each(["deferred", "immediate", "queued"] as const)(
  "initializes the exact Custom/Codex run and attempt atomically on %s dispatch",
  (mode) =>
    Effect.gen(function* () {
      const fixture = yield* seed(`initialize-${mode}`);
      const commands = yield* Orchestrator.OrchestratorV2;
      if (mode === "queued")
        yield* commands.dispatch(initializationDispatch(fixture.threadId, "blocker"));
      const result = yield* commands.dispatch(
        initializationDispatch(
          fixture.threadId,
          mode,
          customPolicy,
          mode === "deferred"
            ? { type: "defer_start" }
            : mode === "queued"
              ? { type: "queue_after_active" }
              : { type: "start_immediately" },
        ),
      );
      const projections = yield* ProjectionStore.ProjectionStoreV2;
      const projection = yield* projections.getThreadProjection(fixture.threadId);
      const created = result.storedEvents.find((stored) => stored.event.type === "run.created")!;
      const run = projection.runs.find((candidate) => candidate.id === created.event.runId)!;
      assert.deepEqual(run.multiAgentController, {
        coordinatorThreadId: fixture.threadId,
        coordinatorRunId: run.id,
        coordinatorAttemptId: run.activeAttemptId!,
        status: "open",
        tasks: [],
      });
      assert.equal(run.multiAgentControllerRevision, 1);
      assert.notEqual(run.id, fixture.runId);
      assert.isUndefined(
        projection.runs.find((candidate) => candidate.id === fixture.runId)!.multiAgentController,
      );
      assert.isTrue(
        projection.attempts.some(
          (attempt) => attempt.id === run.activeAttemptId && attempt.runId === run.id,
        ),
      );
      const initialized = result.storedEvents.find(
        (stored) => stored.event.type === "multi-agent.controller.updated",
      )!;
      assert.isBelow(created.sequence, initialized.sequence);
      assert.equal(created.commandId, initialized.commandId);
      assert.isFalse(result.storedEvents.some((stored) => stored.event.type === "thread.created"));
    }).pipe(Effect.provide(layers)),
);

it.effect.each([undefined, { mode: "off" } as const])(
  "does not enroll Off/legacy configuration %s",
  (config) =>
    Effect.gen(function* () {
      const fixture = yield* seed(`initialize-off-${config?.mode}`);
      const commands = yield* Orchestrator.OrchestratorV2;
      const result = yield* commands.dispatch(
        initializationDispatch(fixture.threadId, "off", config),
      );
      const projections = yield* ProjectionStore.ProjectionStoreV2;
      const run = (yield* projections.getThreadProjection(fixture.threadId)).runs[1]!;
      assert.deepEqual(run.multiAgent, { mode: "off" });
      assert.isUndefined(run.multiAgentController);
      assert.isFalse(
        result.storedEvents.some(
          (stored) => stored.event.type === "multi-agent.controller.updated",
        ),
      );
    }).pipe(Effect.provide(layers)),
);

it.effect.each([
  { name: "missing worker", config: { mode: "custom", roles: {} } },
  {
    name: "unavailable worker",
    config: { mode: "custom", roles: { worker: { ...selection, model: "missing" } } },
  },
  {
    name: "foreign instance",
    config: {
      mode: "custom",
      roles: { worker: { ...selection, instanceId: ProviderInstanceId.make("foreign") } },
    },
  },
  {
    name: "invalid options",
    config: {
      mode: "custom",
      roles: { worker: { ...selection, options: [{ id: "unsupported", value: true }] } },
    },
  },
  {
    name: "invalid optional reviewer",
    config: {
      mode: "custom",
      roles: { worker: selection, reviewer: { ...selection, model: "missing" } },
    },
  },
] satisfies Array<{ name: string; config: MultiAgentConfig }>)(
  "rejects $name without creating a run or scheduling execution",
  ({ config, name }) =>
    Effect.gen(function* () {
      const fixture = yield* seed(`invalid-${name}`);
      const commands = yield* Orchestrator.OrchestratorV2;
      const error = yield* commands
        .dispatch(initializationDispatch(fixture.threadId, name, config))
        .pipe(Effect.flip);
      assert.equal(error._tag, "OrchestratorDispatchError");
      const projections = yield* ProjectionStore.ProjectionStoreV2;
      assert.lengthOf((yield* projections.getThreadProjection(fixture.threadId)).runs, 1);
      const outbox = yield* EffectOutbox.EffectOutboxV2;
      assert.lengthOf(yield* outbox.listByCommandId(CommandId.make(`initialize:${name}`)), 0);
    }).pipe(Effect.provide(layers)),
);

it.effect.each([
  {
    name: "unsupported coordinator provider",
    catalog: [{ ...provider, driver: ProviderDriverKind.make("claudeAgent") }],
  },
  { name: "disabled provider", catalog: [{ ...provider, enabled: false }] },
  { name: "missing catalog", catalog: [] },
  { name: "missing coordinator model", catalog: [{ ...provider, models: [] }] },
] satisfies Array<{ name: string; catalog: ServerProvider[] }>)(
  "rejects $name before committing execution",
  ({ name, catalog }) =>
    Effect.gen(function* () {
      const fixture = yield* seed(name);
      const commands = yield* Orchestrator.OrchestratorV2;
      const error = yield* commands
        .dispatch(initializationDispatch(fixture.threadId, name, customPolicy))
        .pipe(Effect.flip);
      assert.equal(error._tag, "OrchestratorDispatchError");
      const projections = yield* ProjectionStore.ProjectionStoreV2;
      assert.lengthOf((yield* projections.getThreadProjection(fixture.threadId)).runs, 1);
    }).pipe(Effect.provide(makeLayers(database, catalog))),
);

it.effect(
  "preserves initialization and Stop-before-proposal across duplicate dispatch and replay",
  () =>
    Effect.gen(function* () {
      const fixture = yield* seed("initialized-stop");
      const commands = yield* Orchestrator.OrchestratorV2;
      const dispatch = initializationDispatch(fixture.threadId, "stopped", customPolicy);
      yield* commands.dispatch(dispatch);
      const projections = yield* ProjectionStore.ProjectionStoreV2;
      const read = () =>
        projections.getThreadProjection(fixture.threadId).pipe(Effect.map((p) => p.runs[1]!));
      const run = yield* read();
      const stop: ControllerCommand = {
        type: "multi-agent.controller.command",
        commandId: CommandId.make("initialized:stop"),
        threadId: fixture.threadId,
        runId: run.id,
        action: { type: "stop_coordinator" },
      };
      yield* commands.dispatch(stop);
      const stopped = yield* read();
      assert.isDefined(stopped.multiAgentController?.coordinatorStoppedAt);
      assert.lengthOf(stopped.multiAgentController!.tasks, 0);
      yield* commands.dispatch(dispatch);
      yield* commands.dispatch(stop);
      assert.deepEqual((yield* read()).multiAgentController, stopped.multiAgentController);
      assert.equal(
        (yield* read()).multiAgentControllerRevision,
        stopped.multiAgentControllerRevision,
      );
      const sink = yield* EventSink.EventSinkV2;
      const at = yield* DateTime.now;
      for (const coordinatorAttemptId of [undefined, RunAttemptId.make("wrong-attempt")]) {
        const { coordinatorAttemptId: _initialAttempt, ...state } = stopped.multiAgentController!;
        const outcome = yield* sink
          .write({
            events: [
              {
                id: EventId.make(`initial-attempt:${coordinatorAttemptId}`),
                type: "multi-agent.controller.updated",
                threadId: fixture.threadId,
                runId: run.id,
                occurredAt: at,
                payload: {
                  expectedRevision: stopped.multiAgentControllerRevision!,
                  revision: stopped.multiAgentControllerRevision! + 1,
                  state: {
                    ...state,
                    ...(coordinatorAttemptId === undefined ? {} : { coordinatorAttemptId }),
                  },
                },
              },
            ],
          })
          .pipe(Effect.exit);
        assert.equal(outcome._tag, "Failure");
      }
      const proposalId = MessageId.make("initialized:proposal");
      yield* sink.write({
        events: [
          {
            id: EventId.make("initialized:proposal"),
            type: "message.updated",
            threadId: fixture.threadId,
            runId: run.id,
            occurredAt: at,
            payload: {
              id: proposalId,
              threadId: fixture.threadId,
              runId: run.id,
              nodeId: run.rootNodeId,
              role: "assistant",
              text: "Proposal",
              attachments: [],
              streaming: false,
              createdAt: at,
              updatedAt: at,
              createdBy: "agent",
              creationSource: "provider",
            },
          },
        ],
      });
      const admission = { ...fixture.admit("initialized:admit", 1), runId: run.id };
      assert.equal(admission.action.type, "admit");
      if (admission.action.type === "admit") {
        admission.action = { ...admission.action, attemptId: run.activeAttemptId!, proposalId };
      }
      const denied = yield* commands.dispatch(admission).pipe(Effect.flip);
      assert.equal(denied._tag, "OrchestratorDispatchError");
      const store = yield* EventStore.EventStoreV2;
      const events = yield* store.read({ threadId: fixture.threadId }).pipe(Stream.runCollect);
      yield* Effect.gen(function* () {
        const replay = yield* ProjectionStore.ProjectionStoreV2;
        for (const stored of events) yield* replay.apply(stored.event);
        const restored = (yield* replay.getThreadProjection(fixture.threadId)).runs[1]!;
        assert.deepEqual(restored.multiAgentController, stopped.multiAgentController);
        assert.equal(restored.multiAgentControllerRevision, stopped.multiAgentControllerRevision);
      }).pipe(Effect.provide(ProjectionStore.layerMemory));
    }).pipe(Effect.provide(layers)),
);

it.effect(
  "desktop Stop before provider submission closes the controller and survives duplicate Stop",
  () =>
    Effect.gen(function* () {
      const fixture = yield* seed("desktop-stop-before-start");
      const commands = yield* Orchestrator.OrchestratorV2;
      yield* commands.dispatch(
        initializationDispatch(fixture.threadId, "desktop-stop", customPolicy),
      );
      const projections = yield* ProjectionStore.ProjectionStoreV2;
      const read = () =>
        projections
          .getThreadRecords(fixture.threadId, ["runs"])
          .pipe(Effect.map((rows) => rows.runs[1]!));
      const run = yield* read();
      const stop = {
        type: "run.interrupt" as const,
        commandId: CommandId.make("desktop:stop-before-start"),
        threadId: fixture.threadId,
        runId: run.id,
        holdQueue: true,
      };
      yield* commands.dispatch(stop);
      const stopped = yield* read();
      assert.equal(stopped.status, "interrupted");
      assert.isDefined(stopped.multiAgentController?.coordinatorStoppedAt);
      assert.lengthOf(stopped.multiAgentController!.tasks, 0);
      yield* commands.dispatch({
        ...stop,
        commandId: CommandId.make("desktop:stop-before-start:again"),
      });
      assert.deepEqual((yield* read()).multiAgentController, stopped.multiAgentController);
      assert.equal(
        (yield* read()).multiAgentControllerRevision,
        stopped.multiAgentControllerRevision,
      );
    }).pipe(Effect.provide(layers)),
);

it.effect("reopens an initialized, stopped coordinator with its exact initial attempt", () =>
  Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem;
    const path = yield* Path.Path;
    const directory = yield* fs.makeTempDirectoryScoped({ prefix: "t3-initialized-controller-" });
    const filename = path.join(directory, "state.sqlite");
    const expected = yield* Effect.scoped(
      Effect.gen(function* () {
        const fixture = yield* seed("initialized-reopen");
        const commands = yield* Orchestrator.OrchestratorV2;
        yield* commands.dispatch(initializationDispatch(fixture.threadId, "reopen", customPolicy));
        const projections = yield* ProjectionStore.ProjectionStoreV2;
        const run = (yield* projections.getThreadProjection(fixture.threadId)).runs[1]!;
        yield* commands.dispatch({
          type: "multi-agent.controller.command",
          commandId: CommandId.make("reopen:stop"),
          threadId: run.threadId,
          runId: run.id,
          action: { type: "stop_coordinator" },
        });
        return (yield* projections.getThreadProjection(fixture.threadId)).runs[1]!;
      }).pipe(
        Effect.provide(
          makeLayers(
            SqlitePersistence.layerFromPath(filename).pipe(
              Layer.provide(NodeServices.layer),
              Layer.orDie,
            ),
          ),
        ),
      ),
    );
    yield* Effect.scoped(
      Effect.gen(function* () {
        const projections = yield* ProjectionStore.ProjectionStoreV2;
        const run = (yield* projections.getThreadProjection(expected.threadId)).runs[1]!;
        assert.deepEqual(run.multiAgentController, expected.multiAgentController);
        assert.equal(run.multiAgentControllerRevision, expected.multiAgentControllerRevision);
        assert.deepEqual(run.multiAgent, expected.multiAgent);
      }).pipe(
        Effect.provide(
          ProjectionStore.layer.pipe(
            Layer.provide(SqlitePersistence.layerFromPath(filename)),
            Layer.provide(NodeServices.layer),
          ),
        ),
      ),
    );
  }).pipe(Effect.provide(NodeServices.layer)),
);

const seedSubmissionWorker = Effect.fn("seedSubmissionWorker")(function* (key: string) {
  const owner = yield* seed(key);
  const orchestrator = yield* Orchestrator.OrchestratorV2;
  yield* orchestrator.dispatch(owner.admit(`${key}:admit`, 1));
  yield* orchestrator.dispatch(owner.reserve(0, `${key}:reserve`));
  yield* orchestrator.dispatch({
    type: "multi-agent.controller.command",
    commandId: CommandId.make(`${key}:launch`),
    threadId: owner.threadId,
    runId: owner.runId,
    action: { type: "launch", taskId: owner.identities[0]!.taskId },
  });
  const original = owner.events.find((stored) => stored.event.type === "thread.created")!.event;
  if (original.type !== "thread.created") return yield* Effect.die("Missing thread fixture");
  const identity = owner.identities[0]!;
  const workerThread = {
    ...original.payload,
    id: identity.workerThreadId,
    coordinatorThreadId: owner.threadId,
    lineage: {
      parentThreadId: null,
      relationshipToParent: null,
      rootThreadId: identity.workerThreadId,
    },
  };
  const workerAttemptId = RunAttemptId.make(`${key}:worker-attempt`);
  const workerRun: OrchestrationV2Run = {
    ...owner.run,
    id: RunId.make(`${key}:worker-run`),
    threadId: identity.workerThreadId,
    multiAgent: { mode: "off" },
    userMessageId: identity.launchMessageId,
    activeAttemptId: workerAttemptId,
    providerThreadId: ProviderThreadId.make(`${key}:worker-provider-thread`),
    status: "starting",
    completedAt: null,
  };
  const providerThread: OrchestrationV2ProviderThread = {
    id: workerRun.providerThreadId!,
    driver: provider.driver,
    providerInstanceId: instanceId,
    providerSessionId: ProviderSessionId.make(`${key}:worker-session`),
    appThreadId: identity.workerThreadId,
    ownerNodeId: null,
    nativeThreadRef: null,
    nativeConversationHeadRef: null,
    status: "idle",
    firstRunOrdinal: 1,
    lastRunOrdinal: 1,
    handoffIds: [],
    forkedFrom: null,
    createdAt: now,
    updatedAt: now,
  };
  const ownedWorkerRun: OrchestrationV2Run = {
    ...workerRun,
    controlledWorkerOwnership: {
      coordinatorThreadId: owner.threadId,
      coordinatorRunId: owner.runId,
      taskId: identity.taskId,
      workerThreadId: identity.workerThreadId,
      launchCommandId: identity.launchCommandId,
      launchMessageId: identity.launchMessageId,
      workerRunId: workerRun.id,
      workerAttemptId,
      providerInstanceId: instanceId,
      providerThreadId: providerThread.id,
      providerSessionId: providerThread.providerSessionId!,
    },
  };
  const sink = yield* EventSink.EventSinkV2;
  yield* sink.write({
    events: [
      {
        id: EventId.make(`${key}:worker-thread`),
        type: "thread.created",
        threadId: workerThread.id,
        occurredAt: now,
        payload: workerThread,
      },
      {
        id: EventId.make(`${key}:worker-run`),
        type: "run.created",
        threadId: workerThread.id,
        runId: workerRun.id,
        occurredAt: now,
        payload: ownedWorkerRun,
      },
      {
        id: EventId.make(`${key}:worker-attempt`),
        type: "run-attempt.created",
        threadId: workerThread.id,
        runId: workerRun.id,
        occurredAt: now,
        payload: {
          id: workerAttemptId,
          runId: workerRun.id,
          rootNodeId: workerRun.rootNodeId!,
          attemptOrdinal: 1,
          providerInstanceId: instanceId,
          providerThreadId: providerThread.id,
          providerTurnId: null,
          reason: "initial",
          status: "pending",
          startedAt: null,
          completedAt: null,
        },
      },
      {
        id: EventId.make(`${key}:worker-binding`),
        type: "provider-thread.updated",
        threadId: workerThread.id,
        occurredAt: now,
        payload: providerThread,
      },
    ],
  });
  const claimCommand = (id: string): ControllerCommand => ({
    type: "multi-agent.controller.command",
    commandId: CommandId.make(id),
    threadId: owner.threadId,
    runId: owner.runId,
    action: {
      type: "commit_submission",
      claim: {
        coordinatorThreadId: owner.threadId,
        coordinatorRunId: owner.runId,
        taskId: identity.taskId,
        workerThreadId: workerThread.id,
        workerRunId: workerRun.id,
        workerAttemptId,
        providerInstanceId: instanceId,
        providerThreadId: providerThread.id,
        providerSessionId: providerThread.providerSessionId!,
        invocationClaimId: CommandId.make(id),
      },
    },
  });
  const stop = (id: string): ControllerCommand => ({
    type: "multi-agent.controller.command",
    commandId: CommandId.make(id),
    threadId: owner.threadId,
    runId: owner.runId,
    action: { type: "stop_coordinator" },
  });
  return { owner, workerRun: ownedWorkerRun, workerAttemptId, providerThread, claimCommand, stop };
});

it.effect(
  "grants one durable submission winner; replay cannot grant again and Stop cannot erase it",
  () =>
    Effect.gen(function* () {
      const fixture = yield* seedSubmissionWorker("commitment");
      const orchestrator = yield* Orchestrator.OrchestratorV2;
      const projections = yield* ProjectionStore.ProjectionStoreV2;
      const results = yield* Effect.all(
        [
          orchestrator.dispatch(fixture.claimCommand("claim:one")).pipe(Effect.exit),
          orchestrator.dispatch(fixture.claimCommand("claim:two")).pipe(Effect.exit),
        ],
        { concurrency: 2 },
      );
      assert.equal(results.filter((result) => result._tag === "Success").length, 1);
      for (const result of results)
        if (result._tag === "Success") assert.isTrue(result.value.submissionCommitmentGranted);
      const expected = (yield* projections.getThreadProjection(fixture.owner.threadId)).runs[0]!;
      const commitment = expected.multiAgentController!.tasks[0]!.submissionCommitment!;
      assert.isDefined(commitment);
      assert.equal(expected.multiAgentController!.tasks[0]!.status, "launching");
      const replay = yield* orchestrator.dispatch(
        fixture.claimCommand(commitment.invocationClaimId),
      );
      assert.isFalse(replay.submissionCommitmentGranted);
      yield* orchestrator.dispatch(fixture.stop("commitment:stop"));
      assert.equal(
        (yield* orchestrator.dispatch(fixture.claimCommand("claim:after-stop")).pipe(Effect.exit))
          ._tag,
        "Failure",
      );
      const stopped = (yield* projections.getThreadProjection(fixture.owner.threadId)).runs[0]!;
      assert.deepEqual(stopped.multiAgentController!.tasks[0]!.submissionCommitment, commitment);
      const events = yield* EventStore.EventStoreV2.pipe(
        Effect.flatMap((store) => Stream.runCollect(store.read({}))),
      );
      for (const layer of [ProjectionStore.layer, ProjectionStore.layerMemory]) {
        yield* Effect.gen(function* () {
          const restored = yield* ProjectionStore.ProjectionStoreV2;
          if (layer === ProjectionStore.layerMemory)
            for (const record of events) yield* restored.apply(record.event);
          assert.deepEqual(
            (yield* restored.getThreadProjection(fixture.owner.threadId)).runs[0]!
              .multiAgentController,
            stopped.multiAgentController,
          );
        }).pipe(Effect.provide(layer));
      }
      const sink = yield* EventSink.EventSinkV2;
      for (const changed of [
        undefined,
        { ...commitment, invocationClaimId: CommandId.make("replaced") },
      ]) {
        const tasks = stopped.multiAgentController!.tasks.map((task) => {
          const { submissionCommitment: _previous, ...rest } = task;
          return changed ? { ...rest, submissionCommitment: changed } : rest;
        });
        const exit = yield* sink
          .write({
            events: [
              {
                id: EventId.make(`commitment:rewrite:${changed ? "replace" : "clear"}`),
                type: "multi-agent.controller.updated",
                threadId: fixture.owner.threadId,
                runId: fixture.owner.runId,
                occurredAt: now,
                payload: {
                  state: { ...stopped.multiAgentController!, tasks },
                  expectedRevision: stopped.multiAgentControllerRevision!,
                  revision: stopped.multiAgentControllerRevision! + 1,
                },
              },
            ],
          })
          .pipe(Effect.exit);
        assert.equal(exit._tag, "Failure");
      }
      const started = transitionMultiAgentTask(
        stopped.multiAgentController!,
        commitment.taskId,
        { type: "started", workerRunId: commitment.workerRunId },
        0,
      );
      if (started.status === "rejected") return yield* Effect.die("Expected launch acknowledgment");
      const completed = transitionMultiAgentTask(
        started.state,
        commitment.taskId,
        {
          type: "completed",
          result: {
            workerThreadId: commitment.workerThreadId,
            workerRunId: commitment.workerRunId,
            messageId: MessageId.make("commitment:result"),
          },
        },
        0,
      );
      if (completed.status === "rejected") return yield* Effect.die("Expected completion");
      yield* sink.write({
        events: [
          {
            id: EventId.make("commitment:completed"),
            type: "multi-agent.controller.updated",
            threadId: fixture.owner.threadId,
            runId: fixture.owner.runId,
            occurredAt: now,
            payload: {
              state: completed.state,
              expectedRevision: stopped.multiAgentControllerRevision!,
              revision: stopped.multiAgentControllerRevision! + 1,
            },
          },
        ],
      });
      const finished = (yield* projections.getThreadProjection(fixture.owner.threadId)).runs[0]!
        .multiAgentController!;
      assert.equal(finished.tasks[0]!.status, "completed");
      assert.deepEqual(finished.tasks[0]!.submissionCommitment, commitment);
      assert.equal(
        finished.coordinatorStoppedAt,
        stopped.multiAgentController!.coordinatorStoppedAt,
      );
    }).pipe(Effect.provide(layers)),
);

it.effect(
  "orders Stop-first and simultaneous Stop/commitment and rejects stale worker attempts",
  () =>
    Effect.gen(function* () {
      const orchestrator = yield* Orchestrator.OrchestratorV2;
      const projections = yield* ProjectionStore.ProjectionStoreV2;
      const stopped = yield* seedSubmissionWorker("stop-first");
      yield* orchestrator.dispatch(stopped.stop("stop-first:stop"));
      assert.equal(
        (yield* orchestrator.dispatch(stopped.claimCommand("stop-first:claim")).pipe(Effect.exit))
          ._tag,
        "Failure",
      );
      assert.isUndefined(
        (yield* projections.getThreadProjection(stopped.owner.threadId)).runs[0]!
          .multiAgentController!.tasks[0]!.submissionCommitment,
      );
      const race = yield* seedSubmissionWorker("stop-race");
      const results = yield* Effect.all(
        [
          orchestrator.dispatch(race.claimCommand("stop-race:claim")).pipe(Effect.exit),
          orchestrator.dispatch(race.stop("stop-race:stop")),
        ],
        { concurrency: 2 },
      );
      const state = (yield* projections.getThreadProjection(race.owner.threadId)).runs[0]!
        .multiAgentController!;
      assert.isDefined(state.coordinatorStoppedAt);
      assert.equal(
        state.tasks[0]!.submissionCommitment !== undefined,
        results[0]._tag === "Success",
      );
      const stale = yield* seedSubmissionWorker("stale-attempt");
      const command = stale.claimCommand("stale-attempt:claim");
      if (command.action.type !== "commit_submission") return;
      for (const [key, change] of [
        ["attempt", { workerAttemptId: RunAttemptId.make("old-attempt") }],
        ["run", { workerRunId: RunId.make("other-run") }],
        ["session", { providerSessionId: ProviderSessionId.make("old-session") }],
      ] as const) {
        const id = CommandId.make(`stale-attempt:${key}`);
        const bad = {
          ...command,
          commandId: id,
          action: {
            ...command.action,
            claim: { ...command.action.claim, ...change, invocationClaimId: id },
          },
        };
        assert.equal((yield* orchestrator.dispatch(bad).pipe(Effect.exit))._tag, "Failure");
      }
      // A valid plan must also fail if the worker attempt changes before its transaction commits.
      const current = (yield* projections.getThreadProjection(stale.owner.threadId)).runs[0]!;
      const records = yield* projections.getThreadRecords(stale.workerRun.threadId, [
        "runs",
        "attempts",
        "providerThreads",
      ]);
      const planned = grantMultiAgentSubmission(
        current.multiAgentController!,
        { ...command.action.claim, committedAt: DateTime.formatIso(now) },
        {
          thread: records.thread,
          run: records.runs[0]!,
          attempt: records.attempts[0]!,
          providerThread: records.providerThreads[0]!,
        },
      );
      assert.equal(planned.status, "granted");
      if (planned.status !== "granted") return;
      const sink = yield* EventSink.EventSinkV2;
      yield* sink.write({
        events: [
          {
            id: EventId.make("stale-attempt:changed"),
            type: "run.updated",
            threadId: stale.workerRun.threadId,
            runId: stale.workerRun.id,
            occurredAt: now,
            payload: {
              ...stale.workerRun,
              activeAttemptId: RunAttemptId.make("replacement-attempt"),
            },
          },
        ],
      });
      const rejected = yield* sink
        .write({
          events: [
            {
              id: EventId.make("stale-attempt:planned-commit"),
              type: "multi-agent.controller.updated",
              threadId: stale.owner.threadId,
              runId: stale.owner.runId,
              occurredAt: now,
              payload: {
                state: planned.state,
                expectedRevision: current.multiAgentControllerRevision!,
                revision: current.multiAgentControllerRevision! + 1,
              },
            },
          ],
        })
        .pipe(Effect.exit);
      assert.equal(rejected._tag, "Failure");
      assert.isUndefined(
        (yield* projections.getThreadProjection(stale.owner.threadId)).runs[0]!
          .multiAgentController!.tasks[0]!.submissionCommitment,
      );
    }).pipe(Effect.provide(layers)),
);

it.effect(
  "persists exact completed-run ownership, protects stale lifecycle updates, and replays events",
  () =>
    Effect.gen(function* () {
      const owner = yield* seed("durable");
      const orchestrator = yield* Orchestrator.OrchestratorV2;
      const projections = yield* ProjectionStore.ProjectionStoreV2;
      const sink = yield* EventSink.EventSinkV2;
      yield* orchestrator.dispatch(owner.admit("admit:durable"));
      yield* orchestrator.dispatch(owner.reserve(0, "reserve:durable"));
      const sql = yield* SqlClient.SqlClient;
      assert.equal(
        (yield* sql<{
          count: number;
        }>`SELECT count(*) AS count FROM orchestration_v2_effect_outbox`)[0]?.count,
        0,
      );
      const stored = (yield* projections.getThreadProjection(owner.threadId)).runs[0]!;
      assert.equal(stored.status, "completed");
      assert.equal(stored.multiAgentController?.coordinatorRunId, owner.runId);
      assert.equal(stored.multiAgentControllerRevision, 2);
      assert.deepEqual(stored.multiAgent, owner.run.multiAgent);
      assert.deepEqual(
        (yield* projections.getMultiAgentControllerRuns()).map((run) => run.id),
        [owner.runId],
      );
      // Neither missing metadata nor a stale producer's conflicting metadata can overwrite dedicated state.
      for (const payload of [
        owner.run,
        {
          ...owner.run,
          multiAgent: { mode: "off" as const },
          multiAgentController: { ...stored.multiAgentController!, tasks: [] },
          multiAgentControllerRevision: 0,
        },
      ]) {
        yield* sink.write({
          events: [
            {
              id: EventId.make(`stale:${payload.multiAgentControllerRevision ?? "missing"}`),
              type: "run.updated",
              threadId: owner.threadId,
              runId: owner.runId,
              occurredAt: now,
              payload,
            },
          ],
        });
      }
      const after = (yield* projections.getThreadProjection(owner.threadId)).runs[0]!;
      assert.deepEqual(after.multiAgentController, stored.multiAgentController);
      assert.equal(after.multiAgentControllerRevision, 2);
      assert.deepEqual(after.multiAgent, owner.run.multiAgent);
      const events = yield* EventStore.EventStoreV2.pipe(
        Effect.flatMap((store) => Stream.runCollect(store.read({ threadId: owner.threadId }))),
      );
      for (const layer of [ProjectionStore.layerMemory, ProjectionStore.layer]) {
        yield* Effect.gen(function* () {
          const restored = yield* ProjectionStore.ProjectionStoreV2;
          if (layer === ProjectionStore.layerMemory)
            for (const event of events) yield* restored.apply(event.event);
          const run = (yield* restored.getThreadProjection(owner.threadId)).runs[0]!;
          assert.deepEqual(run.multiAgentController, stored.multiAgentController);
          assert.equal(run.multiAgentControllerRevision, 2);
          assert.deepEqual(
            (yield* restored.getMultiAgentControllerRuns()).map((candidate) => candidate.id),
            [owner.runId],
          );
          // Reapplying already-observed controller revisions cannot rewind the controller.
          for (const event of events.filter(
            (record) => record.event.type === "multi-agent.controller.updated",
          ))
            yield* restored.apply(event.event);
          assert.equal(
            (yield* restored.getThreadProjection(owner.threadId)).runs[0]
              ?.multiAgentControllerRevision,
            2,
          );
        }).pipe(Effect.provide(layer));
      }
    }).pipe(Effect.provide(layers)),
);

it.effect("serializes duplicate proposals and reservations, enforcing both capacity limits", () =>
  Effect.gen(function* () {
    const orchestrator = yield* Orchestrator.OrchestratorV2;
    const projections = yield* ProjectionStore.ProjectionStoreV2;
    const first = yield* seed("first");
    const second = yield* seed("second");
    const third = yield* seed("third");
    yield* Effect.all(
      [
        orchestrator.dispatch(first.admit("first:one")),
        orchestrator.dispatch(first.admit("first:two")),
      ],
      { concurrency: "unbounded" },
    );
    assert.lengthOf(
      (yield* projections.getThreadProjection(first.threadId)).runs[0]!.multiAgentController!.tasks,
      4,
    );
    yield* Effect.all(
      [
        orchestrator.dispatch(first.reserve(0, "first:reserve:a")),
        orchestrator.dispatch(first.reserve(0, "first:reserve:a-again")),
      ],
      { concurrency: "unbounded" },
    );
    const admitted = (yield* projections.getThreadProjection(first.threadId)).runs[0]!;
    assert.lengthOf(
      admitted.multiAgentController!.tasks.filter((task) => task.status === "reserved"),
      1,
    );
    yield* orchestrator.dispatch(first.reserve(1, "first:reserve:b"));
    assert.equal(
      (yield* Effect.exit(orchestrator.dispatch(first.reserve(2, "first:reserve:c"))))._tag,
      "Failure",
    );
    yield* orchestrator.dispatch(second.admit("second:admit"));
    yield* orchestrator.dispatch(third.admit("third:admit"));
    const reservations = yield* Effect.all(
      [
        orchestrator.dispatch(second.reserve(0, "second:a")).pipe(Effect.exit),
        orchestrator.dispatch(second.reserve(1, "second:b")).pipe(Effect.exit),
        orchestrator.dispatch(third.reserve(0, "third:a")).pipe(Effect.exit),
      ],
      { concurrency: "unbounded" },
    );
    assert.lengthOf(
      reservations.filter((result) => result._tag === "Success"),
      2,
    );
    assert.equal(
      (yield* projections.getMultiAgentControllerRuns()).reduce(
        (sum, run) =>
          sum + run.multiAgentController!.tasks.filter((task) => task.status === "reserved").length,
        0,
      ),
      4,
    );
    const command = first.admit("first:one");
    yield* orchestrator.dispatch(command);
    assert.equal(
      (yield* Effect.exit(orchestrator.dispatch({ ...command, runId: second.runId })))._tag,
      "Failure",
    );
    assert.equal(
      (yield* Effect.exit(
        orchestrator.dispatch({
          ...command,
          commandId: CommandId.make("conflicting-proposal"),
          action: {
            type: "admit",
            attemptId: RunAttemptId.make("foreign"),
            proposalId: MessageId.make("foreign"),
            proposal: {},
            identities: [],
            providers: [],
          },
        }),
      ))._tag,
      "Failure",
    );
  }).pipe(Effect.provide(layers)),
);

it.effect(
  "recovers cancellation-pending controllers regardless of source run status, and omits settled controllers",
  () =>
    Effect.gen(function* () {
      const owner = yield* seed("cancel");
      const orchestrator = yield* Orchestrator.OrchestratorV2;
      const projections = yield* ProjectionStore.ProjectionStoreV2;
      const sink = yield* EventSink.EventSinkV2;
      yield* orchestrator.dispatch(owner.admit("cancel:admit", 1));
      const run = (yield* projections.getThreadProjection(owner.threadId)).runs[0]!;
      yield* sink.write({
        events: [
          {
            id: EventId.make("simulated-launching"),
            type: "multi-agent.controller.updated",
            threadId: owner.threadId,
            runId: owner.runId,
            occurredAt: now,
            payload: {
              expectedRevision: 1,
              revision: 2,
              state: {
                ...run.multiAgentController!,
                tasks: run.multiAgentController!.tasks.map((task) => ({
                  ...task,
                  status: "launching",
                })),
              },
            },
          },
        ],
      });
      const cancel = {
        type: "multi-agent.controller.command" as const,
        commandId: CommandId.make("cancel:command"),
        threadId: owner.threadId,
        runId: owner.runId,
        action: { type: "cancel" as const },
      };
      const first = yield* orchestrator.dispatch(cancel);
      const replay = yield* orchestrator.dispatch(cancel);
      assert.deepEqual(replay, first);
      assert.equal(
        (yield* projections.getMultiAgentControllerRuns())[0]?.multiAgentController?.status,
        "cancelling",
      );
      const cancelling = (yield* projections.getThreadProjection(owner.threadId)).runs[0]!;
      assert.equal(cancelling.multiAgentController?.tasks[0]?.status, "cancellation_pending");
      const finished = {
        ...cancelling.multiAgentController!,
        status: "cancelled" as const,
        tasks: cancelling.multiAgentController!.tasks.map((task) => ({
          ...task,
          status: "cancelled" as const,
        })),
      };
      yield* sink.write({
        events: [
          {
            id: EventId.make("confirmed-stopped"),
            type: "multi-agent.controller.updated",
            threadId: owner.threadId,
            runId: owner.runId,
            occurredAt: now,
            payload: { expectedRevision: 3, revision: 4, state: finished },
          },
        ],
      });
      assert.isEmpty(yield* projections.getMultiAgentControllerRuns());
      assert.lengthOf(yield* projections.getMultiAgentControllerRuns(false), 1);
    }).pipe(Effect.provide(layers)),
);

it.effect(
  "atomically rejects overbooking even when different threads commit stale capacity decisions",
  () =>
    Effect.gen(function* () {
      const orchestrator = yield* Orchestrator.OrchestratorV2;
      const projections = yield* ProjectionStore.ProjectionStoreV2;
      const sink = yield* EventSink.EventSinkV2;
      const owners = yield* Effect.all([seed("cap-a"), seed("cap-b"), seed("cap-c")]);
      for (const owner of owners)
        yield* orchestrator.dispatch(owner.admit(`admit:${owner.runId}`, 2));
      yield* orchestrator.dispatch(owners[0]!.reserve(0, "cap:a:0"));
      yield* orchestrator.dispatch(owners[0]!.reserve(1, "cap:a:1"));
      yield* orchestrator.dispatch(owners[1]!.reserve(0, "cap:b:0"));
      const decisions = yield* Effect.forEach(owners.slice(1), (owner, index) =>
        Effect.gen(function* () {
          const run = (yield* projections.getThreadProjection(owner.threadId)).runs[0]!;
          const state = {
            ...run.multiAgentController!,
            tasks: run.multiAgentController!.tasks.map((task, position) =>
              position === (index === 0 ? 1 : 0) ? { ...task, status: "reserved" as const } : task,
            ),
          };
          return {
            commandId: CommandId.make(`stale-cap:${index}`),
            threadId: owner.threadId,
            commandType: "multi-agent.controller.command",
            acceptedAt: now,
            effects: [],
            events: [
              {
                id: EventId.make(`stale-cap-event:${index}`),
                type: "multi-agent.controller.updated" as const,
                threadId: owner.threadId,
                runId: owner.runId,
                occurredAt: now,
                payload: {
                  expectedRevision: run.multiAgentControllerRevision!,
                  revision: run.multiAgentControllerRevision! + 1,
                  state,
                },
              },
            ],
          };
        }),
      );
      const commits = yield* Effect.all(
        decisions.map((decision) => sink.commitCommand(decision).pipe(Effect.exit)),
        { concurrency: "unbounded" },
      );
      assert.lengthOf(
        commits.filter((result) => result._tag === "Success"),
        1,
      );
      assert.equal(
        (yield* projections.getMultiAgentControllerRuns()).reduce(
          (sum, run) =>
            sum +
            run.multiAgentController!.tasks.filter((task) => task.status === "reserved").length,
          0,
        ),
        4,
      );
      const receipts = yield* CommandReceiptStore.CommandReceiptStoreV2;
      const recorded = yield* Effect.forEach(decisions, (decision) =>
        receipts.getByCommandId(decision.commandId),
      );
      assert.lengthOf(recorded.filter(Option.isSome), 1);
    }).pipe(Effect.provide(layers)),
);

it.effect("keeps legacy policies Off and rejects controller enrollment for ordinary runs", () =>
  Effect.gen(function* () {
    const owner = yield* seed("legacy");
    const projections = yield* ProjectionStore.ProjectionStoreV2;
    const sink = yield* EventSink.EventSinkV2;
    const sql = yield* SqlClient.SqlClient;
    yield* sql`UPDATE orchestration_v2_projection_runs SET payload_json = json_remove(payload_json, '$.multiAgent') WHERE run_id = ${owner.runId}`;
    yield* sink.write({
      events: [
        {
          id: EventId.make("legacy-update"),
          type: "run.updated",
          threadId: owner.threadId,
          runId: owner.runId,
          occurredAt: now,
          payload: owner.run,
        },
      ],
    });
    assert.deepEqual((yield* projections.getThreadProjection(owner.threadId)).runs[0]?.multiAgent, {
      mode: "off",
    });
    const orchestrator = yield* Orchestrator.OrchestratorV2;
    assert.equal(
      (yield* orchestrator.dispatch(owner.admit("legacy:admit")).pipe(Effect.exit))._tag,
      "Failure",
    );
    assert.isEmpty(yield* projections.getMultiAgentControllerRuns());
  }).pipe(Effect.provide(layers)),
);

it.effect("reloads unfinished controllers after closing and reopening an on-disk database", () =>
  Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem;
    const path = yield* Path.Path;
    const temporary = yield* fs.makeTempDirectoryScoped({ prefix: "t3-controller-restart-" });
    const filename = path.join(temporary, "state.sqlite");
    const firstLayer = makeLayers(
      SqlitePersistence.layerFromPath(filename).pipe(
        Layer.provide(NodeServices.layer),
        Layer.orDie,
      ),
    );
    const expected = yield* Effect.scoped(
      Effect.gen(function* () {
        const fixture = yield* seedSubmissionWorker("restart");
        const owner = fixture.owner;
        const orchestrator = yield* Orchestrator.OrchestratorV2;
        assert.isTrue(
          (yield* orchestrator.dispatch(fixture.claimCommand("restart:claim")))
            .submissionCommitmentGranted,
        );
        yield* orchestrator.dispatch({
          type: "multi-agent.controller.command",
          commandId: CommandId.make("restart:stop"),
          threadId: owner.threadId,
          runId: owner.runId,
          action: { type: "stop_coordinator" },
        });
        const projections = yield* ProjectionStore.ProjectionStoreV2;
        const run = (yield* projections.getMultiAgentControllerRuns())[0]!;
        assert.isDefined(run.multiAgentController?.coordinatorStoppedAt);
        return {
          id: run.id,
          state: run.multiAgentController,
          revision: run.multiAgentControllerRevision,
          policy: run.multiAgent,
          workerOwnership: (yield* projections.getThreadProjection(fixture.workerRun.threadId))
            .runs[0]!.controlledWorkerOwnership,
          workerThreadId: fixture.workerRun.threadId,
        };
      }).pipe(Effect.provide(firstLayer)),
    );
    const reopened = ProjectionStore.layer.pipe(
      Layer.provide(SqlitePersistence.layerFromPath(filename)),
      Layer.provide(NodeServices.layer),
    );
    yield* Effect.scoped(
      Effect.gen(function* () {
        const projections = yield* ProjectionStore.ProjectionStoreV2;
        const run = (yield* projections.getMultiAgentControllerRuns())[0]!;
        assert.deepEqual(
          {
            id: run.id,
            state: run.multiAgentController,
            revision: run.multiAgentControllerRevision,
            policy: run.multiAgent,
            workerOwnership: (yield* projections.getThreadProjection(expected.workerThreadId))
              .runs[0]!.controlledWorkerOwnership,
            workerThreadId: expected.workerThreadId,
          },
          expected,
        );
      }).pipe(Effect.provide(reopened)),
    );
  }).pipe(Effect.provide(NodeServices.layer)),
);

it.effect(
  "serializes Stop against reservation and preserves stopped state through SQL reload, replay and completion",
  () =>
    Effect.gen(function* () {
      const owner = yield* seed("stop-coordinator");
      const orchestrator = yield* Orchestrator.OrchestratorV2;
      const projections = yield* ProjectionStore.ProjectionStoreV2;
      const sink = yield* EventSink.EventSinkV2;
      const read = () =>
        projections.getThreadProjection(owner.threadId).pipe(Effect.map((p) => p.runs[0]!));
      const stop = (id: string): ControllerCommand => ({
        type: "multi-agent.controller.command",
        commandId: CommandId.make(id),
        threadId: owner.threadId,
        runId: owner.runId,
        action: { type: "stop_coordinator" },
      });
      yield* orchestrator.dispatch(owner.admit("stop:admit", 2));
      yield* orchestrator.dispatch(owner.reserve(0, "stop:reserve"));
      yield* orchestrator.dispatch({
        type: "multi-agent.controller.command",
        commandId: CommandId.make("stop:launch"),
        threadId: owner.threadId,
        runId: owner.runId,
        action: { type: "launch", taskId: owner.identities[0]!.taskId },
      });
      yield* Effect.all(
        [
          orchestrator.dispatch(stop("stop:one")),
          orchestrator.dispatch(owner.reserve(1, "stop:race-reserve")).pipe(Effect.exit),
        ],
        { concurrency: 2 },
      );
      const stopped = yield* read();
      assert.isDefined(stopped.multiAgentController?.coordinatorStoppedAt);
      assert.equal(stopped.multiAgentController?.status, "open");
      assert.equal(stopped.multiAgentController?.tasks[0]?.status, "launching");
      assert.include(["queued", "reserved"], stopped.multiAgentController!.tasks[1]!.status);
      yield* orchestrator.dispatch(stop("stop:one"));
      yield* orchestrator.dispatch(stop("stop:two"));
      assert.deepEqual((yield* read()).multiAgentController, stopped.multiAgentController);
      for (const action of [
        { type: "reserve" as const, taskId: owner.identities[1]!.taskId },
        { type: "launch" as const, taskId: owner.identities[0]!.taskId },
      ]) {
        assert.equal(
          (yield* orchestrator
            .dispatch({ ...stop(`stop:reject:${action.type}`), action })
            .pipe(Effect.exit))._tag,
          "Failure",
        );
      }
      assert.deepEqual(
        (yield* projections.getMultiAgentControllerRuns()).map((run) => run.id),
        [owner.runId],
      );
      // Ordinary lifecycle producers cannot remove the marker.
      yield* sink.write({
        events: [
          {
            id: EventId.make("stop:stale-run"),
            type: "run.updated",
            threadId: owner.threadId,
            runId: owner.runId,
            occurredAt: now,
            payload: owner.run,
          },
        ],
      });
      const beforeCompletion = yield* read();
      const writeState = (id: string, state: MultiAgentControllerState) =>
        read().pipe(
          Effect.flatMap((run) =>
            sink.write({
              events: [
                {
                  id: EventId.make(id),
                  type: "multi-agent.controller.updated",
                  threadId: owner.threadId,
                  runId: owner.runId,
                  occurredAt: now,
                  payload: {
                    state,
                    expectedRevision: run.multiAgentControllerRevision!,
                    revision: run.multiAgentControllerRevision! + 1,
                  },
                },
              ],
            }),
          ),
        );
      const { coordinatorStoppedAt: _marker, ...cleared } = beforeCompletion.multiAgentController!;
      assert.equal((yield* writeState("stop:clear", cleared).pipe(Effect.exit))._tag, "Failure");
      assert.equal(
        (yield* writeState("stop:replace", {
          ...beforeCompletion.multiAgentController!,
          coordinatorStoppedAt: "2026-10-15T00:00:00Z",
        }).pipe(Effect.exit))._tag,
        "Failure",
      );
      const started = transitionMultiAgentTask(
        beforeCompletion.multiAgentController!,
        owner.identities[0]!.taskId,
        { type: "started", workerRunId: RunId.make("stop:worker-run") },
        0,
      );
      assert.notEqual(started.status, "rejected");
      if (started.status === "rejected") return;
      const result = {
        workerThreadId: owner.identities[0]!.workerThreadId,
        workerRunId: RunId.make("stop:worker-run"),
        messageId: MessageId.make("stop:result"),
      };
      const completed = transitionMultiAgentTask(
        started.state,
        owner.identities[0]!.taskId,
        { type: "completed", result },
        0,
      );
      assert.notEqual(completed.status, "rejected");
      if (completed.status === "rejected") return;
      yield* writeState("stop:completion", completed.state);
      const expected = yield* read();
      assert.equal(expected.multiAgentController?.tasks[0]?.status, "completed");
      assert.deepEqual(
        expected.multiAgentController?.coordinatorStoppedAt,
        stopped.multiAgentController?.coordinatorStoppedAt,
      );
      const events = yield* EventStore.EventStoreV2.pipe(
        Effect.flatMap((store) => Stream.runCollect(store.read({ threadId: owner.threadId }))),
      );
      for (const layer of [ProjectionStore.layer, ProjectionStore.layerMemory]) {
        yield* Effect.gen(function* () {
          const restored = yield* ProjectionStore.ProjectionStoreV2;
          if (layer === ProjectionStore.layerMemory)
            for (const stored of events) yield* restored.apply(stored.event);
          assert.deepEqual(
            (yield* restored.getThreadProjection(owner.threadId)).runs[0]!.multiAgentController,
            expected.multiAgentController,
          );
          for (const stored of events.filter(
            (event) => event.event.type === "multi-agent.controller.updated",
          ))
            yield* restored.apply(stored.event);
          assert.deepEqual(
            (yield* restored.getThreadProjection(owner.threadId)).runs[0]!.multiAgentController,
            expected.multiAgentController,
          );
        }).pipe(Effect.provide(layer));
      }
    }).pipe(Effect.provide(layers)),
);

it.effect(
  "rolls back events and receipts when a stale controller revision fails projection validation",
  () =>
    Effect.gen(function* () {
      const owner = yield* seed("rollback");
      const orchestrator = yield* Orchestrator.OrchestratorV2;
      const projections = yield* ProjectionStore.ProjectionStoreV2;
      const sink = yield* EventSink.EventSinkV2;
      yield* orchestrator.dispatch(owner.admit("rollback:admit", 1));
      const run = (yield* projections.getThreadProjection(owner.threadId)).runs[0]!;
      const invalid = yield* sink
        .commitCommand({
          commandId: CommandId.make("stale-commit"),
          threadId: owner.threadId,
          commandType: "multi-agent.controller.command",
          acceptedAt: now,
          effects: [],
          events: [
            {
              id: EventId.make("stale-controller-event"),
              type: "multi-agent.controller.updated",
              threadId: owner.threadId,
              runId: owner.runId,
              occurredAt: now,
              payload: { expectedRevision: 0, revision: 2, state: run.multiAgentController! },
            },
          ],
        })
        .pipe(Effect.exit);
      assert.equal(invalid._tag, "Failure");
      const receipts = yield* CommandReceiptStore.CommandReceiptStoreV2;
      assert.isTrue(Option.isNone(yield* receipts.getByCommandId(CommandId.make("stale-commit"))));
      const events = yield* EventStore.EventStoreV2.pipe(
        Effect.flatMap((store) =>
          Stream.runCollect(store.readByCommandId({ commandId: CommandId.make("stale-commit") })),
        ),
      );
      assert.isEmpty(events);
      assert.equal(
        (yield* projections.getThreadProjection(owner.threadId)).runs[0]
          ?.multiAgentControllerRevision,
        1,
      );
    }).pipe(Effect.provide(layers)),
);

const submissionFixture = Effect.fn("submissionFixture")(function* (key: string) {
  const fixture = yield* seedSubmissionWorker(key);
  const projections = yield* ProjectionStore.ProjectionStoreV2;
  const commands = yield* Orchestrator.OrchestratorV2;
  const worker = yield* projections.getThreadProjection(fixture.workerRun.threadId);
  const input: ProviderAdapter.ProviderAdapterV2TurnInput = {
    appThread: worker.thread,
    threadId: worker.thread.id,
    runId: fixture.workerRun.id,
    runOrdinal: 1,
    providerTurnOrdinal: 1,
    attemptId: fixture.workerAttemptId,
    rootNodeId: fixture.workerRun.rootNodeId!,
    providerThread: fixture.providerThread,
    message: {
      messageId: fixture.workerRun.userMessageId!,
      text: "Build the DJ frontend",
      attachments: [],
      createdBy: "system",
      creationSource: "server",
    },
    modelSelection: fixture.workerRun.modelSelection,
    runtimePolicy: {
      runtimeMode: worker.thread.runtimeMode,
      interactionMode: worker.thread.interactionMode,
      cwd: null,
    },
  };
  let calls = 0;
  const native = {
    driver: provider.driver,
    instanceId: instanceId,
    providerSessionId: fixture.providerThread.providerSessionId!,
    startTurn: () =>
      Effect.sync(() => {
        calls += 1;
      }),
    compactThread: () =>
      Effect.sync(() => {
        calls += 1;
      }),
  } as unknown as ProviderAdapter.ProviderAdapterV2SessionRuntime;
  const registry = new ProviderRuntimeRegistry();
  const registerRuntime = (runtime: ProviderAdapter.ProviderAdapterV2SessionRuntime) =>
    registry.register({
      runtime,
      providerInstanceId: instanceId,
      providerSessionId: native.providerSessionId,
      historicalState: "fresh",
    });
  registerRuntime(native);
  const sessions = {
    markControlledWorkerExposure: (
      runtime: ProviderAdapter.ProviderAdapterV2SessionRuntime,
      owner: NonNullable<OrchestrationV2Run["controlledWorkerOwnership"]>,
    ) =>
      Effect.sync(() => registry.exposeControlledWorker(runtime, owner)).pipe(
        Effect.flatMap((reason) =>
          reason === undefined
            ? Effect.void
            : Effect.fail(
                new ProviderSessionActivityError({
                  providerSessionId: runtime.providerSessionId,
                  cause: reason,
                }),
              ),
        ),
      ),
  };
  return {
    ...fixture,
    projections,
    commands,
    input,
    native,
    sessions,
    registerRuntime,
    calls: () => calls,
    guarded: withControlledWorkerSubmission(
      native,
      fixture.workerRun,
      projections,
      commands,
      sessions,
    ),
  };
});

it.effect.each(["stop-first", "commit-first"] as const)(
  "orders final provider submission against %s",
  (order) =>
    Effect.gen(function* () {
      const fixture = yield* submissionFixture(order);
      if (order === "stop-first") yield* fixture.commands.dispatch(fixture.stop("stop-first:stop"));
      const guarded =
        order === "commit-first"
          ? withControlledWorkerSubmission(
              fixture.native,
              fixture.workerRun,
              fixture.projections,
              {
                dispatch: (command) =>
                  fixture.commands
                    .dispatch(command)
                    .pipe(
                      Effect.tap(() =>
                        fixture.commands.dispatch(fixture.stop("commit-first:stop")),
                      ),
                    ),
              },
              fixture.sessions,
            )
          : fixture.guarded;
      const result = yield* guarded.startTurn(fixture.input).pipe(Effect.exit);
      assert.equal(result._tag, order === "stop-first" ? "Failure" : "Success");
      assert.equal(fixture.calls(), order === "stop-first" ? 0 : 1);
      const source = (yield* fixture.projections.getMultiAgentControllerRuns())[0]!;
      assert.isDefined(source.multiAgentController?.coordinatorStoppedAt);
      assert.equal(
        !!source.multiAgentController?.tasks[0]?.submissionCommitment,
        order === "commit-first",
      );
    }).pipe(Effect.provide(layers)),
);

it.effect(
  "concurrent executors, duplicate evaluations and compaction share one durable invocation",
  () =>
    Effect.gen(function* () {
      const fixture = yield* submissionFixture("concurrent-submission");
      const request = fixture.guarded.startTurn(fixture.input);
      const results = yield* Effect.all([request.pipe(Effect.exit), request.pipe(Effect.exit)], {
        concurrency: 2,
      });
      assert.equal(results.filter((result) => result._tag === "Success").length, 1);
      assert.equal(fixture.calls(), 1);
      assert.equal((yield* request.pipe(Effect.exit))._tag, "Failure");
      assert.equal(
        (yield* fixture.guarded.compactThread!(fixture.input).pipe(Effect.exit))._tag,
        "Failure",
      );
      assert.equal(fixture.calls(), 1);
    }).pipe(Effect.provide(layers)),
);

it.effect(
  "records runtime exposure before granting a commitment, without submitting on registry failure",
  () =>
    Effect.gen(function* () {
      const fixture = yield* submissionFixture("exposure-before-commitment");
      let marked = false;
      const guarded = withControlledWorkerSubmission(
        fixture.native,
        fixture.workerRun,
        fixture.projections,
        {
          dispatch: (command) =>
            Effect.sync(() => assert.isTrue(marked)).pipe(
              Effect.andThen(fixture.commands.dispatch(command)),
            ),
        },
        {
          markControlledWorkerExposure: (runtime, owner) =>
            fixture.sessions.markControlledWorkerExposure(runtime, owner).pipe(
              Effect.tap(() =>
                Effect.sync(() => {
                  marked = true;
                }),
              ),
            ),
        },
      );
      yield* guarded.startTurn(fixture.input);
      assert.equal(fixture.calls(), 1);
      const rejected = yield* submissionFixture("exposure-unregistered-runtime");
      const unregistered = withControlledWorkerSubmission(
        { ...rejected.native },
        rejected.workerRun,
        rejected.projections,
        rejected.commands,
        rejected.sessions,
      );
      assert.equal(
        (yield* unregistered.startTurn(rejected.input).pipe(Effect.exit))._tag,
        "Failure",
      );
      assert.equal(rejected.calls(), 0);
      assert.isUndefined(
        (yield* rejected.projections.getMultiAgentControllerRuns()).find(
          (run) => run.id === rejected.workerRun.controlledWorkerOwnership!.coordinatorRunId,
        )?.multiAgentController?.tasks[0]?.submissionCommitment,
      );
    }).pipe(Effect.provide(layers)),
);

it.effect("a lost commitment response cannot authorize a retry", () =>
  Effect.gen(function* () {
    const fixture = yield* submissionFixture("lost-grant");
    const uncertain = withControlledWorkerSubmission(
      fixture.native,
      fixture.workerRun,
      fixture.projections,
      {
        dispatch: (command) =>
          fixture.commands.dispatch(command).pipe(
            Effect.andThen(
              Effect.fail(
                new Orchestrator.OrchestratorDispatchError({
                  commandId: command.commandId,
                  commandType: command.type,
                  cause: "lost response",
                }),
              ),
            ),
          ),
      },
      fixture.sessions,
    );
    assert.equal((yield* uncertain.startTurn(fixture.input).pipe(Effect.exit))._tag, "Failure");
    assert.equal(
      (yield* fixture.guarded.startTurn(fixture.input).pipe(Effect.exit))._tag,
      "Failure",
    );
    assert.equal(fixture.calls(), 0);
    assert.isDefined(
      (yield* fixture.projections.getMultiAgentControllerRuns())[0]!.multiAgentController!.tasks[0]!
        .submissionCommitment,
    );
  }).pipe(Effect.provide(layers)),
);

it.effect(
  "rejects stale invocation ownership before granting permission and leaves ordinary sessions untouched",
  () =>
    Effect.gen(function* () {
      const fixture = yield* submissionFixture("stale-invocation");
      for (const invalid of [
        { ...fixture.input, attemptId: RunAttemptId.make("later-attempt") },
        { ...fixture.input, runId: RunId.make("later-manual-run") },
        {
          ...fixture.input,
          providerThread: {
            ...fixture.providerThread,
            providerSessionId: ProviderSessionId.make("different-session"),
          },
        },
      ]) {
        assert.equal((yield* fixture.guarded.startTurn(invalid).pipe(Effect.exit))._tag, "Failure");
      }
      assert.equal(fixture.calls(), 0);
      const { controlledWorkerOwnership: _owned, ...manual } = fixture.workerRun;
      const ordinary = withControlledWorkerSubmission(
        fixture.native,
        manual,
        fixture.projections,
        fixture.commands,
        fixture.sessions,
      );
      assert.strictEqual(ordinary, fixture.native);
      yield* ordinary.startTurn(fixture.input);
      yield* ordinary.startTurn(fixture.input);
      assert.equal(fixture.calls(), 2);
    }).pipe(Effect.provide(layers)),
);

it.effect("a provider request with a lost response has no second submission authorization", () =>
  Effect.gen(function* () {
    const fixture = yield* submissionFixture("lost-provider-response");
    let submissions = 0;
    const failedRuntime = {
      ...fixture.native,
      startTurn: () =>
        Effect.sync(() => {
          submissions += 1;
        }).pipe(
          Effect.andThen(
            Effect.fail(
              new ProviderAdapter.ProviderAdapterTurnStartError({
                driver: provider.driver,
                threadId: fixture.input.threadId,
                runId: fixture.input.runId,
                providerThreadId: fixture.providerThread.id,
                cause: "request sent, response lost",
              }),
            ),
          ),
        ),
    };
    fixture.registerRuntime(failedRuntime);
    const uncertain = withControlledWorkerSubmission(
      failedRuntime,
      fixture.workerRun,
      fixture.projections,
      fixture.commands,
      fixture.sessions,
    );
    assert.equal((yield* uncertain.startTurn(fixture.input).pipe(Effect.exit))._tag, "Failure");
    assert.equal((yield* uncertain.startTurn(fixture.input).pipe(Effect.exit))._tag, "Failure");
    assert.equal(submissions, 1);
    assert.isDefined(
      (yield* fixture.projections.getMultiAgentControllerRuns())[0]!.multiAgentController!.tasks[0]!
        .submissionCommitment,
    );
  }).pipe(Effect.provide(layers)),
);

it.effect(
  "retains queued automatic trigger provenance through stale updates, SQL reload and replay",
  () =>
    Effect.gen(function* () {
      const trigger: NewRunTrigger = {
        type: "automatic",
        kind: "restart",
        provenance: { type: "unknown" },
      };
      const fixture = yield* seed("trigger", "queued", trigger);
      const sink = yield* EventSink.EventSinkV2;
      const { newRunTrigger: _trigger, ...stale } = fixture.run;
      yield* sink.write({
        events: [
          {
            id: EventId.make("trigger:stale"),
            type: "run.updated",
            threadId: fixture.threadId,
            runId: fixture.runId,
            occurredAt: now,
            payload: { ...stale, status: "completed", completedAt: now },
          },
        ],
      });
      const events = yield* EventStore.EventStoreV2.pipe(
        Effect.flatMap((store) => Stream.runCollect(store.read({}))),
      );
      for (const layer of [ProjectionStore.layer, ProjectionStore.layerMemory]) {
        yield* Effect.gen(function* () {
          const projections = yield* ProjectionStore.ProjectionStoreV2;
          if (layer === ProjectionStore.layerMemory)
            for (const record of events) yield* projections.apply(record.event);
          const persisted = (yield* projections.getThreadProjection(fixture.threadId)).runs[0]!;
          assert.deepEqual(persisted.newRunTrigger, trigger);
          const conflict = yield* projections
            .apply({
              id: EventId.make("trigger:conflict"),
              type: "run.updated",
              threadId: fixture.threadId,
              runId: fixture.runId,
              occurredAt: now,
              payload: { ...persisted, newRunTrigger: { type: "manual" } },
            })
            .pipe(Effect.exit);
          assert.equal(conflict._tag, "Failure");
          assert.deepEqual(
            (yield* projections.getThreadProjection(fixture.threadId)).runs[0]!.newRunTrigger,
            trigger,
          );
        }).pipe(Effect.provide(layer));
      }
      const ordinary = yield* seed("legacy-trigger");
      assert.equal(
        (yield* sink
          .write({
            events: [
              {
                id: EventId.make("trigger:late"),
                type: "run.updated",
                threadId: ordinary.threadId,
                runId: ordinary.runId,
                occurredAt: now,
                payload: { ...ordinary.run, newRunTrigger: { type: "manual" } },
              },
            ],
          })
          .pipe(Effect.exit))._tag,
        "Failure",
      );
    }).pipe(Effect.provide(layers)),
);

type WorkerFinishedAction = Extract<ControllerCommand["action"], { type: "worker_finished" }>;
const seedWorkerSettlement = Effect.fn("seedWorkerSettlement")(function* (
  key: string,
  status: OrchestrationV2Run["status"] = "completed",
  live = false,
) {
  const fixture = yield* seedSubmissionWorker(key);
  const commands = yield* Orchestrator.OrchestratorV2;
  const projections = yield* ProjectionStore.ProjectionStoreV2;
  const sink = yield* EventSink.EventSinkV2;
  yield* commands.dispatch(fixture.claimCommand(`${key}:commitment`));
  const task = fixture.owner.identities[0]!;
  yield* commands.dispatch({
    type: "multi-agent.controller.command",
    commandId: CommandId.make(`${key}:started`),
    threadId: fixture.owner.threadId,
    runId: fixture.owner.runId,
    action: {
      type: "started",
      taskId: task.taskId,
      launchCommandId: task.launchCommandId,
      workerThreadId: task.workerThreadId,
      workerRunId: fixture.workerRun.id,
    },
  });
  const worker = yield* projections.getThreadRecords(task.workerThreadId, ["runs", "attempts"]);
  const terminal = ["completed", "failed", "interrupted", "cancelled", "rolled_back"].includes(
    status,
  );
  const attemptStatus =
    status === "completed" || status === "rolled_back"
      ? "completed"
      : status === "failed" || status === "interrupted" || status === "cancelled"
        ? status
        : "running";
  yield* sink.write({
    commandId: CommandId.make(
      `${live ? "live" : "command:runtime-reconcile"}:${key}:worker-outcome`,
    ),
    events: [
      {
        id: EventId.make(`${key}:worker-outcome`),
        threadId: task.workerThreadId,
        runId: fixture.workerRun.id,
        type: "run.updated",
        occurredAt: now,
        payload: { ...worker.runs[0]!, status, completedAt: terminal ? now : null },
      },
      {
        id: EventId.make(`${key}:attempt-outcome`),
        threadId: task.workerThreadId,
        runId: fixture.workerRun.id,
        type: "run-attempt.updated",
        occurredAt: now,
        payload: {
          ...worker.attempts[0]!,
          status: attemptStatus,
          completedAt: terminal ? now : null,
        },
      },
    ],
  });
  const finish = (
    id: string,
    overrides: Partial<WorkerFinishedAction> = {},
  ): ControllerCommand => ({
    type: "multi-agent.controller.command",
    commandId: CommandId.make(id),
    threadId: fixture.owner.threadId,
    runId: fixture.owner.runId,
    action: {
      type: "worker_finished",
      taskId: task.taskId,
      launchCommandId: task.launchCommandId,
      workerThreadId: task.workerThreadId,
      workerRunId: fixture.workerRun.id,
      workerAttemptId: fixture.workerAttemptId,
      ...overrides,
    },
  });
  const readController = () =>
    projections
      .getThreadRecords(fixture.owner.threadId, ["runs"], { runIds: [fixture.owner.runId] })
      .pipe(Effect.map((rows) => rows.runs[0]!));
  return { ...fixture, commands, projections, sink, finish, readController };
});

const awaitAutomaticSettlement = (
  threadId: ThreadId,
  taskId: NodeId,
  status: "completed" | "failed",
) =>
  Effect.gen(function* () {
    const commands = yield* Orchestrator.OrchestratorV2;
    yield* commands.streamStoredEventsFrom({ threadId, afterSequence: 0 }).pipe(
      Stream.filter(
        (stored) =>
          stored.event.type === "multi-agent.controller.updated" &&
          stored.event.payload.state.tasks.some(
            (task) => task.taskId === taskId && task.status === status,
          ),
      ),
      Stream.take(1),
      Stream.runDrain,
    );
  });

it.effect.each(["completed", "failed", "interrupted", "cancelled"] as const)(
  "automatically settles a committed terminal %s worker event",
  (status) =>
    Effect.gen(function* () {
      const f = yield* seedWorkerSettlement(`automatic:${status}`, status, true);
      yield* awaitAutomaticSettlement(
        f.owner.threadId,
        f.owner.identities[0]!.taskId,
        status === "completed" ? "completed" : "failed",
      );
      const task = (yield* f.readController()).multiAgentController!.tasks[0]!;
      assert.equal(task.status, status === "completed" ? "completed" : "failed");
      assert.deepEqual("result" in task ? task.result : undefined, {
        workerThreadId: f.workerRun.threadId,
        workerRunId: f.workerRun.id,
      });
      assert.isDefined(task.submissionCommitment);
      const timeline = yield* f.projections.getThreadRecords(f.owner.threadId, ["turnItems"]);
      const notices = timeline.turnItems.filter(
        (item) => item.type === "notification" && item.title === "Worker result",
      );
      assert.lengthOf(notices, 1);
      const notice = notices[0]!;
      if (notice.type === "notification") {
        assert.equal(
          notice.outcome,
          status === "completed" ? "completed" : status === "failed" ? "failed" : "cancelled",
        );
        assert.include(notice.detail!, f.workerRun.id);
      }
    }).pipe(Effect.provide(layers)),
);

it.effect(
  "waiting workers do not settle or consume a completion receipt before checkpoint finalization",
  () =>
    Effect.gen(function* () {
      const f = yield* seedWorkerSettlement("automatic:waiting", "waiting", true);
      yield* f.commands.recoverDelegatedTasks;
      assert.equal((yield* f.readController()).multiAgentController!.tasks[0]!.status, "running");
      const worker = yield* f.projections.getThreadRecords(f.workerRun.threadId, [
        "runs",
        "attempts",
      ]);
      const completion = Orchestrator.controlledWorkerFinishedCommand(
        worker.runs[0]!.controlledWorkerOwnership!,
      );
      const receipts = yield* CommandReceiptStore.CommandReceiptStoreV2;
      assert.isTrue(Option.isNone(yield* receipts.getByCommandId(completion.commandId)));
      yield* f.sink.write({
        events: [
          {
            id: EventId.make("automatic:checkpoint-completed-run"),
            threadId: f.workerRun.threadId,
            runId: f.workerRun.id,
            type: "run.updated",
            occurredAt: now,
            payload: { ...worker.runs[0]!, status: "completed", completedAt: now },
          },
          {
            id: EventId.make("automatic:checkpoint-completed-attempt"),
            threadId: f.workerRun.threadId,
            runId: f.workerRun.id,
            type: "run-attempt.updated",
            occurredAt: now,
            payload: { ...worker.attempts[0]!, status: "completed", completedAt: now },
          },
        ],
      });
      yield* awaitAutomaticSettlement(f.owner.threadId, f.owner.identities[0]!.taskId, "completed");
      assert.isTrue(Option.isSome(yield* receipts.getByCommandId(completion.commandId)));
    }).pipe(Effect.provide(layers)),
);

it.effect("duplicate terminal notifications leave controller state and revision unchanged", () =>
  Effect.gen(function* () {
    const first = yield* seedWorkerSettlement("automatic:duplicate", "completed", true);
    yield* awaitAutomaticSettlement(
      first.owner.threadId,
      first.owner.identities[0]!.taskId,
      "completed",
    );
    yield* first.commands
      .streamStoredEventsFrom({ threadId: first.owner.threadId, afterSequence: 0 })
      .pipe(
        Stream.filter(
          (stored) =>
            stored.event.type === "multi-agent.controller.updated" &&
            (stored.event.payload.state.reviews?.length ?? 0) === 1,
        ),
        Stream.take(1),
        Stream.runDrain,
      );
    const before = yield* first.readController();
    const fence = yield* seedWorkerSettlement("automatic:duplicate-fence", "waiting");
    const firstRows = yield* first.projections.getThreadRecords(first.workerRun.threadId, ["runs"]);
    const fenceRows = yield* fence.projections.getThreadRecords(fence.workerRun.threadId, [
      "runs",
      "attempts",
    ]);
    // The existing terminal listener processes these committed events in order;
    // the second task's settlement is a deterministic barrier after the duplicate.
    yield* first.sink.write({
      events: [
        {
          id: EventId.make("automatic:duplicate-notification"),
          type: "run.updated",
          threadId: first.workerRun.threadId,
          runId: first.workerRun.id,
          occurredAt: now,
          payload: firstRows.runs[0]!,
        },
        {
          id: EventId.make("automatic:fence-run"),
          type: "run.updated",
          threadId: fence.workerRun.threadId,
          runId: fence.workerRun.id,
          occurredAt: now,
          payload: { ...fenceRows.runs[0]!, status: "completed", completedAt: now },
        },
        {
          id: EventId.make("automatic:fence-attempt"),
          type: "run-attempt.updated",
          threadId: fence.workerRun.threadId,
          runId: fence.workerRun.id,
          occurredAt: now,
          payload: { ...fenceRows.attempts[0]!, status: "completed", completedAt: now },
        },
      ],
    });
    yield* awaitAutomaticSettlement(
      fence.owner.threadId,
      fence.owner.identities[0]!.taskId,
      "completed",
    );
    const after = yield* first.readController();
    assert.deepEqual(after.multiAgentController, before.multiAgentController);
    assert.equal(after.multiAgentControllerRevision, before.multiAgentControllerRevision);
  }).pipe(Effect.provide(layers)),
);

it.effect("startup reconciliation settles missed completion after Stop and stays idempotent", () =>
  Effect.gen(function* () {
    const f = yield* seedWorkerSettlement("automatic:missed");
    yield* f.commands.dispatch({
      type: "multi-agent.controller.command",
      commandId: CommandId.make("automatic:missed:stop"),
      threadId: f.owner.threadId,
      runId: f.owner.runId,
      action: { type: "stop_coordinator" },
    });
    const stopped = yield* f.readController();
    yield* f.commands.recoverDelegatedTasks;
    const settled = yield* f.readController();
    assert.equal(settled.multiAgentController!.tasks[0]!.status, "completed");
    assert.equal(
      settled.multiAgentController!.coordinatorStoppedAt,
      stopped.multiAgentController!.coordinatorStoppedAt,
    );
    yield* f.commands.recoverDelegatedTasks;
    assert.equal(
      (yield* f.readController()).multiAgentControllerRevision,
      settled.multiAgentControllerRevision,
    );
  }).pipe(Effect.provide(layers)),
);

it.effect("database reopen recovers a terminal owned worker missed before shutdown", () =>
  Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem;
    const path = yield* Path.Path;
    const directory = yield* fs.makeTempDirectoryScoped({ prefix: "t3-auto-settlement-" });
    const filename = path.join(directory, "state.sqlite");
    const disk = () =>
      makeLayers(
        SqlitePersistence.layerFromPath(filename).pipe(
          Layer.provide(NodeServices.layer),
          Layer.orDie,
        ),
      );
    const identity = yield* Effect.scoped(
      Effect.gen(function* () {
        const f = yield* seedWorkerSettlement("automatic:reopen");
        assert.equal((yield* f.readController()).multiAgentController!.tasks[0]!.status, "running");
        return { threadId: f.owner.threadId, runId: f.owner.runId };
      }).pipe(Effect.provide(disk())),
    );
    yield* Effect.scoped(
      Effect.gen(function* () {
        const commands = yield* Orchestrator.OrchestratorV2;
        yield* commands.recoverDelegatedTasks;
        const projections = yield* ProjectionStore.ProjectionStoreV2;
        const read = () =>
          projections.getThreadRecords(identity.threadId, ["runs"], { runIds: [identity.runId] });
        const settled = (yield* read()).runs[0]!;
        assert.equal(settled.multiAgentController!.tasks[0]!.status, "completed");
        yield* commands.recoverDelegatedTasks;
        assert.equal(
          (yield* read()).runs[0]!.multiAgentControllerRevision,
          settled.multiAgentControllerRevision,
        );
      }).pipe(Effect.provide(disk())),
    );
  }).pipe(Effect.provide(NodeServices.layer)),
);

it.effect.each(["completed", "failed", "interrupted", "cancelled"] as const)(
  "settles the exact worker from persisted %s status without creating execution effects",
  (status) =>
    Effect.gen(function* () {
      const fixture = yield* seedWorkerSettlement(`finish:${status}`, status);
      const before = yield* fixture.readController();
      const command = fixture.finish(`${status}:settle`);
      yield* fixture.commands.dispatch(command);
      const after = yield* fixture.readController();
      const task = after.multiAgentController!.tasks[0]!;
      assert.equal(task.status, status === "completed" ? "completed" : "failed");
      assert.deepEqual("result" in task ? task.result : undefined, {
        workerThreadId: fixture.workerRun.threadId,
        workerRunId: fixture.workerRun.id,
      });
      assert.deepEqual(
        task.submissionCommitment,
        before.multiAgentController!.tasks[0]!.submissionCommitment,
      );
      const receipts = yield* CommandReceiptStore.CommandReceiptStoreV2;
      assert.isTrue(Option.isSome(yield* receipts.getByCommandId(command.commandId)));
      assert.isEmpty(
        yield* EffectOutbox.EffectOutboxV2.pipe(
          Effect.flatMap((outbox) => outbox.listByCommandId(command.commandId)),
          Effect.provide(EffectOutbox.layer.pipe(Layer.provide(database))),
        ),
      );
      assert.lengthOf(
        (yield* fixture.projections.getThreadRecords(fixture.workerRun.threadId, ["runs"])).runs,
        1,
      );
    }).pipe(Effect.provide(layers)),
);

it.effect.each(["preparing", "queued", "starting", "running", "waiting", "rolled_back"] as const)(
  "rejects nonterminal or rolled-back worker history: %s",
  (status) =>
    Effect.gen(function* () {
      const fixture = yield* seedWorkerSettlement(`nonterminal:${status}`, status);
      const before = yield* fixture.readController();
      assert.equal(
        (yield* fixture.commands.dispatch(fixture.finish(`${status}:denied`)).pipe(Effect.exit))
          ._tag,
        "Failure",
      );
      assert.deepEqual(yield* fixture.readController(), before);
    }).pipe(Effect.provide(layers)),
);

it.effect.each([
  "taskId",
  "launchCommandId",
  "workerThreadId",
  "workerRunId",
  "workerAttemptId",
] as const)("rejects a mismatched settlement %s", (field) =>
  Effect.gen(function* () {
    const fixture = yield* seedWorkerSettlement(`mismatch:${field}`);
    const before = yield* fixture.readController();
    const valid = fixture.finish(`${field}:denied`);
    const invalid = {
      ...valid,
      action: { ...valid.action, [field]: "wrong-identity" },
    } as ControllerCommand;
    assert.equal((yield* fixture.commands.dispatch(invalid).pipe(Effect.exit))._tag, "Failure");
    assert.deepEqual(yield* fixture.readController(), before);
  }).pipe(Effect.provide(layers)),
);

it.effect(
  "settlement validates coordinator identity and persisted initial attempt/provider ownership",
  () =>
    Effect.gen(function* () {
      const fixture = yield* seedWorkerSettlement("stale-settlement");
      const before = yield* fixture.readController();
      for (const wrong of [
        { ...fixture.finish("wrong-coordinator"), threadId: fixture.workerRun.threadId },
        { ...fixture.finish("wrong-coordinator-run"), runId: fixture.workerRun.id },
      ])
        assert.equal((yield* fixture.commands.dispatch(wrong).pipe(Effect.exit))._tag, "Failure");
      yield* fixture.sink.write({
        events: [
          {
            id: EventId.make("stale-binding"),
            type: "provider-thread.updated",
            threadId: fixture.workerRun.threadId,
            occurredAt: now,
            payload: {
              ...fixture.providerThread,
              providerSessionId: ProviderSessionId.make("later-session"),
            },
          },
        ],
      });
      assert.equal(
        (yield* fixture.commands.dispatch(fixture.finish("stale-binding:finish")).pipe(Effect.exit))
          ._tag,
        "Failure",
      );
      assert.deepEqual(yield* fixture.readController(), before);
    }).pipe(Effect.provide(layers)),
);

it.effect(
  "duplicate settlement, including concurrent commands and receipt replay, changes state exactly once",
  () =>
    Effect.gen(function* () {
      const fixture = yield* seedWorkerSettlement("duplicate-finish");
      const before = yield* fixture.readController();
      const first = fixture.finish("finish:one");
      yield* Effect.all(
        [fixture.commands.dispatch(first), fixture.commands.dispatch(fixture.finish("finish:two"))],
        { concurrency: 2 },
      );
      yield* fixture.commands
        .streamStoredEventsFrom({ threadId: fixture.owner.threadId, afterSequence: 0 })
        .pipe(
          Stream.filter(
            (stored) =>
              stored.event.type === "multi-agent.controller.updated" &&
              (stored.event.payload.state.reviews?.length ?? 0) === 1,
          ),
          Stream.take(1),
          Stream.runDrain,
        );
      const after = yield* fixture.readController();
      assert.equal(after.multiAgentControllerRevision, before.multiAgentControllerRevision! + 2);
      yield* fixture.commands.dispatch(first);
      assert.deepEqual(yield* fixture.readController(), after);
      // A contradictory later outcome must not overwrite the saved successful result.
      const worker = (yield* fixture.projections.getThreadRecords(fixture.workerRun.threadId, [
        "runs",
      ])).runs[0]!;
      yield* fixture.sink.write({
        events: [
          {
            id: EventId.make("conflicting-outcome"),
            type: "run.updated",
            threadId: worker.threadId,
            runId: worker.id,
            occurredAt: now,
            payload: { ...worker, status: "failed" },
          },
        ],
      });
      assert.equal(
        (yield* fixture.commands.dispatch(fixture.finish("finish:conflict")).pipe(Effect.exit))
          ._tag,
        "Failure",
      );
      assert.deepEqual(yield* fixture.readController(), after);
    }).pipe(Effect.provide(layers)),
);

it.effect(
  "completion after Stop preserves the barrier and commitment through SQL reload and event replay",
  () =>
    Effect.gen(function* () {
      const fixture = yield* seedWorkerSettlement("finish-after-stop");
      yield* fixture.commands.dispatch(fixture.stop("finish:stop"));
      const stopped = yield* fixture.readController();
      yield* fixture.commands.dispatch(fixture.finish("finish:after-stop"));
      const finished = yield* fixture.readController();
      assert.deepEqual(
        finished.multiAgentController?.coordinatorStoppedAt,
        stopped.multiAgentController?.coordinatorStoppedAt,
      );
      assert.deepEqual(
        finished.multiAgentController?.tasks[0]?.submissionCommitment,
        stopped.multiAgentController?.tasks[0]?.submissionCommitment,
      );
      assert.equal(finished.multiAgentController?.tasks[0]?.status, "completed");
      const events = yield* EventStore.EventStoreV2.pipe(
        Effect.flatMap((store) => Stream.runCollect(store.read())),
      );
      for (const layer of [ProjectionStore.layer, ProjectionStore.layerMemory]) {
        yield* Effect.gen(function* () {
          const restored = yield* ProjectionStore.ProjectionStoreV2;
          if (layer === ProjectionStore.layerMemory)
            for (const stored of events) yield* restored.apply(stored.event);
          assert.deepEqual(
            (yield* restored.getThreadRecords(fixture.owner.threadId, ["runs"])).runs[0]!
              .multiAgentController,
            finished.multiAgentController,
          );
        }).pipe(Effect.provide(layer.pipe(Layer.provide(database))));
      }
    }).pipe(Effect.provide(layers)),
);

it.effect(
  "Stop targeting the original controller interrupts its pending review and prevents recovery wakes",
  () =>
    Effect.gen(function* () {
      const fixture = yield* seedWorkerSettlement("stop-pending-review");
      yield* fixture.commands.dispatch(fixture.finish("review:finish"));
      yield* fixture.commands
        .streamStoredEventsFrom({ threadId: fixture.owner.threadId, afterSequence: 0 })
        .pipe(
          Stream.filter(
            (stored) =>
              stored.event.type === "multi-agent.controller.updated" &&
              (stored.event.payload.state.reviews?.length ?? 0) === 1,
          ),
          Stream.take(1),
          Stream.runDrain,
        );
      const before = yield* fixture.readController();
      const stop = {
        type: "run.interrupt" as const,
        commandId: CommandId.make("review:stop"),
        threadId: before.threadId,
        runId: before.id,
        holdQueue: true,
      };
      yield* fixture.commands.dispatch(stop);
      yield* fixture.commands.dispatch(stop);
      yield* fixture.commands.recoverDelegatedTasks;
      const after = yield* fixture.readController();
      assert.isDefined(after.multiAgentController!.coordinatorStoppedAt);
      assert.deepEqual(after.multiAgentController!.tasks, before.multiAgentController!.tasks);
      assert.isUndefined(after.multiAgentController!.reviews![0]!.submissionClaimedAt);
      const records = yield* fixture.projections.getThreadRecords(before.threadId, ["runs"]);
      assert.lengthOf(records.runs, 2);
      assert.equal(records.runs[1]!.status, "interrupted");
    }).pipe(Effect.provide(layers)),
);

it.effect("worker result settlement survives database reopen and remains idempotent", () =>
  Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem;
    const path = yield* Path.Path;
    const directory = yield* fs.makeTempDirectoryScoped({ prefix: "t3-worker-result-" });
    const filename = path.join(directory, "state.sqlite");
    const expected = yield* Effect.scoped(
      Effect.gen(function* () {
        const fixture = yield* seedWorkerSettlement("finish-reopen", "failed");
        yield* fixture.commands.dispatch(fixture.finish("finish:before-restart"));
        yield* fixture.commands
          .streamStoredEventsFrom({ threadId: fixture.owner.threadId, afterSequence: 0 })
          .pipe(
            Stream.filter(
              (stored) =>
                stored.event.type === "multi-agent.controller.updated" &&
                (stored.event.payload.state.reviews?.length ?? 0) === 1,
            ),
            Stream.take(1),
            Stream.runDrain,
          );
        const current = yield* fixture.readController();
        const review = current.multiAgentController!.reviews![0]!;
        yield* fixture.sink.write({
          events: [
            {
              id: EventId.make("review:claim-before-restart"),
              type: "multi-agent.controller.updated",
              threadId: current.threadId,
              runId: current.id,
              occurredAt: now,
              payload: {
                expectedRevision: current.multiAgentControllerRevision!,
                revision: current.multiAgentControllerRevision! + 1,
                state: {
                  ...current.multiAgentController!,
                  reviews: [{ ...review, submissionClaimedAt: DateTime.formatIso(now) }],
                },
              },
            },
          ],
        });
        return {
          command: fixture.finish("finish:after-restart"),
          run: yield* fixture.readController(),
        };
      }).pipe(
        Effect.provide(
          makeLayers(
            SqlitePersistence.layerFromPath(filename).pipe(
              Layer.provide(NodeServices.layer),
              Layer.orDie,
            ),
          ),
        ),
      ),
    );
    yield* Effect.scoped(
      Effect.gen(function* () {
        const commands = yield* Orchestrator.OrchestratorV2;
        const projections = yield* ProjectionStore.ProjectionStoreV2;
        yield* commands.dispatch(expected.command);
        const actual = (yield* projections.getThreadRecords(expected.run.threadId, ["runs"]))
          .runs[0]!;
        assert.deepEqual(actual.multiAgentController, expected.run.multiAgentController);
        assert.equal(
          actual.multiAgentControllerRevision,
          expected.run.multiAgentControllerRevision,
        );
        const review = actual.multiAgentController!.reviews![0]!;
        assert.isDefined(review.submissionClaimedAt);
        const records = yield* projections.getThreadRecords(actual.threadId, ["runs"]);
        const reviewRun = records.runs.find((run) => run.id === review.runId)!;
        assert.equal(reviewRun.multiAgentReview?.coordinatorRunId, actual.id);
        assert.isUndefined(reviewRun.multiAgentController);
        yield* commands.recoverDelegatedTasks;
        assert.lengthOf((yield* projections.getThreadRecords(actual.threadId, ["runs"])).runs, 2);
      }).pipe(
        Effect.provide(
          makeLayers(
            SqlitePersistence.layerFromPath(filename).pipe(
              Layer.provide(NodeServices.layer),
              Layer.orDie,
            ),
          ),
        ),
      ),
    );
  }).pipe(Effect.provide(NodeServices.layer)),
);

it.effect.each([
  "stale-active-attempt",
  "retry-attempt",
  "nonterminal-attempt",
  "inconsistent-success",
  "missing-terminal-time",
] as const)("rejects conflicting persisted worker/attempt state: %s", (kind) =>
  Effect.gen(function* () {
    const fixture = yield* seedWorkerSettlement(`invalid-record:${kind}`);
    const before = yield* fixture.readController();
    const worker = yield* fixture.projections.getThreadRecords(fixture.workerRun.threadId, [
      "runs",
      "attempts",
    ]);
    if (kind === "stale-active-attempt" || kind === "missing-terminal-time") {
      yield* fixture.sink.write({
        events: [
          {
            id: EventId.make(`${kind}:update`),
            type: "run.updated",
            threadId: fixture.workerRun.threadId,
            runId: fixture.workerRun.id,
            occurredAt: now,
            payload: {
              ...worker.runs[0]!,
              ...(kind === "stale-active-attempt"
                ? { activeAttemptId: RunAttemptId.make("replacement-attempt") }
                : { completedAt: null }),
            },
          },
        ],
      });
    } else {
      yield* fixture.sink.write({
        events: [
          {
            id: EventId.make(`${kind}:update`),
            type: "run-attempt.updated",
            threadId: fixture.workerRun.threadId,
            runId: fixture.workerRun.id,
            occurredAt: now,
            payload: {
              ...worker.attempts[0]!,
              ...(kind === "retry-attempt"
                ? { reason: "retry" as const, attemptOrdinal: 2 }
                : kind === "nonterminal-attempt"
                  ? { status: "running" as const, completedAt: null }
                  : { status: "failed" as const }),
            },
          },
        ],
      });
    }
    assert.equal(
      (yield* fixture.commands.dispatch(fixture.finish(`${kind}:settle`)).pipe(Effect.exit))._tag,
      "Failure",
    );
    assert.deepEqual(yield* fixture.readController(), before);
  }).pipe(Effect.provide(layers)),
);

it.effect(
  "settlement keeps later manual runs independent and does not enroll ordinary coordinator runs",
  () =>
    Effect.gen(function* () {
      const fixture = yield* seedWorkerSettlement("manual-follow-up");
      const original = (yield* fixture.projections.getThreadRecords(fixture.workerRun.threadId, [
        "runs",
      ])).runs[0]!;
      const { controlledWorkerOwnership: _ownership, ...manualFields } = original;
      const manual = {
        ...manualFields,
        id: RunId.make("manual:run"),
        ordinal: 2,
        userMessageId: MessageId.make("manual:message"),
      };
      yield* fixture.sink.write({
        events: [
          {
            id: EventId.make("manual:create"),
            type: "run.created",
            threadId: manual.threadId,
            runId: manual.id,
            occurredAt: now,
            payload: manual,
          },
        ],
      });
      assert.equal(
        (yield* fixture.commands
          .dispatch(fixture.finish("manual:wrong-run", { workerRunId: manual.id }))
          .pipe(Effect.exit))._tag,
        "Failure",
      );
      yield* fixture.commands.dispatch(fixture.finish("manual:initial-completed"));
      assert.deepEqual(
        (yield* fixture.projections.getThreadRecords(manual.threadId, ["runs"])).runs.find(
          (run) => run.id === manual.id,
        ),
        manual,
      );
      const ordinary = yield* seed("ordinary-no-controller");
      assert.equal(
        (yield* fixture.commands
          .dispatch({
            ...fixture.finish("ordinary:denied"),
            threadId: ordinary.threadId,
            runId: ordinary.runId,
          })
          .pipe(Effect.exit))._tag,
        "Failure",
      );
      assert.isUndefined(
        (yield* fixture.projections.getThreadRecords(ordinary.threadId, ["runs"])).runs[0]!
          .multiAgentController,
      );
    }).pipe(Effect.provide(layers)),
);

const seedSessionHistory = Effect.fn("seedSessionHistory")(function* (key: string) {
  const fixture = yield* seed(key);
  const sink = yield* EventSink.EventSinkV2;
  const projections = yield* ProjectionStore.ProjectionStoreV2;
  const sessionId = ProviderSessionId.make(`historical-session:${key}`);
  assert.isFalse(yield* projections.hasProviderSessionHistory(sessionId));
  yield* sink.write({
    events: [
      {
        id: EventId.make(`${key}:attach`),
        threadId: fixture.threadId,
        type: "provider-session.attached",
        occurredAt: now,
        payload: {
          id: sessionId,
          driver: provider.driver,
          providerInstanceId: instanceId,
          status: "ready",
          cwd: process.cwd(),
          model: selection.model,
          capabilities: CodexProviderCapabilitiesV2,
          createdAt: now,
          updatedAt: now,
          lastError: null,
        },
      },
      {
        id: EventId.make(`${key}:detach`),
        threadId: fixture.threadId,
        type: "provider-session.detached",
        occurredAt: now,
        payload: { providerSessionId: sessionId, detachedAt: now },
      },
    ],
  });
  assert.isEmpty(
    (yield* projections.getThreadRecords(fixture.threadId, ["providerSessions"])).providerSessions,
  );
  assert.isTrue(yield* projections.hasProviderSessionHistory(sessionId));
  return { ...fixture, sessionId };
});

it.effect("global runtime history survives detach, SQL reload and projection replay", () =>
  Effect.gen(function* () {
    const fixture = yield* seedSessionHistory("runtime-history");
    const events = yield* EventStore.EventStoreV2.pipe(
      Effect.flatMap((store) => Stream.runCollect(store.read({ threadId: fixture.threadId }))),
    );
    for (const layer of [ProjectionStore.layer, ProjectionStore.layerMemory]) {
      yield* Effect.gen(function* () {
        const restored = yield* ProjectionStore.ProjectionStoreV2;
        if (layer === ProjectionStore.layerMemory)
          for (const stored of events) yield* restored.apply(stored.event);
        assert.isTrue(yield* restored.hasProviderSessionHistory(fixture.sessionId));
        assert.isFalse(
          yield* restored.hasProviderSessionHistory(
            ProviderSessionId.make("never-registered-session"),
          ),
        );
        assert.isEmpty(
          (yield* restored.getThreadRecords(fixture.threadId, ["providerSessions"]))
            .providerSessions,
        );
      }).pipe(Effect.provide(layer.pipe(Layer.provide(database))));
    }
  }).pipe(Effect.provide(layers)),
);

it.effect(
  "runtime history remains queryable after database reopen with no live thread binding",
  () =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const directory = yield* fs.makeTempDirectoryScoped({ prefix: "t3-runtime-history-" });
      const filename = path.join(directory, "state.sqlite");
      const fixture = yield* Effect.scoped(
        seedSessionHistory("history-restart").pipe(
          Effect.provide(
            makeLayers(
              SqlitePersistence.layerFromPath(filename).pipe(
                Layer.provide(NodeServices.layer),
                Layer.orDie,
              ),
            ),
          ),
        ),
      );
      yield* Effect.scoped(
        Effect.gen(function* () {
          const projections = yield* ProjectionStore.ProjectionStoreV2;
          assert.isTrue(yield* projections.hasProviderSessionHistory(fixture.sessionId));
          assert.isEmpty(
            (yield* projections.getThreadRecords(fixture.threadId, ["providerSessions"]))
              .providerSessions,
          );
        }).pipe(
          Effect.provide(
            ProjectionStore.layer.pipe(
              Layer.provide(SqlitePersistence.layerFromPath(filename)),
              Layer.provide(NodeServices.layer),
            ),
          ),
        ),
      );
    }).pipe(Effect.provide(NodeServices.layer)),
);

it.effect("reopens durable trigger provenance without a migration or manual default", () =>
  Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem;
    const path = yield* Path.Path;
    const directory = yield* fs.makeTempDirectoryScoped({ prefix: "t3-trigger-reopen-" });
    const filename = path.join(directory, "state.sqlite");
    const trigger: NewRunTrigger = {
      type: "automatic",
      kind: "delegated_completion",
      provenance: {
        type: "ambiguous",
        candidates: [{ threadId: ThreadId.make("source"), runId: RunId.make("source-run") }],
      },
    };
    const expected = yield* Effect.scoped(
      Effect.gen(function* () {
        const fixture = yield* seed("disk-trigger", "queued", trigger);
        return { threadId: fixture.threadId, runId: fixture.runId };
      }).pipe(
        Effect.provide(
          makeLayers(
            SqlitePersistence.layerFromPath(filename).pipe(
              Layer.provide(NodeServices.layer),
              Layer.orDie,
            ),
          ),
        ),
      ),
    );
    yield* Effect.scoped(
      Effect.gen(function* () {
        const projections = yield* ProjectionStore.ProjectionStoreV2;
        const run = (yield* projections.getThreadProjection(expected.threadId)).runs.find(
          (row) => row.id === expected.runId,
        )!;
        assert.equal(run.status, "queued");
        assert.deepEqual(run.newRunTrigger, trigger);
      }).pipe(
        Effect.provide(
          ProjectionStore.layer.pipe(
            Layer.provide(SqlitePersistence.layerFromPath(filename)),
            Layer.provide(NodeServices.layer),
          ),
        ),
      ),
    );
  }).pipe(Effect.provide(NodeServices.layer)),
);

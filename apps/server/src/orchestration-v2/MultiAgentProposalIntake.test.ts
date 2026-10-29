import { assert, it } from "@effect/vitest";
import {
  CommandId,
  EventId,
  MessageId,
  ProjectId,
  ProviderDriverKind,
  ProviderInstanceId,
  ProviderTurnId,
  RunAttemptId,
  RunId,
  ThreadId,
  TurnItemId,
  type OrchestrationV2ServerCommand,
  type ServerProvider,
  NodeId,
} from "@t3tools/contracts";
import { admitMultiAgentProposal } from "@t3tools/shared/multiAgentController";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";
import * as NodeServices from "@effect/platform-node/NodeServices";
import * as FileSystem from "effect/FileSystem";
import * as Path from "effect/Path";
import * as ProviderAdapter from "@t3tools/provider-core/server/ProviderAdapter";
import * as SqlitePersistence from "../persistence/Sqlite.ts";
import * as EventSink from "./EventSink.ts";
import * as EventStore from "./EventStore.ts";
import * as EffectOutbox from "./EffectOutbox.ts";
import * as Orchestrator from "./Orchestrator.ts";
import * as ProjectionStore from "./ProjectionStore.ts";
import * as ProviderAdapterRegistry from "./ProviderAdapterRegistry.ts";
import * as ProviderReplayHarness from "./testkit/ProviderReplayHarness.ts";
import { CodexProviderCapabilitiesV2 } from "./Adapters/CodexAdapterV2.ts";

const instanceId = ProviderInstanceId.make("codex");
const selection = { instanceId, model: "proposal-test-model" };
const provider: ServerProvider = {
  instanceId,
  driver: ProviderDriverKind.make("codex"),
  enabled: true,
  installed: true,
  status: "ready",
  version: null,
  auth: { status: "authenticated" },
  checkedAt: "2026-10-14T00:00:00Z",
  slashCommands: [],
  skills: [],
  models: [
    {
      slug: selection.model,
      name: "Scripted model",
      isCustom: true,
      capabilities: {
        optionDescriptors: [
          {
            id: "scripted-option",
            label: "Option",
            type: "select",
            options: [{ id: "valid", label: "Valid" }],
          },
        ],
      },
    },
  ],
};
const workerSelection = { ...selection, options: [{ id: "scripted-option", value: "valid" }] };
const adapter = {
  instanceId,
  driver: provider.driver,
  getCapabilities: () => Effect.succeed(CodexProviderCapabilitiesV2),
  planSelectionTransition: () => Effect.succeed({ type: "apply_on_next_turn" as const }),
  openSession: () => Effect.die("Proposal intake must never invoke a provider"),
} as ProviderAdapter.ProviderAdapterV2["Service"];
const database = SqlitePersistence.layerMemory;
const makeLayers = (
  catalog: readonly ServerProvider[] = [provider],
  databaseLayer: typeof database = database,
) =>
  Layer.mergeAll(
    ProviderReplayHarness.layerWithRegistry(
      { name: "proposal-intake" },
      ProviderAdapterRegistry.layerFromAdapters([adapter]),
      {
        databaseLayer,
        runEffectWorker: false,
        providerCatalog: catalog,
      },
    ),
    ProjectionStore.layer.pipe(Layer.provide(databaseLayer)),
    EffectOutbox.layer.pipe(Layer.provide(databaseLayer)),
    EventStore.layer.pipe(Layer.provide(databaseLayer)),
  );
const layers = makeLayers();
const djTasks = [
  { key: "frontend", role: "worker", prompt: "Build a DJ song-selection interface." },
  { key: "backend", role: "worker", prompt: "Build a song-recommendation API." },
];
type ControllerCommand = Extract<
  OrchestrationV2ServerCommand,
  { type: "multi-agent.controller.command" }
>;
const encodeJson = Schema.encodeSync(Schema.fromJsonString(Schema.Unknown));

const fixture = Effect.fn("proposalIntakeFixture")(function* (key: string, worker = false) {
  const commands = yield* Orchestrator.OrchestratorV2;
  const projections = yield* ProjectionStore.ProjectionStoreV2;
  const sink = yield* EventSink.EventSinkV2;
  const threadId = ThreadId.make(`proposal:${key}`);
  yield* commands.dispatch({
    type: "thread.create",
    commandId: CommandId.make(`${key}:thread`),
    threadId,
    projectId: ProjectId.make("proposal-project"),
    title: "DJ coordinator",
    modelSelection: selection,
    runtimeMode: "full-access",
    interactionMode: "default",
    branch: null,
    worktreePath: null,
    ...(worker ? { coordinatorThreadId: ThreadId.make("parent-coordinator") } : {}),
    createdBy: "user",
    creationSource: "web",
  });
  yield* commands.dispatch({
    type: "message.dispatch",
    commandId: CommandId.make(`${key}:goal`),
    threadId,
    messageId: MessageId.make(`${key}:goal`),
    text: "Build the DJ app",
    attachments: [],
    multiAgent: worker ? { mode: "off" } : { mode: "custom", roles: { worker: workerSelection } },
    dispatchMode: { type: "defer_start" },
    createdBy: "user",
    creationSource: "web",
  });
  const projection = yield* projections.getThreadProjection(threadId);
  const at = yield* DateTime.now;
  const run = { ...projection.runs[0]!, status: "running" as const, startedAt: at };
  const attempt = { ...projection.attempts[0]!, status: "running" as const, startedAt: at };
  const providerThread = projection.providerThreads.find(
    (thread) => thread.id === run.providerThreadId,
  )!;
  const turn = {
    id: ProviderTurnId.make(`${key}:turn`),
    providerThreadId: providerThread.id,
    nodeId: run.rootNodeId!,
    runAttemptId: attempt.id,
    nativeTurnRef: null,
    ordinal: 1,
    status: "running" as const,
    startedAt: at,
    completedAt: null,
  };
  yield* sink.write({
    events: [
      {
        id: EventId.make(`${key}:running`),
        type: "run.updated",
        threadId,
        runId: run.id,
        occurredAt: at,
        payload: run,
      },
      {
        id: EventId.make(`${key}:attempt`),
        type: "run-attempt.updated",
        threadId,
        runId: run.id,
        occurredAt: at,
        payload: attempt,
      },
      {
        id: EventId.make(`${key}:turn`),
        type: "provider-turn.updated",
        threadId,
        runId: run.id,
        occurredAt: at,
        payload: turn,
      },
    ],
  });
  const offer = Effect.fn("persistProposalSource")(function* (
    source: string,
    proposal: unknown = { tasks: djTasks },
    raw?: string,
  ) {
    const text = raw ?? encodeJson(proposal);
    const messageId = MessageId.make(`${key}:message:${source}`);
    const itemId = TurnItemId.make(`${key}:item:${source}`);
    const message = {
      id: messageId,
      threadId,
      runId: run.id,
      nodeId: run.rootNodeId,
      role: "assistant" as const,
      text,
      attachments: [],
      streaming: false,
      createdAt: at,
      updatedAt: at,
      createdBy: "agent" as const,
      creationSource: "provider" as const,
    };
    const item = {
      id: itemId,
      threadId,
      runId: run.id,
      nodeId: run.rootNodeId,
      providerThreadId: providerThread.id,
      providerTurnId: turn.id,
      nativeItemRef: null,
      parentItemId: null,
      ordinal: 2,
      status: "completed" as const,
      title: null,
      startedAt: at,
      completedAt: at,
      updatedAt: at,
      type: "assistant_message" as const,
      messageId,
      text,
      streaming: false,
    };
    yield* sink.write({
      events: [
        {
          id: EventId.make(`${key}:${source}:message`),
          type: "message.updated",
          threadId,
          runId: run.id,
          occurredAt: at,
          payload: message,
        },
        {
          id: EventId.make(`${key}:${source}:item`),
          type: "turn-item.updated",
          threadId,
          runId: run.id,
          occurredAt: at,
          payload: item,
        },
      ],
    });
    const command: ControllerCommand = {
      type: "multi-agent.controller.command",
      commandId: CommandId.make(`${key}:intake:${source}`),
      threadId,
      runId: run.id,
      action: { type: "admit_proposal", attemptId: attempt.id, proposalItemId: itemId },
    };
    return { command, item, message };
  });
  const read = () =>
    projections.getThreadProjection(threadId).pipe(Effect.map((current) => current.runs[0]!));
  return {
    commands,
    sink,
    projections,
    threadId,
    run,
    attempt,
    providerThread,
    turn,
    at,
    offer,
    read,
  };
});

it.effect(
  "admits DJ frontend/backend tasks from the exact initial execution without launches",
  () =>
    Effect.gen(function* () {
      const f = yield* fixture("dj");
      const source = yield* f.offer("two");
      const result = yield* f.commands.dispatch(source.command);
      const state = (yield* f.read()).multiAgentController!;
      assert.lengthOf(state.tasks, 2);
      assert.deepEqual(
        state.tasks.map((task) => task.key),
        ["frontend", "backend"],
      );
      for (const task of state.tasks) {
        assert.equal(task.status, "queued");
        assert.equal(task.proposalId, source.message.id);
        assert.equal(task.proposalAttemptId, f.attempt.id);
        assert.deepEqual(task.modelSelection, workerSelection);
        assert.isUndefined(task.submissionCommitment);
        assert.isNull(yield* f.projections.getThreadShell(task.workerThreadId));
      }
      assert.equal(state.coordinatorRunId, f.run.id);
      assert.equal(state.coordinatorAttemptId, f.attempt.id);
      assert.isTrue(
        result.storedEvents.every(
          (stored) => stored.event.type === "multi-agent.controller.updated",
        ),
      );
      const outbox = yield* EffectOutbox.EffectOutboxV2;
      const effects = yield* outbox.listByCommandId(source.command.commandId);
      assert.lengthOf(effects, 1);
      assert.equal(effects[0]!.request.type, "multi-agent.controller.schedule");
    }).pipe(Effect.provide(layers)),
);

it.effect.each([
  { name: "invalid role", proposal: { tasks: [{ ...djTasks[0], role: "planner" }] } },
  { name: "unassigned role", proposal: { tasks: [{ ...djTasks[0], role: "rescuer" }] } },
  { name: "empty task", proposal: { tasks: [{ ...djTasks[0], prompt: " " }] } },
  { name: "duplicate keys", proposal: { tasks: [djTasks[0], djTasks[0]] } },
  { name: "model override", proposal: { tasks: [{ ...djTasks[0], modelSelection: selection }] } },
  {
    name: "worker identity override",
    proposal: { tasks: [{ ...djTasks[0], workerThreadId: "chosen-worker" }] },
  },
  { name: "run override", proposal: { runId: "chosen-run", tasks: djTasks } },
  { name: "attempt override", proposal: { attemptId: "chosen-attempt", tasks: djTasks } },
  {
    name: "provider override",
    proposal: { tasks: [{ ...djTasks[0], providerInstanceId: "chosen-provider" }] },
  },
  {
    name: "permission override",
    proposal: { tasks: [{ ...djTasks[0], runtimeMode: "full-access" }] },
  },
  {
    name: "workspace override",
    proposal: { tasks: [{ ...djTasks[0], workspaceStrategy: { type: "root" } }] },
  },
  {
    name: "five tasks",
    proposal: {
      tasks: Array.from({ length: 5 }, (_, index) => ({ ...djTasks[0], key: `task-${index}` })),
    },
  },
])("rejects $name without task admission or effects", ({ name, proposal }) =>
  Effect.gen(function* () {
    const f = yield* fixture(name);
    const source = yield* f.offer(name, proposal);
    const before = yield* f.read();
    assert.equal((yield* f.commands.dispatch(source.command).pipe(Effect.exit))._tag, "Failure");
    assert.deepEqual((yield* f.read()).multiAgentController, before.multiAgentController);
    const outbox = yield* EffectOutbox.EffectOutboxV2;
    assert.lengthOf(yield* outbox.listByCommandId(source.command.commandId), 0);
  }).pipe(Effect.provide(layers)),
);

it.effect("revalidates the saved assignment against the current provider catalog", () => {
  const models = [...provider.models];
  return Effect.gen(function* () {
    const f = yield* fixture("unavailable");
    const source = yield* f.offer("removed-model");
    models.length = 0;
    assert.equal((yield* f.commands.dispatch(source.command).pipe(Effect.exit))._tag, "Failure");
    assert.lengthOf((yield* f.read()).multiAgentController!.tasks, 0);
  }).pipe(Effect.provide(makeLayers([{ ...provider, models }])));
});

it.effect("rejects saved options that are no longer valid in the model catalog", () => {
  const models = [...provider.models];
  return Effect.gen(function* () {
    const f = yield* fixture("invalid-current-options");
    const source = yield* f.offer("changed-option");
    models[0] = { ...models[0]!, capabilities: { optionDescriptors: [] } };
    assert.equal((yield* f.commands.dispatch(source.command).pipe(Effect.exit))._tag, "Failure");
    assert.lengthOf((yield* f.read()).multiAgentController!.tasks, 0);
  }).pipe(Effect.provide(makeLayers([{ ...provider, models }])));
});

it.effect("rejects malformed proposal JSON without interrupting or launching work", () =>
  Effect.gen(function* () {
    const f = yield* fixture("malformed-json");
    const source = yield* f.offer("malformed", undefined, '{"tasks":[');
    assert.equal((yield* f.commands.dispatch(source.command).pipe(Effect.exit))._tag, "Failure");
    assert.equal((yield* f.read()).status, "running");
    assert.lengthOf((yield* f.read()).multiAgentController!.tasks, 0);
  }).pipe(Effect.provide(layers)),
);

it.effect.each([
  "wrong-run",
  "wrong-thread",
  "stale-attempt",
  "replacement-attempt",
  "unrelated-turn",
  "unsupported-provider",
  "completed-run",
  "streaming-source",
  "forged-item-text",
] as const)("rejects non-authoritative origin: %s", (problem) =>
  Effect.gen(function* () {
    const f = yield* fixture(problem);
    const source = yield* f.offer("source");
    let command = source.command;
    if (problem === "wrong-run") command = { ...command, runId: RunId.make("unrelated-run") };
    if (problem === "wrong-thread") {
      const other = yield* fixture("other");
      command = { ...command, threadId: other.threadId, runId: other.run.id };
    }
    if (problem === "stale-attempt")
      command = {
        ...command,
        action: {
          type: "admit_proposal",
          attemptId: RunAttemptId.make("later-attempt"),
          proposalItemId: source.item.id,
        },
      };
    if (problem === "replacement-attempt")
      yield* f.sink.write({
        events: [
          {
            id: EventId.make("replacement-attempt"),
            type: "run.updated",
            threadId: f.threadId,
            runId: f.run.id,
            occurredAt: f.at,
            payload: { ...f.run, activeAttemptId: RunAttemptId.make("replacement-attempt") },
          },
        ],
      });
    if (problem === "unrelated-turn")
      yield* f.sink.write({
        events: [
          {
            id: EventId.make("unrelated-turn"),
            type: "provider-turn.updated",
            threadId: f.threadId,
            runId: f.run.id,
            occurredAt: f.at,
            payload: { ...f.turn, runAttemptId: RunAttemptId.make("unrelated-attempt") },
          },
        ],
      });
    if (problem === "unsupported-provider")
      yield* f.sink.write({
        events: [
          {
            id: EventId.make("unsupported-provider"),
            type: "provider-thread.updated",
            threadId: f.threadId,
            runId: f.run.id,
            occurredAt: f.at,
            payload: { ...f.providerThread, driver: ProviderDriverKind.make("claudeAgent") },
          },
        ],
      });
    if (problem === "completed-run")
      yield* f.sink.write({
        events: [
          {
            id: EventId.make("completed-run"),
            type: "run.updated",
            threadId: f.threadId,
            runId: f.run.id,
            occurredAt: f.at,
            payload: { ...f.run, status: "completed", completedAt: f.at },
          },
        ],
      });
    if (problem === "streaming-source" || problem === "forged-item-text")
      yield* f.sink.write({
        events: [
          {
            id: EventId.make(problem),
            type: "turn-item.updated",
            threadId: f.threadId,
            runId: f.run.id,
            occurredAt: f.at,
            payload: {
              ...source.item,
              ...(problem === "streaming-source"
                ? { streaming: true }
                : { text: "different source" }),
            },
          },
        ],
      });
    assert.equal((yield* f.commands.dispatch(command).pipe(Effect.exit))._tag, "Failure");
    assert.lengthOf((yield* f.read()).multiAgentController!.tasks, 0);
  }).pipe(Effect.provide(layers)),
);

it.effect("rejects a grouped Off worker as a proposal source", () =>
  Effect.gen(function* () {
    const f = yield* fixture("worker", true);
    const source = yield* f.offer("worker-source");
    assert.equal((yield* f.commands.dispatch(source.command).pipe(Effect.exit))._tag, "Failure");
    assert.isUndefined((yield* f.read()).multiAgentController);
  }).pipe(Effect.provide(layers)),
);

it.effect("Stop before a proposal closes intake without erasing the source", () =>
  Effect.gen(function* () {
    const f = yield* fixture("stop");
    yield* f.commands.dispatch({
      type: "multi-agent.controller.command",
      commandId: CommandId.make("stop:barrier"),
      threadId: f.threadId,
      runId: f.run.id,
      action: { type: "stop_coordinator" },
    });
    const before = yield* f.read();
    const source = yield* f.offer("late");
    assert.equal((yield* f.commands.dispatch(source.command).pipe(Effect.exit))._tag, "Failure");
    assert.deepEqual((yield* f.read()).multiAgentController, before.multiAgentController);
    assert.isNotNull(
      yield* f.projections.getTurnItem({ threadId: f.threadId, itemId: source.item.id }),
    );
  }).pipe(Effect.provide(layers)),
);

it.effect("deduplicates delivery and retains admitted identities through replay", () =>
  Effect.gen(function* () {
    const f = yield* fixture("duplicate");
    const source = yield* f.offer("source");
    const first = yield* f.commands.dispatch(source.command);
    const before = yield* f.read();
    assert.deepEqual(yield* f.commands.dispatch(source.command), first);
    assert.equal(
      (yield* f.read()).multiAgentControllerRevision,
      before.multiAgentControllerRevision,
    );
    const changedSource = {
      ...source.command,
      action: {
        type: "admit_proposal" as const,
        proposalItemId: source.item.id,
        attemptId: RunAttemptId.make("different-origin"),
      },
    };
    assert.equal((yield* f.commands.dispatch(changedSource).pipe(Effect.exit))._tag, "Failure");
    yield* f.commands.dispatch({
      ...source.command,
      commandId: CommandId.make("duplicate:new-delivery"),
    });
    assert.deepEqual((yield* f.read()).multiAgentController, before.multiAgentController);
    const events = yield* (yield* EventStore.EventStoreV2)
      .read({ threadId: f.threadId })
      .pipe(Stream.runCollect);
    yield* Effect.gen(function* () {
      const projections = yield* ProjectionStore.ProjectionStoreV2;
      for (const stored of events) yield* projections.apply(stored.event);
      assert.deepEqual(
        (yield* projections.getThreadProjection(f.threadId)).runs[0]!.multiAgentController,
        before.multiAgentController,
      );
    }).pipe(Effect.provide(ProjectionStore.layerMemory));
  }).pipe(Effect.provide(layers)),
);

it.effect("admits at most four tasks across separate proposals", () =>
  Effect.gen(function* () {
    const f = yield* fixture("limit");
    for (const key of ["first", "second"])
      yield* f.commands.dispatch((yield* f.offer(key)).command);
    const fifth = yield* f.offer("fifth", { tasks: [{ ...djTasks[0], key: "third-worker" }] });
    assert.equal((yield* f.commands.dispatch(fifth.command).pipe(Effect.exit))._tag, "Failure");
    const tasks = (yield* f.read()).multiAgentController!.tasks;
    assert.lengthOf(tasks, 4);
    assert.equal(new Set(tasks.map((task) => task.taskId)).size, 4);
    assert.isTrue(tasks.every((task) => task.status === "queued"));
  }).pipe(Effect.provide(layers)),
);

it.effect.each(["completed", "replacement-attempt"] as const)(
  "transactionally rejects an admission whose run became %s after validation",
  (change) =>
    Effect.gen(function* () {
      const f = yield* fixture(`commit-race:${change}`);
      const source = yield* f.offer("source");
      const run = yield* f.read();
      const decision = admitMultiAgentProposal({
        run,
        state: run.multiAgentController!,
        source: { proposalId: source.message.id, attemptId: f.attempt.id },
        proposal: { tasks: djTasks },
        catalog: { providers: [provider] },
        identities: djTasks.map((task, index) => ({
          key: task.key,
          taskId: NodeId.make(`planned:${index}`),
          launchCommandId: CommandId.make(`planned:${index}`),
          launchMessageId: MessageId.make(`planned:${index}`),
          workerThreadId: ThreadId.make(`planned:${index}`),
        })),
      });
      assert.equal(decision.status, "admitted");
      if (decision.status !== "admitted")
        return yield* Effect.die("Expected a valid planned admission");
      yield* f.sink.write({
        events: [
          {
            id: EventId.make("race:lifecycle"),
            type: "run.updated",
            threadId: f.threadId,
            runId: run.id,
            occurredAt: f.at,
            payload:
              change === "completed"
                ? { ...run, status: "completed", completedAt: f.at }
                : { ...run, activeAttemptId: RunAttemptId.make("replacement") },
          },
        ],
      });
      const committed = yield* f.sink
        .write({
          events: [
            {
              id: EventId.make("race:admission"),
              type: "multi-agent.controller.updated",
              threadId: f.threadId,
              runId: run.id,
              occurredAt: f.at,
              payload: {
                state: decision.state,
                expectedRevision: run.multiAgentControllerRevision!,
                revision: run.multiAgentControllerRevision! + 1,
              },
            },
          ],
        })
        .pipe(Effect.exit);
      assert.equal(committed._tag, "Failure");
      assert.lengthOf((yield* f.read()).multiAgentController!.tasks, 0);
      if (change === "completed")
        yield* f.commands.streamStoredEventsFrom({ threadId: f.threadId, afterSequence: 0 }).pipe(
          Stream.filter(
            (stored) =>
              stored.event.type === "multi-agent.controller.updated" &&
              stored.event.payload.state.status === "settled",
          ),
          Stream.take(1),
          Stream.runDrain,
        );
      assert.equal(
        (yield* f.read()).multiAgentControllerRevision,
        run.multiAgentControllerRevision! + (change === "completed" ? 1 : 0),
      );
    }).pipe(Effect.provide(layers)),
);

it.effect("schedules two of four tasks and deduplicates repeated scheduling deliveries", () =>
  Effect.gen(function* () {
    const f = yield* fixture("schedule-four");
    const source = yield* f.offer("four", {
      tasks: [
        ...djTasks,
        { ...djTasks[0], key: "review-frontend" },
        { ...djTasks[1], key: "review-backend" },
      ],
    });
    yield* f.commands.dispatch(source.command);
    const schedule = {
      threadId: f.threadId,
      runId: f.run.id,
      schedulingId: CommandId.make("schedule:four"),
    };
    yield* f.commands.scheduleMultiAgentWorkers(schedule);
    const before = yield* f.read();
    assert.deepEqual(
      before.multiAgentController!.tasks.map((task) => task.status),
      ["launching", "launching", "queued", "queued"],
    );
    yield* f.commands.dispatch(source.command);
    yield* f.commands.scheduleMultiAgentWorkers(schedule);
    yield* f.commands.scheduleMultiAgentWorkers({
      ...schedule,
      schedulingId: CommandId.make("schedule:duplicate"),
    });
    assert.deepEqual((yield* f.read()).multiAgentController, before.multiAgentController);
    assert.equal(
      (yield* f.read()).multiAgentControllerRevision,
      before.multiAgentControllerRevision,
    );
    const outbox = yield* EffectOutbox.EffectOutboxV2;
    for (const task of before.multiAgentController!.tasks.slice(0, 2)) {
      const launches = yield* outbox.listByCommandId(
        CommandId.make(`${task.launchCommandId}:scheduled-launch`),
      );
      assert.lengthOf(launches, 1);
      assert.equal(launches[0]!.request.type, "multi-agent.worker.launch");
      assert.deepEqual(task.modelSelection, workerSelection);
    }
  }).pipe(Effect.provide(layers)),
);

it.effect("finishes a durable reservation after a crash before launch authorization", () =>
  Effect.gen(function* () {
    const f = yield* fixture("reserved-recovery");
    yield* f.commands.dispatch((yield* f.offer("two")).command);
    const task = (yield* f.read()).multiAgentController!.tasks[0]!;
    yield* f.commands.dispatch({
      type: "multi-agent.controller.command",
      commandId: CommandId.make("reserve-before-crash"),
      threadId: f.threadId,
      runId: f.run.id,
      action: { type: "reserve", taskId: task.taskId },
    });
    yield* f.commands.scheduleMultiAgentWorkers({
      threadId: f.threadId,
      runId: f.run.id,
      schedulingId: CommandId.make("recovered-schedule"),
    });
    assert.isTrue(
      (yield* f.read()).multiAgentController!.tasks.every((task) => task.status === "launching"),
    );
  }).pipe(Effect.provide(layers)),
);

it.effect.each([false, true])(
  "Stop closes scheduling with an existing reservation: %s",
  (reserved) =>
    Effect.gen(function* () {
      const f = yield* fixture(`stop-scheduler:${reserved}`);
      yield* f.commands.dispatch((yield* f.offer("two")).command);
      if (reserved)
        yield* f.commands.dispatch({
          type: "multi-agent.controller.command",
          commandId: CommandId.make("stop-schedule:reserve"),
          threadId: f.threadId,
          runId: f.run.id,
          action: {
            type: "reserve",
            taskId: (yield* f.read()).multiAgentController!.tasks[0]!.taskId,
          },
        });
      yield* f.commands.dispatch({
        type: "multi-agent.controller.command",
        commandId: CommandId.make("stop-schedule:stop"),
        threadId: f.threadId,
        runId: f.run.id,
        action: { type: "stop_coordinator" },
      });
      const before = yield* f.read();
      yield* f.commands.scheduleMultiAgentWorkers({
        threadId: f.threadId,
        runId: f.run.id,
        schedulingId: CommandId.make("stopped-schedule"),
      });
      assert.deepEqual((yield* f.read()).multiAgentController, before.multiAgentController);
      assert.isFalse(
        (yield* f.read()).multiAgentController!.tasks.some((task) => task.status === "launching"),
      );
    }).pipe(Effect.provide(layers)),
);

it.effect(
  "concurrent controller scheduling respects four environment slots and wakes queued work on verified release",
  () =>
    Effect.gen(function* () {
      const controllers = yield* Effect.forEach(["a", "b", "c"], (key) =>
        Effect.gen(function* () {
          const f = yield* fixture(`environment:${key}`);
          yield* f.commands.dispatch((yield* f.offer("two")).command);
          return f;
        }),
      );
      yield* Effect.forEach(
        controllers,
        (f) =>
          f.commands.scheduleMultiAgentWorkers({
            threadId: f.threadId,
            runId: f.run.id,
            schedulingId: CommandId.make(`environment:${f.threadId}`),
          }),
        { concurrency: "unbounded" },
      );
      const runs = yield* Effect.forEach(controllers, (f) => f.read());
      const occupied = (run: (typeof runs)[number]) =>
        run.multiAgentController!.tasks.filter(
          (task) => task.status === "reserved" || task.status === "launching",
        ).length;
      assert.equal(
        runs.reduce((sum, run) => sum + occupied(run), 0),
        4,
      );
      assert.isTrue(runs.every((run) => occupied(run) <= 2));
      const queuedController =
        controllers[
          runs.findIndex((run) =>
            run.multiAgentController!.tasks.some((task) => task.status === "queued"),
          )
        ]!;
      const active = controllers[runs.findIndex((run) => occupied(run) > 0)]!;
      const task = (yield* active.read()).multiAgentController!.tasks.find(
        (task) => task.status === "launching",
      )!;
      const releaseId = CommandId.make("environment:verified-launch-failure");
      yield* active.commands.dispatch({
        type: "multi-agent.controller.command",
        commandId: releaseId,
        threadId: active.threadId,
        runId: active.run.id,
        action: {
          type: "launch_failed",
          taskId: task.taskId,
          launchCommandId: task.launchCommandId,
        },
      });
      const outbox = yield* EffectOutbox.EffectOutboxV2;
      const wakes = yield* outbox.listByCommandId(releaseId);
      assert.isTrue(
        wakes.some(
          (effect) =>
            effect.threadId === queuedController.threadId &&
            effect.request.type === "multi-agent.controller.schedule",
        ),
      );
      yield* queuedController.commands.scheduleMultiAgentWorkers({
        threadId: queuedController.threadId,
        runId: queuedController.run.id,
        schedulingId: CommandId.make("environment:released-slot"),
      });
      const updated = yield* Effect.forEach(controllers, (f) => f.read());
      assert.equal(
        updated.reduce((sum, run) => sum + occupied(run), 0),
        4,
      );
    }).pipe(Effect.provide(layers)),
);

it.effect("Off workers do not enroll or schedule", () =>
  Effect.gen(function* () {
    const f = yield* fixture("off-schedule", true);
    yield* f.commands.scheduleMultiAgentWorkers({
      threadId: f.threadId,
      runId: f.run.id,
      schedulingId: CommandId.make("off-schedule"),
    });
    assert.isUndefined((yield* f.read()).multiAgentController);
  }).pipe(Effect.provide(layers)),
);

it.effect(
  "rejects a Custom non-Codex worker configuration before committing any run or launch",
  () => {
    const otherId = ProviderInstanceId.make("scripted-other-provider");
    return Effect.gen(function* () {
      const commands = yield* Orchestrator.OrchestratorV2;
      const projections = yield* ProjectionStore.ProjectionStoreV2;
      const outbox = yield* EffectOutbox.EffectOutboxV2;
      const threadId = ThreadId.make("unsupported-custom-worker");
      yield* commands.dispatch({
        type: "thread.create",
        commandId: CommandId.make("unsupported-custom:create"),
        threadId,
        projectId: ProjectId.make("proposal-project"),
        title: "Unsupported worker policy",
        modelSelection: selection,
        runtimeMode: "full-access",
        interactionMode: "default",
        branch: null,
        worktreePath: null,
        createdBy: "user",
        creationSource: "web",
      });
      const result = yield* commands
        .dispatch({
          type: "message.dispatch",
          commandId: CommandId.make("unsupported-custom:send"),
          threadId,
          messageId: MessageId.make("unsupported-custom:goal"),
          text: "Build the DJ app",
          attachments: [],
          multiAgent: {
            mode: "custom",
            roles: { worker: { ...workerSelection, instanceId: otherId } },
          },
          dispatchMode: { type: "start_immediately" },
          createdBy: "user",
          creationSource: "web",
        })
        .pipe(Effect.result);
      assert.equal(result._tag, "Failure");
      const rows = yield* projections.getThreadRecords(threadId, ["runs"]);
      assert.isEmpty(rows.runs);
      assert.isEmpty(yield* outbox.listByCommandId(CommandId.make("unsupported-custom:send")));
    }).pipe(
      Effect.provide(
        makeLayers([
          provider,
          { ...provider, instanceId: otherId, driver: ProviderDriverKind.make("scripted-other") },
        ]),
      ),
    );
  },
);

it.effect("pending scheduling survives database reopen without duplicating launch effects", () =>
  Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem;
    const path = yield* Path.Path;
    const directory = yield* fs.makeTempDirectoryScoped({ prefix: "t3-scheduling-reopen-" });
    const filename = path.join(directory, "state.sqlite");
    const disk = SqlitePersistence.layerFromPath(filename).pipe(
      Layer.provide(NodeServices.layer),
      Layer.orDie,
    );
    const persisted = yield* Effect.scoped(
      Effect.gen(function* () {
        const f = yield* fixture("disk-schedule");
        const source = yield* f.offer("two");
        yield* f.commands.dispatch(source.command);
        return { threadId: f.threadId, runId: f.run.id, sourceId: source.command.commandId };
      }).pipe(Effect.provide(makeLayers([provider], disk))),
    );
    for (const schedulingId of ["restart-first", "restart-duplicate"])
      yield* Effect.scoped(
        Effect.gen(function* () {
          const commands = yield* Orchestrator.OrchestratorV2;
          const outbox = yield* EffectOutbox.EffectOutboxV2;
          yield* outbox.reconcileAfterProcessLoss;
          const schedules = yield* outbox.listByCommandId(persisted.sourceId);
          assert.lengthOf(schedules, 1);
          assert.equal(schedules[0]!.request.type, "multi-agent.controller.schedule");
          yield* commands.scheduleMultiAgentWorkers({
            threadId: persisted.threadId,
            runId: persisted.runId,
            schedulingId: CommandId.make(schedulingId),
          });
          const projections = yield* ProjectionStore.ProjectionStoreV2;
          const run = (yield* projections.getThreadRecords(persisted.threadId, ["runs"])).runs[0]!;
          assert.lengthOf(run.multiAgentController!.tasks, 2);
          for (const task of run.multiAgentController!.tasks) {
            assert.equal(task.status, "launching");
            assert.isUndefined(task.submissionCommitment);
            assert.lengthOf(
              yield* outbox.listByCommandId(
                CommandId.make(`${task.launchCommandId}:scheduled-launch`),
              ),
              1,
            );
          }
        }).pipe(Effect.provide(makeLayers([provider], disk))),
      );
  }).pipe(Effect.provide(NodeServices.layer)),
);

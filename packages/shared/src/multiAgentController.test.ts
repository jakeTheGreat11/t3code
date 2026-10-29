import {
  CommandId,
  MessageId,
  NodeId,
  ProviderDriverKind,
  ProviderInstanceId,
  ProviderSessionId,
  ProviderThreadId,
  RunAttemptId,
  RunId,
  ThreadId,
  MULTI_AGENT_CONTROLLER_LIMITS,
  MultiAgentControllerState,
  OrchestrationV2RunJson,
  OrchestrationV2DomainEventJson,
  type MultiAgentConfig,
  type MultiAgentTaskIdentity,
  type MultiAgentTaskRole,
  type ServerProvider,
} from "@t3tools/contracts";
import * as Schema from "effect/Schema";
import { describe, expect, it } from "vite-plus/test";
import {
  admitMultiAgentProposal,
  multiAgentReviewDecision,
  cancelMultiAgentController,
  multiAgentTaskOccupiesCapacity,
  settleMultiAgentController,
  stopMultiAgentCoordinator,
  grantMultiAgentSubmission,
  type MultiAgentSubmissionWorkerContext,
  transitionMultiAgentTask,
  type MultiAgentTaskAction,
} from "./multiAgentController.ts";

const instanceId = ProviderInstanceId.make("configured-instance");
const modelSelection = {
  instanceId,
  model: "catalog-model",
  options: [
    { id: "effort", value: "supported" },
    { id: "toggle", value: false },
  ],
};
const provider: ServerProvider = {
  instanceId,
  driver: ProviderDriverKind.make("test-driver"),
  enabled: true,
  installed: true,
  version: null,
  status: "ready",
  auth: { status: "authenticated" },
  checkedAt: "2026-10-01T00:00:00Z",
  models: [
    {
      slug: "catalog-model",
      name: "Catalog model",
      isCustom: true,
      capabilities: {
        optionDescriptors: [
          {
            id: "effort",
            label: "Effort",
            type: "select",
            options: [{ id: "supported", label: "Supported" }],
          },
          { id: "toggle", label: "Toggle", type: "boolean" },
        ],
      },
    },
  ],
  slashCommands: [],
  skills: [],
};
const policy: MultiAgentConfig = {
  mode: "custom",
  roles: { worker: modelSelection, rescuer: modelSelection, reviewer: modelSelection },
};
const run = {
  id: RunId.make("coordinator-run"),
  threadId: ThreadId.make("coordinator-thread"),
  multiAgent: policy,
};
const source = {
  proposalId: MessageId.make("proposal-message"),
  attemptId: RunAttemptId.make("coordinator-attempt"),
};
const catalog = { providers: [provider] };
const task = (key: string, role: MultiAgentTaskRole = "worker") => ({
  key,
  role,
  prompt: `Task ${key}`,
});
const identity = (key: string, prefix = "first"): MultiAgentTaskIdentity => ({
  key,
  taskId: NodeId.make(`${prefix}:${key}`),
  launchCommandId: CommandId.make(`launch:${prefix}:${key}`),
  launchMessageId: MessageId.make(`message:${prefix}:${key}`),
  workerThreadId: ThreadId.make(`thread:${prefix}:${key}`),
});
const input = (keys = ["a"]) => ({
  run,
  source,
  catalog,
  proposal: { tasks: keys.map((key) => task(key)) },
  identities: keys.map((key) => identity(key)),
});
function admitted(keys = ["a"]) {
  const result = admitMultiAgentProposal(input(keys));
  if (result.status !== "admitted") throw new Error(`Admission failed: ${result.status}`);
  return result.state;
}
function step(
  state: typeof MultiAgentControllerState.Type,
  action: MultiAgentTaskAction,
  key = "a",
  occupied = state.tasks.filter(multiAgentTaskOccupiesCapacity).length,
) {
  const result = transitionMultiAgentTask(state, identity(key).taskId, action, occupied);
  if (result.status === "rejected") throw new Error(`Transition failed: ${result.reason}`);
  return result.state;
}
const workerRunId = RunId.make("worker-run");
const resultRef = {
  workerThreadId: identity("a").workerThreadId,
  workerRunId,
  messageId: MessageId.make("worker-result"),
};
const decodeState = Schema.decodeUnknownSync(MultiAgentControllerState);

describe("one-use submission commitment", () => {
  const claim = {
    coordinatorThreadId: run.threadId,
    coordinatorRunId: run.id,
    taskId: identity("a").taskId,
    workerThreadId: identity("a").workerThreadId,
    workerRunId,
    workerAttemptId: RunAttemptId.make("worker-attempt"),
    providerInstanceId: instanceId,
    providerThreadId: ProviderThreadId.make("worker-provider-thread"),
    providerSessionId: ProviderSessionId.make("worker-provider-session"),
    invocationClaimId: CommandId.make("invocation-claim"),
    committedAt: "2026-10-14T00:00:00Z",
  };
  const worker: MultiAgentSubmissionWorkerContext = {
    thread: {
      id: claim.workerThreadId,
      coordinatorThreadId: run.threadId,
      archivedAt: null,
      deletedAt: null,
    },
    run: {
      id: workerRunId,
      threadId: claim.workerThreadId,
      userMessageId: identity("a").launchMessageId,
      modelSelection,
      multiAgent: { mode: "off" },
      status: "starting",
      activeAttemptId: claim.workerAttemptId,
      providerInstanceId: instanceId,
      providerThreadId: claim.providerThreadId,
    },
    attempt: {
      id: claim.workerAttemptId,
      runId: workerRunId,
      status: "pending",
      providerInstanceId: instanceId,
      providerThreadId: claim.providerThreadId,
    },
    providerThread: {
      id: claim.providerThreadId,
      appThreadId: claim.workerThreadId,
      providerInstanceId: instanceId,
      providerSessionId: claim.providerSessionId,
      status: "idle",
    },
  };
  const launching = () => step(step(admitted(), { type: "reserve" }), { type: "launch" });
  it("records immutable identity without advancing launch state or releasing capacity", () => {
    const before = launching();
    const grant = grantMultiAgentSubmission(before, claim, worker);
    expect(grant.status).toBe("granted");
    if (grant.status !== "granted") return;
    expect(grant.state.tasks[0]).toMatchObject({
      status: "launching",
      workerRunId,
      submissionCommitment: claim,
    });
    expect(grant.state.tasks.filter(multiAgentTaskOccupiesCapacity)).toHaveLength(1);
    expect(decodeState(grant.state).tasks[0]?.submissionCommitment).toEqual(claim);
    expect(grantMultiAgentSubmission(grant.state, claim, worker)).toMatchObject({
      status: "rejected",
      reason: "already_committed",
    });
    expect(
      grantMultiAgentSubmission(
        grant.state,
        { ...claim, invocationClaimId: CommandId.make("second") },
        worker,
      ),
    ).toMatchObject({ reason: "already_committed" });
    expect(
      transitionMultiAgentTask(grant.state, claim.taskId, { type: "launch_failed" }, 1).status,
    ).toBe("rejected");
    const stopped = stopMultiAgentCoordinator(grant.state, "2026-10-14T01:00:00Z");
    const completed = step(step(stopped, { type: "started", workerRunId }), {
      type: "completed",
      result: resultRef,
    });
    expect(completed.tasks[0]?.submissionCommitment).toEqual(claim);
    expect(completed.coordinatorStoppedAt).toBe(stopped.coordinatorStoppedAt);
  });
  it("rejects Stop-first, cancelled controllers and tasks without launch authorization", () => {
    for (const state of [admitted(), step(admitted(), { type: "reserve" })])
      expect(grantMultiAgentSubmission(state, claim, worker)).toMatchObject({
        reason: "ineligible_task",
      });
    for (const state of [
      stopMultiAgentCoordinator(launching(), "2026-10-14T00:00:00Z"),
      cancelMultiAgentController(launching()),
    ])
      expect(grantMultiAgentSubmission(state, claim, worker)).toMatchObject({
        reason: "controller_closed",
      });
  });
  it("rejects wrong ownership, initial run, attempts and provider bindings", () => {
    for (const bad of [
      { ...claim, coordinatorThreadId: ThreadId.make("other") },
      { ...claim, coordinatorRunId: RunId.make("other") },
      { ...claim, taskId: NodeId.make("other") },
      { ...claim, workerThreadId: ThreadId.make("other") },
      { ...claim, workerRunId: RunId.make("other") },
      { ...claim, workerAttemptId: RunAttemptId.make("other") },
      { ...claim, providerInstanceId: ProviderInstanceId.make("other") },
      { ...claim, providerThreadId: ProviderThreadId.make("other") },
      { ...claim, providerSessionId: ProviderSessionId.make("other") },
    ])
      expect(grantMultiAgentSubmission(launching(), bad, worker).status).toBe("rejected");
    for (const status of ["preparing", "queued", "completed", "failed", "interrupted"] as const)
      expect(
        grantMultiAgentSubmission(launching(), claim, { ...worker, run: { ...worker.run, status } })
          .status,
      ).toBe("rejected");
    expect(
      grantMultiAgentSubmission(launching(), claim, {
        ...worker,
        run: { ...worker.run, userMessageId: MessageId.make("later-input") },
      }).status,
    ).toBe("rejected");
    expect(decodeState(admitted()).tasks[0]?.submissionCommitment).toBeUndefined();
  });
});

describe("Stop Coordinator", () => {
  const stoppedAt = "2026-10-14T00:00:00Z";
  it("preserves every task state, results and capacity; repeated Stop keeps the first timestamp", () => {
    const queued = admitted();
    const reserved = step(queued, { type: "reserve" });
    const launching = step(reserved, { type: "launch" });
    const running = step(launching, { type: "started", workerRunId });
    for (const state of [
      queued,
      reserved,
      launching,
      running,
      cancelMultiAgentController(running),
      step(running, { type: "completed", result: resultRef }),
      step(running, { type: "worker_failed", result: resultRef }),
      cancelMultiAgentController(queued),
    ]) {
      const stopped = stopMultiAgentCoordinator(state, stoppedAt);
      expect(stopped.tasks).toBe(state.tasks);
      expect(stopped.status).toBe(state.status);
      expect(stopped.tasks.filter(multiAgentTaskOccupiesCapacity).length).toBe(
        state.tasks.filter(multiAgentTaskOccupiesCapacity).length,
      );
      expect(stopMultiAgentCoordinator(stopped, "2026-10-15T00:00:00Z")).toBe(stopped);
    }
    expect(decodeState(queued).coordinatorStoppedAt).toBeUndefined();
  });
  it("rejects new proposals and all fresh reserve/launch authorizations without breaking proposal deduplication", () => {
    const states = [
      admitted(),
      step(admitted(), { type: "reserve" }),
      step(step(admitted(), { type: "reserve" }), { type: "launch" }),
    ];
    for (const state of states) {
      const stopped = stopMultiAgentCoordinator(state, stoppedAt);
      expect(
        admitMultiAgentProposal({
          ...input(["b"]),
          state: stopped,
          source: { ...source, proposalId: MessageId.make("new-proposal") },
        }),
      ).toMatchObject({ status: "rejected", reason: "controller_closed" });
      expect(admitMultiAgentProposal({ ...input(), state: stopped })).toMatchObject({
        status: "duplicate",
        state: stopped,
      });
      for (const type of ["reserve", "launch"] as const)
        expect(transitionMultiAgentTask(stopped, identity("a").taskId, { type }, 1)).toMatchObject({
          status: "rejected",
          reason: "controller_closed",
        });
    }
  });
  it("records launch acknowledgments and completion after Stop without reopening admissions", () => {
    const launching = step(step(admitted(), { type: "reserve" }), { type: "launch" });
    const stopped = stopMultiAgentCoordinator(launching, stoppedAt);
    const running = step(stopped, { type: "started", workerRunId });
    const completed = step(running, { type: "completed", result: resultRef });
    expect(completed.tasks[0]).toMatchObject({ status: "completed", result: resultRef });
    expect(completed.coordinatorStoppedAt).toEqual(stoppedAt);
    expect(completed.tasks.filter(multiAgentTaskOccupiesCapacity)).toHaveLength(0);
    const settled = settleMultiAgentController(completed);
    expect(settled.status).toBe("settled");
    if (settled.status === "settled") expect(settled.state.coordinatorStoppedAt).toEqual(stoppedAt);
  });
});
const encodeState = Schema.encodeSync(MultiAgentControllerState);
const decodeRun = Schema.decodeUnknownSync(OrchestrationV2RunJson);
const decodeEvent = Schema.decodeUnknownSync(OrchestrationV2DomainEventJson);
const encodeEvent = Schema.encodeSync(OrchestrationV2DomainEventJson);

describe("proposal admission", () => {
  it.each([undefined, { mode: "off" } as const])(
    "does not enroll Off or legacy runs: %j",
    (multiAgent) => {
      expect(
        admitMultiAgentProposal({
          ...input(),
          run: { id: run.id, threadId: run.threadId, ...(multiAgent ? { multiAgent } : {}) },
        }),
      ).toEqual({ status: "rejected", reason: "off" });
    },
  );
  it.each(["budget", "balanced", "maximum"] as const)(
    "keeps Auto %s unsupported even with explicit resolver policies",
    (preset) => {
      expect(
        admitMultiAgentProposal({
          ...input(),
          run: { ...run, multiAgent: { mode: "auto", preset } },
          catalog: { ...catalog, autoPolicy: { [preset]: { worker: modelSelection } } },
        }),
      ).toEqual({ status: "rejected", reason: "auto_unsupported" });
    },
  );
  it.each([
    null,
    {},
    { tasks: [] },
    { tasks: [task(" ")] },
    { tasks: [{ ...task("a"), prompt: " " }] },
    { tasks: [{ ...task("a"), role: "planner" }] },
    { tasks: [{ ...task("a"), role: "unknown" }] },
    { tasks: Array.from({ length: 5 }, (_, index) => task(String(index))) },
  ])("rejects malformed proposals: %j", (proposal) => {
    expect(admitMultiAgentProposal({ ...input(), proposal })).toMatchObject({
      status: "rejected",
      reason: "malformed_proposal",
    });
  });
  it.each([
    "modelSelection",
    "providerInstanceId",
    "workspaceStrategy",
    "runtimeMode",
    "interactionMode",
    "coordinatorRunId",
    "taskId",
    "launchCommandId",
  ])("rejects injected %s at either proposal level", (field) => {
    for (const proposal of [
      { ...input().proposal, [field]: "override" },
      { tasks: [{ ...task("a"), [field]: "override" }] },
    ]) {
      expect(admitMultiAgentProposal({ ...input(), proposal })).toMatchObject({
        reason: "malformed_proposal",
      });
    }
  });
  it.each(["worker", "rescuer", "reviewer"] as const)(
    "requires only the requested %s assignment and preserves its options",
    (role) => {
      const result = admitMultiAgentProposal({
        ...input(),
        proposal: { tasks: [task("a", role)] },
        run: { ...run, multiAgent: { mode: "custom", roles: { [role]: modelSelection } } },
      });
      expect(result.status).toBe("admitted");
      if (result.status === "admitted") {
        expect(result.state.tasks[0]).toMatchObject({
          role,
          status: "queued",
          modelSelection,
          proposalId: source.proposalId,
          proposalAttemptId: source.attemptId,
          ...identity("a"),
        });
        expect(result.state.tasks[0]?.modelSelection.options).not.toBe(modelSelection.options);
      }
    },
  );
  it("rejects the entire batch if any required role is unassigned", () => {
    expect(
      admitMultiAgentProposal({
        ...input(["a", "b"]),
        proposal: { tasks: [task("a"), task("b", "reviewer")] },
        run: { ...run, multiAgent: { mode: "custom", roles: { worker: modelSelection } } },
      }),
    ).toMatchObject({ reason: "role_ineligible", taskKey: "b", routing: { status: "unassigned" } });
  });
  it.each([
    { instanceId: ProviderInstanceId.make("foreign-instance"), model: modelSelection.model },
    { ...modelSelection, model: "unavailable-model" },
    { ...modelSelection, options: [{ id: "effort", value: "unsupported" }] },
    { ...modelSelection, options: [{ id: "unknown", value: false }] },
  ])("rejects unavailable Custom selections: %j", (selection) => {
    expect(
      admitMultiAgentProposal({
        ...input(),
        run: { ...run, multiAgent: { mode: "custom", roles: { worker: selection } } },
      }),
    ).toMatchObject({ reason: "role_ineligible", routing: { status: "unavailable" } });
  });
  it("honors provider-instance restrictions", () => {
    expect(
      admitMultiAgentProposal({ ...input(), catalog: { ...catalog, allowedInstanceIds: [] } }),
    ).toMatchObject({ routing: { issue: { reason: "provider_not_allowed" } } });
  });
  it("replays identical proposals without revalidation or a second task, including after cancellation", () => {
    const state = admitted();
    expect(admitMultiAgentProposal({ ...input(), state, catalog: { providers: [] } })).toEqual({
      status: "duplicate",
      state,
    });
    const cancelled = cancelMultiAgentController(state);
    expect(admitMultiAgentProposal({ ...input(), state: cancelled })).toEqual({
      status: "duplicate",
      state: cancelled,
    });
  });
  it("rejects reused proposal identity with changed content, attempt, or launch identity", () => {
    const state = admitted();
    for (const patch of [
      { proposal: { tasks: [{ ...task("a"), prompt: "Different" }] } },
      { source: { ...source, attemptId: RunAttemptId.make("another-attempt") } },
      { identities: [identity("a", "another")] },
    ])
      expect(admitMultiAgentProposal({ ...input(), state, ...patch })).toMatchObject({
        reason: "proposal_conflict",
      });
  });
  it("rejects different coordinator run or thread ownership", () => {
    for (const owner of [
      { ...run, id: RunId.make("other-run") },
      { ...run, threadId: ThreadId.make("other-thread") },
    ]) {
      expect(admitMultiAgentProposal({ ...input(), run: owner, state: admitted() })).toMatchObject({
        reason: "controller_origin_mismatch",
      });
    }
  });
  it("rejects duplicate task keys and execution identities", () => {
    expect(
      admitMultiAgentProposal({ ...input(), proposal: { tasks: [task("a"), task("a")] } }),
    ).toMatchObject({ reason: "duplicate_task_key" });
    for (const field of [
      "taskId",
      "launchCommandId",
      "launchMessageId",
      "workerThreadId",
    ] as const) {
      expect(
        admitMultiAgentProposal({
          ...input(["a", "b"]),
          identities: [identity("a"), { ...identity("b"), [field]: identity("a")[field] }],
        }),
      ).toMatchObject({ reason: "invalid_task_identities" });
    }
    expect(
      admitMultiAgentProposal({
        ...input(),
        identities: [{ ...identity("a"), workerThreadId: run.threadId }],
      }),
    ).toMatchObject({ reason: "invalid_task_identities" });
    expect(admitMultiAgentProposal({ ...input(), identities: [] })).toMatchObject({
      reason: "invalid_task_identities",
    });
  });
  it("counts terminal tasks toward the lifetime task limit", () => {
    const state = admitted(["a", "b", "c", "d"]);
    const completed = {
      ...state,
      tasks: state.tasks.map((existing) => ({ ...existing, status: "failed" as const })),
    };
    expect(
      admitMultiAgentProposal({
        ...input(["e"]),
        source: { ...source, proposalId: MessageId.make("second-proposal") },
        state: completed,
      }),
    ).toMatchObject({ reason: "task_limit" });
  });
  it("admits distinct proposals while preventing reuse of another task's launch identities", () => {
    const next = {
      ...input(["b"]),
      source: { ...source, proposalId: MessageId.make("second-proposal") },
      state: admitted(),
    };
    expect(admitMultiAgentProposal(next).status).toBe("admitted");
    expect(
      admitMultiAgentProposal({
        ...next,
        identities: [{ ...identity("b"), launchCommandId: identity("a").launchCommandId }],
      }),
    ).toMatchObject({ reason: "invalid_task_identities" });
  });
});

describe("controller state transitions", () => {
  it("has explicit bounded policy limits", () => {
    expect(MULTI_AGENT_CONTROLLER_LIMITS).toEqual({
      concurrentPerController: 2,
      concurrentPerEnvironment: 4,
      totalTasksPerController: 4,
      automaticReasoningRetries: 0,
    });
  });
  it("reserves at most two tasks and does not confuse effect concurrency with worker capacity", () => {
    let state = admitted(["a", "b", "c", "d"]);
    state = step(state, { type: "reserve" });
    state = step(state, { type: "launch" });
    state = step(state, { type: "started", workerRunId });
    state = step(state, { type: "reserve" }, "b");
    expect(
      transitionMultiAgentTask(state, identity("c").taskId, { type: "reserve" }, 2),
    ).toMatchObject({ reason: "controller_capacity" });
    state = step(state, { type: "completed", result: resultRef });
    expect(
      transitionMultiAgentTask(state, identity("c").taskId, { type: "reserve" }, 1).status,
    ).toBe("changed");
  });
  it("enforces the environment cap and rejects incomplete or malformed occupancy", () => {
    const state = step(admitted(["a", "b"]), { type: "reserve" });
    expect(
      transitionMultiAgentTask(state, identity("b").taskId, { type: "reserve" }, 4),
    ).toMatchObject({ reason: "environment_capacity" });
    for (const occupancy of [-1, 0, 1.5, NaN, Infinity])
      expect(
        transitionMultiAgentTask(state, identity("b").taskId, { type: "reserve" }, occupancy),
      ).toMatchObject({ reason: "invalid_environment_occupancy" });
    expect(
      transitionMultiAgentTask(state, identity("b").taskId, { type: "reserve" }, 3).status,
    ).toBe("changed");
  });
  it("preserves launch identities across idempotent infrastructure transitions", () => {
    let state = admitted();
    for (const action of [
      { type: "reserve" },
      { type: "launch" },
      { type: "started", workerRunId },
    ] as const) {
      state = step(state, action);
      expect(transitionMultiAgentTask(state, identity("a").taskId, action, 1)).toMatchObject({
        status: "unchanged",
        state,
      });
    }
    state = step(state, { type: "completed", result: resultRef });
    expect(step(state, { type: "completed", result: resultRef })).toEqual(state);
    expect(settleMultiAgentController(state)).toMatchObject({
      status: "settled",
      state: { status: "settled" },
    });
  });
  it("rejects skipping launch stages, unknown tasks, and replacement worker runs", () => {
    const queued = admitted();
    for (const action of [
      { type: "launch" },
      { type: "started", workerRunId },
      { type: "launch_failed" },
    ] as const)
      expect(transitionMultiAgentTask(queued, identity("a").taskId, action, 0)).toMatchObject({
        reason: "invalid_transition",
      });
    expect(
      transitionMultiAgentTask(queued, NodeId.make("missing"), { type: "reserve" }, 0),
    ).toMatchObject({ reason: "task_missing" });
    const running = step(step(step(queued, { type: "reserve" }), { type: "launch" }), {
      type: "started",
      workerRunId,
    });
    expect(
      transitionMultiAgentTask(
        running,
        identity("a").taskId,
        { type: "started", workerRunId: RunId.make("other-run") },
        1,
      ),
    ).toMatchObject({ reason: "worker_identity_mismatch" });
    for (const result of [
      { ...resultRef, workerRunId: RunId.make("other-run") },
      { ...resultRef, workerThreadId: ThreadId.make("other-thread") },
    ])
      expect(
        transitionMultiAgentTask(running, identity("a").taskId, { type: "completed", result }, 1),
      ).toMatchObject({ reason: "worker_identity_mismatch" });
  });
  it("does not automatically retry failed reasoning or resurrect terminal tasks", () => {
    let state = step(step(admitted(), { type: "reserve" }), { type: "launch" });
    state = step(state, { type: "started", workerRunId });
    state = step(state, { type: "worker_failed", result: resultRef });
    for (const action of [
      { type: "reserve" },
      { type: "launch" },
      { type: "started", workerRunId },
      { type: "completed", result: resultRef },
    ] as const)
      expect(transitionMultiAgentTask(state, identity("a").taskId, action, 0).status).toBe(
        "rejected",
      );
    expect(
      admitMultiAgentProposal({
        ...input(["b"]),
        state:
          settleMultiAgentController(state).status === "settled"
            ? { ...state, status: "settled" }
            : state,
        source: { ...source, proposalId: MessageId.make("next") },
      }),
    ).toMatchObject({ reason: "controller_closed" });
  });
  it("cancels unlaunched tasks immediately and holds launched capacity until confirmation", () => {
    let state = admitted(["a", "b", "c"]);
    state = step(step(state, { type: "reserve" }), { type: "launch" });
    state = step(state, { type: "reserve" }, "b");
    const cancelled = cancelMultiAgentController(state);
    expect(cancelled.status).toBe("cancelling");
    expect(cancelled.tasks.map((existing) => existing.status)).toEqual([
      "cancellation_pending",
      "cancelled",
      "cancelled",
    ]);
    expect(cancelled.tasks.filter(multiAgentTaskOccupiesCapacity)).toHaveLength(1);
    expect(cancelMultiAgentController(cancelled)).toEqual(cancelled);
    expect(
      transitionMultiAgentTask(cancelled, identity("b").taskId, { type: "reserve" }, 1),
    ).toMatchObject({ reason: "controller_closed" });
    const startedAfterCancel = step(cancelled, { type: "started", workerRunId });
    expect(startedAfterCancel.tasks[0]?.status).toBe("cancellation_pending");
    expect(
      transitionMultiAgentTask(
        startedAfterCancel,
        identity("a").taskId,
        { type: "stop_confirmed", workerThreadId: resultRef.workerThreadId },
        1,
      ),
    ).toMatchObject({ reason: "worker_identity_mismatch" });
    const stopped = step(startedAfterCancel, {
      type: "stop_confirmed",
      workerThreadId: resultRef.workerThreadId,
      workerRunId,
    });
    expect(stopped.status).toBe("cancelled");
    expect(stopped.tasks.some(multiAgentTaskOccupiesCapacity)).toBe(false);
  });
  it("records completion during cancellation without reopening the controller", () => {
    const running = step(step(step(admitted(), { type: "reserve" }), { type: "launch" }), {
      type: "started",
      workerRunId,
    });
    const cancelled = step(cancelMultiAgentController(running), {
      type: "completed",
      result: resultRef,
    });
    expect(cancelled).toMatchObject({
      status: "cancelled",
      tasks: [{ status: "cancelled", result: resultRef }],
    });
    expect(cancelMultiAgentController(cancelled)).toEqual(cancelled);
  });
  it("retains late results after confirmed cancellation without acquiring capacity", () => {
    const running = step(step(step(admitted(), { type: "reserve" }), { type: "launch" }), {
      type: "started",
      workerRunId,
    });
    const stopped = step(cancelMultiAgentController(running), {
      type: "stop_confirmed",
      workerThreadId: resultRef.workerThreadId,
      workerRunId,
    });
    const withResult = step(stopped, { type: "completed", result: resultRef });
    expect(withResult).toMatchObject({
      status: "cancelled",
      tasks: [{ status: "cancelled", result: resultRef }],
    });
    expect(withResult.tasks.some(multiAgentTaskOccupiesCapacity)).toBe(false);
    expect(step(withResult, { type: "completed", result: resultRef })).toEqual(withResult);
  });

  it("releases an ambiguous launch only when failure or stop is confirmed", () => {
    const launching = step(step(admitted(), { type: "reserve" }), { type: "launch" });
    const cancellation = cancelMultiAgentController(launching);
    expect(cancellation.tasks.filter(multiAgentTaskOccupiesCapacity)).toHaveLength(1);
    expect(step(cancellation, { type: "launch_failed" }).status).toBe("cancelled");
    expect(
      step(cancellation, { type: "stop_confirmed", workerThreadId: identity("a").workerThreadId })
        .status,
    ).toBe("cancelled");
    expect(step(launching, { type: "launch_failed" }).tasks[0]?.status).toBe("failed");
  });
  it("does not settle before all tasks are terminal", () => {
    expect(settleMultiAgentController(admitted())).toMatchObject({ reason: "tasks_not_terminal" });
    expect(settleMultiAgentController(cancelMultiAgentController(admitted()))).toMatchObject({
      reason: "controller_closed",
    });
  });
  it("round-trips controller contracts and leaves legacy runs unenrolled", () => {
    const state = step(step(step(admitted(), { type: "reserve" }), { type: "launch" }), {
      type: "started",
      workerRunId,
    });
    expect(decodeState(encodeState(state))).toEqual(state);
    const legacy = {
      id: run.id,
      threadId: run.threadId,
      ordinal: 1,
      providerInstanceId: instanceId,
      modelSelection,
      providerThreadId: null,
      userMessageId: source.proposalId,
      rootNodeId: null,
      activeAttemptId: null,
      status: "running",
      requestedAt: "2026-10-01T00:00:00Z",
      startedAt: null,
      completedAt: null,
      checkpointId: null,
      contextHandoffId: null,
    };
    expect(decodeRun(legacy).multiAgentController).toBeUndefined();
    for (const type of ["run.created", "run.updated"]) {
      const event = {
        id: "controller-event",
        type,
        threadId: run.threadId,
        runId: run.id,
        occurredAt: "2026-10-01T00:00:00Z",
        payload: legacy,
      };
      const old = decodeEvent(event);
      expect(old).toMatchObject({ payload: decodeRun(legacy) });
      const current = decodeEvent({
        ...event,
        payload: { ...legacy, multiAgentController: state },
      });
      expect(decodeEvent(encodeEvent(current))).toEqual(current);
    }
    expect(decodeRun({ ...legacy, multiAgentController: state }).multiAgentController).toEqual(
      state,
    );
  });
  it("rejects invalid controller lifecycle and result-reference contracts", () => {
    const queued = admitted();
    expect(() => decodeState({ ...queued, status: "settled" })).toThrow();
    expect(() => decodeState({ ...queued, status: "cancelling" })).toThrow();
    const finished = step(
      step(step(step(queued, { type: "reserve" }), { type: "launch" }), {
        type: "started",
        workerRunId,
      }),
      { type: "completed", result: resultRef },
    );
    const completed = finished.tasks[0]!;
    expect(() =>
      decodeState({
        ...finished,
        tasks: [
          { ...completed, result: { ...resultRef, workerRunId: RunId.make("unrelated-result") } },
        ],
      }),
    ).toThrow();
  });

  it("does not mutate proposals, policies, identities, or input state", () => {
    const original = input();
    const before = structuredClone(original);
    const state = admitted();
    const priorState = structuredClone(state);
    admitMultiAgentProposal(original);
    step(state, { type: "reserve" });
    cancelMultiAgentController(state);
    expect(original).toEqual(before);
    expect(state).toEqual(priorState);
  });
});

describe("coordinator result batches", () => {
  const terminalRun = (state = admitted(["a", "b"])) => ({
    ...run,
    ordinal: 1,
    status: "completed" as const,
    multiAgentController: state,
  });
  it("waits for the entire batch, including a partial failure", () => {
    const root = terminalRun();
    expect(multiAgentReviewDecision(root, [root])).toBe("none");
    const first = {
      ...root,
      multiAgentController: {
        ...root.multiAgentController,
        tasks: root.multiAgentController.tasks.map((task, index) =>
          index === 0 ? { ...task, status: "failed" as const } : task,
        ),
      },
    };
    expect(multiAgentReviewDecision(first, [first])).toBe("none");
    const both = {
      ...root,
      multiAgentController: {
        ...root.multiAgentController,
        tasks: root.multiAgentController.tasks.map((task) => ({
          ...task,
          status: "failed" as const,
        })),
      },
    };
    expect(multiAgentReviewDecision(both, [both])).toBe("review");
  });
  it("never wakes again without new results, including claimed or failed reviews", () => {
    const root = terminalRun();
    const review = {
      runId: RunId.make("review"),
      attemptId: RunAttemptId.make("review-attempt"),
      taskIds: root.multiAgentController.tasks.map((t) => t.taskId),
      submissionClaimedAt: "2026-10-15T00:00:00Z",
    };
    const delivered = {
      ...root,
      multiAgentController: {
        ...root.multiAgentController,
        reviews: [review],
        tasks: root.multiAgentController.tasks.map((task) => ({
          ...task,
          status: "failed" as const,
        })),
      },
    };
    const execution = { ...root, id: review.runId, ordinal: 2, status: "running" as const };
    expect(multiAgentReviewDecision(delivered, [delivered, execution])).toBe("none");
    expect(
      multiAgentReviewDecision(delivered, [delivered, { ...execution, status: "failed" }]),
    ).toBe("settle");
    const stopped = {
      ...delivered,
      multiAgentController: stopMultiAgentCoordinator(
        delivered.multiAgentController,
        "2026-10-15T00:00:00Z",
      ),
    };
    expect(
      multiAgentReviewDecision(stopped, [stopped, { ...execution, status: "completed" }]),
    ).toBe("none");
  });
  it("finishes zero-worker work and yields to a later manual turn", () => {
    const root = terminalRun(admitted());
    const finished = { ...root, multiAgentController: { ...root.multiAgentController, tasks: [] } };
    expect(multiAgentReviewDecision(finished, [finished])).toBe("settle");
    expect(
      multiAgentReviewDecision(root, [root, { ...root, id: RunId.make("manual"), ordinal: 2 }]),
    ).toBe("none");
  });
});

import { describe, expect, it } from "@effect/vitest";
import * as Schema from "effect/Schema";

import {
  OrchestratorMcpCreateThreadsInput,
  OrchestratorMcpDelegateTaskInput,
  OrchestratorMcpDelegateTaskResult,
  OrchestratorMcpThreadInterruptInput,
  OrchestratorMcpThreadListInput,
  OrchestratorMcpThreadListResult,
  OrchestratorMcpThreadReadInput,
  OrchestratorMcpThreadReadResult,
  OrchestratorMcpThreadSendInput,
  OrchestratorMcpThreadWaitInput,
} from "./orchestratorMcp.ts";

const decodeCreateThreadsInput = Schema.decodeUnknownSync(OrchestratorMcpCreateThreadsInput);
const decodeDelegateTaskInput = Schema.decodeUnknownSync(OrchestratorMcpDelegateTaskInput);
const decodeDelegateTaskResult = Schema.decodeUnknownSync(OrchestratorMcpDelegateTaskResult);
const decodeThreadInterruptInput = Schema.decodeUnknownSync(OrchestratorMcpThreadInterruptInput);
const decodeThreadListInput = Schema.decodeUnknownSync(OrchestratorMcpThreadListInput);
const decodeThreadReadInput = Schema.decodeUnknownSync(OrchestratorMcpThreadReadInput);
const decodeThreadSendInput = Schema.decodeUnknownSync(OrchestratorMcpThreadSendInput);
const decodeThreadWaitInput = Schema.decodeUnknownSync(OrchestratorMcpThreadWaitInput);
const decodeThreadListResult = Schema.decodeUnknownSync(OrchestratorMcpThreadListResult);
const encodeThreadListResult = Schema.encodeSync(OrchestratorMcpThreadListResult);
const decodeThreadReadResult = Schema.decodeUnknownSync(OrchestratorMcpThreadReadResult);
const encodeThreadReadResult = Schema.encodeSync(OrchestratorMcpThreadReadResult);

const discoveryThread = {
  threadId: "worker",
  projectId: "project",
  title: "Worker",
  createdBy: "agent",
  creationSource: "mcp",
  status: "idle",
  latestRunId: null,
  activeRunId: null,
  providerInstanceId: "codex",
  model: "gpt-5",
  runtimeMode: "full-access",
  interactionMode: "default",
  linkedPullRequest: null,
  titleRegeneration: null,
  branch: null,
  worktreePath: null,
  parentThreadId: null,
  relationshipToParent: null,
  runCount: 0,
  itemCount: 0,
  pendingRequestCount: 0,
  archived: false,
  settled: false,
  settledAt: null,
  snoozed: false,
  snoozedUntil: null,
  createdAt: "2026-10-01T00:00:00.000Z",
  updatedAt: "2026-10-01T00:00:00.000Z",
};

describe("orchestrator MCP contracts", () => {
  it.each([
    { name: "grouped worker", relationship: { coordinatorThreadId: "coordinator" } },
    { name: "legacy ordinary thread", relationship: {} },
    { name: "explicitly undefined coordinator", relationship: { coordinatorThreadId: undefined } },
    { name: "unavailable coordinator", relationship: { coordinatorThreadId: "missing-thread" } },
  ])("round-trips discovery responses for $name", ({ relationship }) => {
    const thread = { ...discoveryThread, ...relationship };
    const list = decodeThreadListResult({
      projectId: "project",
      currentThreadId: null,
      threads: [thread],
      nextCursor: null,
      total: 1,
    });
    const read = decodeThreadReadResult({
      thread,
      recentRuns: [],
      items: [],
      nextPosition: null,
      hasMore: false,
    });
    const listWire = encodeThreadListResult(list);
    const readWire = encodeThreadReadResult(read);
    expect(decodeThreadListResult(listWire).threads[0]?.coordinatorThreadId).toBe(
      relationship.coordinatorThreadId,
    );
    expect(decodeThreadReadResult(readWire).thread.coordinatorThreadId).toBe(
      relationship.coordinatorThreadId,
    );
    expect(read.thread.parentThreadId).toBeNull();
    expect(read.thread.relationshipToParent).toBeNull();
    if (relationship.coordinatorThreadId === undefined) {
      expect(JSON.parse(JSON.stringify(listWire)).threads[0]).not.toHaveProperty(
        "coordinatorThreadId",
      );
      expect(JSON.parse(JSON.stringify(readWire)).thread).not.toHaveProperty("coordinatorThreadId");
    }
  });

  it("decodes cross-provider delegated task requests and durable results", () => {
    const request = decodeDelegateTaskInput({
      task: "Inspect the workspace and report the result.",
      target: {
        providerInstanceId: "claudeAgent",
        model: "claude-sonnet-4-6",
      },
      mode: "wait",
      timeoutMs: 5_000,
      clientRequestId: "delegate-1",
      runtimeMode: "inherit",
      interactionMode: "inherit",
    });
    const result = decodeDelegateTaskResult({
      taskId: "node-task-1",
      childThreadId: "thread-child-1",
      childRunId: "run-child-1",
      childNodeId: "node-task-1",
      status: "completed",
      workState: "result_available",
      hasPendingChildRuns: false,
      providerInstanceId: "claudeAgent",
      model: "claude-sonnet-4-6",
      summary: "Workspace inspected.",
      resultContextTransferId: "context-transfer-result-1",
      latestTerminalRunId: "run-child-1",
      latestTerminalStatus: "completed",
      latestTerminalSummary: "Workspace inspected.",
      latestTerminalResultContextTransferId: "context-transfer-result-1",
      waitTimedOut: false,
    });

    expect(request.target?.providerInstanceId).toBe("claudeAgent");
    expect(result.status).toBe("completed");
    expect(result.summary).toBe("Workspace inspected.");
  });

  it("decodes target model options in canonical and shorthand shapes", () => {
    const canonical = decodeDelegateTaskInput({
      task: "Say hello.",
      target: {
        providerInstanceId: "codex",
        model: "gpt-5.6-luna",
        options: [{ id: "reasoning", value: "low" }],
      },
    });
    const shorthand = decodeDelegateTaskInput({
      task: "Say hello.",
      target: {
        providerInstanceId: "codex",
        model: "gpt-5.6-luna",
        options: { reasoning: "low", fastMode: true },
      },
    });

    expect(canonical.target?.options).toEqual([{ id: "reasoning", value: "low" }]);
    expect(shorthand.target?.options).toEqual([
      { id: "reasoning", value: "low" },
      { id: "fastMode", value: true },
    ]);
  });

  it("rejects target model options that are not strings or booleans", () => {
    expect(() =>
      decodeDelegateTaskInput({
        task: "Say hello.",
        target: {
          providerInstanceId: "codex",
          model: "gpt-5.6-luna",
          // Must fail loudly instead of being dropped like legacy persistence.
          options: { reasoning: 3 },
        },
      }),
    ).toThrow();
    expect(() =>
      decodeDelegateTaskInput({
        task: "Say hello.",
        target: {
          providerInstanceId: "codex",
          model: "gpt-5.6-luna",
          options: [{ id: "reasoning", value: null }],
        },
      }),
    ).toThrow();
  });

  it("decodes mixed prompted and empty thread batches", () => {
    const request = decodeCreateThreadsInput({
      clientRequestId: "threads-1",
      threads: [
        { title: "Inherited empty thread" },
        {
          prompt: "Review the API.",
          target: { driverKind: "claudeAgent" },
          runtimeMode: "approval-required",
        },
      ],
    });

    expect(request.threads).toHaveLength(2);
    expect(request.threads[0]?.prompt).toBeUndefined();
    expect(request.threads[1]?.target?.driverKind).toBe("claudeAgent");
  });

  it("decodes project-scoped thread orchestration requests", () => {
    expect(
      decodeThreadListInput({
        statuses: ["running", "completed"],
        includeSubagents: false,
        limit: 25,
      }).statuses,
    ).toEqual(["running", "completed"]);
    expect(
      decodeThreadReadInput({
        threadId: "thread-loop-1",
        view: "activity",
        afterPosition: 10,
      }).afterPosition,
    ).toBe(10);
    expect(
      decodeThreadSendInput({
        threadId: "thread-loop-1",
        message: "Continue with the next iteration.",
        mode: "steer",
        clientRequestId: "send-loop-2",
      }).mode,
    ).toBe("steer");
    expect(
      decodeThreadWaitInput({
        threadId: "thread-loop-1",
        runId: "run-loop-2",
        timeoutMs: 5_000,
      }).runId,
    ).toBe("run-loop-2");
    expect(
      decodeThreadInterruptInput({
        threadId: "thread-loop-1",
        reason: "Loop converged.",
      }).reason,
    ).toBe("Loop converged.");
  });
});

// @vitest-environment jsdom
import { act } from "react";
import { createRoot } from "react-dom/client";
import { expect, it, vi } from "vite-plus/test";
import {
  CommandId,
  MessageId,
  NodeId,
  ProviderInstanceId,
  RunAttemptId,
  RunId,
  ThreadId,
  type OrchestrationV2Run,
} from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import { acceptedDelegation, MultiAgentDelegation } from "./MultiAgentDelegation";

it("shows accepted delegation, updates from live state, opens workers, and retains technical details", async () => {
  const selection = {
    instanceId: ProviderInstanceId.make("codex"),
    model: "catalog-model",
    options: [{ id: "effort", value: "supported" }],
  };
  const id = RunId.make("coordinator");
  const threadId = ThreadId.make("coordinator-thread");
  const raw = '{"tasks":[{"key":"engine","role":"worker","prompt":"Internal task instructions"}]}';
  const run: OrchestrationV2Run = {
    id,
    threadId,
    ordinal: 1,
    providerInstanceId: selection.instanceId,
    modelSelection: selection,
    providerThreadId: null,
    userMessageId: MessageId.make("goal"),
    rootNodeId: null,
    activeAttemptId: null,
    status: "completed",
    requestedAt: DateTime.makeUnsafe("2026-10-15T00:00:00Z"),
    startedAt: null,
    completedAt: null,
    checkpointId: null,
    contextHandoffId: null,
    multiAgentController: {
      coordinatorThreadId: threadId,
      coordinatorRunId: id,
      status: "open",
      tasks: [
        {
          key: "engine",
          taskId: NodeId.make("task"),
          launchCommandId: CommandId.make("launch"),
          launchMessageId: MessageId.make("launch-message"),
          workerThreadId: ThreadId.make("worker"),
          proposalId: MessageId.make("proposal"),
          proposalAttemptId: RunAttemptId.make("attempt"),
          role: "worker",
          prompt: "Build engine",
          modelSelection: selection,
          status: "queued",
        },
      ],
    },
  };
  expect(acceptedDelegation([run], MessageId.make("ordinary-json"))).toBeUndefined();
  expect(acceptedDelegation([run], MessageId.make("rejected-proposal"))).toBeUndefined();
  expect(acceptedDelegation([run], MessageId.make("proposal"))).toBe(run);
  const node = document.createElement("div");
  document.body.append(node);
  const root = createRoot(node);
  const open = vi.fn();
  try {
    await act(async () =>
      root.render(<MultiAgentDelegation run={run} runs={[run]} raw={raw} onOpenThread={open} />),
    );
    expect(node.textContent).toContain("0/1 settled");
    expect(node.textContent).toContain("Waiting for workers");
    expect(node.querySelector("details")?.open).toBe(false);
    expect(node.querySelector("pre")?.textContent).toBe(raw);
    await act(async () => node.querySelector("button")!.click());
    expect(open).toHaveBeenCalledWith(ThreadId.make("worker"));
    const reviewId = RunId.make("review");
    const reviewing: OrchestrationV2Run = {
      ...run,
      multiAgentController: {
        ...run.multiAgentController!,
        tasks: run.multiAgentController!.tasks.map((task) => ({
          ...task,
          status: "completed",
          workerRunId: RunId.make("worker-run"),
          result: { workerThreadId: task.workerThreadId, workerRunId: RunId.make("worker-run") },
        })),
        reviews: [
          {
            runId: reviewId,
            attemptId: RunAttemptId.make("review-attempt"),
            taskIds: [NodeId.make("task")],
          },
        ],
      },
    };
    const review: OrchestrationV2Run = {
      ...run,
      id: reviewId,
      status: "running",
      multiAgentController: undefined,
      multiAgentReview: { coordinatorRunId: run.id },
    };
    await act(async () =>
      root.render(
        <MultiAgentDelegation
          run={reviewing}
          runs={[reviewing, review]}
          raw={raw}
          onOpenThread={open}
        />,
      ),
    );
    expect(node.textContent).toContain("1/1 settled");
    expect(node.textContent).toContain("Coordinator: Reviewing results");
    const completed: OrchestrationV2Run = {
      ...run,
      multiAgentController: {
        ...run.multiAgentController!,
        status: "settled",
        tasks: run.multiAgentController!.tasks.map((task) => ({ ...task, status: "failed" })),
      },
    };
    await act(async () =>
      root.render(
        <MultiAgentDelegation run={completed} runs={[completed]} raw={raw} onOpenThread={open} />,
      ),
    );
    expect(node.textContent).toContain("1/1 settled");
    expect(node.textContent).toContain("failed");
    expect(node.textContent).toContain("Coordinator: Finished");
    expect(node.textContent).toContain("effort: supported");
    expect(node.textContent).toContain("does not confirm integration");
  } finally {
    await act(async () => root.unmount());
    node.remove();
  }
});

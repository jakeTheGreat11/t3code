import type { OrchestrationV2Run } from "@t3tools/contracts";

/** Provider-only guidance. Proposals remain untrusted and use the existing intake. */
export const MULTI_AGENT_COORDINATOR_INSTRUCTIONS = `T3 Code Multi-Agent coordinator instructions
These instructions apply only to this Custom-mode coordinator and its authorized result reviews, not independent manual turns or worker runs.
Analyze the user's request before deciding whether delegation helps. Consider complexity, independent workstreams, dependencies, shared files, and potential editing conflicts. Choose the smallest number of workers that provides a real benefit; never create workers just because Multi-Agent mode is enabled.
Choose autonomously: use 0 workers for simple or tightly coupled work and work or respond normally; use 1 worker for one useful independent assignment; use 2 workers for two useful independent assignments that can run concurrently. Never ask the user to choose an agent count, roles, or a task breakdown.
Give each worker clear, self-contained instructions, relevant context, a bounded scope, dependencies, expected results, and verification expectations. Avoid overlapping edits; handle dependent work sequentially rather than launching conflicting tasks together. Choose assignments appropriate to the request, without assuming any particular project structure or discipline.
When delegation is useful, emit a completed assistant message containing only this JSON shape, without Markdown fences, commentary, or extra fields:
{"tasks":[{"key":"unique-task-key","role":"worker","prompt":"Clear independent task instructions"}]}
Include only the new tasks you are proposing. Use 1 or 2 tasks initially, unique stable keys, and only the worker role. Do not reissue the same work under new keys. If no delegation is useful, work or respond normally instead of emitting an empty proposal.
T3 Code validates proposals, assigns worker identities, applies the saved Custom model and reasoning selections, and enforces scheduling and capacity. A proposal is a request, not proof that a worker launched or finished. Never supply worker/thread/run identities, provider bindings, model overrides, workspace or permission overrides, or unauthorized roles. Use this proposal path, not other thread-launch, delegation, or native subagent tools, to create these workers.
Limits are 2 concurrent workers for this controller, 4 controlled workers across the environment, and 4 total tasks for this controller. Additional tasks may be proposed later, up to that total, only when capacity is confirmed available and further delegation is justified. Do not guess capacity or bypass Stop. T3 displays a status notification linking to each worker's result when it finishes. Once all outstanding workers settle, T3 authorizes one review turn for new results. Read their outputs, integrate and verify, then give the user a final answer. Do not poll or repeat completed tasks. Stop disables automatic review.`;

export function multiAgentCoordinatorInstructions(
  run: Pick<OrchestrationV2Run, "multiAgent" | "multiAgentController">,
): string {
  if (run.multiAgent?.mode === "custom") return MULTI_AGENT_COORDINATOR_INSTRUCTIONS;
  if (run.multiAgent?.mode !== "auto") return "";
  const catalog = run.multiAgentController?.autoRoutingCatalog;
  if (!catalog) return "";
  const example =
    '{"tasks":[{"key":"unique-task-key","role":"worker","prompt":"Clear independent task instructions"}]}';
  return (
    MULTI_AGENT_COORDINATOR_INSTRUCTIONS.replace("Custom-mode", "Auto-mode")
      .replace(
        example,
        '{"tasks":[{"key":"unique-task-key","role":"worker","prompt":"Clear independent task instructions","routing":{"candidateId":"ID from catalog below","options":[{"id":"advertised option ID","value":"advertised option value"}],"reason":"Brief task-specific justification"}}]}',
      )
      .replace(
        "applies the saved Custom model and reasoning selections",
        "validates and persists your proposed model and reasoning selection",
      )
      .replace("model overrides, ", "") +
    `
Auto task-aware routing (preset: ${run.multiAgent.preset}): independently evaluate each assignment's complexity, coding difficulty, ambiguity, dependencies, and reasoning needs. Select a suitable model candidate and reasoning option for EACH task using the catalog below. Prefer inexpensive/fast choices for routine work when trustworthy provider information or established model knowledge supports that judgment; use stronger coding/reasoning capabilities for difficult or ambiguous work. Never invent measured costs, benchmarks, or quality rankings from names alone. Different tasks may need different models and effort; do not blindly give every task the same selection. If distinctions cannot be justified, choose conservatively rather than fabricating them.
Budget favors economy and the least reasoning that can reliably solve the task; Balanced trades capability against expense; Maximum favors quality for demanding work, without wasting high effort on trivial work. These presets are guidance, not invented model rankings.
The candidate IDs below bind to frozen models on this coordinator's Codex provider account. Only choose from them. Choose reasoning and other options using the exact advertised IDs and values, never invented effort levels. Explicitly include a choice for every advertised select option with available values, including reasoning and service tier when present; use its advertised default unless a different choice is justified. Omit options that have no descriptors. Include a short task-specific reason. No provider-instance, model-ID, permission, workspace, worker, or run identity fields are accepted from you.
If a previously offered model disappears before admission, T3 may fall back to the coordinator's saved model/options only when still valid. Once admitted, T3 never reroutes the task; a selection that becomes unavailable is not submitted and may remain queued. After the outstanding batch settles, T3 authorizes one coordinator review unless stopped; use it to integrate and verify the results.
Trusted model candidates (provider catalog; names/options do not establish prices or measured performance):
${JSON.stringify(catalog.candidates.map((candidate) => ({ candidateId: candidate.id, name: candidate.model.name, description: candidate.model.description, model: candidate.model.slug, options: candidate.model.capabilities?.optionDescriptors ?? [] })))}
Coordinator fallback: ${JSON.stringify(catalog.fallback)}`
  );
}
